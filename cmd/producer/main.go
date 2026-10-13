package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Transaction struct {
	Seq        int64   `json:"seq"`
	TxnID      string  `json:"txn_id"`
	AccountID  string  `json:"account_id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	Merchant   string  `json:"merchant"`
	Country    string  `json:"country"`
	TS         string  `json:"ts"`
	RiskScore  float64 `json:"risk_score"`
	ProduceSeq int64   `json:"produce_seq"`
	Topic      string  `json:"topic,omitempty"`
}

func main() {
	brokers := flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers")
	topicsFlag := flag.String("topics", envOr("KAFKA_TOPICS", "payments,transfers,cards"), "comma-separated topics")
	rate := flag.Int("rate", envInt("KAFKA_RATE", 1000), "target produce rate in messages/sec")
	dataPath := flag.String("data", envOr("DATA_PATH", "data/transactions.json"), "path to synthetic transactions JSON")
	flag.Parse()

	topics := splitCSV(*topicsFlag)
	if len(topics) == 0 {
		log.Fatal("at least one topic is required")
	}
	if *rate <= 0 {
		log.Fatal("-rate must be > 0")
	}

	txs, err := loadTransactions(*dataPath)
	if err != nil {
		log.Fatalf("load data: %v", err)
	}
	if len(txs) == 0 {
		log.Fatal("no transactions in data file")
	}
	// Emit records in random order (not file seq order); reshuffle each full pass.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	shuffleTransactions(rng, txs)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(splitCSV(*brokers)...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  true,
		BatchTimeout:           5 * time.Millisecond,
		BatchSize:              1000,
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: true,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("writer close: %v", err)
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var (
		produced    atomic.Int64
		lastSeq     atomic.Int64
		topicCounts = make([]atomic.Int64, len(topics))
	)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var prev int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				total := produced.Load()
				delta := total - prev
				prev = total
				parts := make([]string, len(topics))
				for i, t := range topics {
					parts[i] = fmt.Sprintf("%s:%d", t, topicCounts[i].Load())
				}
				fmt.Printf("\r[producer] rate=%d msg/s  seq=%d  topics=%s          ",
					delta, lastSeq.Load(), strings.Join(parts, ","))
			}
		}
	}()

	fmt.Printf("producing from %s to %v at ~%d msg/s (brokers=%s)\n",
		*dataPath, topics, *rate, *brokers)

	// Pace with fixed ticks per second so high rates stay accurate.
	const ticksPerSec = 100
	perTick := *rate / ticksPerSec
	remainder := *rate % ticksPerSec
	tick := time.NewTicker(time.Second / ticksPerSec)
	defer tick.Stop()

	idx := 0
	var seq int64
	tickN := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Println()
			log.Printf("shutdown after %d messages", produced.Load())
			return
		case <-tick.C:
			n := perTick
			if tickN < remainder {
				n++
			}
			tickN = (tickN + 1) % ticksPerSec
			if n == 0 {
				continue
			}

			msgs := make([]kafka.Message, 0, n)
			for i := 0; i < n; i++ {
				if idx == len(txs) {
					shuffleTransactions(rng, txs)
					idx = 0
				}
				tx := txs[idx]
				idx++
				seq++
				ti := topicIndex(tx.AccountID, topics)
				topic := topics[ti]
				tx.ProduceSeq = seq
				tx.Topic = topic
				payload, err := json.Marshal(tx)
				if err != nil {
					log.Printf("marshal: %v", err)
					continue
				}
				msgs = append(msgs, kafka.Message{
					Topic: topic,
					Key:   []byte(tx.AccountID),
					Value: payload,
					Time:  time.Now(),
				})
				produced.Add(1)
				lastSeq.Store(seq)
				topicCounts[ti].Add(1)
			}
			if err := writer.WriteMessages(ctx, msgs...); err != nil {
				if ctx.Err() != nil {
					fmt.Println()
					log.Printf("shutdown after %d messages", produced.Load())
					return
				}
				log.Printf("write: %v", err)
			}
		}
	}
}

func loadTransactions(path string) ([]Transaction, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var txs []Transaction
	if err := json.Unmarshal(raw, &txs); err != nil {
		return nil, err
	}
	return txs, nil
}

func shuffleTransactions(rng *rand.Rand, txs []Transaction) {
	rng.Shuffle(len(txs), func(i, j int) {
		txs[i], txs[j] = txs[j], txs[i]
	})
}

func topicIndex(accountID string, topics []string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(accountID))
	return int(h.Sum32() % uint32(len(topics)))
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return fallback
	}
	return n
}
