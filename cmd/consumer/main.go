package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const riskThreshold = 0.8

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

type partitionPos struct {
	topic     string
	partition int
	offset    int64 // next offset to read (last consumed + 1)
}

func main() {
	brokers := flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka brokers")
	topicsFlag := flag.String("topics", envOr("KAFKA_TOPICS", "payments,transfers,cards"), "comma-separated topics")
	group := flag.String("group", envOr("KAFKA_GROUP", "risk-alerts"), "consumer group id")
	flag.Parse()

	topics := splitCSV(*topicsFlag)
	if len(topics) == 0 {
		log.Fatal("at least one topic is required")
	}
	brokerList := splitCSV(*brokers)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var (
		consumed atomic.Int64
		alerts   atomic.Int64
		lastSeq  atomic.Int64
		lagTotal atomic.Int64
	)

	var (
		mu   sync.Mutex
		pos  = map[string]partitionPos{} // key: topic-partition
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
				total := consumed.Load()
				delta := total - prev
				prev = total
				fmt.Printf("\r[consumer] rate=%d msg/s  lag=%d  seq=%d  alerts=%d          ",
					delta, lagTotal.Load(), lastSeq.Load(), alerts.Load())
			}
		}
	}()

	// Periodic lag calculation against high watermarks.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lag, err := computeLag(ctx, brokerList[0], &mu, pos)
				if err != nil {
					log.Printf("lag: %v", err)
					continue
				}
				lagTotal.Store(lag)
			}
		}
	}()

	fmt.Printf("consuming topics %v group=%s (brokers=%s)\n", topics, *group, *brokers)
	fmt.Printf("risk alerts when risk_score >= %.1f or amount >= 10000\n", riskThreshold)

	var wg sync.WaitGroup
	for _, topic := range topics {
		topic := topic
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        brokerList,
				Topic:          topic,
				GroupID:        *group,
				MinBytes:       1,
				MaxBytes:       10e6,
				CommitInterval: time.Second,
				StartOffset:    kafka.FirstOffset,
			})
			defer r.Close()

			for {
				m, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("fetch %s: %v", topic, err)
					time.Sleep(time.Second)
					continue
				}

				var tx Transaction
				if err := json.Unmarshal(m.Value, &tx); err != nil {
					log.Printf("unmarshal: %v", err)
				} else {
					if tx.ProduceSeq > 0 {
						lastSeq.Store(tx.ProduceSeq)
					} else {
						lastSeq.Store(tx.Seq)
					}
					if isRiskAlert(tx) {
						alerts.Add(1)
						fmt.Printf("\n[ALERT] topic=%s seq=%d txn=%s account=%s amount=%.2f %s risk=%.2f merchant=%s country=%s\n",
							m.Topic, tx.ProduceSeq, tx.TxnID, tx.AccountID, tx.Amount, tx.Currency, tx.RiskScore, tx.Merchant, tx.Country)
					}
				}

				consumed.Add(1)
				key := fmt.Sprintf("%s-%d", m.Topic, m.Partition)
				mu.Lock()
				pos[key] = partitionPos{topic: m.Topic, partition: m.Partition, offset: m.Offset + 1}
				mu.Unlock()

				if err := r.CommitMessages(ctx, m); err != nil && ctx.Err() == nil {
					log.Printf("commit: %v", err)
				}
			}
		}()
	}

	<-ctx.Done()
	fmt.Println()
	wg.Wait()
	log.Printf("shutdown after %d messages (%d alerts)", consumed.Load(), alerts.Load())
}

func isRiskAlert(tx Transaction) bool {
	return tx.RiskScore >= riskThreshold || tx.Amount >= 10000
}

func computeLag(ctx context.Context, broker string, mu *sync.Mutex, pos map[string]partitionPos) (int64, error) {
	mu.Lock()
	snapshot := make([]partitionPos, 0, len(pos))
	for _, p := range pos {
		snapshot = append(snapshot, p)
	}
	mu.Unlock()

	if len(snapshot) == 0 {
		return 0, nil
	}

	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	var total int64
	for _, p := range snapshot {
		partConn, err := kafka.DialLeader(ctx, "tcp", broker, p.topic, p.partition)
		if err != nil {
			continue
		}
		end, err := partConn.ReadLastOffset()
		_ = partConn.Close()
		if err != nil {
			continue
		}
		lag := end - p.offset
		if lag > 0 {
			total += lag
		}
	}
	return total, nil
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
