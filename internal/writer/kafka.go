package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/supratick/message_producer/internal/models"
)

// KafkaWriter writes transactions to Kafka
type KafkaWriter struct {
	producer  sarama.AsyncProducer
	topic     string
	count     atomic.Int64
	errors    atomic.Int64
	isAsync   bool
	logger    *slog.Logger
}

// NewKafkaWriter creates a new Kafka writer
func NewKafkaWriter(brokers []string, topic string, compression string, batchSize, flushFreq int, async bool, logger *slog.Logger) (*KafkaWriter, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 3
	
	// Set compression
	switch compression {
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		config.Producer.Compression = sarama.CompressionNone
	}
	
	// Batch settings for higher throughput
	config.Producer.Flush.Messages = batchSize
	config.Producer.Flush.Frequency = time.Duration(flushFreq) * time.Millisecond
	config.Producer.Flush.MaxMessages = batchSize * 2
	
	// Channel buffer sizes
	config.ChannelBufferSize = 10000
	
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	kw := &KafkaWriter{
		producer: producer,
		topic:    topic,
		isAsync:  async,
		logger:   logger,
	}

	// Handle successes and errors in background
	go kw.handleResponses()

	return kw, nil
}

func (w *KafkaWriter) handleResponses() {
	for {
		select {
		case success, ok := <-w.producer.Successes():
			if !ok {
				return
			}
			if success != nil {
				w.count.Add(1)
			}
		case err, ok := <-w.producer.Errors():
			if !ok {
				return
			}
			if err != nil {
				w.errors.Add(1)
				// Log error but don't stop production
				w.logger.Error("Kafka producer error", "error", err.Err, "msg_key", err.Msg.Key)
			}
		}
	}
}

// Write writes transactions from the channel to Kafka
func (w *KafkaWriter) Write(ctx context.Context, input <-chan *models.Transaction) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case txn, ok := <-input:
			if !ok {
				// Channel closed, return
				return nil
			}
			
			// Serialize transaction to JSON
			data, err := json.Marshal(txn)
			if err != nil {
				w.errors.Add(1)
				continue
			}
			
			// Create Kafka message
			msg := &sarama.ProducerMessage{
				Topic: w.topic,
				Key:   sarama.StringEncoder(txn.ID),
				Value: sarama.ByteEncoder(data),
			}
			
			// Send to Kafka
			select {
			case w.producer.Input() <- msg:
				// Message queued successfully
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// Close closes the Kafka writer
func (w *KafkaWriter) Close() error {
	// Close producer (this will flush pending messages)
	return w.producer.Close()
}

// Count returns the number of transactions successfully written
func (w *KafkaWriter) Count() int64 {
	return w.count.Load()
}

// Errors returns the number of errors encountered
func (w *KafkaWriter) Errors() int64 {
	return w.errors.Load()
}
