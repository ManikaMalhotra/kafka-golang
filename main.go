package main

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// KafkaConfig holds the configuration details
type KafkaConfig struct {
	Brokers      string
	ConsumerGrp  string
	ConsumeTopic string
	ProduceTopic string
}

var totalMessages int64
var startTime time.Time

func main() {
	// Logger setup
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Starting Kafka Connector...")

	config := KafkaConfig{
		Brokers:      "localhost:9092",
		ConsumerGrp:  "consumer-group-1",
		ConsumeTopic: "input-topic",
		ProduceTopic: "output-topic",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		log.Info().Msg("Shutting down Kafka Connector...")
		cancel()
	}()

	// Start benchmarking
	startTime = time.Now()

	// Start consumer & producer
	go consumeAndProduce(ctx, config)

	// Keep the main function alive
	<-ctx.Done()
}

func consumeAndProduce(ctx context.Context, cfg KafkaConfig) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
		"group.id":          cfg.ConsumerGrp,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create consumer")
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Brokers})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create producer")
	}
	defer producer.Close()

	err = consumer.Subscribe(cfg.ConsumeTopic, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("Subscription failed")
	}

	log.Info().Msgf("Consuming from topic: %s and producing to topic: %s", cfg.ConsumeTopic, cfg.ProduceTopic)

	batchSize := 100
	messageCount := 0
	batchStart := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Consumer stopped")
			return
		default:
			msg, err := consumer.ReadMessage(time.Second)
			if err == nil {
				start := time.Now()

				// Produce message to output topic
				producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &cfg.ProduceTopic, Partition: kafka.PartitionAny},
					Value:          msg.Value,
				}, nil)

				// Flush producer
				producer.Flush(100)

				// Increment counters
				atomic.AddInt64(&totalMessages, 1)
				messageCount++

				// Measure processing time
				messageLatency := time.Since(start)
				log.Debug().Msgf("Message processed in %v", messageLatency)

				// Log benchmark stats every 100 messages
				if messageCount >= batchSize {
					duration := time.Since(batchStart)
					totalProcessed := atomic.LoadInt64(&totalMessages)
					throughput := float64(totalProcessed) / time.Since(startTime).Seconds()

					log.Info().
						Int64("Total Messages", totalProcessed).
						Float64("Throughput (msg/sec)", throughput).
						Dur("Batch Processing Time", duration).
						Msg("Benchmark Stats")

					messageCount = 0
					batchStart = time.Now()
				}
			}
		}
	}
}
