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
var goroutineCounter atomic.Int64

func getNextGoroutineID() int64 {
	return goroutineCounter.Add(1)
}

func main() {
	// Logger setup
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("Starting Kafka Connector...")

	config := KafkaConfig{
		Brokers:      "localhost:9092",
		ConsumerGrp:  "consumer-group-3",
		ConsumeTopic: "input-topic",
		ProduceTopic: "output-topic",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	go func() {
		goroutineID := getNextGoroutineID()
		log.Info().
			Int64("goroutine_id", goroutineID).
			Str("goroutine_type", "signal-handler").
			Msg("Starting signal handler goroutine")

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		log.Info().
			Int64("goroutine_id", goroutineID).
			Str("goroutine_type", "signal-handler").
			Msg("Signal received, initiating shutdown...")
		cancel()

		log.Info().
			Int64("goroutine_id", goroutineID).
			Str("goroutine_type", "signal-handler").
			Msg("Signal handler goroutine shutting down")
	}()

	// Start benchmarking
	startTime = time.Now()

	// Start consumer & producer
	go consumeAndProduce(ctx, config)

	mainGoroutineID := getNextGoroutineID()
	log.Info().
		Int64("goroutine_id", mainGoroutineID).
		Str("goroutine_type", "main").
		Msg("Main goroutine waiting for shutdown signal")

	<-ctx.Done()
	log.Info().
		Int64("goroutine_id", mainGoroutineID).
		Str("goroutine_type", "main").
		Int64("total_messages_processed", atomic.LoadInt64(&totalMessages)).
		Msg("Main goroutine shutting down")
}

func consumeAndProduce(ctx context.Context, cfg KafkaConfig) {
	goroutineID := getNextGoroutineID()
	log.Info().
		Int64("goroutine_id", goroutineID).
		Str("goroutine_type", "consumer-producer").
		Str("consumer_topic", cfg.ConsumeTopic).
		Str("producer_topic", cfg.ProduceTopic).
		Msg("Starting consumer-producer goroutine")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
		"group.id":          cfg.ConsumerGrp,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal().
			Int64("goroutine_id", goroutineID).
			Str("goroutine_type", "consumer-producer").
			Err(err).
			Msg("Failed to create consumer")
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Brokers})
	if err != nil {
		log.Fatal().
			Int64("goroutine_id", goroutineID).
			Str("goroutine_type", "consumer-producer").
			Err(err).
			Msg("Failed to create producer")
	}
	defer producer.Close()

	err = consumer.Subscribe(cfg.ConsumeTopic, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("Subscription failed")
	}

	log.Info().Msgf("Consuming from topic: %s and producing to topic: %s", cfg.ConsumeTopic, cfg.ProduceTopic)

	localMessageCount := int64(0)
	batchSize := 100
	messageCount := 0
	batchStart := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Info().
				Int64("goroutine_id", goroutineID).
				Str("goroutine_type", "consumer-producer").
				Int64("messages_processed", localMessageCount).
				Msg("Consumer-producer goroutine shutting down")
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
				localMessageCount++
				messageCount++

				// Measure processing time
				messageLatency := time.Since(start)
				log.Debug().
					Int64("goroutine_id", goroutineID).
					Str("goroutine_type", "consumer-producer").
					Dur("processing_time", messageLatency).
					Int64("local_messages", localMessageCount).
					Msg("Message processed")

				// Log benchmark stats every 100 messages
				if messageCount >= batchSize {
					duration := time.Since(batchStart)
					totalProcessed := atomic.LoadInt64(&totalMessages)
					throughput := float64(totalProcessed) / time.Since(startTime).Seconds()

					log.Info().
						Int64("goroutine_id", goroutineID).
						Str("goroutine_type", "consumer-producer").
						Int64("total_messages", totalProcessed).
						Int64("local_messages", localMessageCount).
						Float64("throughput_msg_per_sec", throughput).
						Dur("batch_processing_time", duration).
						Msg("Batch processing complete")

					messageCount = 0
					batchStart = time.Now()
				}
			}
		}
	}
}
