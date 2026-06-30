package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	kafkacommon "github.com/Humphrey-He/kafka-practise/internal/common/kafka"
	"github.com/Humphrey-He/kafka-practise/internal/common/logging"
	"github.com/IBM/sarama"
)

func main() {
	var (
		configPath = flag.String("config", "configs/local.yaml", "config file path")
		topic      = flag.String("topic", "kp_topic_demo", "topic name")
		groupID    = flag.String("group", "kp_worker_pool_group", "consumer group id")
		workers    = flag.Int("workers", 4, "worker count")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *topic, *groupID, *workers, logger); err != nil {
		logger.Error("worker pool consumer failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, topic, groupID string, workers int, logger *slog.Logger) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("topic is required")
	}
	if workers <= 0 {
		return errors.New("workers must be greater than 0")
	}

	appCfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	appCfg.Kafka.Consumer.AutoCommit = false

	saramaCfg, err := kafkacommon.NewSaramaConfig(appCfg.Kafka)
	if err != nil {
		return err
	}
	group, err := sarama.NewConsumerGroup(appCfg.Kafka.Brokers, groupID, saramaCfg)
	if err != nil {
		return err
	}
	defer group.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handler := &workerPoolHandler{logger: logger, workers: workers}
	for ctx.Err() == nil {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
		}
	}
	return nil
}

type workerPoolHandler struct {
	logger  *slog.Logger
	workers int
}

func (h *workerPoolHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *workerPoolHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *workerPoolHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	jobs := make(chan *sarama.ConsumerMessage)
	results := make(chan *sarama.ConsumerMessage)

	for i := 0; i < h.workers; i++ {
		go h.worker(i+1, jobs, results)
	}

	for message := range claim.Messages() {
		jobs <- message
		processed := <-results
		h.logger.Info("worker processed message",
			slog.String("topic", processed.Topic),
			slog.Int("partition", int(processed.Partition)),
			slog.Int64("offset", processed.Offset),
		)
		session.MarkMessage(processed, "")
		session.Commit()
	}
	close(jobs)
	return nil
}

func (h *workerPoolHandler) worker(id int, jobs <-chan *sarama.ConsumerMessage, results chan<- *sarama.ConsumerMessage) {
	for message := range jobs {
		time.Sleep(100 * time.Millisecond)
		h.logger.Debug("worker done", slog.Int("worker", id), slog.Int64("offset", message.Offset))
		results <- message
	}
}
