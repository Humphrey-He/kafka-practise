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
		configPath  = flag.String("config", "configs/local.yaml", "config file path")
		topicsCSV   = flag.String("topic", "kp_topic_demo", "comma-separated topic names")
		groupID     = flag.String("group", "kp_manual_commit_group", "consumer group id")
		commitEvery = flag.Int("commit-every", 10, "commit after this many successfully processed messages")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, splitTopics(*topicsCSV), *groupID, *commitEvery, logger); err != nil {
		logger.Error("manual commit consumer failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath string, topics []string, groupID string, commitEvery int, logger *slog.Logger) error {
	if len(topics) == 0 {
		return errors.New("at least one topic is required")
	}
	if commitEvery <= 0 {
		return errors.New("commit-every must be greater than 0")
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

	handler := &manualCommitHandler{
		logger:      logger,
		commitEvery: commitEvery,
	}
	logger.Info("manual commit consumer started", slog.String("group", groupID), slog.Any("topics", topics))
	for ctx.Err() == nil {
		if err := group.Consume(ctx, topics, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
			time.Sleep(time.Second)
		}
	}
	return nil
}

type manualCommitHandler struct {
	logger      *slog.Logger
	commitEvery int
	processed   int
}

func (h *manualCommitHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.processed = 0
	h.logger.Info("consumer group setup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *manualCommitHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	h.logger.Info("consumer group cleanup committed", slog.Any("claims", session.Claims()))
	return nil
}

func (h *manualCommitHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.logger.Info("message processed",
			slog.String("topic", message.Topic),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
		)
		session.MarkMessage(message, "")
		h.processed++
		if h.processed%h.commitEvery == 0 {
			session.Commit()
			h.logger.Info("offset committed", slog.Int("processed", h.processed))
		}
	}
	return nil
}

func splitTopics(value string) []string {
	parts := strings.Split(value, ",")
	topics := make([]string, 0, len(parts))
	for _, part := range parts {
		if topic := strings.TrimSpace(part); topic != "" {
			topics = append(topics, topic)
		}
	}
	return topics
}
