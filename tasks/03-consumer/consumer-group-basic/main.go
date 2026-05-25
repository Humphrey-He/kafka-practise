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

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	kafkacommon "github.com/Humphrey-He/kafka-practise/internal/common/kafka"
	"github.com/Humphrey-He/kafka-practise/internal/common/logging"
	"github.com/IBM/sarama"
)

func main() {
	var (
		configPath = flag.String("config", "configs/local.yaml", "config file path")
		topicsCSV  = flag.String("topic", "kp_topic_demo", "comma-separated topic names")
		groupID    = flag.String("group", "", "consumer group id")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, splitTopics(*topicsCSV), *groupID, logger); err != nil {
		logger.Error("consume failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath string, topics []string, groupID string, logger *slog.Logger) error {
	if len(topics) == 0 {
		return errors.New("at least one topic is required")
	}

	appCfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if groupID == "" {
		groupID = appCfg.Kafka.Consumer.GroupID
	}

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

	handler := &consumerHandler{logger: logger}
	logger.Info("consumer group started", slog.String("group", groupID), slog.Any("topics", topics))
	for ctx.Err() == nil {
		if err := group.Consume(ctx, topics, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
		}
	}
	logger.Info("consumer group stopped", slog.String("group", groupID))
	return nil
}

type consumerHandler struct {
	logger *slog.Logger
}

func (h *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("consumer group setup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *consumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("consumer group cleanup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.logger.Info("message consumed",
			slog.String("topic", message.Topic),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
			slog.String("key", string(message.Key)),
			slog.String("value", string(message.Value)),
		)
		session.MarkMessage(message, "")
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
