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
		topic      = flag.String("topic", "kp_topic_demo", "topic name")
		groupID    = flag.String("group", "kp_at_least_once_group", "consumer group id")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, []string{*topic}, *groupID, logger); err != nil {
		logger.Error("at-least-once consumer failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath string, topics []string, groupID string, logger *slog.Logger) error {
	if len(topics) == 0 || strings.TrimSpace(topics[0]) == "" {
		return errors.New("topic is required")
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

	handler := &atLeastOnceHandler{logger: logger}
	for ctx.Err() == nil {
		if err := group.Consume(ctx, topics, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
		}
	}
	return nil
}

type atLeastOnceHandler struct {
	logger *slog.Logger
}

func (h *atLeastOnceHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *atLeastOnceHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *atLeastOnceHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.logger.Info("business processed",
			slog.String("topic", message.Topic),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
			slog.String("value", string(message.Value)),
		)
		session.MarkMessage(message, "")
		session.Commit()
	}
	return nil
}
