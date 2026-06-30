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
		groupID    = flag.String("group", "kp_token_bucket_group", "consumer group id")
		rate       = flag.Int("rate", 5, "messages per second")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *topic, *groupID, *rate, logger); err != nil {
		logger.Error("token bucket consumer failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, topic, groupID string, rate int, logger *slog.Logger) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("topic is required")
	}
	if rate <= 0 {
		return errors.New("rate must be greater than 0")
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

	interval := time.Second / time.Duration(rate)
	handler := &tokenBucketHandler{
		logger: logger,
		ticker: time.NewTicker(interval),
	}
	defer handler.ticker.Stop()

	for ctx.Err() == nil {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
		}
	}
	return nil
}

type tokenBucketHandler struct {
	logger *slog.Logger
	ticker *time.Ticker
}

func (h *tokenBucketHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *tokenBucketHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *tokenBucketHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		<-h.ticker.C
		h.logger.Info("rate limited message processed",
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
