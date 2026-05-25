package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	kafkacommon "github.com/Humphrey-He/kafka-practise/internal/common/kafka"
	"github.com/Humphrey-He/kafka-practise/internal/common/logging"
	"github.com/IBM/sarama"
)

func main() {
	var (
		configPath   = flag.String("config", "configs/local.yaml", "config file path")
		topic        = flag.String("topic", "kp_source_demo", "source topic")
		dlqTopic     = flag.String("dlq-topic", "kp_source_demo_dlq", "dlq topic")
		groupID      = flag.String("group", "kp_dlq_retry_group", "consumer group id")
		failContains = flag.String("fail-contains", "fail", "value substring that simulates processing failure")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *topic, *dlqTopic, *groupID, *failContains, logger); err != nil {
		logger.Error("dlq retry failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, topic, dlqTopic, groupID, failContains string, logger *slog.Logger) error {
	if strings.TrimSpace(topic) == "" || strings.TrimSpace(dlqTopic) == "" {
		return errors.New("topic and dlq-topic are required")
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

	producer, err := sarama.NewSyncProducer(appCfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	group, err := sarama.NewConsumerGroup(appCfg.Kafka.Brokers, groupID, saramaCfg)
	if err != nil {
		return err
	}
	defer group.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	handler := &dlqHandler{
		logger:       logger,
		producer:     producer,
		dlqTopic:     dlqTopic,
		failContains: failContains,
	}
	for ctx.Err() == nil {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
		}
	}
	return nil
}

type dlqHandler struct {
	logger       *slog.Logger
	producer     sarama.SyncProducer
	dlqTopic     string
	failContains string
}

func (h *dlqHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *dlqHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *dlqHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		value := string(message.Value)
		if h.failContains != "" && strings.Contains(value, h.failContains) {
			if err := h.sendDLQ(message); err != nil {
				h.logger.Error("send dlq failed",
					slog.String("error", err.Error()),
					slog.String("topic", message.Topic),
					slog.Int64("offset", message.Offset),
				)
				continue
			}
			h.logger.Warn("message sent to dlq",
				slog.String("dlq_topic", h.dlqTopic),
				slog.String("source_topic", message.Topic),
				slog.Int64("source_offset", message.Offset),
			)
		} else {
			h.logger.Info("message processed",
				slog.String("topic", message.Topic),
				slog.Int64("offset", message.Offset),
				slog.String("value", value),
			)
		}
		session.MarkMessage(message, "")
		session.Commit()
	}
	return nil
}

func (h *dlqHandler) sendDLQ(message *sarama.ConsumerMessage) error {
	headers := make([]sarama.RecordHeader, 0, len(message.Headers)+3)
	for _, header := range message.Headers {
		headers = append(headers, sarama.RecordHeader{Key: header.Key, Value: header.Value})
	}
	headers = append(headers,
		sarama.RecordHeader{Key: []byte("x-original-topic"), Value: []byte(message.Topic)},
		sarama.RecordHeader{Key: []byte("x-original-partition"), Value: []byte(strconv.FormatInt(int64(message.Partition), 10))},
		sarama.RecordHeader{Key: []byte("x-original-offset"), Value: []byte(strconv.FormatInt(message.Offset, 10))},
	)

	_, _, err := h.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   h.dlqTopic,
		Key:     sarama.ByteEncoder(message.Key),
		Value:   sarama.ByteEncoder(message.Value),
		Headers: headers,
	})
	return err
}
