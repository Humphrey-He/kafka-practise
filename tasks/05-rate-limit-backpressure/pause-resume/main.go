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
	"sync"
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
		groupID    = flag.String("group", "kp_pause_resume_group", "consumer group id")
		pauseAfter = flag.Int("pause-after", 5, "pause a partition after this many messages")
		pauseFor   = flag.Duration("pause-for", 3*time.Second, "pause duration")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *topic, *groupID, *pauseAfter, *pauseFor, logger); err != nil {
		logger.Error("pause resume consumer failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, topic, groupID string, pauseAfter int, pauseFor time.Duration, logger *slog.Logger) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("topic is required")
	}
	if pauseAfter <= 0 {
		return errors.New("pause-after must be greater than 0")
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

	handler := &pauseResumeHandler{
		logger:     logger,
		group:      group,
		pauseAfter: pauseAfter,
		pauseFor:   pauseFor,
		counts:     map[string]int{},
	}
	for ctx.Err() == nil {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
		}
	}
	return nil
}

type pauseResumeHandler struct {
	logger     *slog.Logger
	group      sarama.ConsumerGroup
	pauseAfter int
	pauseFor   time.Duration
	mu         sync.Mutex
	counts     map[string]int
}

func (h *pauseResumeHandler) Setup(sarama.ConsumerGroupSession) error { return nil }

func (h *pauseResumeHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *pauseResumeHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		key := message.Topic + ":" + strconv.FormatInt(int64(message.Partition), 10)
		h.logger.Info("message processed",
			slog.String("topic", message.Topic),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
		)
		session.MarkMessage(message, "")
		session.Commit()

		if h.shouldPause(key) {
			partitions := map[string][]int32{message.Topic: {message.Partition}}
			h.group.Pause(partitions)
			h.logger.Warn("partition paused", slog.String("topic", message.Topic), slog.Int("partition", int(message.Partition)))
			go h.resumeLater(partitions, message.Topic, message.Partition)
		}
	}
	return nil
}

func (h *pauseResumeHandler) shouldPause(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[key]++
	return h.counts[key]%h.pauseAfter == 0
}

func (h *pauseResumeHandler) resumeLater(partitions map[string][]int32, topic string, partition int32) {
	time.Sleep(h.pauseFor)
	h.group.Resume(partitions)
	h.logger.Info("partition resumed", slog.String("topic", topic), slog.Int("partition", int(partition)))
}
