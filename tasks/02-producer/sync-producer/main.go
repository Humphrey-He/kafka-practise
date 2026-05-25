package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	kafkacommon "github.com/Humphrey-He/kafka-practise/internal/common/kafka"
	"github.com/Humphrey-He/kafka-practise/internal/common/logging"
	"github.com/IBM/sarama"
)

func main() {
	var (
		configPath = flag.String("config", "configs/local.yaml", "config file path")
		topic      = flag.String("topic", "kp_topic_demo", "topic name")
		key        = flag.String("key", "demo-key", "message key")
		value      = flag.String("value", "hello kafka", "message value")
		count      = flag.Int("count", 1, "message count")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *topic, *key, *value, *count, logger); err != nil {
		logger.Error("produce failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, topic, key, value string, count int, logger *slog.Logger) error {
	appCfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	saramaCfg, err := kafkacommon.NewSaramaConfig(appCfg.Kafka)
	if err != nil {
		return err
	}

	producer, err := sarama.NewSyncProducer(appCfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 0; i < count; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(fmt.Sprintf("%s #%d", value, i+1)),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			return err
		}
		logger.Info("message sent",
			slog.String("topic", topic),
			slog.String("key", key),
			slog.Int("partition", int(partition)),
			slog.Int64("offset", offset),
		)
	}
	return nil
}
