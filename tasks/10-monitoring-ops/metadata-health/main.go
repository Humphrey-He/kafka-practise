package main

import (
	"flag"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	kafkacommon "github.com/Humphrey-He/kafka-practise/internal/common/kafka"
	"github.com/Humphrey-He/kafka-practise/internal/common/logging"
	"github.com/IBM/sarama"
)

func main() {
	var (
		configPath = flag.String("config", "configs/local.yaml", "config file path")
		topic      = flag.String("topic", "kp_topic_demo", "topic name")
		allTopics  = flag.Bool("all", false, "inspect all topics")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *topic, *allTopics, logger); err != nil {
		logger.Error("metadata health check failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, topic string, allTopics bool, logger *slog.Logger) error {
	appCfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	saramaCfg, err := kafkacommon.NewSaramaConfig(appCfg.Kafka)
	if err != nil {
		return err
	}

	admin, err := sarama.NewClusterAdmin(appCfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return err
	}
	defer admin.Close()

	topics := []string{strings.TrimSpace(topic)}
	if allTopics {
		listed, err := admin.ListTopics()
		if err != nil {
			return err
		}
		topics = make([]string, 0, len(listed))
		for name := range listed {
			topics = append(topics, name)
		}
		sort.Strings(topics)
	}

	metadata, err := admin.DescribeTopics(topics)
	if err != nil {
		return err
	}

	var unhealthy int
	for _, topicMeta := range metadata {
		for _, partition := range topicMeta.Partitions {
			isHealthy := topicMeta.Err == sarama.ErrNoError &&
				partition.Err == sarama.ErrNoError &&
				partition.Leader >= 0 &&
				len(partition.Isr) == len(partition.Replicas)
			if !isHealthy {
				unhealthy++
			}
			logger.Info("partition health",
				slog.String("topic", topicMeta.Name),
				slog.Int("partition", int(partition.ID)),
				slog.Int("leader", int(partition.Leader)),
				slog.Any("replicas", partition.Replicas),
				slog.Any("isr", partition.Isr),
				slog.Bool("healthy", isHealthy),
				slog.String("topic_error", topicMeta.Err.Error()),
				slog.String("partition_error", partition.Err.Error()),
			)
		}
	}
	logger.Info("metadata health summary", slog.Int("topics", len(metadata)), slog.Int("unhealthy_partitions", unhealthy))
	return nil
}
