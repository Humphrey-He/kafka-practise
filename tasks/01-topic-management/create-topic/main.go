package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	kafkacommon "github.com/Humphrey-He/kafka-practise/internal/common/kafka"
	"github.com/Humphrey-He/kafka-practise/internal/common/logging"
	"github.com/IBM/sarama"
)

func main() {
	var (
		configPath        = flag.String("config", "configs/local.yaml", "config file path")
		action            = flag.String("action", "create", "action: create, describe, delete")
		topic             = flag.String("topic", "kp_topic_demo", "topic name")
		partitions        = flag.Int("partitions", 3, "topic partition count")
		replicationFactor = flag.Int("replication-factor", 1, "topic replication factor")
		retentionMs       = flag.String("retention-ms", "86400000", "topic retention.ms")
		cleanupPolicy     = flag.String("cleanup-policy", "delete", "topic cleanup.policy")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *action, *topic, int32(*partitions), int16(*replicationFactor), *retentionMs, *cleanupPolicy, logger); err != nil {
		logger.Error("topic management failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, action, topic string, partitions int32, replicationFactor int16, retentionMs, cleanupPolicy string, logger *slog.Logger) error {
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

	switch action {
	case "create":
		return createTopic(admin, topic, partitions, replicationFactor, retentionMs, cleanupPolicy, logger)
	case "describe":
		return describeTopic(admin, topic, logger)
	case "delete":
		if err := admin.DeleteTopic(topic); err != nil {
			return err
		}
		logger.Info("topic delete requested", slog.String("topic", topic))
		return nil
	default:
		logger.Warn("unknown action, fallback to describe", slog.String("action", action))
		return describeTopic(admin, topic, logger)
	}
}

func createTopic(admin sarama.ClusterAdmin, topic string, partitions int32, replicationFactor int16, retentionMs, cleanupPolicy string, logger *slog.Logger) error {
	detail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries: map[string]*string{
			"retention.ms":   &retentionMs,
			"cleanup.policy": &cleanupPolicy,
		},
	}
	if err := admin.CreateTopic(topic, detail, false); err != nil {
		return err
	}
	logger.Info("topic created",
		slog.String("topic", topic),
		slog.Int("partitions", int(partitions)),
		slog.Int("replication_factor", int(replicationFactor)),
		slog.String("retention_ms", retentionMs),
		slog.String("cleanup_policy", cleanupPolicy),
	)
	return nil
}

func describeTopic(admin sarama.ClusterAdmin, topic string, logger *slog.Logger) error {
	metas, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		return err
	}
	for _, meta := range metas {
		logger.Info("topic metadata",
			slog.String("topic", meta.Name),
			slog.Int("partitions", len(meta.Partitions)),
			slog.String("error", meta.Err.Error()),
		)
		for _, partition := range meta.Partitions {
			logger.Info("partition metadata",
				slog.String("topic", meta.Name),
				slog.Int("partition", int(partition.ID)),
				slog.Int("leader", int(partition.Leader)),
				slog.Any("replicas", partition.Replicas),
				slog.Any("isr", partition.Isr),
			)
		}
	}
	return nil
}
