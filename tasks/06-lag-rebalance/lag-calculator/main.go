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
		groupID    = flag.String("group", "kp_consumer_basic_group", "consumer group id")
	)
	flag.Parse()

	logger := logging.New()
	if err := run(*configPath, *topic, *groupID, logger); err != nil {
		logger.Error("lag calculation failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(configPath, topic, groupID string, logger *slog.Logger) error {
	topic = strings.TrimSpace(topic)
	groupID = strings.TrimSpace(groupID)
	if topic == "" || groupID == "" {
		return errRequired("topic and group are required")
	}

	appCfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	saramaCfg, err := kafkacommon.NewSaramaConfig(appCfg.Kafka)
	if err != nil {
		return err
	}

	client, err := sarama.NewClient(appCfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return err
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return err
	}
	defer admin.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return err
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	offsets, err := admin.ListConsumerGroupOffsets(groupID, map[string][]int32{topic: partitions})
	if err != nil {
		return err
	}

	var totalLag int64
	for _, partition := range partitions {
		endOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return err
		}

		committed := int64(-1)
		if block := offsets.GetBlock(topic, partition); block != nil {
			committed = block.Offset
		}

		lag := int64(-1)
		if committed >= 0 {
			lag = endOffset - committed
			if lag < 0 {
				lag = 0
			}
			totalLag += lag
		}

		logger.Info("partition lag",
			slog.String("topic", topic),
			slog.String("group", groupID),
			slog.Int("partition", int(partition)),
			slog.Int64("end_offset", endOffset),
			slog.Int64("committed_offset", committed),
			slog.Int64("lag", lag),
		)
	}
	logger.Info("total lag", slog.String("topic", topic), slog.String("group", groupID), slog.Int64("lag", totalLag))
	return nil
}

type requiredError string

func (e requiredError) Error() string { return string(e) }

func errRequired(message string) error { return requiredError(message) }
