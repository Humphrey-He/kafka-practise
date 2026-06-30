package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Humphrey-He/kafka-practise/internal/common/config"
	kafkacommon "github.com/Humphrey-He/kafka-practise/internal/common/kafka"
	"github.com/Humphrey-He/kafka-practise/internal/common/logging"
	"github.com/IBM/sarama"
)

const (
	scenarioLoss        = "loss"
	scenarioDuplicate   = "duplicate"
	scenarioOutOfOrder  = "out-of-order"
	scenarioRetry       = "retry"
	scenarioEarlyCommit = "early-commit"
	scenarioRebalance   = "rebalance"
)

type options struct {
	configPath    string
	scenario      string
	topic         string
	retryTopic    string
	groupID       string
	produce       bool
	count         int
	failContains  string
	instance      string
	sleep         time.Duration
	workers       int
	unsafeProduce bool
}

type stoppableHandler interface {
	setStop(func())
}

func main() {
	var opts options
	flag.StringVar(&opts.configPath, "config", "configs/local.yaml", "config file path")
	flag.StringVar(&opts.scenario, "scenario", scenarioLoss, "scenario: loss, duplicate, out-of-order, retry, early-commit, rebalance")
	flag.StringVar(&opts.topic, "topic", "kp_failure_simulation", "source topic")
	flag.StringVar(&opts.retryTopic, "retry-topic", "", "retry topic, defaults to <topic>-retry")
	flag.StringVar(&opts.groupID, "group", "kp_failure_simulation_group", "consumer group id")
	flag.BoolVar(&opts.produce, "produce", false, "produce demo messages before consuming")
	flag.IntVar(&opts.count, "count", 10, "number of demo messages to produce or consume")
	flag.StringVar(&opts.failContains, "fail-contains", "fail", "value substring that simulates business failure")
	flag.StringVar(&opts.instance, "instance", "c1", "consumer instance label for logs")
	flag.DurationVar(&opts.sleep, "sleep", time.Second, "simulated processing time")
	flag.IntVar(&opts.workers, "workers", 3, "worker count for out-of-order scenario")
	flag.BoolVar(&opts.unsafeProduce, "unsafe-produce", false, "use unsafe producer settings for loss scenario")
	flag.Parse()

	if opts.retryTopic == "" {
		opts.retryTopic = opts.topic + "-retry"
	}

	logger := logging.New()
	if err := run(opts, logger); err != nil {
		logger.Error("failure simulation failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(opts options, logger *slog.Logger) error {
	if opts.count <= 0 {
		return errors.New("count must be greater than 0")
	}
	if strings.TrimSpace(opts.topic) == "" {
		return errors.New("topic is required")
	}

	appCfg, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}

	switch opts.scenario {
	// 每个 scenario 都刻意放大一种生产风险，便于用日志观察 offset、partition 和 group 行为。
	case scenarioLoss:
		return runLoss(appCfg, opts, logger)
	case scenarioDuplicate:
		return runDuplicate(appCfg, opts, logger)
	case scenarioOutOfOrder:
		return runOutOfOrder(appCfg, opts, logger)
	case scenarioRetry:
		return runRetry(appCfg, opts, logger)
	case scenarioEarlyCommit:
		return runEarlyCommit(appCfg, opts, logger)
	case scenarioRebalance:
		return runRebalance(appCfg, opts, logger)
	default:
		return fmt.Errorf("unknown scenario %q", opts.scenario)
	}
}

func runLoss(appCfg *config.AppConfig, opts options, logger *slog.Logger) error {
	if opts.unsafeProduce {
		// Producer 侧丢失风险演示：不等待 broker 确认，也不重试。
		// 生产环境的关键消息不要使用这组配置。
		appCfg.Kafka.Producer.RequiredAcks = "0"
		appCfg.Kafka.Producer.RetryMax = 0
		appCfg.Kafka.Producer.ReturnSuccesses = false
		logger.Warn("using unsafe producer settings for loss demo",
			slog.String("acks", appCfg.Kafka.Producer.RequiredAcks),
			slog.Int("retries", appCfg.Kafka.Producer.RetryMax),
		)
	}
	if opts.produce {
		return produceDemoMessages(appCfg, opts.topic, opts.count, "loss", logger)
	}

	appCfg.Kafka.Consumer.AutoCommit = false
	return consumeWithHandler(appCfg, opts, logger, &lossHandler{logger: logger, max: opts.count})
}

func runDuplicate(appCfg *config.AppConfig, opts options, logger *slog.Logger) error {
	if opts.produce {
		return produceDemoMessages(appCfg, opts.topic, opts.count, "duplicate", logger)
	}

	appCfg.Kafka.Consumer.AutoCommit = false
	return consumeWithHandler(appCfg, opts, logger, &duplicateHandler{logger: logger, max: opts.count})
}

func runOutOfOrder(appCfg *config.AppConfig, opts options, logger *slog.Logger) error {
	if opts.produce {
		return produceOrderedMessages(appCfg, opts.topic, opts.count, logger)
	}

	appCfg.Kafka.Consumer.AutoCommit = false
	return consumeWithHandler(appCfg, opts, logger, &outOfOrderHandler{
		logger:  logger,
		max:     opts.count,
		workers: opts.workers,
	})
}

func runRetry(appCfg *config.AppConfig, opts options, logger *slog.Logger) error {
	if opts.produce {
		return produceRetryMessages(appCfg, opts.topic, opts.count, opts.failContains, logger)
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

	// 重试场景需要在消费失败时把原消息转发到 retry topic，
	// 因此 consumer 里同时持有一个同步 producer，保证发送成功后再提交原 offset。
	return consumeWithConfigAndHandler(appCfg, saramaCfg, opts, logger, &retryHandler{
		logger:       logger,
		producer:     producer,
		retryTopic:   opts.retryTopic,
		failContains: opts.failContains,
		max:          opts.count,
	})
}

func runEarlyCommit(appCfg *config.AppConfig, opts options, logger *slog.Logger) error {
	if opts.produce {
		return produceDemoMessages(appCfg, opts.topic, opts.count, "early", logger)
	}

	appCfg.Kafka.Consumer.AutoCommit = true
	saramaCfg, err := kafkacommon.NewSaramaConfig(appCfg.Kafka)
	if err != nil {
		return err
	}
	// 缩短自动提交间隔，让“业务还没成功，offset 已经前进”的现象更容易复现。
	saramaCfg.Consumer.Offsets.AutoCommit.Interval = time.Second
	return consumeWithConfigAndHandler(appCfg, saramaCfg, opts, logger, &earlyCommitHandler{
		logger: logger,
		max:    opts.count,
		sleep:  opts.sleep,
	})
}

func runRebalance(appCfg *config.AppConfig, opts options, logger *slog.Logger) error {
	if opts.produce {
		return produceDemoMessages(appCfg, opts.topic, opts.count, "rebalance", logger)
	}

	appCfg.Kafka.Consumer.AutoCommit = false
	return consumeWithHandler(appCfg, opts, logger, &rebalanceHandler{
		logger:   logger,
		instance: opts.instance,
		sleep:    opts.sleep,
	})
}

func consumeWithHandler(appCfg *config.AppConfig, opts options, logger *slog.Logger, handler sarama.ConsumerGroupHandler) error {
	saramaCfg, err := kafkacommon.NewSaramaConfig(appCfg.Kafka)
	if err != nil {
		return err
	}
	return consumeWithConfigAndHandler(appCfg, saramaCfg, opts, logger, handler)
}

func consumeWithConfigAndHandler(appCfg *config.AppConfig, saramaCfg *sarama.Config, opts options, logger *slog.Logger, handler sarama.ConsumerGroupHandler) error {
	group, err := sarama.NewConsumerGroup(appCfg.Kafka.Brokers, opts.groupID, saramaCfg)
	if err != nil {
		return err
	}
	defer group.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if stoppable, ok := handler.(stoppableHandler); ok {
		// loss/duplicate/retry 等有限实验达到 count 后自动停止；
		// rebalance 场景需要长时间运行，所以不实现 stoppableHandler。
		stoppable.setStop(stop)
	}

	logger.Info("consumer started",
		slog.String("scenario", opts.scenario),
		slog.String("topic", opts.topic),
		slog.String("group", opts.groupID),
	)
	for ctx.Err() == nil {
		if err := group.Consume(ctx, []string{opts.topic}, handler); err != nil {
			logger.Error("consume loop error", slog.String("error", err.Error()))
			time.Sleep(time.Second)
		}
	}
	return nil
}

func produceDemoMessages(appCfg *config.AppConfig, topic string, count int, prefix string, logger *slog.Logger) error {
	return produceMessages(appCfg, topic, count, func(i int) (string, string) {
		key := fmt.Sprintf("%s-key-%02d", prefix, i)
		value := fmt.Sprintf("%s-message-%02d", prefix, i)
		return key, value
	}, logger)
}

func produceOrderedMessages(appCfg *config.AppConfig, topic string, count int, logger *slog.Logger) error {
	return produceMessages(appCfg, topic, count, func(i int) (string, string) {
		// 固定 key 可以保证这些消息进入同一个 partition，
		// 后面再用 consumer 内部并发证明：Kafka 拉取有序不等于业务完成有序。
		return "order-1001", fmt.Sprintf("order-1001-step-%02d", i)
	}, logger)
}

func produceRetryMessages(appCfg *config.AppConfig, topic string, count int, failContains string, logger *slog.Logger) error {
	return produceMessages(appCfg, topic, count, func(i int) (string, string) {
		key := fmt.Sprintf("retry-key-%02d", i)
		if i%3 == 0 {
			return key, fmt.Sprintf("retry-message-%02d-%s", i, failContains)
		}
		return key, fmt.Sprintf("retry-message-%02d-ok", i)
	}, logger)
}

func produceMessages(appCfg *config.AppConfig, topic string, count int, messageAt func(int) (string, string), logger *slog.Logger) error {
	saramaCfg, err := kafkacommon.NewSaramaConfig(appCfg.Kafka)
	if err != nil {
		return err
	}

	producer, err := sarama.NewSyncProducer(appCfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 1; i <= count; i++ {
		key, value := messageAt(i)
		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		})
		if err != nil {
			return fmt.Errorf("send message %d: %w", i, err)
		}
		logger.Info("message produced",
			slog.String("topic", topic),
			slog.String("key", key),
			slog.String("value", value),
			slog.Int("partition", int(partition)),
			slog.Int64("offset", offset),
		)
	}
	return nil
}

type lossHandler struct {
	logger   *slog.Logger
	max      int
	consumed int
	stop     func()
}

func (h *lossHandler) setStop(stop func()) {
	h.stop = stop
}

func (h *lossHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.consumed = 0
	h.logger.Info("loss demo setup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *lossHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *lossHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.consumed++
		h.logger.Warn("committing before business work, then simulating business failure",
			slog.String("topic", message.Topic),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
			slog.String("value", string(message.Value)),
		)
		// 故意先提交 offset，再模拟业务失败。
		// 同一个 consumer group 之后会从新 offset 继续，旧消息不会被正常重放。
		session.MarkMessage(message, "")
		session.Commit()
		h.logger.Error("business failed after offset commit; this message is now lost for this group")
		if h.consumed >= h.max {
			h.stopIfReady()
			return nil
		}
	}
	return nil
}

func (h *lossHandler) stopIfReady() {
	if h.stop != nil {
		h.stop()
	}
}

type duplicateHandler struct {
	logger *slog.Logger
	max    int
	seen   int
	stop   func()
}

func (h *duplicateHandler) setStop(stop func()) {
	h.stop = stop
}

func (h *duplicateHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.seen = 0
	h.logger.Info("duplicate demo setup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *duplicateHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *duplicateHandler) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.seen++
		h.logger.Warn("business succeeded, but offset is intentionally not committed",
			slog.String("topic", message.Topic),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
			slog.String("value", string(message.Value)),
		)
		// 故意不 MarkMessage/Commit：模拟“业务成功，但提交 offset 前宕机”。
		// 用同一个 group 再启动时，会从旧 offset 重新消费，形成重复。
		if h.seen >= h.max {
			h.logger.Warn("stop this process now and run it again with the same group to see duplicates")
			h.stopIfReady()
			return nil
		}
	}
	return nil
}

func (h *duplicateHandler) stopIfReady() {
	if h.stop != nil {
		h.stop()
	}
}

type outOfOrderHandler struct {
	logger  *slog.Logger
	max     int
	workers int
	seen    int
	stop    func()
}

func (h *outOfOrderHandler) setStop(stop func()) {
	h.stop = stop
}

func (h *outOfOrderHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.seen = 0
	h.logger.Info("out-of-order demo setup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *outOfOrderHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *outOfOrderHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if h.workers <= 0 {
		h.workers = 3
	}

	jobs := make(chan *sarama.ConsumerMessage)
	var wg sync.WaitGroup
	for workerID := 1; workerID <= h.workers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for message := range jobs {
				// 用不同延迟模拟不同业务耗时，让同 partition 消息的完成顺序被 worker 并发打乱。
				delay := time.Duration((message.Offset%3)+1) * 300 * time.Millisecond
				time.Sleep(delay)
				h.logger.Warn("worker completed message; completion order may differ from offset order",
					slog.Int("worker", id),
					slog.Int64("offset", message.Offset),
					slog.String("key", string(message.Key)),
					slog.String("value", string(message.Value)),
					slog.Duration("delay", delay),
				)
				// 注意：这里在 worker 内 MarkMessage 只是为了演示乱序风险。
				// 生产上如果同 partition 并发处理，需要额外保证按连续成功 offset 提交。
				session.MarkMessage(message, "")
			}
		}(workerID)
	}

	for message := range claim.Messages() {
		h.seen++
		h.logger.Info("dispatching message to worker",
			slog.Int64("offset", message.Offset),
			slog.String("key", string(message.Key)),
			slog.String("value", string(message.Value)),
		)
		jobs <- message
		if h.seen >= h.max {
			close(jobs)
			wg.Wait()
			session.Commit()
			h.stopIfReady()
			return nil
		}
	}
	close(jobs)
	wg.Wait()
	session.Commit()
	return nil
}

func (h *outOfOrderHandler) stopIfReady() {
	if h.stop != nil {
		h.stop()
	}
}

type retryHandler struct {
	logger       *slog.Logger
	producer     sarama.SyncProducer
	retryTopic   string
	failContains string
	max          int
	seen         int
	stop         func()
}

func (h *retryHandler) setStop(stop func()) {
	h.stop = stop
}

func (h *retryHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.seen = 0
	h.logger.Info("retry demo setup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *retryHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *retryHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.seen++
		value := string(message.Value)
		if strings.Contains(value, h.failContains) {
			// 失败消息先写入 retry topic；只有转发成功后，才提交原 topic offset。
			// 这样既避免坏消息卡住原 partition，又不会在 retry 写失败时丢消息。
			if err := h.forward(message, h.retryTopic, "retryable failure"); err != nil {
				h.logger.Error("send retry topic failed; source offset is not committed",
					slog.String("error", err.Error()),
					slog.Int64("offset", message.Offset),
				)
				continue
			}
			h.logger.Warn("message sent to retry topic, source offset can be committed",
				slog.String("retry_topic", h.retryTopic),
				slog.Int64("source_offset", message.Offset),
				slog.String("value", value),
			)
		} else {
			h.logger.Info("message processed successfully",
				slog.Int64("offset", message.Offset),
				slog.String("value", value),
			)
		}

		session.MarkMessage(message, "")
		session.Commit()
		if h.seen >= h.max {
			h.stopIfReady()
			return nil
		}
	}
	return nil
}

func (h *retryHandler) stopIfReady() {
	if h.stop != nil {
		h.stop()
	}
}

func (h *retryHandler) forward(message *sarama.ConsumerMessage, topic string, reason string) error {
	headers := make([]sarama.RecordHeader, 0, len(message.Headers)+4)
	for _, header := range message.Headers {
		headers = append(headers, sarama.RecordHeader{Key: header.Key, Value: header.Value})
	}
	headers = append(headers,
		// 保留原始位置和错误原因，方便后续补偿、排查和人工处理。
		sarama.RecordHeader{Key: []byte("x-original-topic"), Value: []byte(message.Topic)},
		sarama.RecordHeader{Key: []byte("x-original-partition"), Value: []byte(fmt.Sprint(message.Partition))},
		sarama.RecordHeader{Key: []byte("x-original-offset"), Value: []byte(fmt.Sprint(message.Offset))},
		sarama.RecordHeader{Key: []byte("x-error-reason"), Value: []byte(reason)},
	)

	_, _, err := h.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.ByteEncoder(message.Key),
		Value:   sarama.ByteEncoder(message.Value),
		Headers: headers,
	})
	return err
}

type earlyCommitHandler struct {
	logger *slog.Logger
	max    int
	sleep  time.Duration
	seen   int
	stop   func()
}

func (h *earlyCommitHandler) setStop(stop func()) {
	h.stop = stop
}

func (h *earlyCommitHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.seen = 0
	h.logger.Info("early commit demo setup", slog.Any("claims", session.Claims()))
	return nil
}

func (h *earlyCommitHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *earlyCommitHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.seen++
		h.logger.Warn("message fetched; auto commit may advance offset while business is still running",
			slog.Int64("offset", message.Offset),
			slog.String("value", string(message.Value)),
			slog.Duration("business_time", h.sleep),
		)
		// 故意在业务成功前 MarkMessage。
		// 自动提交线程可能在 sleep 期间提交 offset，随后业务失败就会变成“已提交但未处理”。
		session.MarkMessage(message, "")
		time.Sleep(h.sleep)
		h.logger.Error("business failed after possible auto commit; restart with same group to check whether message is skipped")
		if h.seen >= h.max {
			h.stopIfReady()
			return nil
		}
	}
	return nil
}

func (h *earlyCommitHandler) stopIfReady() {
	if h.stop != nil {
		h.stop()
	}
}

type rebalanceHandler struct {
	logger   *slog.Logger
	instance string
	sleep    time.Duration
}

func (h *rebalanceHandler) Setup(session sarama.ConsumerGroupSession) error {
	// 每次有 consumer 加入、退出或心跳超时，group 都可能重新分配 partition。
	h.logger.Warn("consumer group setup/rebalance assigned partitions",
		slog.String("instance", h.instance),
		slog.Any("claims", session.Claims()),
	)
	return nil
}

func (h *rebalanceHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	// Cleanup 表示本 consumer 持有的 partition 即将被撤销；
	// 这里提交已处理 offset，降低 rebalance 后重复消费的范围。
	session.Commit()
	h.logger.Warn("consumer group cleanup/rebalance revoked partitions",
		slog.String("instance", h.instance),
		slog.Any("claims", session.Claims()),
	)
	return nil
}

func (h *rebalanceHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		h.logger.Info("processing message slowly",
			slog.String("instance", h.instance),
			slog.String("topic", message.Topic),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
			slog.String("value", string(message.Value)),
		)
		// 慢处理让 rebalance 更容易发生在“消息处理中”的窗口，
		// 从而观察短暂停顿、partition 转移以及潜在重复消费。
		time.Sleep(h.sleep)
		session.MarkMessage(message, "")
		session.Commit()
		h.logger.Info("message committed",
			slog.String("instance", h.instance),
			slog.Int("partition", int(message.Partition)),
			slog.Int64("offset", message.Offset),
		)
	}
	return nil
}
