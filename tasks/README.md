# tasks 任务索引

这个目录用于放 Go + Sarama 的可运行练习模块。约定：一个任务一个文件夹；如果任务之间强关联，可以在任务文件夹内继续拆子文件夹。

## 任务目录

```text
tasks/
  01-topic-management/
    README.md
    create-topic/
    delete-topic/
    describe-topic/
    alter-config/
    create-partitions/
  02-producer/
    README.md
    sync-producer/
    async-producer/
    key-partitioning/
    batch-and-compression/
    serializer/
    retry-timeout/
  03-consumer/
    README.md
    partition-consumer/
    consumer-group-basic/
    manual-commit/
    auto-commit/
    offset-reset/
    read-committed/
  04-delivery-semantics/
    README.md
    at-least-once/
    at-most-once/
    idempotent-producer/
    transactional-producer/
    consume-process-produce/
    external-db-consistency/
  05-rate-limit-backpressure/
    README.md
    dlq-retry/
    token-bucket/
    pause-resume/
    bounded-worker-pool/
    fetch-tuning/
    dlq-retry/
  06-lag-rebalance/
    README.md
    lag-calculator/
    rebalance-log/
    static-membership/
    slow-consumer-demo/
    partition-balance-report/
  07-production-scenarios/
    README.md
    six-scenarios/
  07-idempotency-dedup/
    README.md
    producer-idempotent/
    dedup-by-message-id/
    db-unique-key/
    redis-dedup-window/
    duplicate-metrics/
  08-durability-retention/
    README.md
    acks-all-min-isr/
    replication-factor/
    retention-policy/
    compact-topic/
    broker-failure-lab/
  09-throughput-latency/
    README.md
    producer-benchmark/
    consumer-batch-processing/
    partition-parallelism/
    compression-compare/
    latency-histogram/
  10-monitoring-ops/
    README.md
    metadata-health/
    consumer-lag-exporter/
    producer-metrics/
    consumer-metrics/
    ops-checklist/
  11-pipeline-integration/
    README.md
    file-source-to-kafka/
    http-source-to-kafka/
    kafka-to-file-sink/
    kafka-to-db-sink/
    connect-notes/
    streaming-notes/
  12-security-acl-quota/
    README.md
    sasl-plain/
    sasl-scram/
    tls-client/
    acl-checklist/
    quota-notes/
    audit-log-notes/
  13-local-debug-testing/
    README.md
    docker-compose-single/
    docker-compose-cluster/
    mock-producer-consumer/
    integration-test/
    cli-cheatsheet/
    failure-simulation/
```

## 文件约定

每个可运行子任务建议包含：

- `README.md`：说明场景、配置、运行、验证、常见问题。
- `main.go`：最小可运行入口，或放到 `cmd/` 后在 README 中链接。
- `config.example.yaml`：任务专属配置样例。
- `testdata/`：样例输入、消息文件、证书说明、测试数据。
- `*_test.go`：单元测试；依赖 Kafka 的测试使用 `integration` build tag。

## 命名约定

- 任务文件夹使用英文短横线命名，前缀编号固定排序。
- topic 使用 `kp_<任务名>_<用途>`，例如 `kp_producer_basic`。
- group 使用 `kp_<任务名>_<用途>_group`。
- client id 使用 `kp-<任务名>-<用途>`。
- 本地测试资源避免复用生产命名。

## README 链接

新建任务 README 时，直接复制：

- [任务 README 模板](../docs/任务README模板.md)

已落地任务：

- [07-production-scenarios/six-scenarios](07-production-scenarios/six-scenarios/README.md)：模拟丢消息、重复消费、乱序、失败重试、offset 早提交和 consumer 扩容 rebalance。

并至少补齐：

- 场景
- 学习目标
- 关键配置
- 运行方式
- 验证方式
- 常见问题
- 生产注意事项
