# 04-delivery-semantics

本目录用于练习消费一致性：至少一次、至多一次、幂等生产者，以及后续 Kafka 事务和外部系统一致性。

## 当前子任务

- `at-least-once`：处理成功后再提交 offset，可能重复但不轻易丢。
- `at-most-once`：处理前提交 offset，可能丢消息，仅适合可丢弃事件。
- `idempotent-producer`：启用 Sarama producer 幂等，降低生产重试导致的 broker 端重复写入。

## 语义对比

| 语义 | 提交时机 | 风险 | 适用 |
| --- | --- | --- | --- |
| 至少一次 | 处理成功后提交 | 可能重复 | 关键业务默认选择 |
| 至多一次 | 处理前提交 | 可能丢失 | 可丢弃日志、采样指标 |
| Kafka 内精确一次 | 事务生产和 read committed 消费 | 实现复杂 | Kafka 内链路 |

