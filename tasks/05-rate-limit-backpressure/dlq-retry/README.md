# dlq-retry

本任务演示失败消息转发到 DLQ topic。示例使用一个简单规则模拟业务失败：消息 value 包含指定关键字时发送到 DLQ。

## 运行

先创建源 topic 和 DLQ topic：

```powershell
go run ./tasks/01-topic-management/create-topic --topic kp_source_demo --partitions 3 --replication-factor 1
go run ./tasks/01-topic-management/create-topic --topic kp_source_demo_dlq --partitions 3 --replication-factor 1
```

启动消费者：

```powershell
go run ./tasks/05-rate-limit-backpressure/dlq-retry --topic kp_source_demo --dlq-topic kp_source_demo_dlq --fail-contains fail
```

## 语义

只有 DLQ 发送成功后才提交源消息 offset。这样可以避免失败消息被静默吞掉，但 DLQ 发送失败时源消息可能重复消费。

