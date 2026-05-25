# 03-consumer

本任务练习 Sarama consumer group 基础用法。第一阶段先实现最小 consumer group，并在处理成功后标记 offset。

## 子任务

- `consumer-group-basic`：订阅 topic，打印消息，标记 offset。

## 运行

```powershell
go run ./tasks/03-consumer/consumer-group-basic --topic kp_topic_demo --group kp_consumer_basic_group
```

## 关键配置

- `group.id` 在 Sarama 中由 `NewConsumerGroup` 的第二个参数传入。
- `Consumer.Offsets.Initial` 决定无历史提交位移时从最早还是最新开始。
- `MarkMessage` 只是标记待提交 offset；是否自动提交取决于配置，也可显式调用 `Commit`。

## 常见问题

| 现象 | 原因 | 解决 |
| --- | --- | --- |
| 启动后没有消息 | group offset 已在末尾 | 换 group 或 reset offset |
| 重复消费 | 处理后提交前进程退出 | 业务处理必须幂等 |
| rebalance 频繁 | 处理阻塞或实例频繁重启 | 记录 Setup/Cleanup 并检查耗时 |

