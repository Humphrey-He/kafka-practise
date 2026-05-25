# 02-producer

本任务练习 Sarama producer 基础用法，第一阶段先实现同步发送消息。

## 子任务

- `sync-producer`：发送一条或多条消息，并输出 broker 返回的 partition/offset。

## 运行

```powershell
go run ./tasks/02-producer/sync-producer --topic kp_topic_demo --key user-1 --value "hello kafka"
```

发送多条：

```powershell
go run ./tasks/02-producer/sync-producer --topic kp_topic_demo --count 10
```

## 关键配置

- `Producer.RequiredAcks`：默认使用 `WaitForAll`，对应 `acks=all`。
- `Producer.Return.Successes`：同步 producer 必须开启。
- `Producer.Retry.Max`：发送失败时的重试次数。

## 常见问题

| 现象 | 原因 | 解决 |
| --- | --- | --- |
| 发送超时 | topic 不存在或 broker 不可用 | 先运行 topic-management 创建 topic |
| partition 分布不符合预期 | key 或分区器配置不同 | 使用固定 key 验证同 key 同分区 |
| 消息重复 | 重试或调用方重复发送 | 关键业务增加幂等键 |

