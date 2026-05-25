# manual-commit

本任务演示手动提交 offset：消息处理成功后调用 `MarkMessage`，并按批次或时间显式 `Commit`。

## 场景

适合订单、支付、库存等关键业务消费：只有业务处理成功后才允许提交 offset。这样可以降低消息丢失风险，但业务处理必须支持幂等，因为进程可能在处理成功后、提交 offset 前退出。

## 运行

```powershell
go run ./tasks/03-consumer/manual-commit --topic kp_topic_demo --group kp_manual_commit_group --commit-every 10
```

## 验证

查看 group offset：

```powershell
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group kp_manual_commit_group
```

