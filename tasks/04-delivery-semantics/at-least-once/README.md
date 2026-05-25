# at-least-once

至少一次语义：先处理消息，处理成功后再标记并提交 offset。

## 运行

```powershell
go run ./tasks/04-delivery-semantics/at-least-once --topic kp_topic_demo --group kp_at_least_once_group
```

## 风险

如果进程在业务处理成功后、offset 提交前退出，消息会被重新消费。因此业务侧必须幂等。

