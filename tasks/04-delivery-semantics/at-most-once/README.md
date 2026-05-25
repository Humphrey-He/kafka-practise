# at-most-once

至多一次语义：收到消息后先提交 offset，再处理业务。

## 运行

```powershell
go run ./tasks/04-delivery-semantics/at-most-once --topic kp_topic_demo --group kp_at_most_once_group
```

## 风险

如果 offset 已提交但处理失败或进程退出，这条消息不会再被当前 group 自动重放。这个模式通常不适合关键业务。

