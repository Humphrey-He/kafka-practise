# pause-resume

本任务演示使用 Sarama consumer group 的 `Pause` / `Resume` 控制指定分区拉取。

## 运行

```powershell
go run ./tasks/05-rate-limit-backpressure/pause-resume --topic kp_topic_demo --pause-after 5 --pause-for 3s
```

## 场景

当下游依赖变慢或内部队列过长时，可以暂停当前分区，等待压力下降后恢复。注意不要暂停太久导致 group 会话异常。

