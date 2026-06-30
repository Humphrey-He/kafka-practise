# bounded-worker-pool

本任务演示固定 worker pool 处理消息。示例为了保持 offset 顺序安全，worker 成功处理后把结果回传主消费协程，由主协程按读取顺序提交。

## 运行

```powershell
go run ./tasks/05-rate-limit-backpressure/bounded-worker-pool --topic kp_topic_demo --workers 4
```

## 注意

真实生产中如果允许同一分区内并发处理，必须额外维护每个分区的连续完成 offset，不能因为后面的消息先完成就提交前面未完成的 offset。

