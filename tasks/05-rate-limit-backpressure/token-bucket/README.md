# token-bucket

本任务演示消费端令牌桶限流：消费者仍按 Kafka 分配消费，但业务处理按固定速率执行。

## 运行

```powershell
go run ./tasks/05-rate-limit-backpressure/token-bucket --topic kp_topic_demo --rate 5
```

## 注意

限流会主动放慢处理速度，lag 可能上升。生产中需要同时监控处理速率、lag 趋势和下游错误率。

