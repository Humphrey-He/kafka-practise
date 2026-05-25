# 05-rate-limit-backpressure

本目录用于练习消费速率控制、回压、失败隔离和 DLQ。

## 当前子任务

- `dlq-retry`：消费失败时把原始消息转发到 DLQ topic，并在转发成功后提交 offset。

