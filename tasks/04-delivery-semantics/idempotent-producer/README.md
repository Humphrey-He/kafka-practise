# idempotent-producer

本任务演示启用 Sarama producer 幂等。幂等 producer 能减少生产者重试造成的 broker 端重复写入，但不能解决消费端重复处理和外部系统副作用问题。

## 运行

```powershell
go run ./tasks/04-delivery-semantics/idempotent-producer --config configs/idempotent.yaml --topic kp_topic_demo --key order-1 --value "paid"
```

## 配置要点

- `Producer.Idempotent=true`
- `Producer.RequiredAcks=WaitForAll`
- `Producer.Retry.Max > 0`
- `Net.MaxOpenRequests=1`

