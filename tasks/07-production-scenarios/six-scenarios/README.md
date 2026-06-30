# 故障模拟：Kafka 六大生产问题

这个任务用一个命令模拟 6 个生产常见场景：

- 丢消息：`loss`
- 重复消费：`duplicate`
- 乱序：`out-of-order`
- 失败重试：`retry`
- offset 早提交：`early-commit`
- consumer 扩容 rebalance：`rebalance`

这些场景里有些代码是故意写成错误姿势，用来观察风险，不是生产推荐写法。

## 准备 topic

启动本地 Kafka：

```powershell
docker compose -f deployments/docker-compose.single.yml up -d
```

创建实验 topic：

```powershell
go run ./tasks/01-topic-management/create-topic --topic kp_failure_simulation --partitions 3 --replication-factor 1
go run ./tasks/01-topic-management/create-topic --topic kp_failure_simulation-retry --partitions 3 --replication-factor 1
```

如果要验证 broker 副本和 ISR 相关风险，使用三 broker 环境：

```powershell
docker compose -f deployments/docker-compose.cluster.yml up -d
```

## 1. 模拟丢消息

先生产消息：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario loss --produce --count 3
```

再消费：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario loss --group kp_loss_group --count 1
```

这个场景会先提交 offset，再模拟业务失败。再次用同一个 group 启动时，这条消息不会被正常重放。

观察点：

- 日志里先出现 offset commit，再出现 business failed。
- 同一个 group 再启动，会从后续 offset 继续。
- 结论是先提交 offset 再处理业务会造成消费侧丢消息。

也可以观察不安全生产者配置：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario loss --produce --unsafe-produce --count 3
```

此时会使用 `acks=0`、`retries=0`，用于理解 Producer 侧“不等确认”的风险。

## 2. 模拟重复消费

先生产消息：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario duplicate --produce --count 1
```

第一次消费：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario duplicate --group kp_duplicate_group --count 1
```

第二次用同一个 group 再消费：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario duplicate --group kp_duplicate_group --count 1
```

这个场景会模拟“业务处理成功，但 offset 不提交”。第二次启动时，同一条消息会再次出现。

观察点：

- 两次日志里的 topic、partition、offset 相同。
- 结论是处理成功但提交 offset 失败，会导致重复消费。
- 生产上要用业务唯一键、唯一索引、消息 ID 去重表或状态机兜底。

## 3. 模拟乱序

生产同一个 key 的有序消息：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario out-of-order --produce --count 9
```

并发消费：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario out-of-order --group kp_order_group --count 9 --workers 3
```

这些消息都使用 `order-1001` 作为 key，因此 Kafka 会保证它们进入同一个 partition，并按 offset 顺序拉取。但 consumer 内部把同一个 partition 的消息分发给多个 worker，并用不同耗时模拟业务处理，完成顺序会乱。

观察点：

- dispatch 日志按 offset 顺序出现。
- worker completed 日志可能不是按 offset 顺序出现。
- 结论是 Kafka 保证 partition 内读取有序，但 consumer 内部并发可能打破业务完成顺序。

## 4. 模拟失败重试

生产带失败标记的消息：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario retry --produce --count 9 --fail-contains fail
```

消费并转入 retry topic：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario retry --group kp_retry_group --count 9 --fail-contains fail
```

查看 retry topic：

```powershell
kafka-console-consumer --bootstrap-server localhost:9092 --topic kp_failure_simulation-retry --from-beginning
```

这个场景里，包含 `fail` 的消息会被发送到 retry topic，发送成功后提交原 topic offset。

观察点：

- 正常消息直接处理并提交。
- 失败消息先写入 retry topic，再提交原 offset。
- 原 topic 不会被坏消息永久卡住。
- 如果写 retry topic 失败，代码不会提交原 offset。

## 5. 模拟 offset 早提交

生产消息：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario early-commit --produce --count 1
```

启动自动提交消费者：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario early-commit --group kp_early_commit_group --count 1 --sleep 5s
```

这个场景开启自动提交，并在业务成功前先 `MarkMessage`，再让业务处理睡眠 5 秒。自动提交间隔设置为 1 秒，所以业务还没结束时 offset 可能已经提交。

观察点：

- 日志显示 message fetched 后业务还在 sleep。
- 业务失败时，offset 可能已经自动前进。
- 用同一个 group 重启，消息可能被跳过。
- 结论是自动提交配合过早 `MarkMessage`，容易造成“业务没处理完，Kafka 已认为处理完”。

## 6. 模拟 consumer 扩容 rebalance

先生产一批消息：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario rebalance --produce --count 30
```

开第一个窗口：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario rebalance --group kp_rebalance_group --instance c1 --sleep 3s
```

再开第二个窗口：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario rebalance --group kp_rebalance_group --instance c2 --sleep 3s
```

再开第三个窗口：

```powershell
go run ./tasks/07-production-scenarios/six-scenarios --scenario rebalance --group kp_rebalance_group --instance c3 --sleep 3s
```

观察点：

- 新 consumer 加入时，旧 consumer 会出现 cleanup/revoked 日志。
- 所有 consumer 随后出现 setup/assigned 日志。
- partition 会在 group 成员之间重新分配。
- 如果 consumer 数超过 partition 数，多出来的 consumer 会没有分区可消费。
- 正在处理但尚未提交 offset 的消息，可能在 rebalance 后被重新消费。

## 常用检查命令

查看 group offset 和 lag：

```powershell
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group kp_loss_group
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group kp_duplicate_group
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group kp_rebalance_group
```

重置某个实验 group：

```powershell
kafka-consumer-groups --bootstrap-server localhost:9092 --group kp_loss_group --topic kp_failure_simulation --reset-offsets --to-earliest --execute
```

重新创建 topic 前可以删除旧 topic：

```powershell
kafka-topics --bootstrap-server localhost:9092 --delete --topic kp_failure_simulation
```

## 关键结论

| 场景 | 代码里故意做了什么 | 你应该看到什么 |
| --- | --- | --- |
| 丢消息 | 先提交 offset，再模拟业务失败 | 同 group 重启后消息被跳过 |
| 重复消费 | 业务成功后不提交 offset | 同 offset 被再次消费 |
| 乱序 | 同 partition 消息交给多个 worker 并发处理 | 拉取有序，完成乱序 |
| 失败重试 | 失败消息写 retry topic 后提交原 offset | 原 topic 不被卡住 |
| offset 早提交 | 开自动提交，业务处理很慢 | 业务失败时 offset 可能已前进 |
| rebalance | 多个 consumer 加入同一 group | partition 重新分配，可能短暂停顿和重复 |
