# Kafka 六大生产问题

这份文档用于把 Kafka 常见生产问题拆清楚：消息会不会丢、会不会重复、顺序怎么保证、消费失败怎么重试、offset 什么时候提交、消费者扩容会发生什么。

理解这些问题时，不要只背结论。每次都按三段链路拆：

```text
Producer -> Broker -> Consumer
```

每个问题都问三句话：

- 风险发生在哪一段？
- Kafka 自己保证了什么？
- 业务系统还要补什么？

最终目标不是追求“绝不重复”，而是：

```text
消息尽量不丢 + 允许重复 + 消费端幂等 + 失败可重试 + 最终可补偿
```

## 1. 消息会不会丢

会。Kafka 可以通过配置和正确的处理顺序降低丢失概率，但不能替业务系统兜住所有处理失败。

### 风险发生在哪一段

Producer 侧：

- 消息还没发出去，进程就失败。
- 消息发到 Broker，但 Producer 没有等待确认就认为成功。
- 异步发送时没有读取或处理失败回调。
- 发送失败后没有重试，或者重试次数过少。

Broker 侧：

- Broker 收到消息，但副本还没同步完成，leader 宕机。
- topic 副本数太少，单 broker 故障直接丢数据。
- `min.insync.replicas` 配置太低，少数副本确认就返回成功。
- 开启非同步副本选主，落后的副本可能成为 leader。
- 消息超过 retention，被 Kafka 正常清理。

Consumer 侧：

- 消费者拿到消息后，先提交 offset，再处理业务，业务失败后无法从 Kafka 重放。
- 自动提交 offset 导致“还没真正处理完，位移已经前进”。
- 处理失败后错误地提交 offset，没有转入重试队列或死信队列。

### Kafka 保证了什么

Kafka 保证的是持久化日志和基于 offset 的重放能力，但前提是消息已经可靠写入 Broker，并且消费者没有把未处理完成的消息标记为已消费。

对于关键业务，Producer 到 Broker 之间通常需要：

```properties
acks=all
enable.idempotence=true
retries>0
```

Broker/topic 侧通常需要：

```properties
replication.factor>=3
min.insync.replicas>=2
unclean.leader.election.enable=false
```

### 业务还要补什么

消费者侧的核心原则：

```text
业务处理成功后，再提交 offset。
```

错误顺序：

```text
先提交 offset -> 再处理业务 -> 业务失败
结果：Kafka 认为消息已消费，消息无法再从原 offset 自动重放。
```

正确默认顺序：

```text
先处理业务 -> 处理成功 -> 提交 offset
结果：可能重复，但不容易丢。
```

业务上还要补：

- 消费端幂等。
- 失败重试和 DLQ。
- 关键状态可补偿。
- 生产端错误回调和告警。
- 重要 topic 的 retention 与容量评估。

生产结论：

```text
Kafka 可以降低丢消息概率，但最终还要靠业务处理顺序、幂等、事务或补偿兜底。
```

## 2. 消息会不会重复

会。后端系统里应该默认“消息可能重复”。

### 风险发生在哪一段

Producer 侧：

- Broker 已经写入成功，但 ack 响应在网络中丢失，Producer 认为失败并重试。
- 未开启幂等生产者时，同一条业务消息可能被追加多次。

Broker 侧：

- Kafka 以 append log 方式保存消息，不会天然理解“这两条业务上是同一条”。
- Kafka 的 Exactly Once 主要约束 Kafka 内部的生产和消费事务链路，不等于外部数据库也自动 exactly once。

Consumer 侧：

- 业务处理成功，但提交 offset 失败。
- 消费者处理过程中宕机，重启后从旧 offset 继续消费。
- rebalance 发生时，正在处理的消息被其他消费者重新消费。

典型例子：

```text
Consumer 收到消息 A
写数据库成功
还没提交 offset
Consumer 宕机
重启后从旧 offset 继续消费
消息 A 再来一次
```

### Kafka 保证了什么

Kafka 默认更适合实现 at-least-once：

```text
至少处理一次：消息不容易丢，但可能重复。
```

开启幂等生产者后，可以降低 Producer 重试造成的重复写入，但它不能解决 Consumer 写数据库成功、提交 offset 失败导致的重复消费。

### 业务还要补什么

消费端必须幂等。常见手段：

- 业务唯一键。
- 数据库唯一索引。
- 消息 ID 去重表。
- 状态机防重复流转。
- 接口幂等 token。
- 订单号、任务号、事件 ID 做幂等键。

例子：

```text
消费“创建订单”消息时，不能简单 insert。
应该用 order_id 做唯一约束。
如果 order_id 已存在，说明重复消费，直接按成功处理。
```

生产结论：

```text
Kafka 消费端默认追求 at-least-once + 业务幂等。
```

## 3. 顺序怎么保证

Kafka 只保证同一个 partition 内消息有序，不保证不同 partition 之间的全局顺序。

```text
同一个 partition 内：有序
不同 partition 之间：不保证全局顺序
```

### 风险发生在哪一段

Producer 侧：

- 没有设置 key，消息可能按轮询或粘性策略进入不同 partition。
- 同一业务实体的消息用了不同 key，导致进入不同 partition。
- Producer 允许过多 in-flight 请求，异常重试时可能影响发送顺序。开启幂等生产者后客户端会约束相关配置，但仍要关注客户端版本和配置。

Broker 侧：

- partition 内追加顺序稳定。
- 多个 partition 并行写入，不存在跨 partition 的全局时间线。

Consumer 侧：

- 一个 consumer group 内，一个 partition 同一时间只会分配给一个 consumer，这是顺序消费的基础。
- 如果消费者内部对同一个 partition 的消息开启并发处理，业务完成顺序可能被自己打乱。
- 失败消息如果被丢到 retry topic，原 topic 后续消息可能先完成，业务全局顺序需要额外设计。

### Kafka 保证了什么

Kafka 保证：

```text
同一个 partition 内，消息按 offset 递增顺序被读取。
```

Kafka 不保证：

```text
不同 partition 之间的消息按全局业务时间有序。
```

### 业务还要补什么

顺序的关键是 key：

```text
相同 key -> 进入同一个 partition -> 保证局部有序
不同 key -> 可能进入不同 partition -> 不保证顺序
```

常见设计：

```text
user_id=1001 的所有消息用 user_id 作为 key
order_id=888 的所有状态变更用 order_id 作为 key
```

这样可以保证：

```text
同一个用户的事件有序
同一个订单的状态流转有序
```

不能保证：

```text
所有用户的全局事件有序
所有订单的全局状态有序
```

Consumer 侧要避免：

```text
从同一个 partition 拉到消息后，直接开多个 goroutine 并发处理，且没有按 offset 顺序提交和落库。
```

如果业务必须按订单状态顺序处理，可以使用：

- `order_id` 作为消息 key。
- 单 partition 内串行处理。
- 状态机校验，例如只能从 `created` 到 `paid`，不能从 `created` 直接到 `shipped`。
- 对乱序到达的状态做暂存、重试或补偿。

## 4. 消费失败怎么重试

不要只会“失败就不提交 offset”。如果一条坏消息一直失败、不提交 offset，这个 partition 后面的消息都会被卡住。

### 风险发生在哪一段

Producer 侧：

- 重试消息发送到 retry topic 或 DLQ 失败。
- 重试消息缺少原始 topic、partition、offset、错误原因、重试次数，后续无法追踪。

Broker 侧：

- Kafka 本身只保存消息，不理解业务错误是否可恢复。
- Kafka 没有内置“按业务异常类型自动延迟重试”的语义。

Consumer 侧：

- 当前消息一直失败且不提交 offset，阻塞同一个 partition 后续消息。
- 无限重试打爆下游服务。
- 失败消息被直接跳过，造成业务丢失。
- 进入 DLQ 后没有告警和补偿流程，等于换了个地方沉默失败。

### Kafka 保证了什么

Kafka 允许你不提交 offset，从而让消息后续继续被消费；也允许你把失败消息发送到另一个 topic，再提交原 topic 的 offset。

Kafka 不会替你判断：

- 这个错误是不是可恢复。
- 应该等多久再重试。
- 重试多少次应该放弃。
- DLQ 后谁来处理。

### 业务还要补什么

常见重试方案有三种。

第一种：短暂内存重试。

```text
适合：网络抖动、短暂超时
方式：当前消费逻辑里 retry 3 次
成功：提交 offset
失败：进入 retry topic 或 DLQ
```

第二种：延迟重试 topic。

```text
原 topic: order-created
重试 topic: order-created-retry-1m
重试 topic: order-created-retry-5m
死信 topic: order-created-dlq
```

流程：

```text
消费失败
发送到 retry topic
发送成功后提交原消息 offset
稍后 retry topic 再被消费
多次失败后进入 DLQ
```

第三种：死信队列 DLQ。

```text
适合：脏数据、无法解析、业务状态不允许
方式：记录原消息、错误原因、失败时间、重试次数
后续人工或任务补偿
```

生产建议：

```text
可恢复错误：重试
不可恢复错误：DLQ
不确定错误：有限重试后 DLQ
```

推荐伪代码：

```go
msg := consume()

err := handleBusiness(msg)
if err == nil {
    commitOffset(msg)
    return
}

if isRetryable(err) {
    retryErr := sendToRetryTopic(msg, err)
    if retryErr == nil {
        commitOffset(msg)
        return
    }

    return // retry topic 写入失败，不提交 offset，等待原消息再次消费
}

dlqErr := sendToDLQ(msg, err)
if dlqErr == nil {
    commitOffset(msg)
    return
}

return // DLQ 写入失败，不提交 offset
```

关键细节：

```text
如果消息已经成功写入 retry topic 或 DLQ，就可以提交原 topic 的 offset。
```

否则原 topic 会一直卡在这条失败消息上。

## 5. offset 什么时候提交

提交 offset 的含义是：

```text
这条消息，以及它之前的消息，对当前 consumer group 来说已经处理完成。
```

所以核心原则是：

```text
业务处理成功后提交 offset。
```

### 风险发生在哪一段

Producer 侧：

- Producer 不直接决定 Consumer offset，但它可能重复发送消息，所以 offset 提交策略必须和幂等一起设计。

Broker 侧：

- Kafka 保存 consumer group 的已提交 offset。
- Kafka 不知道你的数据库事务、HTTP 调用、缓存写入是否真的成功。

Consumer 侧：

- 自动提交可能在业务处理完成前发生。
- 先提交 offset 再处理业务，业务失败会造成消息丢失。
- 处理成功但提交 offset 失败，会造成重复消费。
- 批量提交 offset 时，如果中间某条业务失败，可能错误提交后续位移。

### Kafka 保证了什么

Kafka 会从 consumer group 已提交的 offset 附近继续消费。提交 offset 后，Kafka 就认为这个 group 已经不需要再从旧位置正常消费。

不同顺序的后果：

```text
先提交 offset，再处理业务：
可能丢消息。

处理业务成功，再提交 offset：
可能重复，但不丢。生产最常用。

处理业务和 offset 放进同一个事务：
最强，但复杂，需要结合 Kafka transaction 或外部事务设计。
```

### 业务还要补什么

推荐默认模式：

```text
关闭自动提交
手动提交 offset
业务成功后提交
业务失败不要直接提交，除非已经转入 retry topic 或 DLQ
```

伪代码：

```go
msg := consume()

err := handleBusiness(msg)
if err == nil {
    commitOffset(msg)
    return
}

retryErr := sendToRetryTopic(msg, err)
if retryErr == nil {
    commitOffset(msg)
    return
}

return // 不提交 offset，等待再次消费
```

批量消费时要特别小心：

```text
offset 只能提交到连续成功处理的最大位置。
```

例如同一个 partition 拉到 offset 10、11、12：

```text
10 成功
11 失败
12 成功
```

此时不能直接提交 12，否则 11 会被跳过。要么卡住并重试 11，要么把 11 成功写入 retry topic 或 DLQ 后，再提交到 12。

## 6. 消费者扩容会发生什么

Kafka 的消费扩容单位是 partition，不是 consumer。

```text
一个 partition 同一时间只能被一个 consumer group 内的一个 consumer 消费。
一个 consumer 可以消费多个 partition。
consumer 数量超过 partition 数，多出来的 consumer 会空闲。
```

### 风险发生在哪一段

Producer 侧：

- partition 数决定了同一个 consumer group 的最大并行消费能力。
- 如果 key 分布不均，扩容 consumer 也可能解决不了热点 partition。

Broker 侧：

- 新 consumer 加入、旧 consumer 离开、心跳超时、订阅 topic 变化，都可能触发 rebalance。
- Broker 协调 consumer group 的成员和 partition 分配。

Consumer 侧：

- rebalance 时部分 consumer 会暂停消费。
- partition 所属权转移后，新 consumer 从已提交 offset 继续消费。
- 正在处理但尚未提交 offset 的消息，可能被新 consumer 再次消费。
- 如果旧 consumer 在处理完成前提交了 offset，可能丢消息。

### Kafka 保证了什么

假设 topic 有 3 个 partition：

```text
1 个 consumer：消费 3 个 partition
2 个 consumer：一个消费 2 个，一个消费 1 个
3 个 consumer：每个消费 1 个
5 个 consumer：3 个工作，2 个空闲
```

扩容时会发生：

```text
新 consumer 加入 group
Kafka 重新分配 partition
部分 consumer 暂停消费
partition 所属权转移
新 consumer 从已提交 offset 继续消费
```

### 业务还要补什么

rebalance 期间的风险：

```text
消费短暂停顿
正在处理的消息可能被重复消费
如果 offset 提交不当，可能丢失或重复
```

所以 consumer 应该做到：

- 无状态，或状态可从外部存储恢复。
- 可重复消费。
- 处理完成后再提交 offset。
- 优雅关闭时尽量提交已处理 offset。
- 单条处理时间不要超过 consumer group 的心跳和 poll 约束。
- 扩容前确认 partition 数足够。
- 监控 rebalance 次数、consumer lag、单 partition lag。

扩容判断：

```text
如果 consumer 数 < partition 数，增加 consumer 可能提升吞吐。
如果 consumer 数 >= partition 数，继续增加 consumer 通常不会提升吞吐。
如果只有少数 partition lag 高，优先排查热点 key。
```

## 总结表

| 问题 | Kafka 的真实情况 | 后端开发该怎么做 |
| --- | --- | --- |
| 会不会丢 | 配置不当或提交 offset 过早会丢 | `acks=all`，多副本，处理成功后提交 |
| 会不会重复 | 会，尤其是重试、宕机和 rebalance | 消费端幂等 |
| 顺序怎么保证 | 只保证 partition 内有序 | 同一业务 key 进同一 partition |
| 失败怎么重试 | Kafka 不自动理解业务重试 | 内存短重试 + retry topic + DLQ |
| offset 何时提交 | 提交代表“这条我处理完了” | 业务成功后提交，转入 retry/DLQ 后可提交原 offset |
| 扩容发生什么 | partition 重新分配，触发 rebalance | 控制 partition 数，消费端无状态和幂等 |

## 生产配置参考

关键 Producer 配置：

```properties
acks=all
enable.idempotence=true
retries>0
max.in.flight.requests.per.connection<=5
delivery.timeout.ms=120000
request.timeout.ms=30000
```

关键 Broker/topic 配置：

```properties
replication.factor>=3
min.insync.replicas>=2
unclean.leader.election.enable=false
retention.ms 按业务可重放窗口设置
```

关键 Consumer 策略：

```text
关闭自动提交
业务成功后手动提交
消费端幂等
失败进入 retry topic 或 DLQ 后再提交原 offset
监控 lag、rebalance、处理耗时、DLQ 数量
```

## 本项目建议练习

可以用代码亲手模拟这 6 个场景：

| 场景 | 建议练习方式 | 观察重点 |
| --- | --- | --- |
| 丢消息 | 先提交 offset 再故意让业务失败 | Kafka 不再重放这条消息 |
| 重复消费 | 业务写入成功后，在提交 offset 前退出进程 | 重启后同一消息再次被消费 |
| 乱序 | 不设置 key 或让同一业务 ID 进入不同 partition | 同一业务实体状态顺序错乱 |
| 失败重试 | 消费失败后写入 retry topic 或 DLQ | 原 topic 不被坏消息永久阻塞 |
| offset 早提交 | 开启自动提交并让处理逻辑慢于提交间隔 | offset 已前进但业务未完成 |
| consumer 扩容 | 启动多个同 group consumer | 观察 partition 分配和 rebalance |

项目中已有相关基础任务：

- 六大问题故障模拟：`tasks/07-production-scenarios/six-scenarios`
- 手动提交 offset：`tasks/03-consumer/manual-commit`
- 至少一次消费：`tasks/04-delivery-semantics/at-least-once`
- 至多一次消费：`tasks/04-delivery-semantics/at-most-once`
- 幂等生产者：`tasks/04-delivery-semantics/idempotent-producer`
- 失败消息转 DLQ：`tasks/05-rate-limit-backpressure/dlq-retry`

建议每个练习都记录三类结果：

- 生产者是否收到成功 ack 或错误回调。
- Broker 中 topic、partition、offset 和 consumer lag 如何变化。
- 消费端业务结果是否丢失、重复、乱序或进入补偿流程。
