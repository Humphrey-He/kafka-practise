# 任务 README 模板

> 每个 `tasks/<编号>-<任务名>/README.md` 都建议使用这个结构。目标是让一个任务文件夹既能当练习代码入口，也能当生产问题复盘文档。

## 任务名称

用一句话说明任务目标，例如：使用 Sarama Consumer Group 实现手动 offset 提交。

## 场景

说明这个任务解决什么实际问题。

示例：

- 订单服务消费支付事件，只有写入数据库成功后才提交 offset。
- 日志采集服务需要高吞吐生产消息，并允许少量延迟换取批量能力。
- 本地需要复现 consumer lag 持续升高的问题。

## 学习目标

- Kafka 概念：列出 topic、partition、offset、ISR、consumer group 等相关概念。
- Sarama API：列出本任务涉及的 API，例如 `NewConsumerGroup`、`ClusterAdmin`、`SyncProducer`。
- 工程能力：列出需要掌握的错误处理、重试、限流、指标、优雅关闭等能力。

## 运行前置条件

- Kafka broker 地址。
- 需要提前创建的 topic。
- 是否需要 SASL/SSL。
- 是否需要多 broker 集群。
- 是否依赖数据库、Redis、Prometheus、Grafana 等外部组件。

## 目录结构

```text
当前任务/
  README.md
  main.go
  config.example.yaml
  testdata/
  docs/
```

如果任务有强关联子任务，可以这样组织：

```text
03-consumer/
  README.md
  consumer-group-basic/
  manual-commit/
  auto-commit/
  offset-reset/
  read-committed/
```

## 关键配置

| 配置 | 示例值 | 影响 | 风险 |
| --- | --- | --- | --- |
| `ClientID` | `consumer-manual-commit-demo` | 方便日志、审计、配额识别 | 随机 client id 会增加排查难度 |
| `Consumer.Offsets.AutoCommit.Enable` | `false` | 手动控制 offset 提交 | 忘记提交会导致重复消费 |
| `Consumer.Offsets.Initial` | `sarama.OffsetOldest` | 无提交位移时从最早消息消费 | 可能消费大量历史数据 |

## 实现思路

建议写清楚：

- 初始化配置。
- 创建 Sarama client/producer/consumer/admin。
- 核心处理流程。
- 错误分类与重试策略。
- offset 或事务处理策略。
- 退出时如何关闭连接、flush 数据和释放资源。

## 运行方式

```powershell
go run ./tasks/<编号>-<任务名>
```

或：

```powershell
go run ./cmd/<命令名> --config ./configs/local.yaml
```

## 预期现象

- 控制台日志里应出现什么。
- Kafka CLI 能看到什么。
- 指标面板或输出中应有什么变化。
- 失败场景下应出现什么错误。

## 验证方式

建议至少提供一种自动验证和一种人工验证。

自动验证：

```powershell
go test ./...
```

人工验证：

```powershell
kafka-console-consumer --bootstrap-server localhost:9092 --topic demo-topic --from-beginning
```

## 常见问题

每个任务至少沉淀 3 个问题：

| 现象 | 可能原因 | 排查方式 | 解决方案 |
| --- | --- | --- | --- |
| 消费不到消息 | group offset 已经在末尾 | describe group 查看 committed offset | 换 group 或 reset offset |
| 重复消费 | 处理成功但提交 offset 前进程退出 | 查看日志和提交时机 | 处理成功后提交，并保证业务幂等 |
| 发送阻塞 | async producer 未消费 Errors/Successes | 查看 goroutine 和 channel | 持续消费回执 channel |

## 生产注意事项

- 当前 demo 与生产用法的差异。
- 哪些配置不能直接照搬。
- 哪些行为需要压测或灰度验证。
- 哪些错误必须接入告警。

## 扩展练习

- 增加结构化日志。
- 增加 Prometheus 指标。
- 增加集成测试。
- 增加故障注入，例如 broker 下线、下游超时、网络抖动。

