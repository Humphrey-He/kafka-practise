# kafka-practise

这是一个用 Go + Sarama 练习 Kafka 日常开发任务的项目。

项目目标不是只堆示例代码，而是把真实工作里常见的 Kafka 操作拆成一组小模块：每个任务一个文件夹，任务强相关的子任务可以建立子文件夹。每个任务都应包含可运行代码、配置示例、说明文档、验证步骤和常见问题排查。

详细规划见：[docs/开发规划.md](docs/开发规划.md)。

配套文档：

- [任务 README 模板](docs/任务README模板.md)：后续每个任务文件夹可直接套用。
- [配置映射](docs/配置映射.md)：Java Kafka Client 常见配置与 Sarama 配置字段对照。
- [排障手册](docs/排障手册.md)：按常见故障现象组织排查路径。
- [术语表](docs/术语表.md)：Kafka 与 Sarama 高频术语速查。
- [运行手册](docs/运行手册.md)：本地环境、验证命令和测试建议。

## 第一阶段

当前已落地第一阶段基础骨架：

- Go module：`github.com/Humphrey-He/kafka-practise`
- 公共配置加载：`internal/common/config`
- Sarama 配置构造：`internal/common/kafka`
- 结构化日志：`internal/common/logging`
- 单节点 Kafka Compose：`deployments/docker-compose.single.yml`
- 主题管理任务：`tasks/01-topic-management/create-topic`
- 同步生产者任务：`tasks/02-producer/sync-producer`
- Consumer Group 基础任务：`tasks/03-consumer/consumer-group-basic`

启动本地 Kafka：

```powershell
docker compose -f deployments/docker-compose.single.yml up -d
```

创建 topic：

```powershell
go run ./tasks/01-topic-management/create-topic --topic kp_topic_demo --partitions 3 --replication-factor 1
```

发送消息：

```powershell
go run ./tasks/02-producer/sync-producer --topic kp_topic_demo --key user-1 --value "hello kafka"
```

消费消息：

```powershell
go run ./tasks/03-consumer/consumer-group-basic --topic kp_topic_demo --group kp_consumer_basic_group
```

## 第二阶段

当前已落地消费语义与可靠性基础示例：

- 手动提交 offset：`tasks/03-consumer/manual-commit`
- 至少一次消费：`tasks/04-delivery-semantics/at-least-once`
- 至多一次消费：`tasks/04-delivery-semantics/at-most-once`
- 幂等生产者：`tasks/04-delivery-semantics/idempotent-producer`
- 失败消息转 DLQ：`tasks/05-rate-limit-backpressure/dlq-retry`
- 三 broker 本地集群：`deployments/docker-compose.cluster.yml`

启动三 broker 集群：

```powershell
docker compose -f deployments/docker-compose.cluster.yml up -d
```

手动提交消费：

```powershell
go run ./tasks/03-consumer/manual-commit --topic kp_topic_demo --group kp_manual_commit_group
```

幂等生产：

```powershell
go run ./tasks/04-delivery-semantics/idempotent-producer --config configs/idempotent.yaml --topic kp_topic_demo --key order-1 --value "paid"
```

DLQ 示例：

```powershell
go run ./tasks/05-rate-limit-backpressure/dlq-retry --topic kp_source_demo --dlq-topic kp_source_demo_dlq --fail-contains fail
```
