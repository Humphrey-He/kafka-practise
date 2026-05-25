# 01-topic-management

本任务练习使用 Sarama `ClusterAdmin` 管理 topic，覆盖创建、查看、删除和基础 topic 配置。

## 子任务

- `create-topic`：按参数创建、查看或删除 topic。

## 运行前置条件

先启动本地 Kafka：

```powershell
docker compose -f deployments/docker-compose.single.yml up -d
```

默认配置文件：

```text
configs/local.yaml
```

## 创建 topic

```powershell
go run ./tasks/01-topic-management/create-topic --topic kp_topic_demo --partitions 3 --replication-factor 1
```

## 查看 topic

```powershell
go run ./tasks/01-topic-management/create-topic --action describe --topic kp_topic_demo
```

## 删除 topic

```powershell
go run ./tasks/01-topic-management/create-topic --action delete --topic kp_topic_demo
```

## 关键配置

- `retention.ms`：按时间保留消息。
- `cleanup.policy`：`delete` 表示按保留策略删除，`compact` 表示按 key 压缩日志。
- `partitions`：分区数决定 consumer group 最大并行度。
- `replication.factor`：副本因子，单节点本地环境只能使用 `1`。

## 常见问题

| 现象 | 原因 | 解决 |
| --- | --- | --- |
| 创建 topic 失败 | Kafka 未启动或 broker 地址不对 | 检查 `configs/local.yaml` 和 Compose 端口 |
| 副本因子报错 | 单节点环境不能创建多副本 topic | 本地单节点使用 `--replication-factor 1` |
| 删除后仍能看到 topic | broker 删除是异步过程 | 等待几秒后重新 describe |

