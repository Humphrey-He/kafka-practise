# lag-calculator

计算 consumer group 在指定 topic 上的 lag。

## 运行

```powershell
go run ./tasks/06-lag-rebalance/lag-calculator --topic kp_topic_demo --group kp_consumer_basic_group
```

## 计算公式

```text
lag = log end offset - committed offset
```

如果 group 在某个 partition 上没有 committed offset，示例会把 committed offset 视为 `-1` 并在输出里标记。

