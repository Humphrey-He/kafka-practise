# metadata-health

检查 topic 元数据健康，包括 leader、replicas、ISR 和分区错误。

## 运行

检查指定 topic：

```powershell
go run ./tasks/10-monitoring-ops/metadata-health --topic kp_topic_demo
```

检查全部 topic：

```powershell
go run ./tasks/10-monitoring-ops/metadata-health --all
```

