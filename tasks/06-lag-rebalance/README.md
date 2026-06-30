# 06-lag-rebalance

本目录用于练习 consumer lag 计算、rebalance 观察和分区分配诊断。

## 当前子任务

- `lag-calculator`：读取 consumer group committed offset 与 topic log end offset，计算每个 partition 的 lag。

