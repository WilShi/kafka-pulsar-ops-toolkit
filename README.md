# Kafka & Pulsar Ops Toolkit 🛠️

> 消息中间件运维的实用工具箱——来自一线 SRE 的日常：巡检、积压分析、容量盘点的脚本化沉淀。

作者在券商负责过 Kafka/Pulsar 的全生命周期（管控能力开发、运维、优化、自动化巡检平台）。这个仓库把其中最通用、最常写的几类工具脚本化、开箱化。

## 工具清单

| 脚本 | 干什么 |
|------|--------|
| `scripts/topic_lag_report.py` | 消费组积压巡检：扫描所有 consumer group 的 lag，超阈值告警，输出巡检报告 |

## 快速开始

```bash
pip install confluent-kafka
python scripts/topic_lag_report.py --bootstrap localhost:9092 --threshold 10000
```

示例输出：

```
📊 Kafka 消费积压巡检报告
==================================================
⚠️  order-service       topic=trade-events      partition=2   lag=52,331  🔴 超阈值
✅  risk-engine         topic=trade-events      partition=0   lag=12
...
--------------------------------------------------
共扫描 34 个消费组，2 个超阈值，详见上表。
```

## 设计原则

- **无侵入**：只读元数据和 offset，不写任何数据
- **可巡检化**：退出码区分正常/告警（0/1），可直接接入定时巡检平台或 CI
- **输出即报告**：文本报告可直接贴进值班群或归档

## 路线图

- [ ] Pulsar 版本（pulsar-admin 包装）
- [ ] 分区倾斜度分析（broker 间数据均衡）
- [ ] 磁盘增长速率预估（按历史写入速率外推打满时间）

## License

MIT
