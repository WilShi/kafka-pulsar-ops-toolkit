#!/usr/bin/env python3
"""Kafka 消费组积压巡检脚本（topic_lag_report）。

扫描所有消费组在各 topic/partition 上的 lag，超阈值告警，输出巡检报告。
退出码：0 = 全部正常，1 = 存在超阈值消费组（可直接接巡检平台/CI）。

依赖：pip install confluent-kafka
用法：python topic_lag_report.py --bootstrap localhost:9092 [--threshold 10000]
"""
import argparse
import sys
from collections import defaultdict

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient


def get_lags(bootstrap: str) -> dict:
    """返回 {(group, topic, partition): lag}"""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    groups = admin.list_consumer_groups()
    result = {}
    for g in groups.result().valid:
        group_id = g.group_id
        try:
            offsets = admin.list_consumer_group_offsets([group_id])
            committed = offsets[group_id].result().topic_partitions
        except KafkaException:
            continue
        if not committed:
            continue
        # 用一次性 consumer 拿高水位
        c = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": f"lag-inspector-{group_id}",
            "enable.auto.commit": False,
        })
        try:
            for tp in committed:
                if tp.offset < 0:
                    continue
                _lo, hi = c.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition), timeout=5
                )
                lag = hi - tp.offset
                result[(group_id, tp.topic, tp.partition)] = lag
        finally:
            c.close()
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description="Kafka 消费组积压巡检")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--threshold", type=int, default=10000, help="lag 告警阈值")
    args = ap.parse_args()

    print("📊 Kafka 消费积压巡检报告")
    print("=" * 50)

    lags = get_lags(args.bootstrap)
    if not lags:
        print("未发现任何有提交位点的消费组。")
        return 0

    by_group = defaultdict(list)
    for (group, topic, part), lag in lags.items():
        by_group[group].append((topic, part, lag))

    violated = 0
    for group in sorted(by_group):
        for topic, part, lag in sorted(by_group[group]):
            mark = "🔴 超阈值" if lag > args.threshold else ""
            icon = "⚠️ " if lag > args.threshold else "✅"
            if lag > args.threshold:
                violated += 1
            print(f"{icon} {group:<24} topic={topic:<24} partition={part:<3} lag={lag:,} {mark}")

    print("-" * 50)
    print(f"共扫描 {len(by_group)} 个消费组，{violated} 个超阈值（>{args.threshold:,}）。")
    return 1 if violated else 0


if __name__ == "__main__":
    sys.exit(main())
