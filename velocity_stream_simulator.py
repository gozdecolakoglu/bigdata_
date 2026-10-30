#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyrefly: ignore-errors
"""
5V - Velocity (Hız) Gerçek Zamanlı Akış (Streaming) Simülatörü
Spark Structured Streaming, Apache Kafka veya Flink ile erken risk tespiti yapmak için
canlı Jira Task durum değişimi, Git PR push, CI/CD pipeline ve SonarQube olayları üretir.
"""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone


EVENT_TYPES = [
    "TASK_STATUS_CHANGED",
    "TASK_SCOPE_UPDATED",
    "TASK_BLOCKER_FLAGGED",
    "PR_OPENED",
    "PR_COMMIT_PUSHED",
    "PR_REVIEW_SUBMITTED",
    "CI_PIPELINE_COMPLETED",
    "SECURITY_SCAN_ALERT",
    "MERGE_CONFLICT_DETECTED",
]

PROJECTS = ["PRJ-FIN", "PRJ-ECOM", "PRJ-AI", "PRJ-CLOUD", "PRJ-CRM", "PRJ-HEALTH", "PRJ-LOG", "PRJ-SEC"]
DEVS = ["ahmet.yilmaz", "zeynep.kaya", "mehmet.demir", "elif.celik", "caner.ozturk", "selin.aydin", "burak.arslan"]
COMPONENTS = ["Backend", "Frontend", "API Gateway", "Database", "DevOps & CI/CD", "Payment Engine", "AI/ML Pipeline"]


def create_stream_event(seq_id: int) -> dict:
    ev_type = random.choice(EVENT_TYPES)
    proj = random.choice(PROJECTS)
    task_id = f"TASK-{random.randint(100001, 180000)}"
    pr_id = f"PR-{random.randint(200001, 355000)}"
    actor = random.choice(DEVS)
    comp = random.choice(COMPONENTS)

    instant_risk_delta = round(random.uniform(-5.0, 22.5), 2)
    if ev_type in ("TASK_BLOCKER_FLAGGED", "SECURITY_SCAN_ALERT", "MERGE_CONFLICT_DETECTED"):
        instant_risk_delta = round(random.uniform(12.0, 35.0), 2)

    return {
        "event_id": f"EVT-{seq_id:08d}",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": ev_type,
        "project_code": proj,
        "component": comp,
        "task_id": task_id,
        "pr_id": pr_id if "PR_" in ev_type or "CI_" in ev_type or "MERGE" in ev_type or "SECURITY" in ev_type else None,
        "actor": actor,
        "payload": {
            "changed_lines": random.randint(5, 950) if "PR_" in ev_type else 0,
            "ci_status": random.choice(["SUCCESS", "FAILED", "RETRYING"]) if "CI_" in ev_type else "N/A",
            "instant_risk_delta": instant_risk_delta,
            "xai_alert_reason": (
                "Kritik darboğaz veya entegrasyon çakışması tespit edildi"
                if instant_risk_delta > 15.0
                else "Normal geliştirme aktivitesi"
            ),
        },
    }


def main():
    parser = argparse.ArgumentParser(description="5V Velocity Canlı Veri Akışı Simülatörü")
    parser.add_argument("--events", type=int, default=10000, help="Üretilecek toplam olay sayısı")
    parser.add_argument("--eps", type=float, default=0.0, help="Saniyedeki olay hızı (0 = maksimum hızda toplu üretim)")
    parser.add_argument("--output", type=str, default="data/velocity_event_stream.jsonl", help="Akış çıktı dosyası")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    delay = (1.0 / args.eps) if args.eps > 0 else 0.0

    print(f"[Velocity Stream] {args.events:,} adet gerçek zamanlı akış olayı '{args.output}' dosyasına yazılıyor...")
    with open(args.output, "w", encoding="utf-8") as f:
        for i in range(1, args.events + 1):
            ev = create_stream_event(i)
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            if delay > 0:
                f.flush()
                time.sleep(delay)
    print(f"[Velocity Stream] Tamamlandı: {args.output}")


if __name__ == "__main__":
    main()

