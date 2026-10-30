#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyrefly: ignore-errors
"""
Vercel Python Serverless API (FastAPI)
Teslimat Risklerinin Erken Tespiti ve Açıklanabilir Karar Destek Sistemi (XAI)
"""

import json
import math
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI  # type: ignore
from fastapi.middleware.cors import CORSMiddleware  # type: ignore
from fastapi.responses import FileResponse, JSONResponse  # type: ignore
from fastapi.staticfiles import StaticFiles  # type: ignore
from pydantic import BaseModel  # type: ignore

app = FastAPI(
    title="Erken Risk Tespiti & XAI Karar Destek API",
    version="1.0.0",
    description="Vercel Serverless Uyumlu Görev (Task) & Pull Request (PR) Erken Risk ve XAI Motoru",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PUBLIC_DIR = os.path.join(BASE_DIR, "public")


class WhatIfRequest(BaseModel):
    story_points: int = 8
    estimated_effort_hours: float = 32.0
    priority: str = "High"
    environment: str = "Production"
    blocker_count: int = 1
    dependency_count: int = 1
    requirement_change_count: int = 1
    reopen_count: int = 0
    pr_changed_files: int = 12
    pr_lines_added: int = 350
    pr_has_conflict: bool = True
    pr_ci_failures: int = 1


@app.get("/api/health")
def health_check() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "xai-early-risk-decision-support",
        "platform": "vercel-serverless",
    }


@app.post("/api/what-if")
def simulate_what_if(req: WhatIfRequest) -> Dict[str, Any]:
    """Karşı-olgusal (Counterfactual / What-If) XAI risk ve gecikme simülasyonu."""
    opt_ratio = max(0.5, min(3.0, (req.story_points * 6.5) / (req.estimated_effort_hours + 1.0)))
    p_w = 14.0 if req.priority == "Critical" else (8.0 if req.priority == "High" else 2.0)
    e_w = 10.0 if req.environment == "Production" else (5.0 if req.environment == "Staging" else 1.0)

    task_risk = max(
        2.0,
        min(
            99.5,
            req.dependency_count * 9.5
            + req.blocker_count * 16.0
            + req.requirement_change_count * 11.5
            + req.reopen_count * 12.0
            + (14.0 if req.story_points >= 13 else 0.0)
            + (13.0 if opt_ratio > 1.25 else 0.0)
            + p_w
            + e_w
            + 8.0,
        ),
    )

    pr_risk = max(
        1.0,
        min(
            99.5,
            req.pr_changed_files * 1.45
            + req.pr_lines_added / 45.0
            + (18.0 if req.pr_has_conflict else 0.0)
            + req.pr_ci_failures * 8.5
            + task_risk * 0.22
            + 6.0,
        ),
    )

    combined_risk = round(task_risk * 0.62 + pr_risk * 0.38, 1)
    delay_prob = round((1.0 / (1.0 + math.exp(-(combined_risk - 47.0) / 11.5))) * 100.0, 1)
    est_delay_days = (
        max(0, int(round((combined_risk - 42.0) * 0.32 + req.blocker_count * 2.5)))
        if combined_risk >= 45.0
        else 0
    )
    risk_level = (
        "Critical"
        if combined_risk >= 75.0
        else ("High" if combined_risk >= 50.0 else ("Medium" if combined_risk >= 28.0 else "Low"))
    )

    prescriptions: List[str] = []
    if req.blocker_count > 0:
        prescriptions.append(
            f"{req.blocker_count} adet Blocker kaldırılırsa bileşik risk skoru doğrudan -{req.blocker_count * 10:.1f} puan düşer."
        )
    if req.story_points >= 13 or opt_ratio > 1.25:
        prescriptions.append(
            f"Görev karmaşıklığı ({req.story_points} SP) veya iyimser efor oranı ({opt_ratio:.2f}) yüksek. Görevi 5 SP'lik alt görevlere bölmek riski ~14 puan azaltır."
        )
    if req.pr_has_conflict or req.pr_ci_failures > 0 or req.pr_changed_files >= 18:
        prescriptions.append(
            "PR dosya sayısını 10'un altına çekmek, hedef branch ile günlük rebase yapmak ve yerel pre-commit testlerini zorunlu kılmak PR riskini %35 iyileştirir."
        )
    if not prescriptions:
        prescriptions.append(
            "Mevcut parametrelerle görev düşük risk bölgesindedir ve zamanında teslimat beklenmektedir."
        )

    return {
        "task_risk_score": round(task_risk, 1),
        "pr_risk_score": round(pr_risk, 1),
        "combined_early_risk_score": combined_risk,
        "early_delay_probability_pct": delay_prob,
        "estimated_delay_days": est_delay_days,
        "risk_level": risk_level,
        "prescriptions": prescriptions,
    }


# Yerel testlerde static dosyaları da sunması için:
if os.path.exists(PUBLIC_DIR):
    @app.get("/")
    def serve_index():
        return FileResponse(os.path.join(PUBLIC_DIR, "index.html"))

    app.mount("/", StaticFiles(directory=PUBLIC_DIR, html=True), name="public")

