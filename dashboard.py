#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyrefly: ignore-errors
"""
Yazılım Projelerinde Teslimat Risklerinin Büyük Veri (5V) ile Erken Tespiti ve
Açıklanabilir Karar Destek Sistemi (XAI) Dashboard'u
"""

import io
import json
import os
import time
from datetime import datetime
from typing import Dict, Optional, Tuple

import altair as alt  # type: ignore # pyrefly: ignore[missing-import]
import numpy as np  # type: ignore # pyrefly: ignore[missing-import]
import pandas as pd  # type: ignore # pyrefly: ignore[missing-import]
import streamlit as st  # type: ignore # pyrefly: ignore[missing-import]
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier  # type: ignore # pyrefly: ignore[missing-import]
from sklearn.metrics import (  # type: ignore # pyrefly: ignore[missing-import]
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split  # type: ignore # pyrefly: ignore[missing-import]

# ---------------------------------------------------------------------------
# SAYFA YAPILANDIRMASI VE ÖZEL STİL (CSS)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Erken Risk Tespiti & XAI Karar Destek Sistemi",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
    .main-header {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-left: 5px solid #3b82f6;
        padding: 1.2rem 1.5rem;
        border-radius: 0.75rem;
        margin-bottom: 1.2rem;
    }
    .main-header h1 {
        margin: 0;
        font-size: 1.65rem;
        color: #f8fafc;
        font-weight: 700;
    }
    .main-header p {
        margin: 0.35rem 0 0 0;
        color: #94a3b8;
        font-size: 0.95rem;
    }
    .kpi-card {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 0.75rem;
        padding: 1rem 1.15rem;
        height: 100%;
    }
    .kpi-title {
        color: #94a3b8;
        font-size: 0.82rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .kpi-value {
        color: #f8fafc;
        font-size: 1.65rem;
        font-weight: 700;
        margin: 0.25rem 0;
    }
    .kpi-sub {
        font-size: 0.8rem;
        color: #cbd5e1;
    }
    .xai-box {
        background: rgba(30, 41, 59, 0.85);
        border: 1px solid #475569;
        border-left: 4px solid #f59e0b;
        border-radius: 0.6rem;
        padding: 1rem 1.2rem;
        margin-bottom: 0.8rem;
    }
    .action-box {
        background: rgba(15, 118, 110, 0.18);
        border: 1px solid #0d9488;
        border-left: 4px solid #14b8a6;
        border-radius: 0.6rem;
        padding: 1rem 1.2rem;
        margin-bottom: 0.8rem;
    }
    .badge-critical {
        background: rgba(239, 68, 68, 0.2);
        color: #fca5a5;
        border: 1px solid #ef4444;
        padding: 0.15rem 0.55rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.78rem;
    }
    .badge-high {
        background: rgba(249, 115, 22, 0.2);
        color: #fdba74;
        border: 1px solid #f97316;
        padding: 0.15rem 0.55rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.78rem;
    }
    .badge-medium {
        background: rgba(234, 179, 8, 0.2);
        color: #fde047;
        border: 1px solid #eab308;
        padding: 0.15rem 0.55rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.78rem;
    }
    .badge-low {
        background: rgba(34, 197, 94, 0.2);
        color: #86efac;
        border: 1px solid #22c55e;
        padding: 0.15rem 0.55rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.78rem;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

RISK_COLORS = {
    "Critical": "#ef4444",
    "High": "#f97316",
    "Medium": "#eab308",
    "Low": "#22c55e",
}
RISK_ORDER = ["Critical", "High", "Medium", "Low"]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, "data")



# ---------------------------------------------------------------------------
# VERİ OKUMA, DOĞRULAMA VE OTOMATİK XAI / ERKEN RİSK HESAPLAMA MOTORU
# ---------------------------------------------------------------------------
def parse_uploaded_or_path(source, file_name: str = "") -> pd.DataFrame:
    """Parquet, CSV veya JSONL formatındaki dosya veya dosya yolunu DataFrame olarak yükler."""
    if isinstance(source, str):
        ext = os.path.splitext(source)[1].lower()
        if ext == ".parquet":
            return pd.read_parquet(source)
        elif ext == ".csv":
            return pd.read_csv(source, low_memory=False)
        elif ext in (".jsonl", ".json"):
            return pd.read_json(source, lines=True)
        else:
            raise ValueError(f"Desteklenmeyen dosya uzantısı: {ext}")
    else:
        ext = os.path.splitext(file_name or getattr(source, "name", ""))[1].lower()
        raw_bytes = source.read()
        source.seek(0)
        buf = io.BytesIO(raw_bytes)
        if ext == ".parquet":
            return pd.read_parquet(buf)
        elif ext == ".csv":
            return pd.read_csv(buf, low_memory=False)
        elif ext in (".jsonl", ".json"):
            return pd.read_json(buf, lines=True)
        else:
            raise ValueError(f"Desteklenmeyen dosya formatı: {ext}. Lütfen .parquet, .csv veya .jsonl yükleyin.")


def ensure_task_risk_and_xai_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yüklenen Task veri setinde risk veya XAI sütunları eksikse ham görev özniteliklerinden
    otomatik olarak erken teslimat riski ve açıklanabilir karar destek önerilerini üretir.
    """
    df = df.copy()
    n = len(df)
    if n == 0:
        return df

    if "story_points" not in df.columns:
        df["story_points"] = 5
    if "estimated_effort_hours" not in df.columns:
        df["estimated_effort_hours"] = df["story_points"] * 6.0
    if "blocker_count" not in df.columns:
        df["blocker_count"] = 0
    if "requirement_change_count" not in df.columns:
        df["requirement_change_count"] = 0
    if "reopen_count" not in df.columns:
        df["reopen_count"] = 0
    if "priority" not in df.columns:
        df["priority"] = "Medium"
    if "environment" not in df.columns:
        df["environment"] = "Development"
    if "status" not in df.columns:
        df["status"] = "In Progress"
    if "dependencies" not in df.columns:
        df["dependencies"] = "Yok"

    # Bağımlılık sayısını çıkar
    dep_str = df["dependencies"].fillna("Yok").astype(str)
    dep_counts = np.where(
        (dep_str == "Yok") | (dep_str == "") | (dep_str == "None"),
        0,
        dep_str.str.count(",") + 1,
    )
    df["dependency_count_calc"] = dep_counts

    est_filled = df["estimated_effort_hours"].fillna(df["story_points"] * 6.0)
    optimism_ratio = np.clip((df["story_points"] * 6.5) / (est_filled + 1.0), 0.5, 3.0)
    df["optimism_ratio_calc"] = np.round(optimism_ratio, 2)

    if "delivery_risk_score" not in df.columns:
        p_weight = np.where(df["priority"] == "Critical", 14.0, np.where(df["priority"] == "High", 8.0, 2.0))
        e_weight = np.where(df["environment"] == "Production", 10.0, np.where(df["environment"] == "Staging", 5.0, 1.0))
        raw_risk = (
            dep_counts * 9.5
            + df["blocker_count"].fillna(0) * 16.0
            + df["requirement_change_count"].fillna(0) * 11.5
            + df["reopen_count"].fillna(0) * 12.0
            + (df["story_points"].fillna(3) >= 13) * 14.0
            + (optimism_ratio > 1.25) * 13.0
            + p_weight
            + e_weight
            + 8.0
        )
        df["delivery_risk_score"] = np.round(np.clip(raw_risk, 2.0, 99.5), 2)

    if "delivery_risk_level" not in df.columns:
        s = df["delivery_risk_score"]
        df["delivery_risk_level"] = np.where(
            s >= 75.0, "Critical", np.where(s >= 50.0, "High", np.where(s >= 28.0, "Medium", "Low"))
        )

    if "delay_days" not in df.columns:
        df["delay_days"] = np.where(
            df["delivery_risk_score"] >= 48.0,
            np.round((df["delivery_risk_score"] - 42.0) * 0.32 + df["blocker_count"].fillna(0) * 2.5).astype(int),
            0,
        )

    if "is_delayed" not in df.columns:
        df["is_delayed"] = (df["delay_days"] > 0).astype(int)

    if "xai_primary_risk_factors" not in df.columns or "xai_recommended_actions" not in df.columns:
        df["xai_primary_risk_factors"] = np.where(
            df["blocker_count"] > 0,
            "Blocker/Engel Sayısı (+%28 risk) | Yüksek Karmaşıklık",
            np.where(
                df["requirement_change_count"] > 0,
                "Kapsam Değişikliği / Scope Creep (+%21 risk)",
                "Standart İş Akışı (Düşük Sapma Riski)",
            ),
        )
        df["xai_recommended_actions"] = np.where(
            df["blocker_count"] > 0,
            "Blokaj yaratan bağımlılıkları eskalasyon ile çözün; görevi alt parçalara bölün.",
            "Mevcut sprint planını koruyun ve PR inceleme hızını takip edin.",
        )

    if "data_quality_flag" not in df.columns:
        df["data_quality_flag"] = np.where(df["estimated_effort_hours"].isna(), "Missing_Estimate", "Clean")

    return df


def ensure_pr_risk_and_xai_columns(df_prs: pd.DataFrame) -> pd.DataFrame:
    """
    Yüklenen PR veri setinde risk veya XAI sütunları eksikse ham PR/CI-CD metriklerinden
    otomatik olarak entegrasyon riski ve karar destek önerilerini üretir.
    """
    df_prs = df_prs.copy()
    if len(df_prs) == 0:
        return df_prs

    for col, default in [
        ("changed_files_count", 4),
        ("lines_added", 120),
        ("lines_deleted", 40),
        ("commit_count", 3),
        ("review_iteration_count", 1),
        ("time_to_first_review_hours", 4.0),
        ("time_to_merge_hours", 16.0),
        ("ci_failure_count", 0),
        ("merge_conflicts", "Çakışma Yok (Clean Merge)"),
        ("pipeline_status", "Succeeded"),
        ("status", "Open"),
    ]:
        if col not in df_prs.columns:
            df_prs[col] = default

    has_conflict = ~df_prs["merge_conflicts"].fillna("").astype(str).str.contains("Yok|Clean", case=False, regex=True)
    df_prs["has_conflict_bool"] = has_conflict

    if "pr_risk_score" not in df_prs.columns:
        raw_pr_risk = (
            df_prs["changed_files_count"] * 1.45
            + df_prs["lines_added"] / 45.0
            + has_conflict * 18.0
            + df_prs["ci_failure_count"] * 8.5
            + df_prs["review_iteration_count"] * 4.2
            + (df_prs["pipeline_status"] == "Failed") * 22.0
            + 12.0
        )
        df_prs["pr_risk_score"] = np.round(np.clip(raw_pr_risk, 1.0, 99.5), 2)

    if "pr_risk_level" not in df_prs.columns:
        s = df_prs["pr_risk_score"]
        df_prs["pr_risk_level"] = np.where(
            s >= 75.0, "Critical", np.where(s >= 50.0, "High", np.where(s >= 28.0, "Medium", "Low"))
        )

    if "xai_pr_risk_factors" not in df_prs.columns:
        df_prs["xai_pr_risk_factors"] = np.where(
            has_conflict,
            "Merge Çakışması (+%26 risk) | CI/CD Pipeline Kırılması",
            "Küçük ve Temiz Değişiklik Seti",
        )
    if "xai_pr_recommended_actions" not in df_prs.columns:
        df_prs["xai_pr_recommended_actions"] = np.where(
            has_conflict,
            "Hedef branch ile günlük rebase yapın ve PR boyutunu küçültün.",
            "Standart otomatik birleştirme (auto-merge) akışı uygulanabilir.",
        )
    if "data_quality_flag" not in df_prs.columns:
        df_prs["data_quality_flag"] = "Clean"

    return df_prs


@st.cache_data(show_spinner=False)
def load_default_datasets(data_dir: str = "data") -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], dict]:
    """Varsayılan data/ klasöründeki Task ve PR veri setlerini hızlı Parquet (veya CSV/JSONL) kaynağından yükler."""
    tasks_df, prs_df, meta = None, None, {}

    meta_path = os.path.join(data_dir, "dataset_metadata_5v.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            meta = {}

    for candidate in ["tasks_dataset.parquet", "tasks_dataset.csv", "tasks_dataset.jsonl"]:
        p = os.path.join(data_dir, candidate)
        if os.path.exists(p):
            tasks_df = parse_uploaded_or_path(p)
            tasks_df = ensure_task_risk_and_xai_columns(tasks_df)
            break

    for candidate in ["pull_requests_dataset.parquet", "pull_requests_dataset.csv", "pull_requests_dataset.jsonl"]:
        p = os.path.join(data_dir, candidate)
        if os.path.exists(p):
            prs_df = parse_uploaded_or_path(p)
            prs_df = ensure_pr_risk_and_xai_columns(prs_df)
            break

    return tasks_df, prs_df, meta


@st.cache_data(show_spinner=False)
def load_velocity_stream(stream_path: str = "data/velocity_event_stream.jsonl", limit: int = 1000) -> pd.DataFrame:
    """Gerçek zamanlı olay akışı (Velocity) dosyasını yükler."""
    if not os.path.exists(stream_path):
        return pd.DataFrame()
    records = []
    with open(stream_path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if idx >= limit:
                break
            line = line.strip()
            if line:
                records.append(json.loads(line))
    if not records:
        return pd.DataFrame()
    df_stream = pd.json_normalize(records)
    return df_stream


@st.cache_data(show_spinner=False)
def build_combined_early_warning_view(df_tasks: pd.DataFrame, df_prs: pd.DataFrame) -> pd.DataFrame:
    """
    Task ve PR veri setlerini ilişkisel anahtar (`task_id` == `linked_task_issue`) üzerinden
    birleştirerek her görev için erken PR sinyallerini ve Bileşik Erken Risk Skorunu hesaplar.
    """
    if df_prs is not None and len(df_prs) > 0 and "linked_task_issue" in df_prs.columns:
        pr_agg = (
            df_prs.groupby("linked_task_issue", as_index=False)
            .agg(
                pr_total_count=("pr_id", "count"),
                pr_avg_risk_score=("pr_risk_score", "mean"),
                pr_max_risk_score=("pr_risk_score", "max"),
                pr_total_lines_added=("lines_added", "sum"),
                pr_total_changed_files=("changed_files_count", "sum"),
                pr_total_ci_failures=("ci_failure_count", "sum"),
                pr_conflict_count=("has_conflict_bool", "sum"),
                pr_avg_review_hours=("time_to_first_review_hours", "mean"),
                pr_avg_merge_hours=("time_to_merge_hours", "mean"),
                pr_max_review_iterations=("review_iteration_count", "max"),
            )
            .rename(columns={"linked_task_issue": "task_id"})
        )
        merged = pd.merge(df_tasks, pr_agg, on="task_id", how="left")
    else:
        merged = df_tasks.copy()
        merged["pr_total_count"] = merged.get("linked_pr_count", 0)
        merged["pr_avg_risk_score"] = merged["delivery_risk_score"] * 0.8
        merged["pr_max_risk_score"] = merged["delivery_risk_score"]
        merged["pr_total_lines_added"] = 0
        merged["pr_total_changed_files"] = 0
        merged["pr_total_ci_failures"] = 0
        merged["pr_conflict_count"] = 0
        merged["pr_avg_review_hours"] = 0.0
        merged["pr_avg_merge_hours"] = 0.0
        merged["pr_max_review_iterations"] = 1

    for col in [
        "pr_total_count",
        "pr_avg_risk_score",
        "pr_max_risk_score",
        "pr_total_lines_added",
        "pr_total_changed_files",
        "pr_total_ci_failures",
        "pr_conflict_count",
        "pr_avg_review_hours",
        "pr_avg_merge_hours",
        "pr_max_review_iterations",
    ]:
        merged[col] = merged[col].fillna(0)

    # Erken Uyarı Bileşik Risk Skoru (Task Planlama Riski %60 + PR/CI-CD Entegrasyon Sinyali %40)
    merged["combined_early_risk_score"] = np.round(
        np.clip(
            merged["delivery_risk_score"] * 0.62 + merged["pr_max_risk_score"] * 0.38,
            1.0,
            99.9,
        ),
        2,
    )
    merged["early_delay_probability"] = np.round(
        1.0 / (1.0 + np.exp(-(merged["combined_early_risk_score"] - 47.0) / 11.5)),
        3,
    )
    return merged


# ---------------------------------------------------------------------------
# MAKİNE ÖĞRENMESİ ERKEN RİSK TAHMİN VE XAI ÖZNİTELİK ÖNEMİ MOTORU
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def train_early_risk_ml_model(df_combined: pd.DataFrame, sample_size: int = 25000):
    """
    Görev tamamlanmadan ÖNCEKİ erken sinyalleri (planlama + PR/CI-CD erken metrikleri)
    kullanarak teslimat gecikmesini (`is_delayed`) tahmin eden açıklanabilir ML modeli eğitir.
    """
    work_df = df_combined.copy()
    if len(work_df) > sample_size:
        work_df = work_df.sample(n=sample_size, random_state=42)

    priority_map = {"Low": 1, "Medium": 2, "High": 3, "Critical": 4}
    env_map = {"Development": 1, "Test": 2, "Staging": 3, "Production": 4}

    work_df["priority_num"] = work_df["priority"].map(priority_map).fillna(2)
    work_df["env_num"] = work_df["environment"].map(env_map).fillna(1)
    work_df["est_effort_clean"] = work_df["estimated_effort_hours"].fillna(work_df["story_points"] * 6.0)

    feature_cols = [
        "blocker_count",
        "dependency_count_calc",
        "requirement_change_count",
        "reopen_count",
        "story_points",
        "est_effort_clean",
        "optimism_ratio_calc",
        "priority_num",
        "env_num",
        "pr_total_changed_files",
        "pr_total_ci_failures",
        "pr_conflict_count",
        "pr_max_review_iterations",
    ]
    feature_labels = {
        "blocker_count": "Blocker (Engel) Sayısı",
        "dependency_count_calc": "Görev Bağımlılık Sayısı (Dependencies)",
        "requirement_change_count": "Kapsam Değişikliği (Scope Creep)",
        "reopen_count": "Testten Geri Dönüş (Reopen) Sayısı",
        "story_points": "Karmaşıklık Puanı (Story Points)",
        "est_effort_clean": "Tahmini Efor (Saat)",
        "optimism_ratio_calc": "İyimserlik Oranı (SP / Tahmini Efor)",
        "priority_num": "Görev Öncelik Seviyesi (Priority)",
        "env_num": "Hedef Ortam Kritikliliği (Environment)",
        "pr_total_changed_files": "PR Değişen Dosya Sayısı",
        "pr_total_ci_failures": "PR CI/CD Pipeline Hata Sayısı",
        "pr_conflict_count": "PR Merge Çakışması (Conflict) Sayısı",
        "pr_max_review_iterations": "PR Kod İnceleme Döngüsü (Review Tur)",
    }

    X = work_df[feature_cols].fillna(0).to_numpy(dtype=float)
    y = work_df["is_delayed"].fillna(0).to_numpy(dtype=int)

    if len(np.unique(y)) < 2:
        y[0] = 1 - y[0]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

    model = RandomForestClassifier(
        n_estimators=120,
        max_depth=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_test, y_prob)),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
    }

    importances = pd.DataFrame({
        "feature_key": feature_cols,
        "Öznitelik (Erken Risk Sinyali)": [feature_labels[k] for k in feature_cols],
        "Etki Ağırlığı (%)": np.round(model.feature_importances_ * 100.0, 2),
    }).sort_values("Etki Ağırlığı (%)", ascending=False)

    return model, metrics, importances, feature_cols, feature_labels


def compute_local_xai_contributions(row: pd.Series) -> pd.DataFrame:
    """
    Tek bir görev (Task + PR birleşimi) için her bir risk faktörünün baz skora olan
    yerel (local SHAP-benzeri) puan katkısını hesaplar.
    """
    contributions = []
    base_score = 10.0

    blockers = int(row.get("blocker_count", 0))
    deps = int(row.get("dependency_count_calc", 0))
    req_changes = int(row.get("requirement_change_count", 0))
    reopens = int(row.get("reopen_count", 0))
    sp = int(row.get("story_points", 3))
    opt = float(row.get("optimism_ratio_calc", 1.0))
    prio = str(row.get("priority", "Medium"))
    env = str(row.get("environment", "Development"))
    pr_files = int(row.get("pr_total_changed_files", 0))
    ci_fails = int(row.get("pr_total_ci_failures", 0))
    conflicts = int(row.get("pr_conflict_count", 0))

    contributions.append(("Baz Sistem Karmaşıklığı", base_score, "Sabit referans risk seviyesi"))
    if blockers > 0:
        contributions.append((f"Blocker / Engel ({blockers} adet)", blockers * 16.0, "Aktif görev blokajı"))
    else:
        contributions.append(("Blocker Bulunmaması (0 adet)", -3.5, "Akışı durduran engel yok"))

    if deps > 0:
        contributions.append((f"Görev Bağımlılıkları ({deps} ön koşul)", deps * 9.5, "Ön koşul görev gecikme yayılımı"))
    else:
        contributions.append(("Bağımsız Görev (0 bağımlılık)", -2.5, "Dış görev bağımlılığı yok"))

    if req_changes > 0:
        contributions.append((f"Scope Creep / Kapsam Değişimi ({req_changes} kez)", req_changes * 11.5, "Sprint içi gereksinim revizyonu"))

    if reopens > 0:
        contributions.append((f"Testten Geri Dönüş / Reopen ({reopens} kez)", reopens * 12.0, "Kabul kriteri / kalite uyumsuzluğu"))

    if sp >= 13:
        contributions.append((f"Yüksek Karmaşıklık ({sp} Story Points)", 14.0, "Büyük görev boyutu (parçalanmalı)"))
    elif sp <= 3:
        contributions.append((f"Atomik Görev Boyutu ({sp} Story Points)", -4.0, "Yönetilebilir küçük kapsam"))

    if opt > 1.25:
        contributions.append((f"İyimser Efor Tahmini (Oran: {opt:.2f})", 13.0, "Story point'e kıyasla düşük saat tahmini"))

    prio_pts = 14.0 if prio == "Critical" else (8.0 if prio == "High" else 2.0)
    contributions.append((f"Öncelik Seviyesi ({prio})", prio_pts, "SLA ve teslimat baskısı"))

    env_pts = 10.0 if env == "Production" else (5.0 if env == "Staging" else 1.0)
    contributions.append((f"Hedef Ortam ({env})", env_pts, "Dağıtım ve doğrulama karmaşıklığı"))

    if conflicts > 0 or ci_fails > 0 or pr_files >= 18:
        pr_impact = min(28.0, conflicts * 9.0 + ci_fails * 4.5 + (4.0 if pr_files >= 18 else 0.0))
        contributions.append((
            f"PR & CI/CD Darboğazı ({conflicts} Conflict, {ci_fails} CI Hata, {pr_files} Dosya)",
            round(pr_impact, 1),
            "Kod birleştirme ve pipeline kırılma etkisi",
        ))
    else:
        contributions.append(("Temiz PR & CI/CD Akışı", -3.0, "Entegrasyon darboğazı görülmedi"))

    df_contrib = pd.DataFrame(contributions, columns=["Faktör", "Puan Katkısı", "Açıklama"])
    df_contrib["Etki Yönü"] = np.where(
        df_contrib["Puan Katkısı"] > 0, "Risk Artırıcı (+)", "Risk Azaltıcı (-)"
    )
    return df_contrib.sort_values("Puan Katkısı", ascending=False)


def format_risk_badge(level: str) -> str:
    cls = {
        "Critical": "badge-critical",
        "High": "badge-high",
        "Medium": "badge-medium",
        "Low": "badge-low",
    }.get(level, "badge-medium")
    return f'<span class="{cls}">{level}</span>'


# ---------------------------------------------------------------------------
# KENAR ÇUBUĞU (SIDEBAR): VERİ YÜKLEME VE FİLTRELEME MERKEZİ
# ---------------------------------------------------------------------------
def render_sidebar_and_load_data():
    st.sidebar.markdown("## 📂 1. Veri Seti Yükleme (Task & PR)")
    st.sidebar.caption(
        "Kendi **Task** ve **Pull Request (PR)** dosyalarınızı (`.parquet`, `.csv`, `.jsonl`) yükleyin "
        "veya mevcut büyük veri setini kullanın."
    )

    uploaded_task_file = st.sidebar.file_uploader(
        "📋 Görev (Task) Veri Seti Yükle",
        type=["parquet", "csv", "jsonl", "json"],
        key="sidebar_task_uploader",
        help="tasks_dataset.parquet / .csv / .jsonl dosyasını seçin",
    )
    uploaded_pr_file = st.sidebar.file_uploader(
        "🔀 Pull Request (PR) Veri Seti Yükle",
        type=["parquet", "csv", "jsonl", "json"],
        key="sidebar_pr_uploader",
        help="pull_requests_dataset.parquet / .csv / .jsonl dosyasını seçin",
    )

    auto_load_default = st.sidebar.checkbox(
        "Dosya yüklenmediyse hazır data/ veri setini kullan",
        value=True,
        help="İşaretli olduğunda data/ klasöründeki 80.000 Task ve ~151.000 PR verisi otomatik yüklenir.",
    )

    return uploaded_task_file, uploaded_pr_file, auto_load_default


# ---------------------------------------------------------------------------
# ANA UYGULAMA AKIŞI
# ---------------------------------------------------------------------------
def main():
    uploaded_task_file, uploaded_pr_file, auto_load_default = render_sidebar_and_load_data()

    # Üst Başlık
    st.markdown(
        """
        <div class="main-header">
            <h1>🛡️ Teslimat Risklerinin Erken Tespiti ve Açıklanabilir Karar Destek Sistemi (XAI)</h1>
            <p>Büyük Veri (5V) Tabanlı Görev (Task) & Pull Request (PR / CI-CD) Erken Uyarı Radarı, Nedensel XAI Açıklamaları ve Karşı-Olgusal (What-If) Karar Simülatörü</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Ana ekranda da hızlı dosya yükleme kutusu (Expander)
    with st.expander("📤 Veri Seti Yükleme ve Kaynak Yönetimi (Task & PR Dosyalarını Buradan da Yükleyebilirsiniz)", expanded=(uploaded_task_file is None and not auto_load_default)):
        up_col1, up_col2 = st.columns(2)
        with up_col1:
            main_task_upload = st.file_uploader(
                "Görev (Task) Dosyası Seçin (.parquet, .csv, .jsonl)",
                type=["parquet", "csv", "jsonl", "json"],
                key="main_task_uploader",
            )
        with up_col2:
            main_pr_upload = st.file_uploader(
                "Pull Request (PR) Dosyası Seçin (.parquet, .csv, .jsonl)",
                type=["parquet", "csv", "jsonl", "json"],
                key="main_pr_uploader",
            )

    active_task_upload = uploaded_task_file or main_task_upload
    active_pr_upload = uploaded_pr_file or main_pr_upload

    default_tasks, default_prs, meta_info = load_default_datasets(DEFAULT_DATA_DIR)

    df_tasks: Optional[pd.DataFrame] = None
    df_prs: Optional[pd.DataFrame] = None
    task_source_label = ""
    pr_source_label = ""

    # Task Veri Kaynağı Belirleme
    if active_task_upload is not None:
        with st.spinner(f"Task veri seti yükleniyor ({active_task_upload.name})..."):
            df_tasks = ensure_task_risk_and_xai_columns(parse_uploaded_or_path(active_task_upload, active_task_upload.name))
            task_source_label = f"Yüklenen Dosya: {active_task_upload.name}"
    elif auto_load_default and default_tasks is not None:
        df_tasks = default_tasks
        task_source_label = "Hazır Büyük Veri Seti (data/tasks_dataset.parquet)"

    # PR Veri Kaynağı Belirleme
    if active_pr_upload is not None:
        with st.spinner(f"PR veri seti yükleniyor ({active_pr_upload.name})..."):
            df_prs = ensure_pr_risk_and_xai_columns(parse_uploaded_or_path(active_pr_upload, active_pr_upload.name))
            pr_source_label = f"Yüklenen Dosya: {active_pr_upload.name}"
    elif auto_load_default and default_prs is not None:
        df_prs = default_prs
        pr_source_label = "Hazır Büyük Veri Seti (data/pull_requests_dataset.parquet)"

    if df_tasks is None or len(df_tasks) == 0:
        st.info(
            "👋 **Hoş Geldiniz!** Analizi ve Açıklanabilir Karar Destek Sistemini başlatmak için lütfen yukarıdaki veya "
            "sol menüdeki yükleme alanından **Task** ve **Pull Request (PR)** veri setlerinizi yükleyin "
            "(veya sol menüden *'Hazır data/ veri setini kullan'* seçeneğini işaretleyin)."
        )
        return

    # Veri Kalitesi / Veracity Ön İşleme Seçeneği (Sidebar)
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 🧹 2. 5V Veracity & Ön İşleme")
    clean_veracity = st.sidebar.toggle(
        "Veri Kusurlarını Otomatik Temizle (Clean & Impute)",
        value=False,
        help="Missing_Estimate olan eksik efor tahminlerini doldurur ve Outlier_Effort / Outlier_Diff_Size aykırı değerlerini kırpar.",
    )

    if clean_veracity:
        df_tasks = df_tasks.copy()
        df_tasks["estimated_effort_hours"] = df_tasks["estimated_effort_hours"].fillna(df_tasks["story_points"] * 5.8)
        outlier_mask = df_tasks["data_quality_flag"] == "Outlier_Effort"
        df_tasks.loc[outlier_mask, "actual_effort_hours"] = np.round(
            df_tasks.loc[outlier_mask, "actual_effort_hours"] / 3.8, 1
        )
        if df_prs is not None:
            df_prs = df_prs.copy()
            pr_outlier = df_prs["data_quality_flag"] == "Outlier_Diff_Size"
            df_prs.loc[pr_outlier, "lines_added"] = np.clip(df_prs.loc[pr_outlier, "lines_added"], 5, 800)

    # Task + PR Birleşik Erken Uyarı Tablosu
    df_combined = build_combined_early_warning_view(df_tasks, df_prs)

    # Küresel Filtreler (Sidebar)
    st.sidebar.markdown("---")
    st.sidebar.markdown("## 🔍 3. Küresel Karar Filtreleri")

    all_projects = sorted(df_combined["project"].dropna().unique().tolist())
    selected_projects = st.sidebar.multiselect(
        "Proje Filtresi",
        options=all_projects,
        default=all_projects,
    )

    all_risk_levels = [r for r in RISK_ORDER if r in df_combined["delivery_risk_level"].unique()]
    selected_risk_levels = st.sidebar.multiselect(
        "Risk Seviyesi",
        options=all_risk_levels,
        default=all_risk_levels,
    )

    early_detection_mode = st.sidebar.radio(
        "Görev Kapsamı (Erken Tespit Odak Modu)",
        options=[
            "Tüm Görevler (Tamamlanan + Aktif)",
            "Sadece Aktif / Açık Görevler (Erken Müdahale Odaklı)",
            "Sadece Tamamlanan Görevler (Geçmiş Performans)",
        ],
        index=0,
    )

    all_components = sorted(df_combined["component_module"].dropna().unique().tolist())
    selected_components = st.sidebar.multiselect(
        "Yazılım Bileşeni (Component)",
        options=all_components,
        default=all_components,
    )

    # Filtreleri Uygula
    mask = (
        df_combined["project"].isin(selected_projects or all_projects)
        & df_combined["delivery_risk_level"].isin(selected_risk_levels or all_risk_levels)
        & df_combined["component_module"].isin(selected_components or all_components)
    )
    if early_detection_mode == "Sadece Aktif / Açık Görevler (Erken Müdahale Odaklı)":
        mask = mask & (df_combined["status"] != "Done")
    elif early_detection_mode == "Sadece Tamamlanan Görevler (Geçmiş Performans)":
        mask = mask & (df_combined["status"] == "Done")

    filtered_tasks = df_combined[mask].copy()

    if df_prs is not None and len(df_prs) > 0:
        filtered_prs = df_prs[df_prs["linked_task_issue"].isin(filtered_tasks["task_id"])].copy()
    else:
        filtered_prs = pd.DataFrame()

    # Veri Kaynağı Durum Çubuğu
    st.caption(
        f"✅ **Aktif Görev Kaynağı:** `{task_source_label}` ({len(filtered_tasks):,} / {len(df_tasks):,} kayıt) &nbsp;|&nbsp; "
        f"🔀 **Aktif PR Kaynağı:** `{pr_source_label}` ({len(filtered_prs):,} / {len(df_prs) if df_prs is not None else 0:,} kayıt) &nbsp;|&nbsp; "
        f"🧹 **Veracity Temizleme:** `{'Aktif' if clean_veracity else 'Ham Veri'}`"
    )

    if len(filtered_tasks) == 0:
        st.warning("Seçilen filtre kriterlerine uygun görev bulunamadı. Lütfen sol menüdeki filtreleri genişletin.")
        return

    # ---------------------------------------------------------------------------
    # ÜST DÜZEY KPI KARTLARI
    # ---------------------------------------------------------------------------
    active_open_tasks = filtered_tasks[filtered_tasks["status"] != "Done"]
    early_at_risk_open = active_open_tasks[active_open_tasks["combined_early_risk_score"] >= 50.0]
    delayed_ratio = filtered_tasks["is_delayed"].mean() * 100.0
    avg_risk_score = filtered_tasks["combined_early_risk_score"].mean()
    critical_high_count = int((filtered_tasks["delivery_risk_level"].isin(["Critical", "High"])).sum())
    pr_critical_ratio = (
        (filtered_prs["pr_risk_level"].isin(["Critical", "High"])).mean() * 100.0
        if len(filtered_prs) > 0
        else 0.0
    )

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">📋 Analiz Edilen Görev & PR</div>
                <div class="kpi-value">{len(filtered_tasks):,}</div>
                <div class="kpi-sub">🔗 {len(filtered_prs):,} İlişkili Pull Request</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">🚨 Erken Uyarı (Açık Riskli İşler)</div>
                <div class="kpi-value" style="color: #f87171;">{len(early_at_risk_open):,}</div>
                <div class="kpi-sub">{len(active_open_tasks):,} açık görevden erken müdahale bekleyen</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">🎯 Bileşik Erken Risk Skoru</div>
                <div class="kpi-value" style="color: #fbbf24;">{avg_risk_score:.1f} / 100</div>
                <div class="kpi-sub">Kritik/Yüksek: {critical_high_count:,} görev (%{critical_high_count/max(1,len(filtered_tasks))*100:.1f})</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with k4:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">⏳ Teslimat Gecikme Oranı</div>
                <div class="kpi-value" style="color: #fb923c;">%{delayed_ratio:.1f}</div>
                <div class="kpi-sub">Ort. Sapma: {filtered_tasks[filtered_tasks['delay_days']>0]['delay_days'].mean():.1f} gün (Gecikenlerde)</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with k5:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">🔀 PR & CI/CD Darboğaz Oranı</div>
                <div class="kpi-value" style="color: #38bdf8;">%{pr_critical_ratio:.1f}</div>
                <div class="kpi-sub">Yüksek/Kritik entegrasyon riskli PR oranı</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='margin-top: 1rem;'></div>", unsafe_allow_html=True)

    # ---------------------------------------------------------------------------
    # 7 ANA SEKME (TABS)
    # ---------------------------------------------------------------------------
    tabs = st.tabs([
        "📡 1. Erken Uyarı Radarı & Özet",
        "🤖 2. ML Erken Tespit Motoru & Küresel XAI",
        "🔍 3. Tekil Görev/PR XAI Karar Müfettişi",
        "🎛️ 4. What-If (Karşı-Olgusal) Karar Simülatörü",
        "🔀 5. PR, Kod İnceleme & CI/CD Analitiği",
        "🏛️ 6. Büyük Veri (5V), Veracity & Canlı Akış",
        "📥 7. Aksiyon Planı & Rapor İndir",
    ])

    # ===========================================================================
    # SEKME 1: ERKEN UYARI RADARI VE YÖNETİCİ ÖZETİ
    # ===========================================================================
    with tabs[0]:
        st.subheader("📡 Teslimat Risklerinin Erken Tespiti — Henüz Tamamlanmamış Görevlerde Kritik Radar")
        st.markdown(
            "Bu modül, **henüz tamamlanmamış (`status != Done`)** görevleri ve bunlara bağlı açık **Pull Request / CI-CD** "
            "sinyallerini birleştirerek teslimat tarihi (`due_date`) gelmeden önce gecikme olasılığını ve alınması gereken aksiyonu listeler."
        )

        # Erken Uyarı Tablosu (Açık & Yüksek Riskli Görevler)
        early_candidates = (
            active_open_tasks if len(active_open_tasks) > 0 else filtered_tasks
        ).sort_values("combined_early_risk_score", ascending=False)

        top_early_alerts = early_candidates.head(25)[
            [
                "task_id",
                "project",
                "component_module",
                "status",
                "priority",
                "assignee",
                "due_date",
                "combined_early_risk_score",
                "early_delay_probability",
                "blocker_count",
                "pr_conflict_count",
                "pr_total_ci_failures",
                "xai_primary_risk_factors",
                "xai_recommended_actions",
            ]
        ].rename(
            columns={
                "task_id": "Görev ID",
                "project": "Proje",
                "component_module": "Bileşen",
                "status": "Durum",
                "priority": "Öncelik",
                "assignee": "Sorumlu",
                "due_date": "Hedef Teslim",
                "combined_early_risk_score": "Erken Risk Skoru (0-100)",
                "early_delay_probability": "Erken Gecikme Olasılığı",
                "blocker_count": "Blocker",
                "pr_conflict_count": "PR Conflict",
                "pr_total_ci_failures": "CI Hata",
                "xai_primary_risk_factors": "XAI Risk Nedenleri",
                "xai_recommended_actions": "Önerilen Erken Müdahale Aksiyonu",
            }
        )

        st.dataframe(
            top_early_alerts,
            width="stretch",
            hide_index=True,
            column_config={
                "Erken Risk Skoru (0-100)": st.column_config.ProgressColumn(
                    "Erken Risk Skoru",
                    format="%.1f",
                    min_value=0,
                    max_value=100,
                ),
                "Erken Gecikme Olasılığı": st.column_config.ProgressColumn(
                    "Gecikme Olasılığı",
                    format="%.1f%%",
                    min_value=0.0,
                    max_value=1.0,
                ),
            },
        )

        # Görselleştirmeler: Risk Dağılımı & Proje-Bileşen Isı Haritası
        c_left, c_right = st.columns([1, 1.35])

        with c_left:
            st.markdown("#### 📊 Proje Bazlı Risk Sınıfı Dağılımı")
            proj_risk_df = (
                filtered_tasks.groupby(["project", "delivery_risk_level"], as_index=False)
                .size()
                .rename(columns={"size": "Görev Sayısı"})
            )
            chart_proj = (
                alt.Chart(proj_risk_df)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    y=alt.Y("project:N", title="Proje", sort="-x"),
                    x=alt.X("Görev Sayısı:Q", title="Görev Sayısı"),
                    color=alt.Color(
                        "delivery_risk_level:N",
                        title="Risk Seviyesi",
                        scale=alt.Scale(domain=RISK_ORDER, range=[RISK_COLORS[k] for k in RISK_ORDER]),
                    ),
                    tooltip=["project:N", "delivery_risk_level:N", "Görev Sayısı:Q"],
                )
                .properties(height=340)
            )
            st.altair_chart(chart_proj, width="stretch")

        with c_right:
            st.markdown("#### 🔥 Proje x Yazılım Bileşeni Erken Risk Isı Haritası (Heatmap)")
            heat_df = (
                filtered_tasks.groupby(["project", "component_module"], as_index=False)
                .agg(
                    Ort_Risk=("combined_early_risk_score", "mean"),
                    Gecikme_Orani=("is_delayed", "mean"),
                    Gorev_Sayisi=("task_id", "count"),
                )
            )
            heat_df["Ort_Risk"] = heat_df["Ort_Risk"].round(1)
            heat_df["Gecikme_Yuzde"] = (heat_df["Gecikme_Orani"] * 100).round(1)

            heatmap = (
                alt.Chart(heat_df)
                .mark_rect(cornerRadius=3)
                .encode(
                    x=alt.X("component_module:N", title="Yazılım Bileşeni (Component)", axis=alt.Axis(labelAngle=-35)),
                    y=alt.Y("project:N", title="Proje"),
                    color=alt.Color(
                        "Ort_Risk:Q",
                        title="Ort. Risk Skoru",
                        scale=alt.Scale(scheme="redyellowgreen", reverse=True),
                    ),
                    tooltip=[
                        alt.Tooltip("project:N", title="Proje"),
                        alt.Tooltip("component_module:N", title="Bileşen"),
                        alt.Tooltip("Ort_Risk:Q", title="Ort. Erken Risk Skoru"),
                        alt.Tooltip("Gecikme_Yuzde:Q", title="Gecikme Oranı (%)"),
                        alt.Tooltip("Gorev_Sayisi:Q", title="Görev Sayısı"),
                    ],
                )
                .properties(height=340)
            )
            st.altair_chart(heatmap, width="stretch")

        # Sprint Bazlı Zaman Serisi Trendi
        if "sprint_iteration" in filtered_tasks.columns:
            st.markdown("#### 📈 Sprint Dönemlerine Göre Teslimat Riski ve Gecikme Eğilimi (Velocity Trend)")
            sprint_df = (
                filtered_tasks.groupby("sprint_iteration", as_index=False)
                .agg(
                    Ortalama_Risk_Skoru=("combined_early_risk_score", "mean"),
                    Gecikme_Orani_Yuzde=("is_delayed", lambda s: round(s.mean() * 100.0, 1)),
                    Gorev_Sayisi=("task_id", "count"),
                )
                .sort_values("sprint_iteration")
            )
            sprint_df["Ortalama_Risk_Skoru"] = sprint_df["Ortalama_Risk_Skoru"].round(1)

            base_sp = alt.Chart(sprint_df).encode(
                x=alt.X("sprint_iteration:N", title="Sprint Dönemi", axis=alt.Axis(labelAngle=-45))
            )
            line_risk = base_sp.mark_line(color="#38bdf8", strokeWidth=3, point=True).encode(
                y=alt.Y("Ortalama_Risk_Skoru:Q", title="Ortalama Risk Skoru / Gecikme (%)"),
                tooltip=["sprint_iteration:N", "Ortalama_Risk_Skoru:Q", "Gecikme_Orani_Yuzde:Q", "Gorev_Sayisi:Q"],
            )
            line_delay = base_sp.mark_line(color="#f97316", strokeDash=[5, 3], strokeWidth=2.5, point=True).encode(
                y=alt.Y("Gecikme_Orani_Yuzde:Q"),
                tooltip=["sprint_iteration:N", "Gecikme_Orani_Yuzde:Q", "Ortalama_Risk_Skoru:Q"],
            )
            st.altair_chart((line_risk + line_delay).properties(height=280), width="stretch")
            st.caption("🔵 Mavi Çizgi: Ortalama Bileşik Erken Risk Skoru | 🟠 Turuncu Kesikli Çizgi: Geciken Görev Oranı (%)")

    # ===========================================================================
    # SEKME 2: MAKİNE ÖĞRENMESİ ERKEN TESPİT MOTORU VE KÜRESEL XAI
    # ===========================================================================
    with tabs[1]:
        st.subheader("🤖 Makine Öğrenmesi ile Erken Gecikme Tahmini ve Küresel Öznitelik Önemi (Global XAI)")
        st.markdown(
            "Bu modül, **görev bitiş tarihi gerçekleşmeden önce** bilinen 13 erken sinyali (Görev karmaşıklığı, "
            "iyimser efor tahmini, bağımlılık sayısı, blocker, kapsam değişimi ve ilişkili PR'ların ilk kod inceleme/CI-CD sinyalleri) "
            "kullanarak `RandomForestClassifier` modeli eğitir ve hangi faktörün gecikmeyi ne kadar tetiklediğini açıklar."
        )

        with st.spinner("Erken Risk Tespiti ML modeli ve XAI öznitelik ağırlıkları hesaplanıyor..."):
            model, ml_metrics, feat_imp_df, feature_cols, feature_labels = train_early_risk_ml_model(df_combined)

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("ROC-AUC Skoru", f"{ml_metrics['roc_auc']:.3f}", "Yüksek Ayırt Edicilik")
        m2.metric("Doğruluk (Accuracy)", f"%{ml_metrics['accuracy']*100:.1f}", f"{ml_metrics['test_rows']:,} Test Kaydı")
        m3.metric("F1-Skoru (Dengeli)", f"{ml_metrics['f1']:.3f}")
        m4.metric("Kesinlik (Precision)", f"%{ml_metrics['precision']*100:.1f}")
        m5.metric("Duyarlılık (Recall)", f"%{ml_metrics['recall']*100:.1f}", "Erken Yakalama Oranı")

        col_imp, col_xai_dist = st.columns([1.2, 1])

        with col_imp:
            st.markdown("#### 🌐 Küresel XAI: Teslimat Gecikmesini Tetikleyen En Önemli Erken Sinyaller")
            imp_chart = (
                alt.Chart(feat_imp_df)
                .mark_bar(cornerRadiusTopRight=5, cornerRadiusBottomRight=5)
                .encode(
                    y=alt.Y("Öznitelik (Erken Risk Sinyali):N", sort="-x", title="Erken Uyarı Özniteliği"),
                    x=alt.X("Etki Ağırlığı (%):Q", title="Model Karar Etki Ağırlığı (%)"),
                    color=alt.Color(
                        "Etki Ağırlığı (%):Q",
                        scale=alt.Scale(scheme="blues"),
                        legend=None,
                    ),
                    tooltip=["Öznitelik (Erken Risk Sinyali):N", "Etki Ağırlığı (%):Q"],
                )
                .properties(height=380)
            )
            st.altair_chart(imp_chart, width="stretch")

        with col_xai_dist:
            st.markdown("#### 🧠 En Sık Karşılaşılan Nedensel XAI Risk Örüntüleri")
            top_xai_reasons = (
                filtered_tasks["xai_primary_risk_factors"]
                .value_counts()
                .head(8)
                .reset_index()
            )
            top_xai_reasons.columns = ["XAI Birincil Risk Faktörü Kombinasyonu", "Görev Sayısı"]
            top_xai_reasons["Oran (%)"] = np.round(
                top_xai_reasons["Görev Sayısı"] / len(filtered_tasks) * 100.0, 2
            )
            st.dataframe(top_xai_reasons, width="stretch", hide_index=True)

            cm = ml_metrics["confusion_matrix"]
            st.markdown("#### 🎯 Doğrulama Karmaşıklık Matrisi (Confusion Matrix)")
            cm_df = pd.DataFrame(
                cm,
                index=["Gerçek: Zamanında (0)", "Gerçek: Gecikti (1)"],
                columns=["Tahmin: Zamanında (0)", "Tahmin: Gecikecek (1)"],
            )
            st.dataframe(cm_df, width="stretch")

    # ===========================================================================
    # SEKME 3: TEKİL GÖREV / PR XAI KARAR MÜFETTİŞİ (LOCAL EXPLAINABILITY)
    # ===========================================================================
    with tabs[2]:
        st.subheader("🔍 Tekil Görev & Pull Request Açıklanabilir Karar Destek Müfettişi (Local XAI)")
        st.markdown(
            "İncelemek istediğiniz görevi seçin. Sistem, bu görevin **neden riskli olduğunu (Local Feature Attribution)**, "
            "**hangi somut yönetim aksiyonunun alınması gerektiğini** ve göreve bağlı **tüm Pull Request / CI-CD / Kod İnceleme** "
            "detaylarını uçtan uca açıklar."
        )

        insp_col1, insp_col2 = st.columns([1, 2])
        with insp_col1:
            sort_mode = st.selectbox(
                "Görev Sıralama Kriteri",
                options=[
                    "En Yüksek Erken Risk Skoruna Göre (Önerilen)",
                    "En Fazla Blocker & PR Çakışmasına Göre",
                    "Görev Numarasına Göre (TASK-ID)",
                ],
            )
            if sort_mode == "En Yüksek Erken Risk Skoruna Göre (Önerilen)":
                candidate_pool = filtered_tasks.sort_values("combined_early_risk_score", ascending=False).head(300)
            elif sort_mode == "En Fazla Blocker & PR Çakışmasına Göre":
                candidate_pool = filtered_tasks.sort_values(
                    ["blocker_count", "pr_conflict_count", "combined_early_risk_score"], ascending=False
                ).head(300)
            else:
                candidate_pool = filtered_tasks.head(300)

            task_options = candidate_pool["task_id"].tolist()
            selected_task_id = st.selectbox("İncelenecek Görevi Seçin (Task ID)", options=task_options)

        with insp_col2:
            custom_search_id = st.text_input(
                "Veya Doğrudan Görev ID Arayın (Örn: TASK-100005)",
                value="",
                placeholder="TASK-100001...",
            ).strip()
            if custom_search_id and custom_search_id in df_combined["task_id"].values:
                selected_task_id = custom_search_id

        task_row = df_combined[df_combined["task_id"] == selected_task_id].iloc[0]
        linked_prs_df = (
            df_prs[df_prs["linked_task_issue"] == selected_task_id]
            if (df_prs is not None and "linked_task_issue" in df_prs.columns)
            else pd.DataFrame()
        )

        # Görev Kimlik & XAI Özet Kartı
        st.markdown(
            f"""
            <div class="kpi-card" style="margin-bottom: 1rem;">
                <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap;">
                    <div>
                        <span style="font-size:1.25rem; font-weight:700; color:#f8fafc;">{task_row['task_id']}: {task_row.get('task_title', '')}</span>
                        <div style="color:#94a3b8; font-size:0.9rem; margin-top:0.25rem;">
                            🏢 <b>Proje:</b> {task_row.get('project', '-')} &nbsp;|&nbsp;
                            🧩 <b>Bileşen:</b> {task_row.get('component_module', '-')} &nbsp;|&nbsp;
                            👤 <b>Sorumlu:</b> {task_row.get('assignee', '-')} &nbsp;|&nbsp;
                            📌 <b>Durum:</b> <code>{task_row.get('status', '-')}</code> &nbsp;|&nbsp;
                            ⚡ <b>Öncelik:</b> <code>{task_row.get('priority', '-')}</code>
                        </div>
                    </div>
                    <div style="text-align:right;">
                        {format_risk_badge(str(task_row.get('delivery_risk_level', 'Medium')))}
                        <div style="font-size:1.35rem; font-weight:700; color:#fbbf24; margin-top:0.2rem;">
                            Erken Risk: {task_row['combined_early_risk_score']:.1f} / 100
                        </div>
                        <div style="font-size:0.82rem; color:#cbd5e1;">
                            Erken Gecikme Olasılığı: <b>%{task_row['early_delay_probability']*100:.1f}</b>
                        </div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        xcol1, xcol2 = st.columns(2)
        with xcol1:
            st.markdown(
                f"""
                <div class="xai-box">
                    <div style="font-weight:700; color:#fbbf24; margin-bottom:0.35rem;">
                        🧠 XAI Nedensel Risk Açıklaması (Neden Bu Görev Riskli?)
                    </div>
                    <div style="color:#f8fafc; font-size:0.95rem;">
                        {task_row.get('xai_primary_risk_factors', 'Standart iş akışı')}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with xcol2:
            st.markdown(
                f"""
                <div class="action-box">
                    <div style="font-weight:700; color:#2dd4bf; margin-bottom:0.35rem;">
                        💡 Önerilen Karar Destek Aksiyonu (Ne Yapılmalı?)
                    </div>
                    <div style="color:#f8fafc; font-size:0.95rem;">
                        {task_row.get('xai_recommended_actions', 'Rutin sprint takibini sürdürün.')}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Lokal Faktör Katkı Grafiği & Görev Metrikleri
        l_col, r_col = st.columns([1.3, 1])
        local_contrib_df = compute_local_xai_contributions(task_row)

        with l_col:
            st.markdown("#### 📊 Lokal XAI Faktör Katkı Analizi (SHAP-Stili Puan Dağılımı)")
            local_chart = (
                alt.Chart(local_contrib_df)
                .mark_bar(cornerRadius=4)
                .encode(
                    y=alt.Y("Faktör:N", sort="-x", title="Görev & PR Risk Faktörü"),
                    x=alt.X("Puan Katkısı:Q", title="Risk Skoruna Puan Katkısı (+ / -)"),
                    color=alt.Color(
                        "Etki Yönü:N",
                        scale=alt.Scale(
                            domain=["Risk Artırıcı (+)", "Risk Azaltıcı (-)"],
                            range=["#ef4444", "#22c55e"],
                        ),
                    ),
                    tooltip=["Faktör:N", "Puan Katkısı:Q", "Açıklama:N"],
                )
                .properties(height=320)
            )
            st.altair_chart(local_chart, width="stretch")

        with r_col:
            st.markdown("#### 📋 Görev Planlama & Kapsam Detayları")
            st.markdown(
                f"""
                - **Sprint / Epic:** `{task_row.get('sprint_iteration', '-')}` / `{task_row.get('epic_parent_task', '-')}`
                - **Planlanan Tarihler:** `{task_row.get('start_date', '-')}` → `{task_row.get('due_date', '-')}`
                - **Karmaşıklık & Efor:** `{task_row.get('story_points', '-')}` SP | Tahmini: `{task_row.get('estimated_effort_hours', '-')} saat` | Gerçekleşen: `{task_row.get('actual_effort_hours', '-')} saat`
                - **Blocker / Kapsam Değişimi / Reopen:** `{task_row.get('blocker_count', 0)}` Blocker | `{task_row.get('requirement_change_count', 0)}` Scope Creep | `{task_row.get('reopen_count', 0)}` Reopen
                - **Ön Koşul Bağımlılıklar:** `{task_row.get('dependencies', 'Yok')}`
                - **Veri Kalitesi Bayrağı (Veracity):** `{task_row.get('data_quality_flag', 'Clean')}`
                """
            )
            with st.expander("📄 Görev Açıklaması ve Kabul Kriterleri (Unstructured Text)", expanded=True):
                st.write("**Açıklama:**", task_row.get("description", "-"))
                st.write("**Kabul Kriterleri:**", task_row.get("acceptance_criteria", "-"))

        # Bağlı Pull Request'ler (1:N İlişkisel Görünüm)
        st.markdown(f"#### 🔀 Bu Göreve Bağlı Pull Request (PR) ve CI/CD Kayıtları ({len(linked_prs_df)} Adet)")
        if len(linked_prs_df) == 0:
            st.info("Bu göreve bağlı PR kaydı bulunamadı.")
        else:
            for _, pr in linked_prs_df.iterrows():
                with st.expander(
                    f"🔹 {pr['pr_id']} | {pr.get('pr_title', '')} — Durum: {pr.get('status', '')} | "
                    f"PR Risk Skoru: {pr.get('pr_risk_score', 0):.1f} ({pr.get('pr_risk_level', '')})",
                    expanded=True,
                ):
                    p1, p2, p3 = st.columns([1.2, 1.2, 1.2])
                    with p1:
                        st.markdown(
                            f"""
                            - **Repo & Branch:** `{pr.get('repository', '-')}` (`{pr.get('source_branch', '-')}` → `{pr.get('target_branch', '-')}`)
                            - **Geliştirici & Reviewer:** `{pr.get('pr_author_created_by', '-')}` | İnceleyen: `{pr.get('reviewers', '-')}`
                            - **Kod Değişimi:** `{pr.get('changed_files_count', 0)}` dosya (`+{pr.get('lines_added', 0)}` / `-{pr.get('lines_deleted', 0)}` satır, `{pr.get('commit_count', 0)}` commit)
                            """
                        )
                    with p2:
                        st.markdown(
                            f"""
                            - **Merge Çakışması:** `{pr.get('merge_conflicts', '-')}`
                            - **Pipeline & Build:** `{pr.get('pipeline_status', '-')}` (Build: `{pr.get('build_status', '-')}`, CI Hata: `{pr.get('ci_failure_count', 0)}`)
                            - **İnceleme & Birleştirme Süresi:** İlk İnceleme `{pr.get('time_to_first_review_hours', 0)} sa` | Merge `{pr.get('time_to_merge_hours', 0)} sa` (`{pr.get('review_iteration_count', 1)}` tur)
                            """
                        )
                    with p3:
                        st.markdown(
                            f"""
                            - **SonarQube Analizi:** `{pr.get('code_quality_static_analysis', '-')}`
                            - **Güvenlik (SAST/CVE):** `{pr.get('security_scan_status', '-')}`
                            - **XAI PR Nedeni:** {pr.get('xai_pr_risk_factors', '-')}
                            - **XAI PR Önerisi:** **{pr.get('xai_pr_recommended_actions', '-')}**
                            """
                        )
                    st.code(str(pr.get("diff_code_changes", "")), language="diff")
                    st.caption(f"💬 **Reviewer Yorumu:** {pr.get('review_comments', '-')}")

    # ===========================================================================
    # SEKME 4: WHAT-IF (KARŞI-OLGUSAL) KARAR SİMÜLATÖRÜ
    # ===========================================================================
    with tabs[3]:
        st.subheader("🎛️ Karşı-Olgusal (Counterfactual / What-If) XAI Karar Destek Simülatörü")
        st.markdown(
            "Yönetici veya Scrum Master olarak bir görevin parametrelerine **erken müdahale ettiğinizde** "
            "(örneğin blocker'ları çözdüğünüzde, büyük görevi alt görevlere böldüğünüzde veya PR boyutunu küçülttüğünüzde) "
            "**teslimat risk skorunun ve gecikme gününün nasıl düşeceğini** canlı olarak simüle edin."
        )

        sim_base_row = df_combined[df_combined["task_id"] == selected_task_id].iloc[0]

        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.markdown("##### 1️⃣ Görev Kapsam & Planlama Ayarları")
            sim_sp = st.select_slider(
                "Karmaşıklık Puanı (Story Points)",
                options=[1, 2, 3, 5, 8, 13, 21],
                value=int(sim_base_row.get("story_points", 8)) if int(sim_base_row.get("story_points", 8)) in [1, 2, 3, 5, 8, 13, 21] else 8,
            )
            sim_est_hours = st.slider(
                "Tahmini Efor (Saat)",
                min_value=2.0,
                max_value=160.0,
                value=float(np.nan_to_num(sim_base_row.get("estimated_effort_hours", 32.0), nan=32.0)),
                step=2.0,
            )
            sim_prio = st.selectbox(
                "Görev Önceliği (Priority)",
                options=["Low", "Medium", "High", "Critical"],
                index=["Low", "Medium", "High", "Critical"].index(str(sim_base_row.get("priority", "High")))
                if str(sim_base_row.get("priority", "High")) in ["Low", "Medium", "High", "Critical"]
                else 2,
            )
            sim_env = st.selectbox(
                "Hedef Ortam (Environment)",
                options=["Development", "Test", "Staging", "Production"],
                index=["Development", "Test", "Staging", "Production"].index(str(sim_base_row.get("environment", "Production")))
                if str(sim_base_row.get("environment", "Production")) in ["Development", "Test", "Staging", "Production"]
                else 3,
            )

        with sc2:
            st.markdown("##### 2️⃣ Süreç Darboğazları & Bağımlılıklar")
            sim_blockers = st.slider(
                "Blocker (Engel) Sayısı",
                min_value=0,
                max_value=5,
                value=int(sim_base_row.get("blocker_count", 1)),
            )
            sim_deps = st.slider(
                "Ön Koşul Görev Bağımlılığı (Dependencies)",
                min_value=0,
                max_value=5,
                value=int(sim_base_row.get("dependency_count_calc", 1)),
            )
            sim_scope_creep = st.slider(
                "Kapsam Değişikliği / Scope Creep Sayısı",
                min_value=0,
                max_value=5,
                value=int(sim_base_row.get("requirement_change_count", 1)),
            )
            sim_reopen = st.slider(
                "Testten Geri Dönüş (Reopen) Sayısı",
                min_value=0,
                max_value=4,
                value=int(sim_base_row.get("reopen_count", 0)),
            )

        with sc3:
            st.markdown("##### 3️⃣ PR & CI/CD Entegrasyon Kalitesi")
            sim_pr_files = st.slider(
                "PR Değişen Dosya Sayısı",
                min_value=1,
                max_value=50,
                value=max(1, int(sim_base_row.get("pr_total_changed_files", 12))),
            )
            sim_pr_lines = st.slider(
                "PR Eklenen Kod Satırı (+Lines)",
                min_value=10,
                max_value=2500,
                value=max(10, int(sim_base_row.get("pr_total_lines_added", 350))),
                step=25,
            )
            sim_conflict = st.checkbox(
                "PR Merge Çakışması (Conflict) Var mı?",
                value=bool(sim_base_row.get("pr_conflict_count", 0) > 0),
            )
            sim_ci_fails = st.slider(
                "CI/CD Pipeline Kırılma Sayısı",
                min_value=0,
                max_value=6,
                value=int(sim_base_row.get("pr_total_ci_failures", 1)),
            )

        # Simülasyon Hesaplaması
        sim_opt_ratio = np.clip((sim_sp * 6.5) / (sim_est_hours + 1.0), 0.5, 3.0)
        sim_p_w = 14.0 if sim_prio == "Critical" else (8.0 if sim_prio == "High" else 2.0)
        sim_e_w = 10.0 if sim_env == "Production" else (5.0 if sim_env == "Staging" else 1.0)

        sim_task_risk = np.clip(
            sim_deps * 9.5
            + sim_blockers * 16.0
            + sim_scope_creep * 11.5
            + sim_reopen * 12.0
            + (14.0 if sim_sp >= 13 else 0.0)
            + (13.0 if sim_opt_ratio > 1.25 else 0.0)
            + sim_p_w
            + sim_e_w
            + 8.0,
            2.0,
            99.5,
        )
        sim_pr_risk = np.clip(
            sim_pr_files * 1.45
            + sim_pr_lines / 45.0
            + (18.0 if sim_conflict else 0.0)
            + sim_ci_fails * 8.5
            + sim_task_risk * 0.22
            + 6.0,
            1.0,
            99.5,
        )
        sim_combined_risk = round(sim_task_risk * 0.62 + sim_pr_risk * 0.38, 1)
        sim_delay_prob = round(float(1.0 / (1.0 + np.exp(-(sim_combined_risk - 47.0) / 11.5))) * 100.0, 1)
        sim_est_delay_days = max(0, int(round((sim_combined_risk - 42.0) * 0.32 + sim_blockers * 2.5))) if sim_combined_risk >= 45.0 else 0

        orig_risk = float(sim_base_row["combined_early_risk_score"])
        orig_prob = round(float(sim_base_row["early_delay_probability"]) * 100.0, 1)
        orig_delay = max(0, int(sim_base_row.get("delay_days", 0)))

        st.markdown("---")
        res1, res2, res3, res4 = st.columns(4)
        res1.metric(
            "Simüle Edilen Bileşik Risk Skoru",
            f"{sim_combined_risk:.1f} / 100",
            delta=f"{sim_combined_risk - orig_risk:+.1f} puan (Referans: {orig_risk:.1f})",
            delta_color="inverse",
        )
        res2.metric(
            "Simüle Edilen Gecikme Olasılığı",
            f"%{sim_delay_prob:.1f}",
            delta=f"{sim_delay_prob - orig_prob:+.1f}% (Referans: %{orig_prob:.1f})",
            delta_color="inverse",
        )
        res3.metric(
            "Tahmini Gecikme Süresi",
            f"{sim_est_delay_days} Gün",
            delta=f"{sim_est_delay_days - orig_delay:+d} gün (Referans: {orig_delay} gün)",
            delta_color="inverse",
        )
        sim_level = (
            "Critical" if sim_combined_risk >= 75 else ("High" if sim_combined_risk >= 50 else ("Medium" if sim_combined_risk >= 28 else "Low"))
        )
        res4.metric("Yeni Risk Sınıfı", sim_level)

        # Otomatik Karşı-Olgusal Reçete
        prescriptions = []
        if sim_blockers > 0:
            prescriptions.append(f"🔴 **{sim_blockers} adet Blocker** kaldırılırsa risk skoru doğrudan **-{sim_blockers*10:.1f} puan** düşer.")
        if sim_sp >= 13 or sim_opt_ratio > 1.25:
            prescriptions.append(
                f"🟠 **Görev Karmaşıklığı / Efor Dengesi:** Görev {sim_sp} SP boyutunda veya efor tahmini iyimser (Oran: {sim_opt_ratio:.2f}). Görevi 5 SP'lik alt görevlere bölmek veya tahmini eforu **{int(sim_sp*6)} saate** güncellemek riski **~14 puan** azaltır."
            )
        if sim_conflict or sim_ci_fails > 0 or sim_pr_files >= 18:
            prescriptions.append(
                "🔵 **PR & CI/CD Müdahalesi:** PR dosya sayısını 10'un altına çekmek, hedef branch ile günlük rebase yapmak ve yerel pre-commit testlerini çalıştırmak PR entegrasyon riskini **-%35** oranında iyileştirir."
            )
        if not prescriptions:
            prescriptions.append("🟢 **Optimum Konfigürasyon:** Mevcut parametrelerle görev düşük risk bölgesindedir ve zamanında teslimat beklenmektedir.")

        st.markdown("#### 🩺 XAI Karşı-Olgusal (Counterfactual) Karar Reçetesi")
        for p_item in prescriptions:
            st.markdown(f"- {p_item}")

    # ===========================================================================
    # SEKME 5: PULL REQUEST (PR), KOD İNCELEME & CI/CD ANALİTİĞİ
    # ===========================================================================
    with tabs[4]:
        st.subheader("🔀 Pull Request (PR), Kod Kalitesi ve CI/CD Entegrasyon Darboğaz Analizi")
        if len(filtered_prs) == 0:
            st.warning("Görüntülenecek PR verisi bulunamadı. Lütfen PR veri setini yükleyin.")
        else:
            p_col1, p_col2, p_col3, p_col4 = st.columns(4)
            p_col1.metric("Toplam İncelenen PR", f"{len(filtered_prs):,}")
            p_col2.metric(
                "Merge Çakışması (Conflict) Oranı",
                f"%{filtered_prs['has_conflict_bool'].mean()*100:.1f}",
                f"{int(filtered_prs['has_conflict_bool'].sum()):,} PR Çakışmalı",
            )
            p_col3.metric(
                "Ort. Birleştirme Süresi (Time to Merge)",
                f"{filtered_prs['time_to_merge_hours'].mean():.1f} Saat",
                f"İlk İnceleme: {filtered_prs['time_to_first_review_hours'].mean():.1f} sa",
            )
            p_col4.metric(
                "CI/CD Pipeline Hata Oranı",
                f"%{(filtered_prs['ci_failure_count'] > 0).mean()*100:.1f}",
                f"Ort. {filtered_prs['review_iteration_count'].mean():.1f} İnceleme Turu",
            )

            ch1, ch2 = st.columns(2)
            with ch1:
                st.markdown("#### ⚔️ Merge Conflict Durumuna Göre PR Risk ve Birleştirme Süresi")
                conf_summary = (
                    filtered_prs.groupby("merge_conflicts", as_index=False)
                    .agg(
                        PR_Sayisi=("pr_id", "count"),
                        Ort_PR_Risk=("pr_risk_score", "mean"),
                        Ort_Merge_Suresi_Saat=("time_to_merge_hours", "mean"),
                    )
                )
                conf_summary["Ort_PR_Risk"] = conf_summary["Ort_PR_Risk"].round(1)
                conf_summary["Ort_Merge_Suresi_Saat"] = conf_summary["Ort_Merge_Suresi_Saat"].round(1)

                c_conf = (
                    alt.Chart(conf_summary)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                    .encode(
                        y=alt.Y("merge_conflicts:N", sort="-x", title="Merge Çakışma Durumu"),
                        x=alt.X("Ort_Merge_Suresi_Saat:Q", title="Ortalama Birleştirme Süresi (Saat)"),
                        color=alt.Color("Ort_PR_Risk:Q", title="Ort. PR Riski", scale=alt.Scale(scheme="orangered")),
                        tooltip=["merge_conflicts:N", "PR_Sayisi:Q", "Ort_Merge_Suresi_Saat:Q", "Ort_PR_Risk:Q"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(c_conf, width="stretch")

            with ch2:
                st.markdown("#### 🛡️ Güvenlik Taraması (SAST/CVE) & SonarQube Kalite Kapısı Dağılımı")
                sec_summary = (
                    filtered_prs.groupby(["security_scan_status", "pr_risk_level"], as_index=False)
                    .size()
                    .rename(columns={"size": "PR Sayısı"})
                )
                c_sec = (
                    alt.Chart(sec_summary)
                    .mark_bar()
                    .encode(
                        y=alt.Y("security_scan_status:N", sort="-x", title="Güvenlik Taraması Durumu"),
                        x=alt.X("PR Sayısı:Q", title="PR Sayısı"),
                        color=alt.Color(
                            "pr_risk_level:N",
                            title="PR Risk Sınıfı",
                            scale=alt.Scale(domain=RISK_ORDER, range=[RISK_COLORS[k] for k in RISK_ORDER]),
                        ),
                        tooltip=["security_scan_status:N", "pr_risk_level:N", "PR Sayısı:Q"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(c_sec, width="stretch")

            # Geliştirici İş Yükü ve Darboğaz Analizi
            st.markdown("#### 👨‍💻 Geliştirici / Sorumlu Bazlı Görev ve PR Risk Yükü Matrisi")
            dev_df = (
                filtered_tasks.groupby("assignee", as_index=False)
                .agg(
                    Toplam_Gorev=("task_id", "count"),
                    Acik_Gorev=("status", lambda s: int((s != "Done").sum())),
                    Kritik_Yuksek_Riskli=("delivery_risk_level", lambda s: int(s.isin(["Critical", "High"]).sum())),
                    Ort_Erken_Risk=("combined_early_risk_score", "mean"),
                    Gecikme_Orani_Yuzde=("is_delayed", lambda s: round(s.mean() * 100.0, 1)),
                    Toplam_PR_Conflict=("pr_conflict_count", "sum"),
                )
                .sort_values("Kritik_Yuksek_Riskli", ascending=False)
            )
            dev_df["Ort_Erken_Risk"] = dev_df["Ort_Erken_Risk"].round(1)
            st.dataframe(dev_df, width="stretch", hide_index=True)

    # ===========================================================================
    # SEKME 6: BÜYÜK VERİ (5V) MİMARİSİ, VERACITY VE CANLI AKIŞ (VELOCITY)
    # ===========================================================================
    with tabs[5]:
        st.subheader("🏛️ Büyük Veri (5V) Mimarisi, Veracity (Veri Kalitesi) ve Canlı Akış (Velocity)")

        v1, v2 = st.columns(2)
        with v1:
            st.markdown("#### 🔍 4V - Veracity (Veri Doğruluğu ve Kusur Dağılımı - Task)")
            t_veracity = (
                df_tasks["data_quality_flag"]
                .value_counts()
                .reset_index()
            )
            t_veracity.columns = ["Veri Kalitesi Etiketi (data_quality_flag)", "Kayıt Sayısı"]
            t_veracity["Oran (%)"] = np.round(t_veracity["Kayıt Sayısı"] / len(df_tasks) * 100.0, 2)
            st.dataframe(t_veracity, width="stretch", hide_index=True)

        with v2:
            st.markdown("#### 🔍 4V - Veracity (Veri Doğruluğu ve Kusur Dağılımı - PR)")
            if df_prs is not None and len(df_prs) > 0:
                p_veracity = (
                    df_prs["data_quality_flag"]
                    .value_counts()
                    .reset_index()
                )
                p_veracity.columns = ["PR Veri Kalitesi Etiketi (data_quality_flag)", "Kayıt Sayısı"]
                p_veracity["Oran (%)"] = np.round(p_veracity["Kayıt Sayısı"] / len(df_prs) * 100.0, 2)
                st.dataframe(p_veracity, width="stretch", hide_index=True)

        st.markdown("---")
        st.markdown("#### ⚡ 2V - Velocity: Gerçek Zamanlı CI/CD & PR Olay Akışı (Streaming Event Radar)")
        stream_df = load_velocity_stream(os.path.join(DEFAULT_DATA_DIR, "velocity_event_stream.jsonl"), limit=500)
        if len(stream_df) > 0:
            sc_a, sc_b, sc_c = st.columns(3)
            high_delta_events = stream_df[stream_df["payload.instant_risk_delta"] > 15.0]
            sc_a.metric("Okunan Canlı Akış Olayı", f"{len(stream_df):,} Olay (JSONL Stream)")
            sc_b.metric("Anlık Kritik Risk Sıçraması (> +15 Puan)", f"{len(high_delta_events):,} Olay")
            sc_c.metric("Ortalama Anlık Risk Değişimi", f"{stream_df['payload.instant_risk_delta'].mean():+.2f} Puan")

            st.dataframe(
                stream_df.sort_values("payload.instant_risk_delta", ascending=False).head(20)[
                    [
                        "event_id",
                        "event_timestamp",
                        "event_type",
                        "project_code",
                        "component",
                        "task_id",
                        "pr_id",
                        "actor",
                        "payload.instant_risk_delta",
                        "payload.xai_alert_reason",
                    ]
                ].rename(
                    columns={
                        "event_id": "Olay ID",
                        "event_timestamp": "Zaman Damgası",
                        "event_type": "Olay Türü",
                        "project_code": "Proje",
                        "component": "Bileşen",
                        "task_id": "Görev ID",
                        "pr_id": "PR ID",
                        "actor": "Geliştirici",
                        "payload.instant_risk_delta": "Anlık Risk Deltası",
                        "payload.xai_alert_reason": "XAI Akış Uyarısı",
                    }
                ),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("`data/velocity_event_stream.jsonl` dosyası bulunamadı veya boş.")

    # ===========================================================================
    # SEKME 7: KARAR DESTEK RAPORU VE DIŞA AKTARIM (EXPORT)
    # ===========================================================================
    with tabs[6]:
        st.subheader("📥 Erken Müdahale Aksiyon Planı ve Karar Destek Raporu İndir")
        st.markdown(
            "Seçili filtrelere göre **erken müdahale gerektiren yüksek riskli görevlerin** ve **XAI aksiyon önerilerinin** "
            "tam listesini yönetim toplantıları veya Jira/DevOps entegrasyonu için dışa aktarın."
        )

        export_df = (
            filtered_tasks.sort_values("combined_early_risk_score", ascending=False)
            .head(2000)[
                [
                    "task_id",
                    "task_title",
                    "project",
                    "component_module",
                    "status",
                    "priority",
                    "assignee",
                    "due_date",
                    "combined_early_risk_score",
                    "delivery_risk_level",
                    "early_delay_probability",
                    "blocker_count",
                    "pr_conflict_count",
                    "pr_total_ci_failures",
                    "xai_primary_risk_factors",
                    "xai_recommended_actions",
                ]
            ]
        )

        ex1, ex2 = st.columns(2)
        with ex1:
            csv_bytes = export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                label="📊 XAI Erken Risk ve Aksiyon Raporunu İndir (.CSV)",
                data=csv_bytes,
                file_name="xai_erken_risk_karar_destek_raporu.csv",
                mime="text/csv",
                width="stretch",
            )
        with ex2:
            json_bytes = export_df.to_json(orient="records", force_ascii=False, indent=2).encode("utf-8")
            st.download_button(
                label="📄 XAI Erken Risk ve Aksiyon Raporunu İndir (.JSON)",
                data=json_bytes,
                file_name="xai_erken_risk_karar_destek_raporu.json",
                mime="application/json",
                width="stretch",
            )

        st.dataframe(export_df.head(50), width="stretch", hide_index=True)


if __name__ == "__main__":
    main()

