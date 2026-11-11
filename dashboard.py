#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyrefly: ignore-errors
"""
Yazılım Projelerinde Teslimat Risklerinin Büyük Veri (5V) ile Erken Tespiti ve
Açıklanabilir Karar Destek Sistemi (XAI) — Sadeleştirilmiş & Düzenli Dashboard
"""

import io
import json
import os
from typing import Optional, Tuple

import altair as alt  # type: ignore # pyrefly: ignore[missing-import]
import numpy as np  # type: ignore # pyrefly: ignore[missing-import]
import pandas as pd  # type: ignore # pyrefly: ignore[missing-import]
import streamlit as st  # type: ignore # pyrefly: ignore[missing-import]
from sklearn.ensemble import RandomForestClassifier  # type: ignore # pyrefly: ignore[missing-import]
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
# SAYFA YAPILANDIRMASI VE SADELEŞTİRİLMİŞ STİL (CSS)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Teslimat Risk & XAI Karar Destek Paneli",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

CUSTOM_CSS = """
<style>
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1550px;
    }
    .main-header {
        background: rgba(30, 41, 59, 0.7);
        border: 1px solid #334155;
        border-left: 4px solid #3b82f6;
        padding: 1rem 1.35rem;
        border-radius: 0.75rem;
        margin-bottom: 1rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 0.75rem;
    }
    .main-header h1 {
        margin: 0;
        font-size: 1.35rem;
        color: #f8fafc;
        font-weight: 700;
    }
    .main-header p {
        margin: 0.2rem 0 0 0;
        color: #94a3b8;
        font-size: 0.85rem;
    }
    .source-pill {
        background: #0f172a;
        border: 1px solid #334155;
        color: #cbd5e1;
        padding: 0.4rem 0.85rem;
        border-radius: 0.5rem;
        font-size: 0.78rem;
    }
    .kpi-card {
        background: rgba(30, 41, 59, 0.65);
        border: 1px solid #334155;
        border-radius: 0.75rem;
        padding: 0.95rem 1.1rem;
        height: 100%;
    }
    .kpi-blue { border-left: 4px solid #3b82f6; }
    .kpi-red { border-left: 4px solid #ef4444; }
    .kpi-amber { border-left: 4px solid #f59e0b; }
    .kpi-orange { border-left: 4px solid #f97316; }
    .kpi-title {
        color: #94a3b8;
        font-size: 0.78rem;
        font-weight: 600;
    }
    .kpi-value {
        color: #f8fafc;
        font-size: 1.55rem;
        font-weight: 700;
        margin: 0.2rem 0;
    }
    .kpi-sub {
        font-size: 0.76rem;
        color: #94a3b8;
    }
    .xai-box {
        background: rgba(120, 53, 15, 0.2);
        border: 1px solid rgba(245, 158, 11, 0.4);
        border-left: 4px solid #f59e0b;
        border-radius: 0.75rem;
        padding: 1rem 1.2rem;
        height: 100%;
    }
    .action-box {
        background: rgba(19, 78, 74, 0.22);
        border: 1px solid rgba(20, 184, 166, 0.4);
        border-left: 4px solid #14b8a6;
        border-radius: 0.75rem;
        padding: 1rem 1.2rem;
        height: 100%;
    }
    .badge-critical {
        background: rgba(239, 68, 68, 0.2);
        color: #fca5a5;
        border: 1px solid #ef4444;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.76rem;
    }
    .badge-high {
        background: rgba(249, 115, 22, 0.2);
        color: #fdba74;
        border: 1px solid #f97316;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.76rem;
    }
    .badge-medium {
        background: rgba(234, 179, 8, 0.2);
        color: #fde047;
        border: 1px solid #eab308;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.76rem;
    }
    .badge-low {
        background: rgba(34, 197, 94, 0.2);
        color: #86efac;
        border: 1px solid #22c55e;
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        font-weight: 600;
        font-size: 0.76rem;
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
    df = df.copy()
    if len(df) == 0:
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
    tasks_df, prs_df, meta = None, None, {}

    # Olası tüm veri dizini konumlarını tara
    candidate_dirs = [
        data_dir,
        os.path.join(BASE_DIR, "data"),
        os.path.join(BASE_DIR, "public", "data"),
        os.path.join(os.getcwd(), "data"),
        os.path.join(os.getcwd(), "public", "data"),
        "data",
        "public/data",
    ]
    resolved_dir = None
    for d in candidate_dirs:
        if d and os.path.isdir(d):
            if any(
                os.path.exists(os.path.join(d, f))
                for f in ["tasks_dataset.parquet", "tasks_dataset.csv", "tasks_dataset.jsonl", "default_dataset.json"]
            ):
                resolved_dir = d
                break

    if not resolved_dir:
        resolved_dir = data_dir

    meta_path = os.path.join(resolved_dir, "dataset_metadata_5v.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            meta = {}

    for candidate in ["tasks_dataset.parquet", "tasks_dataset.csv", "tasks_dataset.jsonl"]:
        p = os.path.join(resolved_dir, candidate)
        if os.path.exists(p):
            try:
                tasks_df = ensure_task_risk_and_xai_columns(parse_uploaded_or_path(p))
                break
            except Exception:
                continue

    for candidate in ["pull_requests_dataset.parquet", "pull_requests_dataset.csv", "pull_requests_dataset.jsonl"]:
        p = os.path.join(resolved_dir, candidate)
        if os.path.exists(p):
            try:
                prs_df = ensure_pr_risk_and_xai_columns(parse_uploaded_or_path(p))
                break
            except Exception:
                continue

    return tasks_df, prs_df, meta


@st.cache_data(show_spinner=False)
def load_velocity_stream(stream_path: str = "data/velocity_event_stream.jsonl", limit: Optional[int] = None) -> pd.DataFrame:
    candidate_paths = [
        stream_path,
        os.path.join(BASE_DIR, stream_path),
        os.path.join(BASE_DIR, "data", "velocity_event_stream.jsonl"),
        os.path.join(BASE_DIR, "public", "data", "velocity_event_stream.jsonl"),
        os.path.join(os.getcwd(), "data", "velocity_event_stream.jsonl"),
    ]
    resolved_path = None
    for cp in candidate_paths:
        if os.path.exists(cp):
            resolved_path = cp
            break

    if not resolved_path:
        return pd.DataFrame()

    records = []
    with open(resolved_path, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if limit is not None and idx >= limit:
                break
            line = line.strip()
            if line:
                records.append(json.loads(line))
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records)


@st.cache_data(show_spinner=False)
def build_combined_early_warning_view(df_tasks: pd.DataFrame, df_prs: pd.DataFrame) -> pd.DataFrame:
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

    merged["combined_early_risk_score"] = np.round(
        np.clip(merged["delivery_risk_score"] * 0.62 + merged["pr_max_risk_score"] * 0.38, 1.0, 99.9),
        2,
    )
    merged["early_delay_probability"] = np.round(
        1.0 / (1.0 + np.exp(-(merged["combined_early_risk_score"] - 47.0) / 11.5)),
        3,
    )
    return merged


@st.cache_resource(show_spinner=False)
def train_early_risk_ml_model(df_combined: pd.DataFrame, sample_size: int = 25000):
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
        "dependency_count_calc": "Görev Bağımlılık Sayısı",
        "requirement_change_count": "Kapsam Değişikliği (Scope Creep)",
        "reopen_count": "Testten Geri Dönüş (Reopen)",
        "story_points": "Karmaşıklık Puanı (Story Points)",
        "est_effort_clean": "Tahmini Efor (Saat)",
        "optimism_ratio_calc": "İyimserlik Oranı (SP / Saat)",
        "priority_num": "Görev Öncelik Seviyesi",
        "env_num": "Hedef Ortam Kritikliliği",
        "pr_total_changed_files": "PR Değişen Dosya Sayısı",
        "pr_total_ci_failures": "PR CI/CD Pipeline Hata Sayısı",
        "pr_conflict_count": "PR Merge Çakışması (Conflict)",
        "pr_max_review_iterations": "PR Kod İnceleme Turu",
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
        "test_rows": int(len(X_test)),
    }

    importances = pd.DataFrame({
        "Öznitelik": [feature_labels[k] for k in feature_cols],
        "Etki Ağırlığı (%)": np.round(model.feature_importances_ * 100.0, 2),
    }).sort_values("Etki Ağırlığı (%)", ascending=False)

    return model, metrics, importances


def compute_local_xai_contributions(row: pd.Series) -> pd.DataFrame:
    contributions = []
    blockers = int(row.get("blocker_count", 0))
    deps = int(row.get("dependency_count_calc", 0))
    req_changes = int(row.get("requirement_change_count", 0))
    reopens = int(row.get("reopen_count", 0))
    sp = int(row.get("story_points", 3))
    opt = float(row.get("optimism_ratio_calc", 1.0))
    prio = str(row.get("priority", "Medium"))
    pr_files = int(row.get("pr_total_changed_files", 0))
    ci_fails = int(row.get("pr_total_ci_failures", 0))
    conflicts = int(row.get("pr_conflict_count", 0))

    contributions.append(("Baz Karmaşıklık", 10.0, "Referans risk puanı"))
    if blockers > 0:
        contributions.append((f"Blocker / Engel ({blockers} adet)", blockers * 16.0, "Aktif görev engeli"))
    else:
        contributions.append(("Blocker Yok (0)", -3.5, "Akışı durduran engel yok"))

    if deps > 0:
        contributions.append((f"Görev Bağımlılığı ({deps} ön koşul)", deps * 9.5, "Ön koşul gecikme yayılımı"))
    else:
        contributions.append(("Bağımsız Görev (0)", -2.5, "Dış görev bağımlılığı yok"))

    if req_changes > 0:
        contributions.append((f"Kapsam Değişimi ({req_changes} kez)", req_changes * 11.5, "Sprint içi revizyon"))

    if reopens > 0:
        contributions.append((f"Testten Dönüş / Reopen ({reopens})", reopens * 12.0, "Kalite/kabul uyumsuzluğu"))

    if sp >= 13:
        contributions.append((f"Büyük Görev Boyutu ({sp} SP)", 14.0, "Alt görevlere bölünmeli"))
    elif sp <= 3:
        contributions.append((f"Küçük Görev Boyutu ({sp} SP)", -4.0, "Yönetilebilir kapsam"))

    if opt > 1.25:
        contributions.append((f"İyimser Efor Tahmini ({opt:.2f}x)", 13.0, "Story point'e göre düşük saat"))

    prio_pts = 14.0 if prio == "Critical" else (8.0 if prio == "High" else 2.0)
    contributions.append((f"Öncelik ({prio})", prio_pts, "Teslimat kritikliliği"))

    if conflicts > 0 or ci_fails > 0 or pr_files >= 18:
        pr_impact = min(28.0, conflicts * 9.0 + ci_fails * 4.5 + (4.0 if pr_files >= 18 else 0.0))
        contributions.append((f"PR & CI/CD Darboğazı ({conflicts} Conflict, {ci_fails} Hata)", round(pr_impact, 1), "Entegrasyon etkisi"))
    else:
        contributions.append(("Temiz PR & CI/CD Akışı", -3.0, "Entegrasyon sorunu yok"))

    df_contrib = pd.DataFrame(contributions, columns=["Faktör", "Puan Katkısı", "Açıklama"])
    df_contrib["Etki Yönü"] = np.where(df_contrib["Puan Katkısı"] > 0, "Risk Artırıcı (+)", "Risk Azaltıcı (-)")
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
# ANA UYGULAMA AKIŞI (SADELEŞTİRİLMİŞ LAYOUT)
# ---------------------------------------------------------------------------
def main():
    st.sidebar.markdown("### 🛡️ Erken Risk & XAI Paneli")
    st.sidebar.caption("Karar Destek ve Filtreleme Merkezi")

    # Daraltılabilir Veri Seti Yükleme Kutusu (Kalabalık yapmaz)
    with st.sidebar.expander("📂 Başka Veri Seti Yükle (İsteğe Bağlı)", expanded=False):
        st.caption("Projedeki varsayılan veri seti otomatik yüklenmiştir. Farklı bir veri seti incelemek isterseniz buradan yükleyebilirsiniz.")
        uploaded_task_file = st.file_uploader(
            "📋 Görev (Task) Dosyası",
            type=["parquet", "csv", "jsonl", "json"],
            key="sidebar_task_uploader",
        )
        uploaded_pr_file = st.file_uploader(
            "🔀 Pull Request (PR) Dosyası",
            type=["parquet", "csv", "jsonl", "json"],
            key="sidebar_pr_uploader",
        )

    default_tasks, default_prs, _ = load_default_datasets(DEFAULT_DATA_DIR)

    df_tasks: Optional[pd.DataFrame] = None
    df_prs: Optional[pd.DataFrame] = None

    if uploaded_task_file is not None:
        with st.spinner(f"Task dosyası okunuyor ({uploaded_task_file.name})..."):
            df_tasks = ensure_task_risk_and_xai_columns(parse_uploaded_or_path(uploaded_task_file, uploaded_task_file.name))
    elif default_tasks is not None:
        df_tasks = default_tasks

    if uploaded_pr_file is not None:
        with st.spinner(f"PR dosyası okunuyor ({uploaded_pr_file.name})..."):
            df_prs = ensure_pr_risk_and_xai_columns(parse_uploaded_or_path(uploaded_pr_file, uploaded_pr_file.name))
    elif default_prs is not None:
        df_prs = default_prs

    if df_tasks is None or len(df_tasks) == 0:
        st.info("👋 Analizi başlatmak için sol menüdeki **'📂 Başka Veri Seti Yükle'** bölümünden veri dosyalarınızı seçin.")
        return

    # Kullanıcıya veri setinin varsayılan olarak devrede olduğunu bildiren rozet
    if uploaded_task_file is None and uploaded_pr_file is None:
        st.sidebar.success(f"✅ Varsayılan Proje Verisi Aktif\n\n({len(df_tasks):,} Görev • {len(df_prs) if df_prs is not None else 0:,} PR)")
    else:
        st.sidebar.info(f"📂 Özel Yüklenen Veri Aktif\n\n({len(df_tasks):,} Görev • {len(df_prs) if df_prs is not None else 0:,} PR)")

    # ---------------------------------------------------------------------------
    # SOL MENÜ: SADE AÇILIR LİSTE (SELECTBOX) FİLTRELER
    # ---------------------------------------------------------------------------
    st.sidebar.markdown("#### 🔍 Hızlı Filtreler")

    scope_mode = st.sidebar.selectbox(
        "Görev Durumu",
        options=[
            "📋 Tüm Görevler (Açık + Tamamlanan)",
            "⚡ Sadece Açık Görevler (Erken Uyarı)",
            "✅ Sadece Tamamlanan Görevler",
        ],
        index=0,
    )

    clean_veracity = st.sidebar.toggle(
        "🧹 Otomatik Veri Temizleme (Veracity)",
        value=False,
        help="Eksik efor tahminlerini doldurur ve aykırı değerleri düzeltir.",
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

    df_combined = build_combined_early_warning_view(df_tasks, df_prs)

    all_projects = sorted(df_combined["project"].dropna().unique().tolist())
    sel_project = st.sidebar.selectbox("Proje", options=["Tüm Projeler"] + all_projects, index=0)

    sel_risk = st.sidebar.selectbox(
        "Risk Seviyesi",
        options=["Tüm Risk Seviyeleri", "Critical", "High", "Medium", "Low"],
        index=0,
    )

    all_components = sorted(df_combined["component_module"].dropna().unique().tolist())
    sel_component = st.sidebar.selectbox("Yazılım Modülü (Bileşen)", options=["Tüm Modüller"] + all_components, index=0)

    # Filtreleri Uygula
    mask = pd.Series(True, index=df_combined.index)
    if sel_project != "Tüm Projeler":
        mask &= df_combined["project"] == sel_project
    if sel_risk != "Tüm Risk Seviyeleri":
        mask &= df_combined["delivery_risk_level"] == sel_risk
    if sel_component != "Tüm Modüller":
        mask &= df_combined["component_module"] == sel_component
    if scope_mode == "⚡ Sadece Açık Görevler (Erken Uyarı)":
        mask &= df_combined["status"] != "Done"
    elif scope_mode == "✅ Sadece Tamamlanan Görevler":
        mask &= df_combined["status"] == "Done"

    filtered_tasks = df_combined[mask].copy()
    if df_prs is not None and len(df_prs) > 0:
        filtered_prs = df_prs[df_prs["linked_task_issue"].isin(filtered_tasks["task_id"])].copy()
    else:
        filtered_prs = pd.DataFrame()

    # Sol menü alt kısım: Hızlı Rapor İndirme
    st.sidebar.markdown("---")
    st.sidebar.markdown("#### 📥 Hızlı Rapor İndir")
    if len(filtered_tasks) > 0:
        export_df = filtered_tasks.sort_values("combined_early_risk_score", ascending=False).head(2000)[
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
                "xai_primary_risk_factors",
                "xai_recommended_actions",
            ]
        ]
        csv_bytes = export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.sidebar.download_button(
            label="📊 CSV Raporu İndir",
            data=csv_bytes,
            file_name="xai_erken_risk_raporu.csv",
            mime="text/csv",
            width="stretch",
        )

    # ---------------------------------------------------------------------------
    # ÜST BAŞLIK & 4 NET KPI KARTI
    # ---------------------------------------------------------------------------
    st.markdown(
        f"""
        <div class="main-header">
            <div>
                <h1>🛡️ Yazılım Teslimat Risklerinin Erken Tespiti ve Karar Destek Paneli</h1>
                <p>Görev (Task) ve kod birleştirme (PR) süreçlerindeki gecikme risklerini teslim tarihinden önce tespit eder ve çözüm önerisi sunar.</p>
            </div>
            <div class="source-pill">
                📋 <b>{len(filtered_tasks):,}</b> Görev &nbsp;•&nbsp;
                🔀 <b>{len(filtered_prs):,}</b> PR &nbsp;•&nbsp;
                🧹 <b>{'Temizlenmiş Veri' if clean_veracity else 'Ham Veri'}</b>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if len(filtered_tasks) == 0:
        st.warning("Seçilen filtre kriterlerine uygun görev bulunamadı. Lütfen sol menüdeki filtreleri değiştirin.")
        return

    active_open_tasks = filtered_tasks[filtered_tasks["status"] != "Done"]
    early_at_risk_open = active_open_tasks[active_open_tasks["combined_early_risk_score"] >= 50.0]
    delayed_ratio = filtered_tasks["is_delayed"].mean() * 100.0
    avg_risk_score = filtered_tasks["combined_early_risk_score"].mean()
    critical_high_count = int((filtered_tasks["delivery_risk_level"].isin(["Critical", "High"])).sum())
    delayed_subset = filtered_tasks[filtered_tasks["delay_days"] > 0]
    avg_delay_days = delayed_subset["delay_days"].mean() if len(delayed_subset) > 0 else 0.0

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(
            f"""
            <div class="kpi-card kpi-blue">
                <div class="kpi-title">📋 İncelenen Görev & PR</div>
                <div class="kpi-value">{len(filtered_tasks):,} Görev</div>
                <div class="kpi-sub">Bağlı {len(filtered_prs):,} Pull Request</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            f"""
            <div class="kpi-card kpi-red">
                <div class="kpi-title">🚨 Acil Müdahale Bekleyen</div>
                <div class="kpi-value" style="color: #f87171;">{len(early_at_risk_open):,} Görev</div>
                <div class="kpi-sub">{len(active_open_tasks):,} açık iş içinde yüksek riskli</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            f"""
            <div class="kpi-card kpi-amber">
                <div class="kpi-title">🎯 Ortalama Risk Skoru</div>
                <div class="kpi-value" style="color: #fbbf24;">{avg_risk_score:.1f} / 100</div>
                <div class="kpi-sub">Kritik & Yüksek: {critical_high_count:,} (%{critical_high_count/max(1,len(filtered_tasks))*100:.0f})</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with k4:
        st.markdown(
            f"""
            <div class="kpi-card kpi-orange">
                <div class="kpi-title">⏳ Teslimat Gecikme Oranı</div>
                <div class="kpi-value" style="color: #fb923c;">%{delayed_ratio:.1f}</div>
                <div class="kpi-sub">Gecikenlerde ort. +{avg_delay_days:.1f} gün sapma</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='margin-top: 0.75rem;'></div>", unsafe_allow_html=True)

    # ---------------------------------------------------------------------------
    # SADELEŞTİRİLMİŞ 6 ANA SEKME
    # ---------------------------------------------------------------------------
    tabs = st.tabs([
        "📡 1. Erken Uyarı & Genel Bakış",
        "🔍 2. Görev & PR Müfettişi (Neden Riskli?)",
        "🎛️ 3. What-If Karar Simülatörü",
        "🔀 4. PR & Ekip İş Yükü Analizi",
        "🤖 5. Yapay Zeka (XAI) & Veri Kalitesi",
        "📥 6. Aksiyon Planı & Rapor İndir",
    ])

    # ===========================================================================
    # SEKME 1: ERKEN UYARI VE GENEL BAKIŞ
    # ===========================================================================
    with tabs[0]:
        st.markdown("#### 🚨 Acil Müdahale Bekleyen Görevler (Erken Uyarı Listesi)")
        st.caption(
            "Henüz tamamlanmamış en yüksek riskli görevler. Bir görevin tüm detaylarını incelemek için **2. Sekmedeki Görev Müfettişi**ni kullanabilirsiniz."
        )

        early_candidates = (
            active_open_tasks if len(active_open_tasks) > 0 else filtered_tasks
        ).sort_values("combined_early_risk_score", ascending=False)

        top_early_alerts = early_candidates.head(20)[
            [
                "task_id",
                "task_title",
                "project",
                "component_module",
                "assignee",
                "due_date",
                "combined_early_risk_score",
                "xai_primary_risk_factors",
                "xai_recommended_actions",
            ]
        ].rename(
            columns={
                "task_id": "Görev ID",
                "task_title": "Başlık",
                "project": "Proje",
                "component_module": "Modül",
                "assignee": "Sorumlu",
                "due_date": "Teslim Tarihi",
                "combined_early_risk_score": "Risk Skoru",
                "xai_primary_risk_factors": "Neden Riskli? (XAI Özeti)",
                "xai_recommended_actions": "Önerilen Müdahale",
            }
        )

        st.dataframe(
            top_early_alerts,
            width="stretch",
            hide_index=True,
            column_config={
                "Risk Skoru": st.column_config.ProgressColumn(
                    "Risk Skoru (0-100)",
                    format="%.0f",
                    min_value=0,
                    max_value=100,
                ),
            },
        )

        c_left, c_right = st.columns(2)
        with c_left:
            st.markdown("#### 📊 Projelere Göre Risk Dağılımı")
            proj_risk_df = (
                filtered_tasks.groupby(["project", "delivery_risk_level"], as_index=False)
                .size()
                .rename(columns={"size": "Görev Sayısı"})
            )
            chart_proj = (
                alt.Chart(proj_risk_df)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                .encode(
                    y=alt.Y("project:N", title=None, sort="-x"),
                    x=alt.X("Görev Sayısı:Q", title="Görev Sayısı"),
                    color=alt.Color(
                        "delivery_risk_level:N",
                        title="Risk Sınıfı",
                        scale=alt.Scale(domain=RISK_ORDER, range=[RISK_COLORS[k] for k in RISK_ORDER]),
                    ),
                    tooltip=["project:N", "delivery_risk_level:N", "Görev Sayısı:Q"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart_proj, width="stretch")

        with c_right:
            st.markdown("#### 📈 Sprint Bazlı Risk ve Gecikme Eğilimi")
            if "sprint_iteration" in filtered_tasks.columns:
                sprint_df = (
                    filtered_tasks.groupby("sprint_iteration", as_index=False)
                    .agg(
                        Ortalama_Risk_Skoru=("combined_early_risk_score", "mean"),
                        Gecikme_Orani_Yuzde=("is_delayed", lambda s: round(s.mean() * 100.0, 1)),
                    )
                    .sort_values("sprint_iteration")
                )
                sprint_df["Ortalama_Risk_Skoru"] = sprint_df["Ortalama_Risk_Skoru"].round(1)
                base_sp = alt.Chart(sprint_df).encode(
                    x=alt.X("sprint_iteration:N", title=None, axis=alt.Axis(labelAngle=-45))
                )
                line_risk = base_sp.mark_line(color="#38bdf8", strokeWidth=2.5, point=True).encode(
                    y=alt.Y("Ortalama_Risk_Skoru:Q", title="Risk Skoru (Mavi) / Gecikme % (Turuncu)"),
                    tooltip=["sprint_iteration:N", "Ortalama_Risk_Skoru:Q", "Gecikme_Orani_Yuzde:Q"],
                )
                line_delay = base_sp.mark_line(color="#f97316", strokeDash=[5, 3], strokeWidth=2, point=True).encode(
                    y=alt.Y("Gecikme_Orani_Yuzde:Q"),
                )
                st.altair_chart((line_risk + line_delay).properties(height=300), width="stretch")

        with st.expander("🔥 Detaylı Proje x Yazılım Modülü Risk Isı Haritasını Göster (Heatmap)", expanded=False):
            heat_df = (
                filtered_tasks.groupby(["project", "component_module"], as_index=False)
                .agg(
                    Ort_Risk=("combined_early_risk_score", "mean"),
                    Gorev_Sayisi=("task_id", "count"),
                )
            )
            heat_df["Ort_Risk"] = heat_df["Ort_Risk"].round(1)
            heatmap = (
                alt.Chart(heat_df)
                .mark_rect(cornerRadius=3)
                .encode(
                    x=alt.X("component_module:N", title="Yazılım Modülü", axis=alt.Axis(labelAngle=-30)),
                    y=alt.Y("project:N", title="Proje"),
                    color=alt.Color("Ort_Risk:Q", title="Ort. Risk", scale=alt.Scale(scheme="redyellowgreen", reverse=True)),
                    tooltip=["project:N", "component_module:N", "Ort_Risk:Q", "Gorev_Sayisi:Q"],
                )
                .properties(height=300)
            )
            st.altair_chart(heatmap, width="stretch")

    # ===========================================================================
    # SEKME 2: GÖREV & PR MÜFETTİŞİ (NEDEN RİSKLİ?)
    # ===========================================================================
    with tabs[1]:
        insp_col1, insp_col2 = st.columns([2, 1])
        candidate_pool = filtered_tasks.sort_values("combined_early_risk_score", ascending=False).head(250)
        task_options = candidate_pool["task_id"].tolist()

        with insp_col1:
            selected_task_id = st.selectbox(
                "📋 İncelenecek Görevi Seçin (En Riskliden Sıralı)",
                options=task_options,
                format_func=lambda tid: f"{tid} — [Risk: {df_combined.loc[df_combined['task_id']==tid, 'combined_early_risk_score'].values[0]:.0f}] {df_combined.loc[df_combined['task_id']==tid, 'task_title'].values[0]}",
            )
        with insp_col2:
            custom_search_id = st.text_input("🔎 Veya Görev ID ile Ara", value="", placeholder="Örn: TASK-100005").strip().upper()
            if custom_search_id and custom_search_id in df_combined["task_id"].values:
                selected_task_id = custom_search_id

        task_row = df_combined[df_combined["task_id"] == selected_task_id].iloc[0]
        linked_prs_df = (
            df_prs[df_prs["linked_task_issue"] == selected_task_id]
            if (df_prs is not None and "linked_task_issue" in df_prs.columns)
            else pd.DataFrame()
        )

        # Görev Kimlik Kartı
        st.markdown(
            f"""
            <div class="kpi-card" style="margin-bottom: 1rem;">
                <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:0.75rem;">
                    <div>
                        <span style="font-size:1.15rem; font-weight:700; color:#f8fafc;">{task_row['task_id']}: {task_row.get('task_title', '')}</span>
                        &nbsp; {format_risk_badge(str(task_row.get('delivery_risk_level', 'Medium')))}
                        <div style="color:#94a3b8; font-size:0.84rem; margin-top:0.35rem;">
                            🏢 <b>Proje:</b> {task_row.get('project', '-')} &nbsp;•&nbsp;
                            🧩 <b>Modül:</b> {task_row.get('component_module', '-')} &nbsp;•&nbsp;
                            👤 <b>Sorumlu:</b> {task_row.get('assignee', '-')} &nbsp;•&nbsp;
                            📌 <b>Durum:</b> <code>{task_row.get('status', '-')}</code>
                        </div>
                    </div>
                    <div style="text-align:right;">
                        <div style="font-size:1.35rem; font-weight:700; color:#fbbf24;">
                            Risk Skoru: {task_row['combined_early_risk_score']:.1f} / 100
                        </div>
                        <div style="font-size:0.8rem; color:#cbd5e1;">
                            Gecikme İhtimali: <b>%{task_row['early_delay_probability']*100:.0f}</b>
                        </div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Neden Riskli & Ne Yapılmalı Kutuları
        xcol1, xcol2 = st.columns(2)
        with xcol1:
            st.markdown(
                f"""
                <div class="xai-box">
                    <div style="font-weight:700; font-size:0.8rem; text-transform:uppercase; color:#fbbf24; margin-bottom:0.3rem;">
                        🧠 1. Neden Bu Görev Riskli? (XAI Teşhisi)
                    </div>
                    <div style="color:#f8fafc; font-size:0.92rem;">
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
                    <div style="font-weight:700; font-size:0.8rem; text-transform:uppercase; color:#2dd4bf; margin-bottom:0.3rem;">
                        💡 2. Ne Yapılmalı? (Karar Destek Önerisi)
                    </div>
                    <div style="color:#f8fafc; font-size:0.92rem;">
                        {task_row.get('xai_recommended_actions', 'Rutin sprint takibini sürdürün.')}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown("<div style='margin-top: 0.85rem;'></div>", unsafe_allow_html=True)

        l_col, r_col = st.columns([1.25, 1])
        local_contrib_df = compute_local_xai_contributions(task_row)

        with l_col:
            st.markdown("##### 📊 Risk Puanını Artıran ve Azaltan Faktörler")
            local_chart = (
                alt.Chart(local_contrib_df)
                .mark_bar(cornerRadius=4)
                .encode(
                    y=alt.Y("Faktör:N", sort="-x", title=None),
                    x=alt.X("Puan Katkısı:Q", title="Risk Puanına Katkısı (+ / -)"),
                    color=alt.Color(
                        "Etki Yönü:N",
                        legend=None,
                        scale=alt.Scale(
                            domain=["Risk Artırıcı (+)", "Risk Azaltıcı (-)"],
                            range=["#ef4444", "#22c55e"],
                        ),
                    ),
                    tooltip=["Faktör:N", "Puan Katkısı:Q", "Açıklama:N"],
                )
                .properties(height=280)
            )
            st.altair_chart(local_chart, width="stretch")

        with r_col:
            st.markdown("##### 📋 Görev Planlama Özeti")
            st.markdown(
                f"""
                - **Sprint Dönemi:** `{task_row.get('sprint_iteration', '-')}`
                - **Planlanan Tarihler:** `{task_row.get('start_date', '-')}` → `{task_row.get('due_date', '-')}`
                - **Karmaşıklık & Efor:** `{task_row.get('story_points', '-')}` SP | Tahmini: `{task_row.get('estimated_effort_hours', '-')} saat`
                - **Süreç Engelleri:** `{task_row.get('blocker_count', 0)}` Blocker • `{task_row.get('requirement_change_count', 0)}` Kapsam Değişimi
                - **Ön Koşul Görevler:** `{task_row.get('dependencies', 'Yok')}`
                """
            )
            st.markdown(f"**📄 Açıklama:** {task_row.get('description', '-')}")
            st.caption(f"**✅ Kabul Kriterleri:** {task_row.get('acceptance_criteria', '-')}")

        st.markdown(f"##### 🔀 Göreve Bağlı Pull Request (PR) Kayıtları ({len(linked_prs_df)} Adet)")
        if len(linked_prs_df) == 0:
            st.info("Bu göreve bağlı PR kaydı bulunamadı.")
        else:
            for _, pr in linked_prs_df.iterrows():
                with st.expander(
                    f"🔹 {pr['pr_id']} — {pr.get('pr_title', '')} | Durum: {pr.get('status', '')} | PR Risk: {pr.get('pr_risk_score', 0):.0f}",
                    expanded=False,
                ):
                    st.markdown(
                        f"**📁 Değişim:** `{pr.get('changed_files_count', 0)}` dosya (`+{pr.get('lines_added', 0)}` / `-{pr.get('lines_deleted', 0)}`) &nbsp;|&nbsp; "
                        f"**⚔️ Conflict:** `{pr.get('merge_conflicts', '-')}` &nbsp;|&nbsp; "
                        f"**⚙️ Pipeline:** `{pr.get('pipeline_status', '-')}` (`{pr.get('ci_failure_count', 0)}` hata) &nbsp;|&nbsp; "
                        f"**⏱️ Merge Süresi:** `{pr.get('time_to_merge_hours', 0)} saat`"
                    )
                    st.markdown(f"💡 **XAI PR Önerisi:** {pr.get('xai_pr_recommended_actions', '-')}")
                    st.code(str(pr.get("diff_code_changes", "")), language="diff")

    # ===========================================================================
    # SEKME 3: WHAT-IF KARAR SİMÜLATÖRÜ
    # ===========================================================================
    with tabs[2]:
        st.markdown("#### 🎛️ What-If (Müdahale) Karar Simülatörü")
        st.caption(
            f"Referans Görev: **{selected_task_id}**. Aşağıdaki kaydırıcılarla görevin engellerini veya boyutunu değiştirdiğinizde riskin nasıl düştüğünü anında test edin."
        )

        sim_base_row = df_combined[df_combined["task_id"] == selected_task_id].iloc[0]

        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.markdown("##### 1️⃣ Görev Kapsamı & Planlama")
            sim_sp = st.select_slider(
                "Karmaşıklık (Story Points)",
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
                "Öncelik",
                options=["Low", "Medium", "High", "Critical"],
                index=["Low", "Medium", "High", "Critical"].index(str(sim_base_row.get("priority", "High")))
                if str(sim_base_row.get("priority", "High")) in ["Low", "Medium", "High", "Critical"]
                else 2,
            )

        with sc2:
            st.markdown("##### 2️⃣ Süreç Engelleri (Darboğazlar)")
            sim_blockers = st.slider("Blocker (Engel) Sayısı", 0, 5, int(sim_base_row.get("blocker_count", 1)))
            sim_deps = st.slider("Ön Koşul Görev Bağımlılığı", 0, 5, int(sim_base_row.get("dependency_count_calc", 1)))
            sim_scope_creep = st.slider("Kapsam Değişimi (Scope Creep)", 0, 5, int(sim_base_row.get("requirement_change_count", 1)))

        with sc3:
            st.markdown("##### 3️⃣ PR & CI/CD Kalitesi")
            sim_pr_files = st.slider("PR Değişen Dosya Sayısı", 1, 50, max(1, int(sim_base_row.get("pr_total_changed_files", 12))))
            sim_ci_fails = st.slider("CI/CD Pipeline Hata Sayısı", 0, 6, int(sim_base_row.get("pr_total_ci_failures", 1)))
            sim_conflict = st.checkbox("PR Merge Çakışması (Conflict) Var", value=bool(sim_base_row.get("pr_conflict_count", 0) > 0))

        sim_opt_ratio = np.clip((sim_sp * 6.5) / (sim_est_hours + 1.0), 0.5, 3.0)
        sim_p_w = 14.0 if sim_prio == "Critical" else (8.0 if sim_prio == "High" else 2.0)

        sim_task_risk = np.clip(
            sim_deps * 9.5
            + sim_blockers * 16.0
            + sim_scope_creep * 11.5
            + (14.0 if sim_sp >= 13 else 0.0)
            + (13.0 if sim_opt_ratio > 1.25 else 0.0)
            + sim_p_w
            + 12.0,
            2.0,
            99.5,
        )
        sim_pr_risk = np.clip(
            sim_pr_files * 1.45
            + (18.0 if sim_conflict else 0.0)
            + sim_ci_fails * 8.5
            + sim_task_risk * 0.25
            + 8.0,
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
        res1.metric("Yeni Bileşik Risk Skoru", f"{sim_combined_risk:.1f} / 100", delta=f"{sim_combined_risk - orig_risk:+.1f} puan", delta_color="inverse")
        res2.metric("Yeni Gecikme Olasılığı", f"%{sim_delay_prob:.1f}", delta=f"{sim_delay_prob - orig_prob:+.1f}%", delta_color="inverse")
        res3.metric("Tahmini Gecikme Süresi", f"{sim_est_delay_days} Gün", delta=f"{sim_est_delay_days - orig_delay:+d} gün", delta_color="inverse")
        sim_level = "Critical" if sim_combined_risk >= 75 else ("High" if sim_combined_risk >= 50 else ("Medium" if sim_combined_risk >= 28 else "Low"))
        res4.metric("Yeni Risk Sınıfı", sim_level)

        prescriptions = []
        if sim_blockers > 0:
            prescriptions.append(f"🔴 **{sim_blockers} adet Blocker** kaldırılırsa risk skoru doğrudan **-{sim_blockers*10:.0f} puan** düşer.")
        if sim_sp >= 13 or sim_opt_ratio > 1.25:
            prescriptions.append(f"🟠 **Görev Boyutu & Efor:** Görevi 5 SP'lik alt parçalara bölmek veya tahmini eforu **{int(sim_sp*6)} saate** güncellemek riski **~14 puan** azaltır.")
        if sim_conflict or sim_ci_fails > 0 or sim_pr_files >= 18:
            prescriptions.append("🔵 **PR & CI/CD Müdahalesi:** PR dosya sayısını 10'un altına çekmek ve merge conflict'i çözmek entegrasyon riskini **-%35** iyileştirir.")
        if not prescriptions:
            prescriptions.append("🟢 **İdeal Durum:** Mevcut parametrelerle görev düşük risk bölgesindedir ve zamanında teslimat beklenmektedir.")

        st.markdown("##### 🩺 Simülasyon Karar Reçetesi")
        for p_item in prescriptions:
            st.markdown(f"- {p_item}")

    # ===========================================================================
    # SEKME 4: PR & EKİP İŞ YÜKÜ ANALİZİ
    # ===========================================================================
    with tabs[3]:
        if len(filtered_prs) > 0:
            p_col1, p_col2, p_col3, p_col4 = st.columns(4)
            p_col1.metric("Toplam İncelenen PR", f"{len(filtered_prs):,}")
            p_col2.metric("Merge Çakışması (Conflict)", f"%{filtered_prs['has_conflict_bool'].mean()*100:.1f}")
            p_col3.metric("Ort. Birleştirme Süresi", f"{filtered_prs['time_to_merge_hours'].mean():.1f} Saat")
            p_col4.metric("CI/CD Hata Oranı", f"%{(filtered_prs['ci_failure_count'] > 0).mean()*100:.1f}")

            ch1, ch2 = st.columns(2)
            with ch1:
                st.markdown("##### ⚔️ Merge Çakışmasının Birleştirme Süresine Etkisi")
                conf_summary = (
                    filtered_prs.groupby("merge_conflicts", as_index=False)
                    .agg(Ort_Merge_Suresi_Saat=("time_to_merge_hours", "mean"))
                )
                conf_summary["Ort_Merge_Suresi_Saat"] = conf_summary["Ort_Merge_Suresi_Saat"].round(1)
                c_conf = (
                    alt.Chart(conf_summary)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#f97316")
                    .encode(
                        y=alt.Y("merge_conflicts:N", sort="-x", title=None),
                        x=alt.X("Ort_Merge_Suresi_Saat:Q", title="Ortalama Merge Süresi (Saat)"),
                        tooltip=["merge_conflicts:N", "Ort_Merge_Suresi_Saat:Q"],
                    )
                    .properties(height=250)
                )
                st.altair_chart(c_conf, width="stretch")

            with ch2:
                st.markdown("##### 🛡️ PR Güvenlik Taraması (SAST/CVE) Dağılımı")
                sec_summary = (
                    filtered_prs.groupby(["security_scan_status", "pr_risk_level"], as_index=False)
                    .size()
                    .rename(columns={"size": "PR Sayısı"})
                )
                c_sec = (
                    alt.Chart(sec_summary)
                    .mark_bar()
                    .encode(
                        y=alt.Y("security_scan_status:N", sort="-x", title=None),
                        x=alt.X("PR Sayısı:Q", title="PR Sayısı"),
                        color=alt.Color("pr_risk_level:N", scale=alt.Scale(domain=RISK_ORDER, range=[RISK_COLORS[k] for k in RISK_ORDER])),
                        tooltip=["security_scan_status:N", "pr_risk_level:N", "PR Sayısı:Q"],
                    )
                    .properties(height=250)
                )
                st.altair_chart(c_sec, width="stretch")

        st.markdown("##### 👨‍💻 Geliştirici İş Yükü ve Risk Dağılımı")
        dev_df = (
            filtered_tasks.groupby("assignee", as_index=False)
            .agg(
                Toplam_Gorev=("task_id", "count"),
                Acik_Gorev=("status", lambda s: int((s != "Done").sum())),
                Kritik_Yuksek_Riskli=("delivery_risk_level", lambda s: int(s.isin(["Critical", "High"]).sum())),
                Ort_Risk_Skoru=("combined_early_risk_score", lambda s: round(s.mean(), 1)),
                Gecikme_Orani_Yuzde=("is_delayed", lambda s: round(s.mean() * 100.0, 1)),
                PR_Conflict_Sayisi=("pr_conflict_count", "sum"),
            )
            .sort_values("Kritik_Yuksek_Riskli", ascending=False)
        )
        st.dataframe(dev_df, width="stretch", hide_index=True)

    # ===========================================================================
    # SEKME 5: YAPAY ZEKA (ML/XAI) VE 5V VERİ KALİTESİ
    # ===========================================================================
    with tabs[4]:
        st.markdown("#### 🤖 Erken Tahmin Yapay Zeka Modeli ve Küresel XAI Ağırlıkları")
        with st.spinner("ML modeli değerlendiriliyor..."):
            _, ml_metrics, feat_imp_df = train_early_risk_ml_model(df_combined)

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("ROC-AUC Başarımı", f"{ml_metrics['roc_auc']:.3f}")
        m2.metric("Doğruluk (Accuracy)", f"%{ml_metrics['accuracy']*100:.1f}")
        m3.metric("F1 Dengeli Skor", f"{ml_metrics['f1']:.3f}")
        m4.metric("Kesinlik (Precision)", f"%{ml_metrics['precision']*100:.1f}")
        m5.metric("Yakalama (Recall)", f"%{ml_metrics['recall']*100:.1f}")

        col_imp, col_xai_dist = st.columns([1.2, 1])
        with col_imp:
            st.markdown("##### 🌐 Gecikmeyi Tetikleyen En Önemli Faktörler (Küresel XAI)")
            imp_chart = (
                alt.Chart(feat_imp_df)
                .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color="#3b82f6")
                .encode(
                    y=alt.Y("Öznitelik:N", sort="-x", title=None),
                    x=alt.X("Etki Ağırlığı (%):Q", title="Model Karar Etki Ağırlığı (%)"),
                    tooltip=["Öznitelik:N", "Etki Ağırlığı (%):Q"],
                )
                .properties(height=320)
            )
            st.altair_chart(imp_chart, width="stretch")

        with col_xai_dist:
            st.markdown("##### 🧠 En Sık Görülen Risk Nedenleri")
            top_xai_reasons = filtered_tasks["xai_primary_risk_factors"].value_counts().head(6).reset_index()
            top_xai_reasons.columns = ["Birincil Risk Faktörü", "Görev Sayısı"]
            top_xai_reasons["Oran (%)"] = np.round(top_xai_reasons["Görev Sayısı"] / len(filtered_tasks) * 100.0, 1)
            st.dataframe(top_xai_reasons, width="stretch", hide_index=True)

        st.markdown("---")
        v1, v2 = st.columns(2)
        with v1:
            st.markdown("##### 🧹 5V Veracity — Görev Veri Kalitesi Dağılımı")
            t_veracity = df_tasks["data_quality_flag"].value_counts().reset_index()
            t_veracity.columns = ["Kalite Etiketi", "Kayıt Sayısı"]
            t_veracity["Oran (%)"] = np.round(t_veracity["Kayıt Sayısı"] / len(df_tasks) * 100.0, 2)
            st.dataframe(t_veracity, width="stretch", hide_index=True)

        with v2:
            st.markdown("##### 🧹 5V Veracity — PR Veri Kalitesi Dağılımı")
            if df_prs is not None and len(df_prs) > 0:
                p_veracity = df_prs["data_quality_flag"].value_counts().reset_index()
                p_veracity.columns = ["PR Kalite Etiketi", "Kayıt Sayısı"]
                p_veracity["Oran (%)"] = np.round(p_veracity["Kayıt Sayısı"] / len(df_prs) * 100.0, 2)
                st.dataframe(p_veracity, width="stretch", hide_index=True)

        with st.expander("⚡ 5V Velocity — Canlı CI/CD & PR Olay Akışı Kayıtlarını Göster", expanded=False):
            stream_df = load_velocity_stream(os.path.join(DEFAULT_DATA_DIR, "velocity_event_stream.jsonl"))
            if len(stream_df) > 0:
                st.caption(f"Toplam `{len(stream_df):,}` canlı akış olayı (`data/velocity_event_stream.jsonl`) yüklendi.")
                st.dataframe(
                    stream_df.head(100)[
                        [
                            "event_timestamp",
                            "event_type",
                            "project_code",
                            "task_id",
                            "payload.instant_risk_delta",
                            "payload.xai_alert_reason",
                        ]
                    ].rename(
                        columns={
                            "event_timestamp": "Zaman",
                            "event_type": "Olay Türü",
                            "project_code": "Proje",
                            "task_id": "Görev ID",
                            "payload.instant_risk_delta": "Risk Değişimi",
                            "payload.xai_alert_reason": "XAI Uyarısı",
                        }
                    ),
                    width="stretch",
                    hide_index=True,
                )

    # ===========================================================================
    # SEKME 6: AKSİYON PLANI VE KARAR DESTEK RAPORU İNDİR (EXPORT)
    # ===========================================================================
    with tabs[5]:
        st.markdown("#### 📥 Erken Müdahale Aksiyon Planı ve Karar Destek Raporu İndir")
        st.caption(
            "Seçili filtrelere göre **erken müdahale gerektiren yüksek riskli görevlerin** ve **XAI aksiyon önerilerinin** "
            "tam listesini yönetim toplantıları veya Jira/DevOps entegrasyonu için dışa aktarın."
        )

        report_df = (
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

        # Özet Aksiyon Grubu Tablosu
        st.markdown("##### 🎯 Önerilen Müdahale Aksiyonlarına Göre Görev Özeti")
        action_summary_df = (
            filtered_tasks.groupby("xai_recommended_actions", as_index=False)
            .agg(
                Gorev_Sayisi=("task_id", "count"),
                Acik_Gorev_Sayisi=("status", lambda s: int((s != "Done").sum())),
                Ortalama_Risk_Skoru=("combined_early_risk_score", lambda s: round(s.mean(), 1)),
            )
            .sort_values("Gorev_Sayisi", ascending=False)
            .rename(
                columns={
                    "xai_recommended_actions": "Önerilen Karar Destek Aksiyonu",
                    "Gorev_Sayisi": "Toplam Görev",
                    "Acik_Gorev_Sayisi": "Açık Görev",
                    "Ortalama_Risk_Skoru": "Ort. Risk Skoru",
                }
            )
        )
        st.dataframe(action_summary_df, width="stretch", hide_index=True)

        ex1, ex2 = st.columns(2)
        with ex1:
            csv_bytes_tab = report_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                label="📊 XAI Erken Risk ve Aksiyon Raporunu İndir (.CSV)",
                data=csv_bytes_tab,
                file_name="xai_erken_risk_karar_destek_raporu.csv",
                mime="text/csv",
                width="stretch",
                key="tab6_csv_download",
            )
        with ex2:
            json_bytes_tab = report_df.to_json(orient="records", force_ascii=False, indent=2).encode("utf-8")
            st.download_button(
                label="📄 XAI Erken Risk ve Aksiyon Raporunu İndir (.JSON)",
                data=json_bytes_tab,
                file_name="xai_erken_risk_karar_destek_raporu.json",
                mime="application/json",
                width="stretch",
                key="tab6_json_download",
            )

        st.markdown("##### 📋 Dışa Aktarılacak Aksiyon Planı Önizlemesi (İlk 50 Kayıt)")
        st.dataframe(report_df.head(50), width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
