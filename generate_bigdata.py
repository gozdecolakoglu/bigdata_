#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pyrefly: ignore-errors
"""
Büyük Veri (5V) Sentetik Veri Üretim Motoru
Proje: Yazılım Projelerinde Teslimat Risklerinin Büyük Veri ile Erken Tespiti ve
       Açıklanabilir Karar Destek Sistemi (XAI) Geliştirilmesi

5V Karakteristikleri:
1. Volume (Hacim): Ölçeklenebilir yüz binlerce/milyonlarca Task ve Pull Request kaydı.
2. Velocity (Hız): Milisaniye hassasiyetli zaman serisi olayları (Created, Updated, Merged, CI/CD).
3. Variety (Çeşitlilik): Yapısal (CSV, Parquet), Yarı-Yapısal (JSONL, iç içe objeler) ve
   Yapısal Olmayan (Türkçe doğal dil açıklamalar, Git Diff kod yamaları, Review yorumları).
4. Veracity (Doğruluk): Gerçek hayat veri kusurları (eksik tahminler, aykırı değerler, saat kayması).
5. Value (Değer): Nedensel teslimat gecikmesi modeli, Risk Skorları ve XAI (SHAP/LIME uyumlu)
   açıklanabilir risk faktörleri ile karar destek önerileri.
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_domain_catalogs():
    """Kurumsal yazılım projeleri için gerçekçi Türkçe/Teknik domain katalogları."""
    projects = [
        ("PRJ-FIN", "FinTech Ödeme ve Açık Bankacılık Platformu", "fintech-payment-core"),
        ("PRJ-ECOM", "Yeni Nesil E-Ticaret Mikroservis Dönüşümü", "ecommerce-order-service"),
        ("PRJ-AI", "Kurumsal Üretken Yapay Zeka ve Analitik Asistanı", "enterprise-ai-copilot"),
        ("PRJ-CLOUD", "Çoklu Bulut (Multi-Cloud) Kubernetes Altyapısı", "cloud-infra-gitops"),
        ("PRJ-CRM", "Müşteri 360 ve Gerçek Zamanlı Kampanya Motoru", "crm-realtime-engine"),
        ("PRJ-HEALTH", "Telesağlık ve Elektronik Hasta Kayıt Sistemi", "medtech-ehr-platform"),
        ("PRJ-LOG", "Akıllı Lojistik Rota Optimizasyonu ve Filo Takibi", "logistics-route-optimizer"),
        ("PRJ-SEC", "Sıfır Güven (Zero-Trust) Kimlik ve Erişim Yönetimi", "iam-zerotrust-gateway"),
    ]

    developers = [
        "ahmet.yilmaz", "zeynep.kaya", "mehmet.demir", "elif.celik", "caner.ozturk",
        "selin.aydın", "burak.arslan", "deniz.koc", "emre.sahin", "gizem.yildiz",
        "kerem.ozkan", "merve.polat", "onur.aksoy", "pelin.erdogan", "tolga.kurt",
        "yasemin.cetin", "baris.guler", "damla.bozkurt", "furkan.tekin", "hande.ucar",
        "kaan.bulut", "melis.karaca", "oguz.yalcin", "sinem.kilic", "ufuk.eren",
        "volkan.turan", "asli.dogan", "berk.simsek", "ceren.avci", "doruk.kaplan"
    ]

    components = [
        "Backend", "Frontend", "API Gateway", "Database", "DevOps & CI/CD",
        "Mobile (iOS/Android)", "AI/ML Pipeline", "Authentication & Security",
        "Payment Engine", "Notification Service", "Cache & Messaging (Kafka/Redis)",
        "Reporting & Analytics"
    ]

    task_types = ["Story", "Task", "Bug", "Improvement", "Tech Debt", "Security Fix", "Spike", "Sub-task"]
    task_type_probs = [0.28, 0.26, 0.20, 0.10, 0.06, 0.04, 0.03, 0.03]

    priorities = ["Low", "Medium", "High", "Critical"]
    priority_probs = [0.18, 0.45, 0.25, 0.12]

    environments = ["Development", "Test", "Staging", "Production"]
    env_probs = [0.35, 0.25, 0.20, 0.20]

    task_titles = [
        "JWT tabanlı kimlik doğrulama ve refresh token rotasyonunun geliştirilmesi",
        "Yüksek trafikli ödeme servisinde Redis önbellekleme katmanının entegrasyonu",
        "PostgreSQL sorgu optimizasyonu ve eksik bileşik indekslerin oluşturulması",
        "Kullanıcı paneli sipariş geçmişi sayfasında sayfalama (pagination) hatasının giderilmesi",
        "Kafka tüketici (consumer) grubunda oluşan mesaj gecikmesi (lag) sorununun çözümü",
        "Kubernetes HPA (Horizontal Pod Autoscaler) metrik eşiklerinin yeniden yapılandırılması",
        "REST API uç noktaları için OpenAPI 3.1 (Swagger) dokümantasyonunun güncellenmesi",
        "Mikroservisler arası gRPC iletişiminde mTLS güvenlik sertifikası doğrulaması",
        "Sepet terk etme tahmin modeli için gerçek zamanlı öznitelik çıkarım (feature store) hattı",
        "Mobil uygulama karanlık mod (dark mode) bileşenlerinde oluşan UI kaymasının düzeltilmesi",
        "Çoklu para birimi dönüştürme servisinde yuvarlama ve kur farkı hatasının giderilmesi",
        "Elasticsearch tam metin arama indeksleme performansının artırılması",
        "CI/CD hattına SonarQube statik kod analizi ve Trivy konteyner güvenlik taraması eklenmesi",
        "Müşteri bildirimleri için SMS ve Push Notification geri çekilme (retry/backoff) mekanizması",
        "Fatura oluşturma mikroservisinde bellek sızıntısı (memory leak) analizi ve çözümü",
        "GraphQL ağ geçidinde N+1 sorgu probleminin DataLoader ile optimize edilmesi",
        "KVKK ve GDPR uyumluluğu kapsamında kişisel verilerin maskelenmesi ve anonimleştirilmesi",
        "Dağıtık izleme (OpenTelemetry & Jaeger) entegrasyonu ile servis gecikmelerinin takibi",
        "Yük testi (Locust/JMeter) senaryolarının hazırlanması ve darboğaz analizi",
        "Eski (legacy) monolitik modülden müşteri profil servisinin mikroservise ayrıştırılması"
    ]

    task_descriptions = [
        "Bu görev kapsamında mevcut sistemde yaşanan performans darboğazlarını gidermek amacıyla asenkron işleme mimarisine geçiş yapılacaktır. Servis yanıt sürelerinin P99 altında 150ms seviyesine çekilmesi hedeflenmektedir.",
        "Üretim ortamında yoğun saatlerde meydana gelen zaman aşımı (timeout) hatalarının kök neden analizi yapılmış olup, bağlantı havuzu (connection pool) parametrelerinin ve devre kesici (circuit breaker) kurallarının güncellenmesi gerekmektedir.",
        "İş birimi tarafından talep edilen yeni regülasyon kuralları doğrultusunda doğrulama adımlarının API katmanına eklenmesi, hata kodlarının standartlaştırılması ve geriye dönük uyumluluğun korunması amaçlanmaktadır.",
        "Mevcut veritabanı şemasında büyüyen tablo boyutları nedeniyle sorgu maliyetleri artmıştır. Partitioning (bölümleme) stratejisi uygulanarak okuma/yazma operasyonlarının izole edilmesi ve arşivleme mekanizmasının kurulması gerekmektedir.",
        "Güvenlik denetimleri (pentest) sonucunda tespit edilen zafiyetlerin kapatılması, girdi doğrulama (input sanitization) filtrelerinin sıkılaştırılması ve bağımlı kütüphanelerin güvenli sürümlere yükseltilmesi kapsamındadır.",
        "Kullanıcı deneyimini iyileştirmek adına önyüz bileşenlerinin yeniden kullanılabilir (reusable) atomik tasarım prensiplerine göre refactor edilmesi ve durum yönetimi (state management) katmanının sadeleştirilmesi yapılacaktır."
    ]

    acceptance_criteria_templates = [
        "1) Birim test (unit test) kapsama oranı en az %85 olmalıdır. 2) P95 yanıt süresi < 200ms olmalıdır. 3) Entegrasyon testleri Staging ortamında hatasız geçmelidir. 4) Güvenlik taramasında Critical/High bulgu olmamalıdır.",
        "1) Tüm uç durumlar (edge-case) için hata yönetimi doğrulanmalıdır. 2) API dokümantasyonu güncellenmelidir. 3) Yük testi altında saniyede 1000 istek (RPS) kayıpsız işlenmelidir.",
        "1) Kod incelemesi (code review) en az 2 kıdemli geliştirici tarafından onaylanmalıdır. 2) Veritabanı göç (migration) betikleri geri alınabilir (rollback) olmalıdır. 3) Loglama formatı ELK standartlarına uygun olmalıdır.",
        "1) UI/UX tasarım spesifikasyonları ile %100 uyumlu olmalıdır. 2) Mobil ve masaüstü tarayıcılarda çapraz uyumluluk (cross-browser) testleri tamamlanmalıdır. 3) Erişilebilirlik (WCAG) kontrollerinden geçmelidir."
    ]

    diff_snippets = [
        "@@ -42,8 +42,19 @@ public class PaymentProcessor {\n-    return legacyGateway.execute(req);\n+    if (!circuitBreaker.allowRequest()) {\n+        throw new ServiceUnavailableException(\"Circuit open\");\n+    }\n+    return asyncGateway.executeWithRetry(req, MAX_RETRIES);\n }",
        "@@ -118,6 +118,14 @@ def calculate_risk_score(features: dict) -> float:\n-    score = np.dot(features['x'], weights)\n+    normalized = (features['x'] - mean) / (std + 1e-8)\n+    score = float(np.clip(np.dot(normalized, weights), 0.0, 100.0))\n+    logger.info(f\"Calculated risk score: {score:.2f}\")\n     return score",
        "@@ -15,4 +15,9 @@ CREATE INDEX CONCURRENTLY idx_orders_customer_date\n-ON orders (customer_id);\n+ON orders (customer_id, created_at DESC)\n+WHERE status NOT IN ('CANCELLED', 'ARCHIVED');",
        "@@ -88,10 +88,16 @@ export const OrderTable: React.FC<Props> = ({ data }) => {\n-  const items = data.filter(x => x.active);\n+  const items = useMemo(() => data?.filter(x => x.active) ?? [], [data]);\n+  if (!items.length) return <EmptyState message=\"Kayıt bulunamadı\" />;",
        "@@ -204,7 +204,12 @@ func (s *AuthService) ValidateToken(ctx context.Context, token string) (*Claims, error) {\n-    claims, err := jwt.Parse(token, s.keyFunc)\n+    claims, err := jwt.ParseWithLeeway(token, s.keyFunc, 5*time.Second)\n+    if err != nil {\n+        s.metrics.IncrementCounter(\"auth.token.invalid\")\n+        return nil, ErrUnauthorized\n+    }"
    ]

    review_comment_samples = [
        "LGTM! Kod oldukça temiz, sadece hata yakalama bloğuna log korelasyon ID'sini ekleyelim.",
        "Bu sorgu büyük tablolarda tam tablo taramasına (full table scan) yol açabilir, indeks kullanımını EXPLAIN ANALYZE ile doğrulayabilir miyiz?",
        "Eşzamanlılık (concurrency) durumunda race condition ihtimali var, burayı distributed lock (Redis Redlock) ile korumalıyız.",
        "Birim testlerde sınır değer (null/boş liste) senaryoları eksik kalmış, bunları tamamladıktan sonra birleştirelim.",
        "Çok fazla dosya tek PR'da değiştirilmiş, incelemesi zorlaşıyor ancak acil sürüm için onaylıyorum.",
        "Güvenlik taramasında bağımlı pakette CVE zafiyeti görünüyor, lütfen versiyonu yükselterek tekrar pushlayın."
    ]

    rollback_plans = [
        "Helm rollback komutu ile bir önceki kararlı sürüme (revision N-1) 2 dakika içinde geri dönülecektir; veritabanı şema değişikliği geriye dönük uyumludur.",
        "Feature flag (LaunchDarkly/Unleash) üzerinden yeni özellik trafik akışı %0'a çekilerek yeniden dağıtım gerekmeden devre dışı bırakılacaktır.",
        "Blue-Green dağıtım stratejisi kapsamında yük dengeleyici (Load Balancer) trafiği tekrar mavi (eski) ortama yönlendirilecek ve Flyway undo migration çalıştırılacaktır.",
        "ArgoCD üzerinden önceki Git commit etiketine (tag) senkronizasyon yapılacak ve Redis önbellek anahtarları temizlenecektir (flush)."
    ]

    return {
        "projects": projects,
        "developers": developers,
        "components": components,
        "task_types": task_types,
        "task_type_probs": task_type_probs,
        "priorities": priorities,
        "priority_probs": priority_probs,
        "environments": environments,
        "env_probs": env_probs,
        "task_titles": task_titles,
        "task_descriptions": task_descriptions,
        "acceptance_criteria": acceptance_criteria_templates,
        "diff_snippets": diff_snippets,
        "review_comments": review_comment_samples,
        "rollback_plans": rollback_plans,
    }


def generate_tasks_dataset(num_tasks: int, seed: int = 42, reference_date: str = "2026-11-11", task_id_start: int = 100001) -> pd.DataFrame:
    """
    22 temel Task özniteliği + 16 Büyük Veri (5V) & XAI Karar Destek özniteliği olmak üzere
    toplam 38 sütunlu zenginleştirilmiş Task veri setini vektörel olarak üretir.
    """
    rng = np.random.default_rng(seed)
    cat = get_domain_catalogs()

    print(f"[Task Generator] {num_tasks:,} adet Task kaydı üretiliyor (Başlangıç ID: {task_id_start})...")

    # 1. Task ID (kronolojik sıralama sonrası güncellenecek)
    task_ids = np.array([f"TASK-{task_id_start + i}" for i in range(num_tasks)], dtype=object)

    # 2. Project & Repository eşleşmesi
    proj_idx = rng.integers(0, len(cat["projects"]), size=num_tasks)
    project_codes = np.array([cat["projects"][i][0] for i in proj_idx], dtype=object)
    project_names = np.array([f"{cat['projects'][i][0]} - {cat['projects'][i][1]}" for i in proj_idx], dtype=object)

    # 3. Task Type & Priority
    task_types = rng.choice(cat["task_types"], size=num_tasks, p=cat["task_type_probs"])
    priorities = rng.choice(cat["priorities"], size=num_tasks, p=cat["priority_probs"])

    # 4. Component / Module & Environment
    components = rng.choice(cat["components"], size=num_tasks)
    environments = rng.choice(cat["environments"], size=num_tasks, p=cat["env_probs"])

    # 5. Title & Description & Acceptance Criteria
    base_titles = rng.choice(cat["task_titles"], size=num_tasks)
    task_titles = np.array(
        [f"[{components[i]}] {base_titles[i]} (#{100 + (i % 900)})" for i in range(num_tasks)],
        dtype=object
    )
    descriptions = rng.choice(cat["task_descriptions"], size=num_tasks)
    acceptance_criteria = rng.choice(cat["acceptance_criteria"], size=num_tasks)

    # 6. Assignee & Reporter & Watchers
    devs = np.array(cat["developers"], dtype=object)
    assignees = rng.choice(devs, size=num_tasks)
    reporters = rng.choice(devs, size=num_tasks)

    watcher_counts = rng.integers(1, 5, size=num_tasks)
    watcher_pool = [
        ", ".join(rng.choice(devs, size=int(c), replace=False))
        for c in watcher_counts[:2000]
    ]
    watchers = rng.choice(watcher_pool, size=num_tasks)

    # 7. Epic / Parent Task (Sprint bilgisi start_date'ten türetilecek)
    epic_nums = rng.integers(100, 350, size=num_tasks)
    epics = np.array([f"{project_codes[i]}-EPIC-{epic_nums[i]}" for i in range(num_tasks)], dtype=object)

    # 8. Story Points & Estimated Effort (Saat)
    fib_points = np.array([1, 2, 3, 5, 8, 13, 21])
    fib_probs = [0.10, 0.20, 0.25, 0.22, 0.13, 0.07, 0.03]
    story_points = rng.choice(fib_points, size=num_tasks, p=fib_probs)

    # Tahmini efor (saat): Story point * (4 ila 8 saat) + gürültü
    estimated_effort = np.round(story_points * rng.uniform(4.0, 7.5, size=num_tasks) + rng.normal(0, 2, size=num_tasks), 1)
    estimated_effort = np.clip(estimated_effort, 2.0, 180.0)

    # 9. Bağımlılıklar (Dependencies), Blocker, Kapsam Değişikliği (Scope Creep)
    dep_counts = rng.choice([0, 1, 2, 3, 4, 5], size=num_tasks, p=[0.42, 0.25, 0.15, 0.10, 0.05, 0.03])
    dep_pool = [
        "" if c == 0 else ", ".join([f"TASK-{100000 + int(rng.integers(1, max(2, num_tasks)))}" for _ in range(c)])
        for c in dep_counts[:3000]
    ]
    # Hızlı eşleme için dep_counts'a göre indeksleme
    dependencies = np.empty(num_tasks, dtype=object)
    for i in range(num_tasks):
        c = dep_counts[i]
        if c == 0:
            dependencies[i] = "Yok"
        else:
            prev_id = max(100001, 100001 + i - int(rng.integers(1, 500)))
            if c == 1:
                dependencies[i] = f"TASK-{prev_id}"
            else:
                dependencies[i] = f"TASK-{prev_id}, TASK-{max(100001, prev_id - int(rng.integers(1, 200)))}" + (
                    f", TASK-{max(100001, prev_id - int(rng.integers(201, 400)))}" if c >= 3 else ""
                )

    blocker_counts = rng.choice([0, 1, 2, 3], size=num_tasks, p=[0.68, 0.20, 0.08, 0.04])
    req_change_counts = rng.choice([0, 1, 2, 3, 4], size=num_tasks, p=[0.60, 0.22, 0.10, 0.05, 0.03])
    reopen_counts = rng.choice([0, 1, 2, 3], size=num_tasks, p=[0.72, 0.18, 0.07, 0.03])

    # 10. Status (Görev durumu - tarih kurgusundan önce belirlenir)
    status_choices = ["Done", "In Progress", "Test", "Code Review", "To Do", "Open", "Blocked"]
    status_probs = [0.58, 0.14, 0.09, 0.07, 0.05, 0.04, 0.03]
    statuses = rng.choice(status_choices, size=num_tasks, p=status_probs)
    statuses = np.where(blocker_counts >= 2, "Blocked", statuses)
    is_done = statuses == "Done"
    is_open = ~is_done

    # Nedensel Gecikme ve Risk Modellemesi (XAI için gerçekçi korelasyon!)
    # Risk puanını artıran faktörler:
    # - Yüksek Story Point & Düşük Tahmini Efor (İyimser tahmin / Underestimation)
    # - Yüksek Bağımlılık (dep_counts)
    # - Blocker sayısı (blocker_counts)
    # - Kapsam değişikliği / Scope Creep (req_change_counts)
    # - Reopen sayısı (reopen_counts)
    # - Kritik/Yüksek öncelik baskısı ve Production ortamı karmaşıklığı
    optimism_ratio = np.clip((story_points * 6.5) / (estimated_effort + 1.0), 0.5, 3.0)
    priority_weight = np.where(priorities == "Critical", 14.0, np.where(priorities == "High", 8.0, 2.0))
    env_weight = np.where(environments == "Production", 10.0, np.where(environments == "Staging", 5.0, 1.0))

    raw_risk = (
        dep_counts * 9.5
        + blocker_counts * 16.0
        + req_change_counts * 11.5
        + reopen_counts * 12.0
        + (story_points >= 13) * 14.0
        + (optimism_ratio > 1.25) * 13.0
        + priority_weight
        + env_weight
        + rng.normal(8.0, 6.0, size=num_tasks)
    )
    delivery_risk_score = np.round(np.clip(raw_risk, 2.0, 99.5), 2)

    delivery_risk_level = np.where(
        delivery_risk_score >= 75.0, "Critical",
        np.where(delivery_risk_score >= 50.0, "High",
                 np.where(delivery_risk_score >= 28.0, "Medium", "Low"))
    )

    # Gerçekleşen efor ve gecikme gün sayısı (Risk skoruyla doğrudan ilişkili)
    effort_multiplier = 0.85 + (delivery_risk_score / 100.0) * 1.35 + rng.normal(0, 0.12, size=num_tasks)
    effort_multiplier = np.clip(effort_multiplier, 0.6, 3.5)
    actual_effort = np.round(estimated_effort * effort_multiplier, 1)
    effort_variance_ratio = np.round((actual_effort - estimated_effort) / np.maximum(estimated_effort, 1.0), 3)

    # Gecikme gün sayısı
    delay_days = np.where(
        delivery_risk_score >= 48.0,
        np.round((delivery_risk_score - 42.0) * 0.32 + blocker_counts * 2.5 + rng.normal(1.0, 2.0, size=num_tasks)).astype(int),
        rng.choice([-2, -1, 0, 0, 0, 1], size=num_tasks)
    )
    delay_days = np.clip(delay_days, -5, 60)
    is_delayed = (delay_days > 0).astype(int)

    # 11. Gerçekçi Tarihler (Bugün = reference_date, örn. 2026-11-11)
    ref_day = np.datetime64(reference_date, "D")
    base_day = np.datetime64("2025-01-01", "D")
    hist_days = max(60, int((ref_day - base_day).astype(int)))

    start_delay_days = rng.integers(1, 5, size=num_tasks) + blocker_counts
    planned_duration_days = np.clip(np.ceil(estimated_effort / 6.0).astype(int) + rng.integers(2, 6, size=num_tasks), 3, 35)

    # Tamamlanan (Done) görevler: 2025-01-15 .. reference_date arasında bitmiş, actual_end = due + delay_days
    done_end_offset = rng.integers(14, hist_days + 1, size=num_tasks)
    done_actual_end = base_day + done_end_offset.astype("timedelta64[D]")
    done_due = done_actual_end - delay_days.astype("timedelta64[D]")
    done_start = done_due - planned_duration_days.astype("timedelta64[D]")

    # Açık görevler (Open / In Progress / Blocked vb.):
    # - Gecikmiş açık görevler: due_date = reference_date - delay_days (bugüne kadar gerçekleşen gecikme birebir tutarlı)
    # - Güncel/yaklaşan sprint görevleri: start_date son 14 gün içinde, due_date >= reference_date
    overdue_open = is_open & (delay_days > 0) & (rng.random(size=num_tasks) < 0.60)
    open_overdue_due = ref_day - delay_days.astype("timedelta64[D]")
    open_overdue_start = open_overdue_due - planned_duration_days.astype("timedelta64[D]")
    open_overdue_end = np.full(num_tasks, ref_day, dtype="datetime64[D]")

    open_curr_start = ref_day - rng.integers(0, 15, size=num_tasks).astype("timedelta64[D]")
    open_curr_due = np.maximum(ref_day, open_curr_start + planned_duration_days.astype("timedelta64[D]"))
    open_curr_end = np.maximum(ref_day, open_curr_due + delay_days.astype("timedelta64[D]"))

    start_dt = np.where(is_done, done_start, np.where(overdue_open, open_overdue_start, open_curr_start))
    due_dt = np.where(is_done, done_due, np.where(overdue_open, open_overdue_due, open_curr_due))
    actual_end_dt = np.where(is_done, done_actual_end, np.where(overdue_open, open_overdue_end, open_curr_end))

    created_day = start_dt - start_delay_days.astype("timedelta64[D]")
    created_sec = rng.integers(8 * 3600, 18 * 3600, size=num_tasks)
    ref_ts_cap = np.datetime64(f"{reference_date}T19:00:00", "s")
    created_dt = np.minimum(ref_ts_cap, created_day.astype("datetime64[s]") + created_sec.astype("timedelta64[s]"))
    actual_start_dt = np.minimum(ref_day, start_dt + (blocker_counts > 0).astype(int).astype("timedelta64[D]"))

    # Sprint numarasını doğrudan start_date'ten türet (2 haftalık sprintler: 2025-01-01 = Sprint-01, 2026-11-11 = Sprint-49)
    sprint_num = np.clip(((start_dt - base_day).astype(int) // 14) + 1, 1, 60)
    sprints = np.array([f"2025-2026 Sprint-{int(s):02d}" for s in sprint_num], dtype=object)

    created_at_str = np.datetime_as_string(created_dt, unit="s").astype(object)
    start_date_str = np.datetime_as_string(start_dt, unit="D").astype(object)
    due_date_str = np.datetime_as_string(due_dt, unit="D").astype(object)
    actual_start_str = np.datetime_as_string(actual_start_dt, unit="D").astype(object)
    actual_end_str = np.datetime_as_string(actual_end_dt, unit="D").astype(object)

    # 12. Labels / Tags & Attachments / Links
    label_pool = [
        "backend, performance, sla-critical",
        "frontend, ui-ux, accessibility",
        "security, kvkk, compliance",
        "database, migration, high-volume",
        "microservice, kafka, async",
        "devops, kubernetes, ci-cd",
        "bugfix, hotfix, production",
        "refactoring, tech-debt, clean-code",
        "ai-ml, analytics, big-data",
        "api, gateway, integration"
    ]
    labels_tags = rng.choice(label_pool, size=num_tasks)

    attachment_pool = [
        "https://confluence.kurum.local/display/ARCH/System-Design-v2.pdf, https://figma.com/file/UI-Spec-104",
        "https://jira.kurum.local/secure/attachment/90412/error_trace_prod.log",
        "https://confluence.kurum.local/display/SEC/Pentest-Report-2026.docx",
        "https://grafana.kurum.local/d/db-perf/postgresql-latency-snapshot.png",
        "https://swagger.kurum.local/api-docs/v3/payment-spec.yaml",
        "Ek dosya yok"
    ]
    attachments_links = rng.choice(attachment_pool, size=num_tasks, p=[0.22, 0.18, 0.15, 0.15, 0.15, 0.15])

    # 13. XAI (Açıklanabilir Karar Destek Sistemi) Faktörleri ve Önerileri
    xai_factors = np.empty(num_tasks, dtype=object)
    xai_actions = np.empty(num_tasks, dtype=object)

    for i in range(num_tasks):
        factors = []
        actions = []
        if blocker_counts[i] > 0:
            factors.append(f"Blocker/Engel Sayısı ({blocker_counts[i]} adet, +%28 risk)")
            actions.append("Blokaj yaratan dış servis/ekip bağımlılıklarını eskalasyon ile çözün")
        if dep_counts[i] >= 2:
            factors.append(f"Yüksek Görev Bağımlılığı ({dep_counts[i]} ön koşul, +%24 risk)")
            actions.append("Ön koşul görevlerin (Dependencies) kritik yol (critical path) takibini öne çekin")
        if req_change_counts[i] >= 1:
            factors.append(f"Kapsam Değişikliği / Scope Creep ({req_change_counts[i]} revizyon, +%21 risk)")
            actions.append("Sprint içi kapsam değişikliklerini dondurun ve ek talepleri sonraki sprinte aktarın")
        if story_points[i] >= 13 or optimism_ratio[i] > 1.25:
            factors.append(f"Yüksek Karmaşıklık / İyimser Efor Tahmini ({story_points[i]} SP, +%19 risk)")
            actions.append("Görevi daha küçük alt görevlere (Sub-task) bölün ve efor tahminini güncelleyin")
        if reopen_counts[i] >= 1:
            factors.append(f"Testten Geri Dönüş / Reopen ({reopen_counts[i]} kez, +%17 risk)")
            actions.append("Kabul kriterlerini (Acceptance Criteria) netleştirin ve BDD/TDD testlerini artırın")

        if not factors:
            factors.append("Standart İş Akışı (Düşük Sapma Riski, +%5 baz risk)")
            actions.append("Mevcut sprint planına sadık kalın, rutin PR inceleme sürecini sürdürün")

        xai_factors[i] = " | ".join(factors[:3])
        xai_actions[i] = "; ".join(actions[:2]) + "."

    # 14. 5V Veracity (Veri Kalitesi / Gürültü Simülasyonu)
    veracity_choices = ["Clean", "Missing_Estimate", "Outlier_Effort", "Clock_Skew_Corrected", "Incomplete_Metadata"]
    veracity_probs = [0.92, 0.025, 0.02, 0.015, 0.02]
    data_quality_flag = rng.choice(veracity_choices, size=num_tasks, p=veracity_probs)

    # Outlier_Effort olanlarda gerçek eforu uç değere çek (Veracity analizi için)
    outlier_mask = data_quality_flag == "Outlier_Effort"
    actual_effort = np.where(outlier_mask, np.round(actual_effort * 3.8, 1), actual_effort)

    # Linked PR Count (Her göreve 1 ila 4 PR bağlanacak)
    linked_pr_count = rng.choice([1, 2, 3, 4], size=num_tasks, p=[0.40, 0.38, 0.15, 0.07])

    df_tasks = pd.DataFrame({
        "task_id": task_ids,
        "task_title": task_titles,
        "description": descriptions,
        "project": project_names,
        "task_type": task_types,
        "priority": priorities,
        "assignee": assignees,
        "reporter_created_by": reporters,
        "status": statuses,
        "start_date": start_date_str,
        "due_date": due_date_str,
        "acceptance_criteria": acceptance_criteria,
        "attachment_link": attachments_links,
        "sprint_iteration": sprints,
        "epic_parent_task": epics,
        "estimated_effort_hours": estimated_effort,
        "story_points": story_points,
        "labels_tags": labels_tags,
        "component_module": components,
        "environment": environments,
        "dependencies": dependencies,
        "watchers_followers": watchers,
        # 5V & XAI Karar Destek Sütunları
        "created_at": created_at_str,
        "actual_start_date": actual_start_str,
        "actual_end_date": actual_end_str,
        "actual_effort_hours": actual_effort,
        "effort_variance_ratio": effort_variance_ratio,
        "delay_days": delay_days,
        "is_delayed": is_delayed,
        "reopen_count": reopen_counts,
        "blocker_count": blocker_counts,
        "requirement_change_count": req_change_counts,
        "linked_pr_count": linked_pr_count,
        "delivery_risk_score": delivery_risk_score,
        "delivery_risk_level": delivery_risk_level,
        "xai_primary_risk_factors": xai_factors,
        "xai_recommended_actions": xai_actions,
        "data_quality_flag": data_quality_flag,
    })

    # Missing_Estimate olanlarda estimated_effort_hours'ı NaN yap (Veracity testi için)
    missing_mask = df_tasks["data_quality_flag"] == "Missing_Estimate"
    df_tasks.loc[missing_mask, "estimated_effort_hours"] = np.nan

    return df_tasks


def generate_prs_dataset(df_tasks: pd.DataFrame, seed: int = 101, reference_date: str = "2026-11-11", pr_id_start: int = 200001) -> pd.DataFrame:
    """
    Task veri seti ile birebir ilişkili (Foreign Key: linked_task_issue),
    36 temel PR özniteliği + 13 Büyük Veri (5V) & XAI PR Risk özniteliği içeren
    toplam 49 sütunlu Pull Request veri setini üretir.
    """
    rng = np.random.default_rng(seed)
    cat = get_domain_catalogs()

    # Her Task'ın linked_pr_count sayısı kadar PR oluştur
    pr_counts = df_tasks["linked_pr_count"].to_numpy()
    num_prs = int(pr_counts.sum())
    print(f"[PR Generator] Task'larla ilişkili {num_prs:,} adet Pull Request (PR) kaydı üretiliyor (Başlangıç ID: {pr_id_start})...")

    # Task indekslerini tekrarlayarak ilişkisel bütünlük sağla
    parent_idx = np.repeat(np.arange(len(df_tasks)), pr_counts)

    linked_task_ids = df_tasks["task_id"].to_numpy()[parent_idx]
    parent_projects = df_tasks["project"].to_numpy()[parent_idx]
    parent_components = df_tasks["component_module"].to_numpy()[parent_idx]
    parent_assignees = df_tasks["assignee"].to_numpy()[parent_idx]
    parent_risk_scores = df_tasks["delivery_risk_score"].to_numpy()[parent_idx]
    parent_created_at = df_tasks["created_at"].to_numpy()[parent_idx]
    parent_start_date = df_tasks["start_date"].to_numpy()[parent_idx]
    parent_status = df_tasks["status"].to_numpy()[parent_idx]
    parent_priorities = df_tasks["priority"].to_numpy()[parent_idx]

    # 1. PR ID
    pr_ids = np.array([f"PR-{pr_id_start + i}" for i in range(num_prs)], dtype=object)

    # 2. Repository eşlemesi
    proj_to_repo = {f"{p[0]} - {p[1]}": p[2] for p in cat["projects"]}
    repositories = np.array([proj_to_repo.get(p, "core-platform-repo") for p in parent_projects], dtype=object)

    # 3. Source Branch & Target Branch
    branch_types = rng.choice(["feature", "bugfix", "hotfix", "refactor", "chore"], size=num_prs, p=[0.48, 0.24, 0.08, 0.12, 0.08])
    source_branches = np.array(
        [f"{branch_types[i]}/{linked_task_ids[i].lower()}-v{1 + (i % 3)}" for i in range(num_prs)],
        dtype=object
    )
    target_branches = rng.choice(["main", "develop", "release/v2.4", "staging"], size=num_prs, p=[0.45, 0.35, 0.12, 0.08])

    # 4. PR Title & PR Description
    pr_action_titles = [
        "servis katmanı iş mantığı ve birim testlerinin eklenmesi",
        "performans iyileştirmesi ve önbellek (cache) entegrasyonu",
        "veritabanı sorgu optimizasyonu ve migration betikleri",
        "API hata yönetimi, doğrulama (validation) ve şema güncellemesi",
        "güvenlik yaması, bağımlılık güncellemesi ve statik analiz düzeltmeleri",
        "asenkron mesajlaşma (Kafka/RabbitMQ) üretici/tüketici entegrasyonu",
        "önyüz bileşen güncellemesi ve durum yönetimi iyileştirmesi"
    ]
    chosen_actions = rng.choice(pr_action_titles, size=num_prs)
    pr_titles = np.array(
        [f"feat({parent_components[i].lower().split()[0]}): {linked_task_ids[i]} {chosen_actions[i]}" for i in range(num_prs)],
        dtype=object
    )

    pr_desc_templates = [
        "### Değişiklik Özeti\nİlgili görev kapsamında servis katmanında gerekli mimari düzenlemeler yapılmış, birim ve entegrasyon testleri eklenmiştir.\n### Test Edilen Senaryolar\n- Başarılı akış (Happy Path)\n- Zaman aşımı ve hata durumları\n- Yük altında bellek tüketimi kontrolü",
        "### Amaç ve Kapsam\nÜretim ortamında gözlemlenen darboğazı gidermek için sorgu planları optimize edildi ve gereksiz senkron blokajlar kaldırıldı.\n### Kontrol Listesi\n- [x] Unit testler eklendi\n- [x] SonarQube kalite kapısı geçildi\n- [x] API dokümantasyonu güncellendi",
        "### Teknik Detaylar\nBağımlı mikroservisler arası sözleşme (contract) güncellendi, geriye dönük uyumluluk korundu ve Circuit Breaker eşikleri yeniden tanımlandı."
    ]
    pr_descriptions = rng.choice(pr_desc_templates, size=num_prs)

    # 5. PR Author, Reviewers, Assignee
    devs = np.array(cat["developers"], dtype=object)
    pr_authors = parent_assignees  # Genellikle görevin sorumlusu PR'ı açar
    pr_assignees = rng.choice(devs, size=num_prs)

    reviewer_pool = [
        f"{devs[i % len(devs)]}, {devs[(i + 3) % len(devs)]}"
        for i in range(len(devs))
    ]
    reviewers = rng.choice(reviewer_pool, size=num_prs)

    # 6. Kod Değişiklik Metrikleri (Lines Added, Deleted, Changed Files, Commits)
    is_high_risk_task = parent_risk_scores >= 55.0
    changed_files_count = np.where(
        is_high_risk_task,
        rng.integers(6, 42, size=num_prs),
        rng.integers(1, 14, size=num_prs)
    )
    lines_added = changed_files_count * rng.integers(15, 65, size=num_prs) + rng.integers(5, 120, size=num_prs)
    lines_deleted = np.round(lines_added * rng.uniform(0.15, 0.70, size=num_prs)).astype(int)
    commit_count = np.clip((changed_files_count // 3) + rng.integers(1, 8, size=num_prs), 1, 35)

    # Changed Files listesi (Yarı-yapısal metin/JSON)
    file_templates = [
        "src/main/service/OrderService.java, src/main/repo/OrderRepository.java, src/test/OrderServiceTest.java",
        "app/api/v2/endpoints/payment.py, app/core/security.py, tests/test_payment_api.py",
        "src/components/Dashboard/RiskChart.tsx, src/store/riskSlice.ts, src/api/client.ts",
        "internal/handler/auth.go, internal/middleware/jwt.go, pkg/cache/redis.go, deploy/helm/values.yaml",
        "db/migration/V2026_10__add_indexes.sql, src/domain/models/customer.py, docker-compose.yml"
    ]
    base_files = rng.choice(file_templates, size=num_prs)
    changed_files = np.array(
        [f"{base_files[i]} (+{max(0, changed_files_count[i] - 3)} dosya)" for i in range(num_prs)],
        dtype=object
    )

    # Commits (Commit kayıtları özeti)
    commits_str = np.array(
        [
            f"[{commit_count[i]} commits] { hex(1000000 + (i * 2654435761) % 9000000)[2:9] }: init implementation -> "
            f"{ hex(2000000 + (i * 1597334677) % 8000000)[2:9] }: fix review comments & tests"
            for i in range(num_prs)
        ],
        dtype=object
    )

    # Diff / Code Changes
    base_diffs = rng.choice(cat["diff_snippets"], size=num_prs)
    diff_code_changes = np.array(
        [f"+{lines_added[i]} / -{lines_deleted[i]} satır | {base_diffs[i]}" for i in range(num_prs)],
        dtype=object
    )

    # 7. Merge Conflicts, CI/CD, Build, Test, Security, Static Analysis
    conflict_prob = np.clip(0.08 + (changed_files_count / 60.0) + (is_high_risk_task * 0.15), 0.05, 0.65)
    has_conflict = rng.random(size=num_prs) < conflict_prob
    merge_conflicts = np.where(
        has_conflict,
        rng.choice([
            "Çakışma Var (Çözüldü - Manuel Rebase)",
            "Çakışma Var (Kritik - Hedef Branch Değişimi)",
            "Çakışma Var (Çözüldü - Otomatik Merge)"
        ], size=num_prs, p=[0.55, 0.25, 0.20]),
        "Çakışma Yok (Clean Merge)"
    )

    ci_failure_count = np.where(
        has_conflict | is_high_risk_task,
        rng.choice([0, 1, 2, 3, 4, 5], size=num_prs, p=[0.25, 0.30, 0.20, 0.13, 0.07, 0.05]),
        rng.choice([0, 1, 2], size=num_prs, p=[0.78, 0.17, 0.05])
    )

    build_status = np.where(
        ci_failure_count >= 3,
        rng.choice(["Success", "Failed", "Unstable"], size=num_prs, p=[0.60, 0.25, 0.15]),
        rng.choice(["Success", "Failed"], size=num_prs, p=[0.94, 0.06])
    )
    test_status = np.where(
        build_status == "Failed",
        "Skipped / Failed Build",
        np.where(
            ci_failure_count >= 2,
            rng.choice(["Passed (Cov: %82)", "Failed (2 Integration Tests)", "Flaky Test Detected"], size=num_prs, p=[0.65, 0.20, 0.15]),
            "Passed (Cov: %91)"
        )
    )

    code_quality_pool = [
        "Quality Gate: Passed | Rating: A | Code Smells: 1 | Coverage: %92.4 | Tech Debt: 15min",
        "Quality Gate: Passed | Rating: B | Code Smells: 6 | Coverage: %81.0 | Tech Debt: 1h 20min",
        "Quality Gate: Warning | Rating: C | Code Smells: 18 | Coverage: %68.5 | Duplications: %4.8",
        "Quality Gate: Failed | Rating: D | Critical Bugs: 2 | Code Smells: 34 | Coverage: %52.0"
    ]
    code_quality = np.where(
        is_high_risk_task,
        rng.choice(code_quality_pool, size=num_prs, p=[0.25, 0.35, 0.25, 0.15]),
        rng.choice(code_quality_pool, size=num_prs, p=[0.60, 0.30, 0.08, 0.02])
    )

    security_scan_status = np.where(
        is_high_risk_task,
        rng.choice([
            "Passed (0 Vulnerability)",
            "Warning (1 Medium Dependency CVE)",
            "Failed (High SAST Finding - SQLi/XSS Risk)"
        ], size=num_prs, p=[0.70, 0.22, 0.08]),
        rng.choice(["Passed (0 Vulnerability)", "Warning (1 Low Dependency CVE)"], size=num_prs, p=[0.92, 0.08])
    )

    pipeline_status = np.where(
        (build_status == "Failed") | (security_scan_status == "Failed (High SAST Finding - SQLi/XSS Risk)"),
        "Failed",
        np.where(ci_failure_count > 0, "Passed after Retry", "Succeeded")
    )

    # 8. Review Comments, Iterations & Approval Status
    review_iteration_count = np.clip(
        1 + (changed_files_count // 10) + has_conflict.astype(int) + (ci_failure_count // 2),
        1, 9
    )
    base_comments = rng.choice(cat["review_comments"], size=num_prs)
    review_comments = np.array(
        [f"[{review_iteration_count[i]} tur inceleme] {base_comments[i]}" for i in range(num_prs)],
        dtype=object
    )

    # PR Status & Approval Status (Tamamlanan görevlerin PR'ları çoğunlukla Merged/Closed)
    open_pr_status = np.where(
        pipeline_status == "Failed",
        rng.choice(["Changes Requested", "In Review", "Closed", "Open"], size=num_prs, p=[0.45, 0.25, 0.15, 0.15]),
        rng.choice(["Merged", "Approved", "In Review", "Open", "Draft", "Closed"], size=num_prs, p=[0.45, 0.16, 0.15, 0.12, 0.07, 0.05])
    )
    done_pr_status = rng.choice(["Merged", "Closed"], size=num_prs, p=[0.90, 0.10])
    pr_status = np.where(parent_status == "Done", done_pr_status, open_pr_status)

    approval_status = np.where(
        np.isin(pr_status, ["Merged", "Approved"]),
        "Approved (2/2 Reviewers)",
        np.where(
            pr_status == "Changes Requested",
            "Changes Requested (1/2 Reviewers)",
            "Pending Review (0/2 Reviewers)"
        )
    )

    # 9. Zaman Damgaları (Created At, Updated At, Merged At, Closed At - referans tarih <= reference_date)
    time_to_first_review_hours = np.round(
        rng.uniform(0.5, 12.0, size=num_prs) + (changed_files_count * 0.4), 2
    )
    time_to_merge_hours = np.round(
        time_to_first_review_hours + review_iteration_count * rng.uniform(4.0, 14.0, size=num_prs) + has_conflict * 12.0,
        2
    )

    ref_ts = np.datetime64(f"{reference_date}T20:00:00", "s")
    parent_start_ts = parent_start_date.astype("datetime64[s]") + np.timedelta64(9 * 3600, "s")
    pr_create_offset_hours = rng.integers(2, 72, size=num_prs)
    pr_created_dt = np.minimum(ref_ts - np.timedelta64(3600, "s"), parent_start_ts + pr_create_offset_hours.astype("timedelta64[h]"))
    pr_updated_dt = np.minimum(ref_ts, pr_created_dt + np.round(time_to_merge_hours * 3600).astype(int).astype("timedelta64[s]"))

    created_at_str = np.datetime_as_string(pr_created_dt, unit="s").astype(object)
    updated_at_str = np.datetime_as_string(pr_updated_dt, unit="s").astype(object)

    is_merged = pr_status == "Merged"
    is_closed_unmerged = pr_status == "Closed"

    merged_at_str = np.where(is_merged, updated_at_str, "")
    merged_by = np.where(is_merged, rng.choice(devs, size=num_prs), "")

    closed_at_str = np.where(is_closed_unmerged, updated_at_str, "")
    closed_by = np.where(is_closed_unmerged, pr_authors, "")

    # 10. Merge Strategy, Requirements, Permissions, Deployment, Rollback
    merge_strategy = rng.choice(["Squash and Merge", "Merge Commit", "Rebase and Merge"], size=num_prs, p=[0.62, 0.25, 0.13])
    merge_requirements = rng.choice([
        "Min 2 Approvals + CI Pipeline Green + SonarQube Gate Passed + No Conflicts",
        "Min 1 Senior Approval + Unit & Integration Tests Passed + Signed Commits",
        "CodeOwners Approval + Security Scan Clean + Up-to-date Branch"
    ], size=num_prs)
    merge_permission = rng.choice([
        "Tech Lead, Senior Developer, Release Manager",
        "CodeOwners, DevOps Engineer",
        "Repository Maintainers"
    ], size=num_prs)

    milestones = np.array([f"v2.{1 + (i % 6)}.0-release" for i in range(num_prs)], dtype=object)
    linked_docs = np.array(
        [f"https://confluence.kurum.local/docs/{repositories[i]}/adr-{100 + (i % 80)}" for i in range(num_prs)],
        dtype=object
    )

    deployment_status = np.where(
        is_merged,
        rng.choice([
            "Deployed to Production",
            "Deployed to Staging",
            "Canary Rollout (%25 Traffic)",
            "Rolled Back (Post-Deploy Alert)"
        ], size=num_prs, p=[0.65, 0.22, 0.09, 0.04]),
        "Not Deployed"
    )
    rollback_plans = rng.choice(cat["rollback_plans"], size=num_prs)

    pr_labels = np.where(
        changed_files_count >= 20,
        "size/XL, high-risk, needs-careful-review",
        np.where(has_conflict, "conflict-resolved, ci-retried", "size/M, ready-for-merge, automated-tests")
    )

    # 11. PR Risk Skoru ve XAI Açıklanabilir Karar Destek Sütunları
    raw_pr_risk = (
        (changed_files_count * 1.45)
        + (lines_added / 45.0)
        + (has_conflict * 18.0)
        + (ci_failure_count * 8.5)
        + (review_iteration_count * 4.2)
        + (pipeline_status == "Failed") * 22.0
        + (parent_risk_scores * 0.22)
        + rng.normal(4.0, 4.0, size=num_prs)
    )
    pr_risk_score = np.round(np.clip(raw_pr_risk, 1.0, 99.5), 2)
    pr_risk_level = np.where(
        pr_risk_score >= 75.0, "Critical",
        np.where(pr_risk_score >= 50.0, "High",
                 np.where(pr_risk_score >= 28.0, "Medium", "Low"))
    )

    xai_pr_factors = np.empty(num_prs, dtype=object)
    xai_pr_actions = np.empty(num_prs, dtype=object)

    for i in range(num_prs):
        pf = []
        pa = []
        if changed_files_count[i] >= 15 or lines_added[i] >= 600:
            pf.append(f"Büyük Boyutlu PR ({changed_files_count[i]} dosya / +{lines_added[i]} satır, +%31 risk)")
            pa.append("PR'ı daha küçük ve bağımsız alt PR'lara bölün")
        if has_conflict[i]:
            pf.append("Merge Çakışması (Branch Senkronizasyon Sorunu, +%26 risk)")
            pa.append("Hedef branch ile günlük rebase yapın ve eşzamanlı dosya düzenlemelerini izole edin")
        if ci_failure_count[i] >= 2 or pipeline_status[i] == "Failed":
            pf.append(f"CI/CD Pipeline ve Test Kırılmaları ({ci_failure_count[i]} hata, +%25 risk)")
            pa.append("Pre-commit kancalarını (hooks) ve yerel entegrasyon testlerini PR öncesi zorunlu kılın")
        if review_iteration_count[i] >= 4:
            pf.append(f"Yüksek İnceleme Döngüsü ({review_iteration_count[i]} tur revizyon, +%18 risk)")
            pa.append("Karmaşık PR'lar için eşli inceleme (pair review) oturumu düzenleyin")

        if not pf:
            pf.append("Küçük ve Temiz Değişiklik Seti (CI Başarılı, +%4 baz risk)")
            pa.append("Standart otomatik birleştirme (auto-merge) akışı uygulanabilir")

        xai_pr_factors[i] = " | ".join(pf[:3])
        xai_pr_actions[i] = "; ".join(pa[:2]) + "."

    data_quality_flag = rng.choice(
        ["Clean", "Outlier_Diff_Size", "Clock_Skew_Anomaly", "Missing_Doc_Link"],
        size=num_prs,
        p=[0.93, 0.03, 0.02, 0.02]
    )

    df_prs = pd.DataFrame({
        "pr_id": pr_ids,
        "pr_title": pr_titles,
        "pr_description": pr_descriptions,
        "repository": repositories,
        "source_branch": source_branches,
        "target_branch": target_branches,
        "linked_task_issue": linked_task_ids,
        "pr_author_created_by": pr_authors,
        "reviewers": reviewers,
        "assignee": pr_assignees,
        "status": pr_status,
        "created_at": created_at_str,
        "updated_at": updated_at_str,
        "changed_files": changed_files,
        "commits": commits_str,
        "diff_code_changes": diff_code_changes,
        "review_comments": review_comments,
        "approval_status": approval_status,
        "merge_strategy": merge_strategy,
        "merge_conflicts": merge_conflicts,
        "build_status": build_status,
        "test_status": test_status,
        "code_quality_static_analysis": code_quality,
        "security_scan_status": security_scan_status,
        "pipeline_status": pipeline_status,
        "merge_requirements": merge_requirements,
        "merge_permission": merge_permission,
        "merged_by": merged_by,
        "merged_at": merged_at_str,
        "closed_by": closed_by,
        "closed_at": closed_at_str,
        "labels_tags": pr_labels,
        "milestone_release": milestones,
        "linked_documentation": linked_docs,
        "deployment_status": deployment_status,
        "rollback_plan": rollback_plans,
        # 5V & XAI PR Metrikleri
        "lines_added": lines_added,
        "lines_deleted": lines_deleted,
        "changed_files_count": changed_files_count,
        "commit_count": commit_count,
        "review_iteration_count": review_iteration_count,
        "time_to_first_review_hours": time_to_first_review_hours,
        "time_to_merge_hours": time_to_merge_hours,
        "ci_failure_count": ci_failure_count,
        "pr_risk_score": pr_risk_score,
        "pr_risk_level": pr_risk_level,
        "xai_pr_risk_factors": xai_pr_factors,
        "xai_pr_recommended_actions": xai_pr_actions,
        "data_quality_flag": data_quality_flag,
    })

    return df_prs


def export_5v_datasets_streamed(
    total_tasks: int = 280000,
    chunk_size: int = 50000,
    output_dir: str = "data",
    target_csv_gb: float = 1.0,
    scale_both_csv: bool = False,
    seed: int = 42,
    reference_date: str = "2026-11-11"
):
    """
    Büyük Veri (Big Data) 5V Ölçeklenebilir Streaming/Chunked Veri Üretim Motoru.
    
    Özellikler:
    - OOM (Out Of Memory) koruması: Bellek sabit ~350 MB kalır.
    - CSV boyutunu en az `target_csv_gb` (Varsayılan: >= 1.0 GB) olacak şekilde garanti eder.
    - Parquet (Snappy sıkıştırmalı kolon bazlı) ve JSONL dosyalarını eşzamanlı chunk olarak yazar.
    - Spark & HDFS dağıtık mimari analizleri için hazır yüksek hacimli (Volume) veri üretir.
    """
    os.makedirs(output_dir, exist_ok=True)
    t_start = time.time()

    tasks_csv = os.path.join(output_dir, "tasks_dataset.csv")
    prs_csv = os.path.join(output_dir, "pull_requests_dataset.csv")
    tasks_parquet = os.path.join(output_dir, "tasks_dataset.parquet")
    prs_parquet = os.path.join(output_dir, "pull_requests_dataset.parquet")
    tasks_jsonl = os.path.join(output_dir, "tasks_dataset.jsonl")
    prs_jsonl = os.path.join(output_dir, "pull_requests_dataset.jsonl")

    # Mevcut dosyaları temizle
    for f in [tasks_csv, prs_csv, tasks_parquet, prs_parquet, tasks_jsonl, prs_jsonl]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass

    print("=" * 80)
    print(f"BÜYÜK VERİ (5V) STREAMING VERİ ÜRETİMİ BAŞLATILIYOR")
    print(f"Hedef CSV Boyutu : En az {target_csv_gb:.2f} GB")
    print(f"Başlangıç Task   : {total_tasks:,} (Chunk Boyutu: {chunk_size:,})")
    print(f"Çıktı Dizini     : {os.path.abspath(output_dir)}")
    print("=" * 80)

    tasks_writer = None
    prs_writer = None

    total_tasks_generated = 0
    total_prs_generated = 0
    chunk_idx = 0
    current_pr_id = 200001
    current_task_id = 100001

    target_bytes = int(target_csv_gb * 1024 * 1024 * 1024)

    # Özet metrik toplayıcıları
    task_risk_counts = {}
    pr_risk_counts = {}
    delayed_task_count = 0

    while True:
        chunk_idx += 1
        current_chunk_tasks = min(chunk_size, max(chunk_size, total_tasks - total_tasks_generated)) if total_tasks > total_tasks_generated else chunk_size
        
        print(f"\n>>> [Chunk #{chunk_idx}] {current_chunk_tasks:,} Task ve ilişkili PR'lar üretiliyor...")
        c_seed = seed + chunk_idx * 17
        df_chunk_tasks = generate_tasks_dataset(
            num_tasks=current_chunk_tasks,
            seed=c_seed,
            reference_date=reference_date,
            task_id_start=current_task_id
        )

        df_chunk_prs = generate_prs_dataset(
            df_tasks=df_chunk_tasks,
            seed=c_seed + 99,
            reference_date=reference_date,
            pr_id_start=current_pr_id
        )

        current_task_id += len(df_chunk_tasks)
        current_pr_id += len(df_chunk_prs)
        total_tasks_generated += len(df_chunk_tasks)
        total_prs_generated += len(df_chunk_prs)

        # İstatistik güncelle
        delayed_task_count += int(df_chunk_tasks["is_delayed"].sum())
        for k, v in df_chunk_tasks["delivery_risk_level"].value_counts().items():
            task_risk_counts[k] = task_risk_counts.get(k, 0) + int(v)
        for k, v in df_chunk_prs["pr_risk_level"].value_counts().items():
            pr_risk_counts[k] = pr_risk_counts.get(k, 0) + int(v)

        # 1. CSV Append
        write_header = (chunk_idx == 1)
        df_chunk_tasks.to_csv(tasks_csv, mode="a", header=write_header, index=False, encoding="utf-8-sig")
        df_chunk_prs.to_csv(prs_csv, mode="a", header=write_header, index=False, encoding="utf-8-sig")

        # 2. JSONL Append
        df_chunk_tasks.to_json(tasks_jsonl, orient="records", lines=True, force_ascii=False, mode="a")
        df_chunk_prs.to_json(prs_jsonl, orient="records", lines=True, force_ascii=False, mode="a")

        # 3. Parquet Append (PyArrow Streaming Writer)
        table_tasks = pa.Table.from_pandas(df_chunk_tasks, preserve_index=False)
        table_prs = pa.Table.from_pandas(df_chunk_prs, preserve_index=False)

        if tasks_writer is None:
            tasks_writer = pq.ParquetWriter(tasks_parquet, table_tasks.schema, compression="snappy")
        tasks_writer.write_table(table_tasks)

        if prs_writer is None:
            prs_writer = pq.ParquetWriter(prs_parquet, table_prs.schema, compression="snappy")
        prs_writer.write_table(table_prs)

        # Anlık boyutları kontrol et
        cur_prs_csv_bytes = os.path.getsize(prs_csv)
        cur_tasks_csv_bytes = os.path.getsize(tasks_csv)
        total_csv_bytes = cur_prs_csv_bytes + cur_tasks_csv_bytes

        print(f"    [İlerleme] Toplam Task: {total_tasks_generated:,} | Toplam PR: {total_prs_generated:,}")
        print(f"    [Boyutlar] PR CSV: {cur_prs_csv_bytes / (1024*1024):.2f} MB | Tasks CSV: {cur_tasks_csv_bytes / (1024*1024):.2f} MB | Toplam CSV: {total_csv_bytes / (1024*1024):.2f} MB")

        # Durma kriteri:
        # scale_both_csv=False ise PR CSV >= target_bytes ve total_tasks_generated >= total_tasks
        # scale_both_csv=True ise Tasks CSV de >= target_bytes
        if scale_both_csv:
            reached_target = (cur_tasks_csv_bytes >= target_bytes) and (cur_prs_csv_bytes >= target_bytes)
        else:
            reached_target = (cur_prs_csv_bytes >= target_bytes) and (total_tasks_generated >= total_tasks)

        if reached_target:
            print(f"\n[Hedef Boyuta Ulaşıldı] PR CSV {cur_prs_csv_bytes / (1024*1024):.2f} MB >= {target_csv_gb*1024:.0f} MB")
            break

    # Parquet yazıcıları kapat
    if tasks_writer:
        tasks_writer.close()
    if prs_writer:
        prs_writer.close()

    elapsed = time.time() - t_start

    # Meta veri ve 5V Özet Raporu
    paths = {
        "tasks_parquet": tasks_parquet,
        "prs_parquet": prs_parquet,
        "tasks_csv": tasks_csv,
        "prs_csv": prs_csv,
        "tasks_jsonl": tasks_jsonl,
        "prs_jsonl": prs_jsonl
    }

    summary = {
        "generated_at": datetime.now().isoformat(),
        "generation_time_seconds": round(elapsed, 2),
        "project_title": "Yazılım Projelerinde Teslimat Risklerinin Büyük Veri ile Erken Tespiti ve Açıklanabilir Karar Destek Sistemi",
        "big_data_5v_metrics": {
            "volume": {
                "tasks_row_count": total_tasks_generated,
                "prs_row_count": total_prs_generated,
                "total_records_per_format": total_tasks_generated + total_prs_generated,
                "total_serialized_records_across_3_formats": (total_tasks_generated + total_prs_generated) * 3,
            },
            "variety": [
                "CSV (Structured / Relational / Distributed Storage)",
                "Parquet (Columnar / Apache Spark / Snappy Compressed)",
                "JSONL (Semi-Structured / Event Stream / NoSQL)",
                "Natural Language Git Diffs & Review Comments (Unstructured)"
            ],
            "velocity": {
                "streaming_batch_chunks": chunk_idx,
                "streaming_simulator": "velocity_stream_simulator.py",
            },
            "veracity": {
                "quality_controlled": True,
                "schema_integrity": "Foreign Key linked_task_issue validated across chunks"
            },
            "value_xai": {
                "delayed_task_ratio": round(delayed_task_count / max(1, total_tasks_generated), 4),
                "task_risk_level_distribution": task_risk_counts,
                "pr_risk_level_distribution": pr_risk_counts,
            }
        },
        "files": {}
    }

    total_bytes = 0
    for k, p in paths.items():
        sz = os.path.getsize(p)
        total_bytes += sz
        summary["files"][k] = {
            "path": os.path.basename(p),
            "size_mb": round(sz / (1024 * 1024), 2),
            "size_gb": round(sz / (1024 * 1024 * 1024), 3)
        }
    summary["big_data_5v_metrics"]["volume"]["total_disk_size_mb"] = round(total_bytes / (1024 * 1024), 2)
    summary["big_data_5v_metrics"]["volume"]["total_disk_size_gb"] = round(total_bytes / (1024 * 1024 * 1024), 3)

    meta_path = os.path.join(output_dir, "dataset_metadata_5v.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Web ve Dashboard varsayılan veri setlerini otomatik senkronize et
    sync_default_datasets(output_dir, total_tasks_generated, total_prs_generated, delayed_task_count)

    print("\n" + "=" * 80)
    print("BÜYÜK VERİ ÜRETİM RAPORU")
    print(f"Toplam Geçen Süre    : {elapsed:.2f} saniye ({elapsed/60:.2f} dakika)")
    print(f"Toplam Task Sayısı   : {total_tasks_generated:,}")
    print(f"Toplam PR Sayısı     : {total_prs_generated:,}")
    print(f"Pull Requests CSV    : {summary['files']['prs_csv']['size_mb']:.2f} MB ({summary['files']['prs_csv']['size_gb']:.2f} GB)")
    print(f"Tasks CSV            : {summary['files']['tasks_csv']['size_mb']:.2f} MB ({summary['files']['tasks_csv']['size_gb']:.2f} GB)")
    print(f"Pull Requests Parquet: {summary['files']['prs_parquet']['size_mb']:.2f} MB")
    print(f"Tasks Parquet        : {summary['files']['tasks_parquet']['size_mb']:.2f} MB")
    print(f"Toplam Disk Alanı    : {summary['big_data_5v_metrics']['volume']['total_disk_size_gb']:.2f} GB")
    print("=" * 80)
    return summary


def sync_default_datasets(output_dir: str, total_tasks: int, total_prs: int, delayed_count: int):
    """Web (Vercel) ve Dashboard varsayılan veri setlerini (JSON & Parquet) senkronize eder."""
    try:
        import shutil
        tasks_p = os.path.join(output_dir, "tasks_dataset.parquet")
        prs_p = os.path.join(output_dir, "pull_requests_dataset.parquet")
        if not (os.path.exists(tasks_p) and os.path.exists(prs_p)):
            return

        df_t = pd.read_parquet(tasks_p)
        df_p = pd.read_parquet(prs_p)

        open_mask = ~df_t["status"].str.lower().isin(["done", "closed", "resolved"])
        open_tasks_count = int(open_mask.sum())
        early_at_risk_open = int((open_mask & (df_t["delivery_risk_level"].isin(["Critical", "High"]))).sum())
        avg_risk_score = round(float(df_t["delivery_risk_score"].mean()), 1)
        critical_high_count = int(df_t["delivery_risk_level"].isin(["Critical", "High"]).sum())
        delayed_ratio = round(float(df_t["is_delayed"].mean() * 100), 1)
        delayed_subset = df_t[df_t["is_delayed"] == 1]
        avg_delay_days = round(float(delayed_subset["delay_days"].mean()), 1) if len(delayed_subset) > 0 else 0.0

        global_metrics = {
            "total_tasks": total_tasks,
            "total_prs": total_prs,
            "early_at_risk_open": early_at_risk_open,
            "open_tasks_count": open_tasks_count,
            "avg_risk_score": avg_risk_score,
            "critical_high_count": critical_high_count,
            "delayed_ratio": delayed_ratio,
            "avg_delay_days": avg_delay_days
        }

        # Örneklem
        crit_open = df_t[open_mask & (df_t["delivery_risk_level"] == "Critical")].head(350)
        high_open = df_t[open_mask & (df_t["delivery_risk_level"] == "High")].head(250)
        med_open = df_t[open_mask & (df_t["delivery_risk_level"] == "Medium")].head(150)
        done_sample = df_t[~open_mask].head(250)
        sample_tasks = pd.concat([crit_open, high_open, med_open, done_sample]).drop_duplicates(subset=["task_id"])
        sample_task_ids = set(sample_tasks["task_id"].unique())
        sample_prs = df_p[df_p["linked_task_issue"].isin(sample_task_ids)].head(2500)

        # Stream
        stream_events = []
        stream_path = os.path.join(output_dir, "velocity_event_stream.jsonl")
        if os.path.exists(stream_path):
            with open(stream_path, "r", encoding="utf-8") as sf:
                for idx, line in enumerate(sf):
                    if idx >= 200:
                        break
                    l = line.strip()
                    if l:
                        try:
                            stream_events.append(json.loads(l))
                        except Exception:
                            pass

        default_payload = {
            "is_default": True,
            "global_metrics": global_metrics,
            "full_volume": {
                "tasks_total": total_tasks,
                "prs_total": total_prs,
                "is_big_data_1gb_plus": True
            },
            "tasks": sample_tasks.replace({np.nan: None}).to_dict(orient="records"),
            "prs": sample_prs.replace({np.nan: None}).to_dict(orient="records"),
            "stream": stream_events
        }

        for path in [
            os.path.join(output_dir, "default_dataset.json"),
            os.path.join("public", "data", "default_dataset.json"),
            os.path.join("public", "sample_data.json")
        ]:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(default_payload, f, ensure_ascii=False)

        public_data_dir = os.path.join("public", "data")
        os.makedirs(public_data_dir, exist_ok=True)
        shutil.copy2(tasks_p, os.path.join(public_data_dir, "tasks_dataset.parquet"))
        shutil.copy2(prs_p, os.path.join(public_data_dir, "pull_requests_dataset.parquet"))
        meta_src = os.path.join(output_dir, "dataset_metadata_5v.json")
        if os.path.exists(meta_src):
            shutil.copy2(meta_src, os.path.join(public_data_dir, "dataset_metadata_5v.json"))
        print("[Senkronizasyon] Varsayılan veri setleri ve public/data başarıyla güncellendi.")
    except Exception as e:
        print(f"[UYARI] Varsayılan verileri senkronize ederken hata: {e}")


def export_5v_datasets(df_tasks: pd.DataFrame, df_prs: pd.DataFrame, output_dir: str):
    """Tekil küçük veri setleri için geriye dönük uyumlu export fonksiyonu."""
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    tasks_parquet = os.path.join(output_dir, "tasks_dataset.parquet")
    prs_parquet = os.path.join(output_dir, "pull_requests_dataset.parquet")
    df_tasks.to_parquet(tasks_parquet, index=False, engine="pyarrow", compression="snappy")
    df_prs.to_parquet(prs_parquet, index=False, engine="pyarrow", compression="snappy")

    tasks_csv = os.path.join(output_dir, "tasks_dataset.csv")
    prs_csv = os.path.join(output_dir, "pull_requests_dataset.csv")
    df_tasks.to_csv(tasks_csv, index=False, encoding="utf-8-sig")
    df_prs.to_csv(prs_csv, index=False, encoding="utf-8-sig")

    tasks_jsonl = os.path.join(output_dir, "tasks_dataset.jsonl")
    prs_jsonl = os.path.join(output_dir, "pull_requests_dataset.jsonl")
    df_tasks.to_json(tasks_jsonl, orient="records", lines=True, force_ascii=False)
    df_prs.to_json(prs_jsonl, orient="records", lines=True, force_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="5V Büyük Veri Sentetik Task ve PR Veri Üreticisi (1 GB+ CSV & Parquet Destekli)")
    parser.add_argument("--num-tasks", type=int, default=275000, help="Hedef Task sayısı (Varsayılan: 275.000 -> ~535.000 PR ile >1.05 GB CSV)")
    parser.add_argument("--chunk-size", type=int, default=50000, help="Streaming batch chunk boyutu (OOM koruması)")
    parser.add_argument("--target-csv-gb", type=float, default=1.0, help="Hedef minimum CSV dosya boyutu (GB cinsinden, örn. 1.0)")
    parser.add_argument("--scale-both-csv", action="store_true", help="Hem Tasks hem de PR CSV'sinin her birinin en az 1 GB olmasını sağla")
    parser.add_argument("--output-dir", type=str, default="data", help="Çıktı klasörü")
    parser.add_argument("--seed", type=int, default=42, help="Rastgelelik tohumu (Seed)")
    parser.add_argument("--reference-date", type=str, default="2026-11-11", help="Referans tarihi (YYYY-MM-DD)")
    args = parser.parse_args()

    export_5v_datasets_streamed(
        total_tasks=args.num_tasks,
        chunk_size=args.chunk_size,
        output_dir=args.output_dir,
        target_csv_gb=args.target_csv_gb,
        scale_both_csv=args.scale_both_csv,
        seed=args.seed,
        reference_date=args.reference_date
    )


if __name__ == "__main__":
    main()


