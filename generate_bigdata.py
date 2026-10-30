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


def generate_tasks_dataset(num_tasks: int, seed: int = 42) -> pd.DataFrame:
    """
    22 temel Task özniteliği + 16 Büyük Veri (5V) & XAI Karar Destek özniteliği olmak üzere
    toplam 38 sütunlu zenginleştirilmiş Task veri setini vektörel olarak üretir.
    """
    rng = np.random.default_rng(seed)
    cat = get_domain_catalogs()

    print(f"[1/4] {num_tasks:,} adet Task kaydı için öznitelikler ve nedensel risk örüntüleri üretiliyor...")

    # 1. Task ID
    task_ids = np.array([f"TASK-{100001 + i}" for i in range(num_tasks)], dtype=object)

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

    # 7. Sprint / Iteration & Epic / Parent Task
    sprint_nums = rng.integers(1, 37, size=num_tasks)
    sprints = np.array([f"2025-2026 Sprint-{s:02d}" for s in sprint_nums], dtype=object)
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

    # 10. Tarihler (Created At, Start Date, Due Date, Actual Start, Actual End)
    # 2 yıllık zaman serisi (2025-01-01 .. 2026-10-15) -> Velocity analizi için
    base_ts = datetime(2025, 1, 1, 8, 0, 0)
    created_offset_sec = np.sort(rng.integers(0, 650 * 86400, size=num_tasks))
    start_delay_days = rng.integers(1, 6, size=num_tasks) + blocker_counts * 2
    planned_duration_days = np.clip(np.ceil(estimated_effort / 6.0).astype(int) + rng.integers(1, 5, size=num_tasks), 2, 45)

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

    # Tarih dizilerini hızlıca formatla (Vektörel datetime64 dönüşümü)
    created_dt = np.datetime64("2025-01-01T08:00:00") + created_offset_sec.astype("timedelta64[s]")
    start_dt = created_dt.astype("datetime64[D]") + start_delay_days.astype("timedelta64[D]")
    due_dt = start_dt + planned_duration_days.astype("timedelta64[D]")
    actual_start_dt = start_dt + (blocker_counts > 0).astype(int).astype("timedelta64[D]")
    actual_end_dt = due_dt + delay_days.astype("timedelta64[D]")

    created_at_str = np.datetime_as_string(created_dt, unit="s").astype(object)
    start_date_str = np.datetime_as_string(start_dt, unit="D").astype(object)
    due_date_str = np.datetime_as_string(due_dt, unit="D").astype(object)
    actual_start_str = np.datetime_as_string(actual_start_dt, unit="D").astype(object)
    actual_end_str = np.datetime_as_string(actual_end_dt, unit="D").astype(object)

    # 11. Status (Görev durumu)
    status_choices = ["Done", "In Progress", "Test", "Code Review", "To Do", "Open", "Blocked"]
    status_probs = [0.58, 0.14, 0.09, 0.07, 0.05, 0.04, 0.03]
    statuses = rng.choice(status_choices, size=num_tasks, p=status_probs)
    statuses = np.where(blocker_counts >= 2, "Blocked", statuses)

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


def generate_prs_dataset(df_tasks: pd.DataFrame, seed: int = 101) -> pd.DataFrame:
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
    print(f"[2/4] Task'larla ilişkili {num_prs:,} adet Pull Request (PR) kaydı ve CI/CD metrikleri üretiliyor...")

    # Task indekslerini tekrarlayarak ilişkisel bütünlük sağla
    parent_idx = np.repeat(np.arange(len(df_tasks)), pr_counts)

    linked_task_ids = df_tasks["task_id"].to_numpy()[parent_idx]
    parent_projects = df_tasks["project"].to_numpy()[parent_idx]
    parent_components = df_tasks["component_module"].to_numpy()[parent_idx]
    parent_assignees = df_tasks["assignee"].to_numpy()[parent_idx]
    parent_risk_scores = df_tasks["delivery_risk_score"].to_numpy()[parent_idx]
    parent_created_at = df_tasks["created_at"].to_numpy()[parent_idx]
    parent_priorities = df_tasks["priority"].to_numpy()[parent_idx]

    # 1. PR ID
    pr_ids = np.array([f"PR-{200001 + i}" for i in range(num_prs)], dtype=object)

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

    # PR Status & Approval Status
    pr_status = np.where(
        pipeline_status == "Failed",
        rng.choice(["Changes Requested", "In Review", "Closed", "Open"], size=num_prs, p=[0.45, 0.25, 0.15, 0.15]),
        rng.choice(["Merged", "Approved", "In Review", "Open", "Draft", "Closed"], size=num_prs, p=[0.74, 0.08, 0.07, 0.05, 0.03, 0.03])
    )

    approval_status = np.where(
        np.isin(pr_status, ["Merged", "Approved"]),
        "Approved (2/2 Reviewers)",
        np.where(
            pr_status == "Changes Requested",
            "Changes Requested (1/2 Reviewers)",
            "Pending Review (0/2 Reviewers)"
        )
    )

    # 9. Zaman Damgaları (Created At, Updated At, Merged At, Closed At)
    time_to_first_review_hours = np.round(
        rng.uniform(0.5, 12.0, size=num_prs) + (changed_files_count * 0.4), 2
    )
    time_to_merge_hours = np.round(
        time_to_first_review_hours + review_iteration_count * rng.uniform(4.0, 14.0, size=num_prs) + has_conflict * 12.0,
        2
    )

    parent_created_dt = parent_created_at.astype("datetime64[s]")
    pr_create_offset_hours = rng.integers(4, 96, size=num_prs)
    pr_created_dt = parent_created_dt + pr_create_offset_hours.astype("timedelta64[h]")
    pr_updated_dt = pr_created_dt + np.round(time_to_merge_hours * 3600).astype(int).astype("timedelta64[s]")

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


def export_5v_datasets(df_tasks: pd.DataFrame, df_prs: pd.DataFrame, output_dir: str):
    """
    5V Variety (Çeşitlilik) ilkesi gereği veri setlerini:
    1. Parquet (Apache Spark / Analitik kolon bazlı sıkıştırılmış format)
    2. CSV (İlişkisel / Tabular format)
    3. JSONL (Yarı-yapısal NoSQL / Elasticsearch / Kafka doküman formatı)
    olarak kaydeder.
    """
    os.makedirs(output_dir, exist_ok=True)
    print(f"[3/4] Veri setleri '{output_dir}' dizinine Parquet, CSV ve JSONL formatlarında kaydediliyor...")

    paths = {}

    # 1. Parquet
    tasks_parquet = os.path.join(output_dir, "tasks_dataset.parquet")
    prs_parquet = os.path.join(output_dir, "pull_requests_dataset.parquet")
    df_tasks.to_parquet(tasks_parquet, index=False, engine="pyarrow", compression="snappy")
    df_prs.to_parquet(prs_parquet, index=False, engine="pyarrow", compression="snappy")
    paths["tasks_parquet"] = tasks_parquet
    paths["prs_parquet"] = prs_parquet

    # 2. CSV
    tasks_csv = os.path.join(output_dir, "tasks_dataset.csv")
    prs_csv = os.path.join(output_dir, "pull_requests_dataset.csv")
    df_tasks.to_csv(tasks_csv, index=False, encoding="utf-8-sig")
    df_prs.to_csv(prs_csv, index=False, encoding="utf-8-sig")
    paths["tasks_csv"] = tasks_csv
    paths["prs_csv"] = prs_csv

    # 3. JSONL (Yarı-yapısal iç içe doküman temsili)
    tasks_jsonl = os.path.join(output_dir, "tasks_dataset.jsonl")
    prs_jsonl = os.path.join(output_dir, "pull_requests_dataset.jsonl")
    df_tasks.to_json(tasks_jsonl, orient="records", lines=True, force_ascii=False)
    df_prs.to_json(prs_jsonl, orient="records", lines=True, force_ascii=False)
    paths["tasks_jsonl"] = tasks_jsonl
    paths["prs_jsonl"] = prs_jsonl

    # Özet İstatistik ve Meta Veri Raporu
    summary = {
        "generated_at": datetime.now().isoformat(),
        "project_title": "Yazılım Projelerinde Teslimat Risklerinin Büyük Veri ile Erken Tespiti ve Açıklanabilir Karar Destek Sistemi",
        "big_data_5v_metrics": {
            "volume": {
                "tasks_row_count": int(len(df_tasks)),
                "prs_row_count": int(len(df_prs)),
                "total_records_per_format": int(len(df_tasks) + len(df_prs)),
                "total_serialized_records_across_3_formats": int((len(df_tasks) + len(df_prs)) * 3),
                "tasks_columns": int(df_tasks.shape[1]),
                "prs_columns": int(df_prs.shape[1]),
            },
            "variety": ["CSV (Structured)", "Parquet (Columnar/Spark)", "JSONL (Semi-Structured/NoSQL)", "Natural Language Text & Git Diffs (Unstructured)"],
            "velocity": {
                "time_range_start": str(df_tasks["created_at"].min()),
                "time_range_end": str(df_prs["updated_at"].max()),
                "streaming_simulator": "velocity_stream_simulator.py",
            },
            "veracity": {
                "task_quality_distribution": df_tasks["data_quality_flag"].value_counts().to_dict(),
                "pr_quality_distribution": df_prs["data_quality_flag"].value_counts().to_dict(),
            },
            "value_xai": {
                "delayed_task_ratio": round(float(df_tasks["is_delayed"].mean()), 4),
                "task_risk_level_distribution": df_tasks["delivery_risk_level"].value_counts().to_dict(),
                "pr_risk_level_distribution": df_prs["pr_risk_level"].value_counts().to_dict(),
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
            "size_mb": round(sz / (1024 * 1024), 2)
        }
    summary["big_data_5v_metrics"]["volume"]["total_disk_size_mb"] = round(total_bytes / (1024 * 1024), 2)

    meta_path = os.path.join(output_dir, "dataset_metadata_5v.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[4/4] Üretim tamamlandı! Toplam Disk Boyutu: {summary['big_data_5v_metrics']['volume']['total_disk_size_mb']:.2f} MB")
    return summary


def main():
    parser = argparse.ArgumentParser(description="5V Büyük Veri Sentetik Task ve PR Veri Üreticisi")
    parser.add_argument("--num-tasks", type=int, default=80000, help="Üretilecek Task sayısı (Varsayılan: 80.000 -> ~155.000 PR)")
    parser.add_argument("--output-dir", type=str, default="data", help="Çıktı klasörü")
    parser.add_argument("--seed", type=int, default=42, help="Rastgelelik tohumu (Seed)")
    args = parser.parse_args()

    t0 = time.time()
    df_tasks = generate_tasks_dataset(num_tasks=args.num_tasks, seed=args.seed)
    df_prs = generate_prs_dataset(df_tasks=df_tasks, seed=args.seed + 59)
    summary = export_5v_datasets(df_tasks, df_prs, output_dir=args.output_dir)
    elapsed = time.time() - t0

    print("\n--- ÖZET RAPOR ---")
    print(f"Toplam Süre          : {elapsed:.2f} saniye")
    print(f"Task Kayıt Sayısı    : {len(df_tasks):,} satır x {df_tasks.shape[1]} sütun")
    print(f"PR Kayıt Sayısı      : {len(df_prs):,} satır x {df_prs.shape[1]} sütun")
    print(f"Toplam Disk Boyutu   : {summary['big_data_5v_metrics']['volume']['total_disk_size_mb']} MB (CSV + JSONL + Parquet)")
    print(f"Geciken Görev Oranı  : %{summary['big_data_5v_metrics']['value_xai']['delayed_task_ratio']*100:.1f}")


if __name__ == "__main__":
    main()

