# Yazılım Projelerinde Teslimat Risklerinin Büyük Veri (5V) ile Erken Tespiti ve Açıklanabilir Karar Destek Sistemi (XAI)

Bu proje, yazılım geliştirme yaşam döngüsündeki (SDLC) **Görev (Task / Issue)** ve **Kod Birleştirme (Pull Request - PR / CI-CD)** süreçlerinden elde edilen büyük veriyi kullanarak **teslimat gecikmelerini önceden tahmin etmek** ve **Açıklanabilir Yapay Zeka (XAI - SHAP/LIME uyumlu)** ile yöneticilere aksiyon alınabilir karar destek önerileri sunmak üzere tasarlanmıştır.

---

## 1. Büyük Veri (5V) Mimarisi ve Veri Seti Tasarımı

Üretilen sentetik veri seti, Büyük Veri'nin **5V** kriterlerini doğrudan karşılayacak şekilde modellenmiştir:

| 5V Boyutu | Projede Karşılığı ve Uygulama Detayı |
| :--- | :--- |
| **1. Volume (Hacim)** | Varsayılan pakette **80.000 Task** (38 öznitelik) ve bunlarla ilişkili **~151.000 Pull Request** (49 öznitelik) olmak üzere format başına **~231.000 kayıt** (CSV, JSONL ve Parquet genelinde toplam **~693.000 satır** + **10.000 akış olayı**) yer alır. `--num-tasks` parametresi ile milyonlarca satıra (GB/TB ölçeğine) doğrusal ölçeklenir. |
| **2. Velocity (Hız)** | 2 yıllık milisaniye/saniye hassasiyetli zaman serisi (`created_at`, `updated_at`, `merged_at`) ve gerçek zamanlı CI/CD & PR olay akışı üreten `velocity_stream_simulator.py` (Apache Kafka / Spark Structured Streaming uyumlu). |
| **3. Variety (Çeşitlilik)** | **Yapısal (Structured):** Sayısal efor, story point, gecikme günleri (`CSV`, `Parquet`).<br>**Yarı-Yapısal (Semi-Structured):** Commit zincirleri, değişen dosya listeleri, statik analiz metrikleri (`JSONL`).<br>**Yapısal Olmayan (Unstructured):** Türkçe doğal dil görev açıklamaları, kabul kriterleri, PR inceleme yorumları ve `git diff` kod yamaları. |
| **4. Veracity (Doğruluk)** | Gerçek dünya veri kirliliğini ve ön işleme (preprocessing) başarısını test etmek amacıyla `%7-%8` oranında kontrollü veri kusurları (`data_quality_flag`: `Missing_Estimate`, `Outlier_Effort`, `Clock_Skew_Corrected`, `Outlier_Diff_Size`) içerir. |
| **5. Value (Değer / XAI)** | Rastgele gürültü yerine **nedensel (causal) yazılım mühendisliği kuralları** içerir: Bağımlılık sayısı, blocker, kapsam kayması (scope creep), PR büyüklüğü, merge çakışması ve CI/CD hataları; görev gecikmesini (`is_delayed`), risk skorunu (`delivery_risk_score`) ve **XAI açıklama/aksiyon sütunlarını** doğrudan belirler. |

---

## 2. Veri Seti Dosyaları (`data/` Klasörü)

- `data/tasks_dataset.parquet` / `.csv` / `.jsonl` — **Görev (Task) Veri Seti** (38 Sütun)
- `data/pull_requests_dataset.parquet` / `.csv` / `.jsonl` — **Pull Request (PR) Veri Seti** (49 Sütun)
- `data/velocity_event_stream.jsonl` — **Gerçek Zamanlı Olay Akışı Veri Seti** (Kafka / Spark Streaming için)
- `data/dataset_metadata_5v.json` — Veri seti hacim, kalite, risk dağılımı ve 5V özet metrikleri

> **İlişkisel Bütünlük (Foreign Key):** `pull_requests_dataset` içindeki `linked_task_issue` sütunu, `tasks_dataset` içindeki `task_id` sütunu ile birebir (1:N) ilişkilidir.

---

## 3. Veri Sözlüğü (Data Dictionary)

### A. Task (Görev) Veri Seti Sütunları (38 Öznitelik)

#### Temel Görev Öznitelikleri (22 Sütun)
1. `task_id`: Her göreve özel benzersiz kimlik numarası (`TASK-100001` vb.)
2. `task_title`: Görevin kısa ve açıklayıcı başlığı
3. `description`: Görevin kapsamını, amacını ve yapılacak işleri açıklayan metin
4. `project`: Görevin bağlı olduğu proje kodu ve adı
5. `task_type`: Görevin türü (`Story`, `Task`, `Bug`, `Improvement`, `Tech Debt`, `Security Fix`, `Spike`, `Sub-task`)
6. `priority`: Görevin öncelik seviyesi (`Critical`, `High`, `Medium`, `Low`)
7. `assignee`: Görevin atandığı sorumlu geliştirici
8. `reporter_created_by`: Görevi oluşturan kullanıcı
9. `status`: Görevin mevcut durumu (`Open`, `To Do`, `In Progress`, `Code Review`, `Test`, `Blocked`, `Done`)
10. `start_date`: Görevin planlanan başlangıç tarihi
11. `due_date`: Görevin tamamlanması için hedeflenen son tarih
12. `acceptance_criteria`: Görevin tamamlanmış sayılması için karşılanması gereken koşullar
13. `attachment_link`: Görevle ilişkili dosya, doküman, tasarım veya bağlantılar
14. `sprint_iteration`: Görevin dahil olduğu sprint dönemi (`2025-2026 Sprint-01` .. `Sprint-36`)
15. `epic_parent_task`: Görevin bağlı olduğu üst görev / Epic kimliği
16. `estimated_effort_hours`: Tahmin edilen çalışma süresi (saat)
17. `story_points`: Görevin göreceli karmaşıklık puanı (`1, 2, 3, 5, 8, 13, 21`)
18. `labels_tags`: Görevi kategorize etmek için kullanılan etiketler
19. `component_module`: İlişkili yazılım bileşeni (`Backend`, `Frontend`, `API Gateway`, `Database`, `DevOps & CI/CD`, vb.)
20. `environment`: Görevin geçerli olduğu ortam (`Development`, `Test`, `Staging`, `Production`)
21. `dependencies`: Ön koşul oluşturan diğer görev kimlikleri
22. `watchers_followers`: Görevdeki gelişmeleri takip eden kullanıcılar

#### 5V, Teslimat Riski ve XAI Karar Destek Öznitelikleri (16 Ek Sütun)
23. `created_at`: Görev oluşturulma zaman damgası
24. `actual_start_date`: Gerçekleşen başlangıç tarihi
25. `actual_end_date`: Gerçekleşen bitiş tarihi
26. `actual_effort_hours`: Gerçekleşen çalışma süresi (saat)
27. `effort_variance_ratio`: Efor sapma oranı `(actual - estimated) / estimated`
28. `delay_days`: Teslimat gecikme süresi (gün)
29. `is_delayed`: **Hedef Değişken 1 (Binary Classification)** — Görev gecikti mi? (`1`: Evet, `0`: Hayır)
30. `reopen_count`: Görevin testten/incelemeden geri dönme sayısı
31. `blocker_count`: Görev sürecinde karşılaşılan blokaj/engel sayısı
32. `requirement_change_count`: Kapsam/gereksinim değişikliği sayısı (Scope Creep)
33. `linked_pr_count`: Göreve bağlı açılan toplam PR sayısı
34. `delivery_risk_score`: **Hedef Değişken 2 (Regression)** — `0-100` arası teslimat risk skoru
35. `delivery_risk_level`: **Hedef Değişken 3 (Multi-class)** — Risk sınıfı (`Low`, `Medium`, `High`, `Critical`)
36. `xai_primary_risk_factors`: **XAI Açıklaması** — Riski tetikleyen en önemli nedenler ve katkı yüzdeleri
37. `xai_recommended_actions`: **Karar Destek Önerisi** — Gecikmeyi önlemek için önerilen somut yönetim aksiyonu
38. `data_quality_flag`: **5V Veracity Bayrağı** — Veri kalitesi etiketi (`Clean`, `Missing_Estimate`, `Outlier_Effort`, vb.)

---

### B. Pull Request (PR) Veri Seti Sütunları (49 Öznitelik)

#### Temel PR Öznitelikleri (36 Sütun)
1. `pr_id`: Pull Request benzersiz kimlik numarası (`PR-200001` vb.)
2. `pr_title`: Yapılan değişikliği özetleyen başlık
3. `pr_description`: Değişikliğin amacını, kapsamını ve yapılan işlemleri açıklayan metin
4. `repository`: Değişikliğin yapıldığı kod deposu
5. `source_branch`: Değişikliklerin gönderildiği kaynak branch
6. `target_branch`: Değişikliklerin birleştirileceği hedef branch (`main`, `develop`, vb.)
7. `linked_task_issue`: PR ile ilişkili Task ID (`tasks_dataset.task_id` referansı)
8. `pr_author_created_by`: PR'ı oluşturan geliştirici
9. `reviewers`: Kod incelemesini yapan kişiler
10. `assignee`: PR sürecinin takibinden sorumlu kişi
11. `status`: PR durumu (`Draft`, `Open`, `In Review`, `Approved`, `Changes Requested`, `Merged`, `Closed`)
12. `created_at`: PR oluşturulma tarihi ve saati
13. `updated_at`: PR son güncellenme tarihi ve saati
14. `changed_files`: Değiştirilen dosyaların listesi
15. `commits`: PR'a dahil edilen commit kayıtları ve özetleri
16. `diff_code_changes`: Kaynak ve hedef branch arasındaki kod değişiklikleri (`git diff` yaması)
17. `review_comments`: Reviewer'ların kod üzerindeki yorumları ve geri bildirimleri
18. `approval_status`: Gerekli kod inceleme onaylarının durumu
19. `merge_strategy`: Birleştirme yöntemi (`Squash and Merge`, `Merge Commit`, `Rebase and Merge`)
20. `merge_conflicts`: Birleştirme sırasında oluşan çakışmalar ve çözüm durumu
21. `build_status`: Otomatik build işleminin sonucu (`Success`, `Failed`, `Unstable`)
22. `test_status`: Otomatik testlerin ve kapsama oranının sonucu
23. `code_quality_static_analysis`: SonarQube kod kalitesi, lint ve statik analiz sonuçları
24. `security_scan_status`: Güvenlik taramalarının (SAST/Dependency) sonucu
25. `pipeline_status`: CI/CD süreçlerinin genel durumu
26. `merge_requirements`: Birleştirme için gereken koşullar
27. `merge_permission`: PR'ı birleştirme yetkisine sahip roller
28. `merged_by`: PR'ı birleştiren kullanıcı
29. `merged_at`: Birleştirme tarihi ve saati
30. `closed_by`: Birleştirilmeden kapatılan PR'ı kapatan kullanıcı
31. `closed_at`: PR kapatılma tarihi ve saati
32. `labels_tags`: PR etiketleri
33. `milestone_release`: İlişkili kilometre taşı veya sürüm (`v2.1.0-release` vb.)
34. `linked_documentation`: İlişkili teknik dokümantasyon (ADR/Confluence) bağlantısı
35. `deployment_status`: İlgili ortama dağıtım durumu (`Deployed to Production`, `Canary Rollout`, `Rolled Back`, vb.)
36. `rollback_plan`: Sorun yaşanması durumunda geri dönüş planı

#### 5V, PR Entegrasyon Riski ve XAI Öznitelikleri (13 Ek Sütun)
37. `lines_added`: Eklenen kod satırı sayısı
38. `lines_deleted`: Silinen kod satırı sayısı
39. `changed_files_count`: Değiştirilen toplam dosya sayısı
40. `commit_count`: PR içindeki toplam commit sayısı
41. `review_iteration_count`: Kaç tur kod inceleme/revizyon döngüsü yaşandığı
42. `time_to_first_review_hours`: İlk incelemeye kadar geçen süre (saat)
43. `time_to_merge_hours`: PR açılışından birleştirmeye kadar geçen toplam süre (saat)
44. `ci_failure_count`: CI/CD pipeline kırılma/hata sayısı
45. `pr_risk_score`: `0-100` arası PR entegrasyon ve teslimat risk skoru
46. `pr_risk_level`: PR risk seviyesi (`Low`, `Medium`, `High`, `Critical`)
47. `xai_pr_risk_factors`: PR özelinde açıklanabilir risk faktörleri
48. `xai_pr_recommended_actions`: PR darboğazını çözmek için karar destek önerisi
49. `data_quality_flag`: 5V Veracity veri kalitesi bayrağı

---

## 4. Kullanım ve Ölçeklendirme Komutları

Daha büyük hacimli (örneğin 500.000 Task ve ~1.000.000 PR) veri üretmek için:

```bash
python generate_bigdata.py --num-tasks 500000 --output-dir data
```

Gerçek zamanlı akış (Velocity / Streaming) olayları üretmek için:

```bash
# 50.000 olayı toplu üret:
python velocity_stream_simulator.py --events 50000 --output data/velocity_event_stream.jsonl

# Saniyede 200 olay hızında canlı akış simülasyonu başlat:
python velocity_stream_simulator.py --events 10000 --eps 200 --output data/live_stream.jsonl
```

---

## 5. Erken Risk Tespiti ve Açıklanabilir Karar Destek Sistemi (XAI) Dashboard'u

Görev (Task) ve Pull Request (PR) veri setleri yüklendiğinde teslimat gecikmelerini erken aşamada tahmin eden ve yöneticilere açıklanabilir karar destek önerileri sunan interaktif dashboard'u başlatmak için:

```powershell
# Doğrudan çalıştırma (Windows - Tarayıcıyı otomatik açar):
.\run_dashboard.bat

# Veya Streamlit komutu ile:
py -m streamlit run dashboard.py --server.headless true
```

### A. Sol Kenar Çubuğu (Sidebar) & Üst KPI Gösterge Paneli
- **1. Veri Seti Yükleme Merkezi (Task & PR):** `.parquet`, `.csv` ve `.jsonl` formatlarında Görev (`tasks_dataset`) ve Pull Request (`pull_requests_dataset`) dosyalarını sürükle-bırak ile yükleme veya hazır 5V veri setini tek tıkla analiz etme. Eksik risk veya XAI sütunları varsa sistem ham veriden otomatik olarak hesaplar.
- **2. 5V Veracity & Ön İşleme (Clean & Impute Toggle):** Tek tıkla `Missing_Estimate` eksik efor tahminlerini Story Point bazlı doldurma (`story_points * 5.8`) ve `Outlier_Effort` / `Outlier_Diff_Size` aykırı değerlerini otomatik temizleme.
- **3. Küresel Karar Filtreleri:** Proje, Risk Seviyesi (`Critical`, `High`, `Medium`, `Low`), Görev Kapsamı (*Tüm Görevler*, *Sadece Aktif/Açık Görevler - Erken Müdahale Odaklı*, *Sadece Tamamlanan Görevler*) ve Yazılım Bileşeni (`Component`) filtreleri.
- **5 Üst Düzey KPI Kartı:**
  1. **📋 Analiz Edilen Görev & PR:** Filtrelenen toplam görev ve bunlara bağlı ilişkisel (`1:N`) Pull Request sayısı.
  2. **🚨 Erken Uyarı (Açık Riskli İşler):** Henüz tamamlanmamış (`status != Done`) ve Bileşik Erken Risk Skoru `>= 50` olan acil müdahale bekleyen görev sayısı.
  3. **🎯 Bileşik Erken Risk Skoru:** Görev planlama riski (`%62`) ve PR entegrasyon riskinin (`%38`) ağırlıklı ortalaması (`0-100`) ile Kritik/Yüksek riskli görev oranı.
  4. **⏳ Teslimat Gecikme Oranı:** Geciken görevlerin yüzdesi (`is_delayed == 1`) ve geciken işlerde ortalama gün sapması (`delay_days`).
  5. **🔀 PR & CI/CD Darboğaz Oranı:** Yüksek veya Kritik entegrasyon riski taşıyan Pull Request yüzdesi.

---

### B. Dashboard'da Yer Alan 7 Ana Başlık (Sekme) ve İçerik Detayları

#### 📡 1. Erken Uyarı Radarı & Özet (`Sekme 1`)
Henüz tamamlanmamış (`status != Done`) görevleri ve bunlara bağlı açık Pull Request / CI-CD sinyallerini (`task_id == linked_task_issue`) birleştirerek teslimat tarihi (`due_date`) gelmeden önce gecikme olasılığını tespit eder:
- **Kritik Erken Uyarı Radarı Tablosu:** En yüksek riskli açık görevleri; *Görev ID, Proje, Bileşen, Durum, Öncelik, Sorumlu, Hedef Teslim Tarihi, Bileşik Erken Risk Skoru (0-100 ilerleme çubuğu), Erken Gecikme Olasılığı (%), Blocker sayısı, PR Conflict sayısı, CI Hata sayısı, XAI Risk Nedenleri* ve *Önerilen Erken Müdahale Aksiyonu* sütunlarıyla listeler (Web arayüzünde satıra tıklandığında doğrudan 3. sekmedeki Tekil Müfettiş'te açılır).
- **📊 Proje Bazlı Risk Sınıfı Dağılımı Grafiği:** Her projedeki görevlerin `Critical`, `High`, `Medium` ve `Low` risk sınıflarına göre yığılmış sütun/çubuk dağılımı.
- **🔥 Proje x Yazılım Bileşeni Erken Risk Isı Haritası (Heatmap):** Hangi projenin hangi yazılım modülünde (`Backend`, `Frontend`, `API Gateway`, `Database`, `DevOps & CI/CD` vb.) ortalama erken risk skorunun ve gecikme yüzdesinin yoğunlaştığını gösteren 2 boyutlu ısı matrisi.
- **📈 Sprint Dönemlerine Göre Teslimat Riski ve Gecikme Eğilimi (Velocity Trend):** `Sprint-01`'den `Sprint-36`'ya kadar tüm sprint dönemlerinde *Ortalama Bileşik Erken Risk Skoru* (mavi çizgi) ile *Geciken Görev Oranı (%)* (turuncu kesikli çizgi) zaman serisi karşılaştırması.

---

#### 🤖 2. ML Erken Tespit Motoru & Küresel XAI (`Sekme 2`)
Görev tamamlanmadan **önce** bilinen 13 erken sinyali kullanarak teslimat gecikmesini (`is_delayed`) tahmin eden makine öğrenmesi (`RandomForestClassifier`) motoru ve küresel açıklanabilirlik (Global XAI) katmanıdır:
- **Kullanılan 13 Erken Uyarı Özniteliği:**
  - *Görev Planlama Sinyalleri (9 öznitelik):* `blocker_count` (Blocker sayısı), `dependency_count_calc` (Bağımlılık sayısı), `requirement_change_count` (Scope Creep / kapsam değişimi), `reopen_count` (Testten geri dönüş), `story_points` (Karmaşıklık puanı), `est_effort_clean` (Tahmini efor saati), `optimism_ratio_calc` (SP / Tahmini efor iyimserlik oranı), `priority_num` (Öncelik seviyesi), `env_num` (Hedef ortam kritikliliği).
  - *PR & CI/CD Erken Sinyalleri (4 öznitelik):* `pr_total_changed_files` (Değişen dosya sayısı), `pr_total_ci_failures` (CI/CD pipeline kırılma sayısı), `pr_conflict_count` (Merge çakışması sayısı), `pr_max_review_iterations` (Kod inceleme döngü/tur sayısı).
- **5 Model Performans Metriği Kartı:** Test veri seti üzerinde hesaplanan **ROC-AUC Skoru** (`~0.920`), **Doğruluk / Accuracy** (`~%84.9`), **F1-Skoru** (`~0.874`), **Kesinlik / Precision** (`~%88.6`) ve **Duyarlılık / Recall - Erken Yakalama Oranı** (`~%86.1`).
- **🌐 Küresel XAI Öznitelik Önemi Grafiği (Global Feature Importance):** 13 erken sinyalin model kararındaki yüzde etki ağırlıklarını büyükten küçüğe sıralayan yatay bar grafiği.
- **🧠 En Sık Karşılaşılan Nedensel XAI Risk Örüntüleri Tablosu:** Veri setinde gecikmeye en çok yol açan birincil XAI risk faktörü kombinasyonlarının (`xai_primary_risk_factors`) görev sayısı ve oran (%) tablosu.
- **🎯 Doğrulama Karmaşıklık Matrisi (Confusion Matrix):** *Gerçek Zamanında (0) / Gerçek Gecikti (1)* ile *Tahmin Zamanında (0) / Tahmin Gecikecek (1)* dağılımını gösteren doğrulama tablosu.

---

#### 🔍 3. Tekil Görev/PR XAI Karar Müfettişi (`Sekme 3`)
Seçilen veya ID ile aranan (`TASK-100001` vb.) tek bir görevin neden riskli olduğunu (**Local Explainability / SHAP-stili yerel katkı**) ve bağlı PR/CI-CD darboğazlarını uçtan uca inceleyen derinlemesine analiz ekranıdır:
- **Akıllı Görev Seçici ve Arama Motoru:** Görevleri *En Yüksek Erken Risk Skoruna Göre*, *En Fazla Blocker & PR Çakışmasına Göre* veya *Görev Numarasına Göre* sıralayarak seçme ya da doğrudan Task ID yazarak bulma.
- **Görev Kimlik & Risk Özet Kartı:** Görev başlığı, proje, yazılım bileşeni, sorumlu geliştirici, mevcut durum, öncelik rozeti, **Bileşik Erken Risk Skoru (`0-100`)** ve **Erken Gecikme Olasılığı (`%`)**.
- **🧠 XAI Nedensel Risk Açıklaması Kutusu:** Görevin gecikme riskini tetikleyen birincil faktörleri ve yüzde katkılarını (`xai_primary_risk_factors`) vurgular.
- **💡 Önerilen Karar Destek Aksiyonu Kutusu:** Gecikmeyi önlemek için yöneticiye/Scrum Master'a önerilen somut müdahale aksiyonunu (`xai_recommended_actions`) sunar.
- **📊 Lokal XAI Faktör Katkı Analizi (SHAP-Stili Puan Dağılımı Grafiği):** Baz sistem karmaşıklığı (`+10 puan`) üzerine her bir faktörün (*Blocker, Bağımlılıklar, Scope Creep, Reopen, Story Points, İyimser Efor, Öncelik, Hedef Ortam, PR & CI/CD Darboğazı*) risk skorunu kaç puan artırdığını (**kırmızı `+` barlar**) veya azalttığını (**yeşil `-` barlar**) gösteren yerel açıklanabilirlik grafiği.
- **📋 Görev Planlama, Kapsam ve Metin Detayları:** Sprint/Epic bilgisi, planlanan başlangıç ve teslim tarihleri, Story Points, tahmini vs. gerçekleşen efor, ön koşul görevler (`dependencies`), `data_quality_flag` ve yapısal olmayan (**Unstructured**) Türkçe **Görev Açıklaması (`description`)** ile **Kabul Kriterleri (`acceptance_criteria`)**.
- **🔀 Bağlı Pull Request (PR) ve CI/CD Kayıtları (`1:N` İlişkisel Görünüm):** Göreve bağlı açılan tüm PR'ların detaylı kartları:
  - Repo ve kaynak/hedef branch (`feature/... -> main`), PR yazarı ve reviewer listesi.
  - Değişen dosya sayısı, eklenen/silinen kod satırı (`+lines / -lines`), commit sayısı.
  - Merge Conflict durumu, Pipeline & Build durumu, CI hata sayısı, ilk inceleme süresi (`time_to_first_review_hours`) ve birleştirme süresi (`time_to_merge_hours`).
  - **SonarQube Statik Kod Analizi**, **SAST/CVE Güvenlik Taraması**, **PR XAI Risk Nedeni**, **PR XAI Karar Önerisi**, gerçek **`git diff` kod yaması (`diff_code_changes`)** ve **Reviewer yorumları (`review_comments`)**.

---

#### 🎛️ 4. What-If (Karşı-Olgusal / Counterfactual) Karar Simülatörü (`Sekme 4`)
Yöneticilerin veya takım liderlerinin bir görevin parametrelerine **erken müdahale ettiklerinde** teslimat riskinin ve tahmini gecikme süresinin nasıl değişeceğini canlı olarak test ettikleri karşı-olgusal (counterfactual) simülasyon laboratuvarıdır:
- **1️⃣ Görev Kapsam & Planlama Kontrolleri:** *Karmaşıklık Puanı (`Story Points`: 1–21)*, *Tahmini Efor (`Saat`: 2–160)*, *Görev Önceliği (`Low`, `Medium`, `High`, `Critical`)* ve *Hedef Ortam (`Development`, `Test`, `Staging`, `Production`)* kaydırıcı ve seçicileri.
- **2️⃣ Süreç Darboğazları & Bağımlılık Kontrolleri:** *Blocker Sayısı (`0–5`)*, *Ön Koşul Görev Bağımlılığı (`0–5`)*, *Scope Creep / Kapsam Değişimi (`0–5`)* ve *Testten Geri Dönüş / Reopen (`0–4`)* kaydırıcıları.
- **3️⃣ PR & CI/CD Entegrasyon Kalitesi Kontrolleri:** *PR Değişen Dosya Sayısı (`1–50`)*, *PR Eklenen Kod Satırı (`10–2500`)*, *CI/CD Pipeline Kırılma Sayısı (`0–6`)* ve *PR Merge Çakışması (`Conflict Var/Yok`)* kontrolleri.
- **Canlı Simülasyon Sonuç Kartları (Referans Görev ile Karşılaştırmalı Delta):**
  1. **Simüle Edilen Bileşik Risk Skoru (`0-100`):** Görevin orijinal risk skoruna göre kaç puan düştüğünü/arttığını anlık gösterir.
  2. **Simüle Edilen Gecikme Olasılığı (`%`):** Orijinal olasılığa kıyasla yüzdesel iyileşme deltası.
  3. **Tahmini Gecikme Süresi (`Gün`):** Müdahale sonrası kaç iş günü kazanılacağını (`-X gün`) hesaplar.
  4. **Simüle Edilen Yeni Risk Sınıfı:** `Critical` → `High` → `Medium` → `Low` sınıf geçişini gösterir.
- **🩺 XAI Karşı-Olgusal (Counterfactual) Karar Reçetesi:** Seçilen parametrelere göre hangi müdahalenin (örn. *"Blocker kaldırılırsa risk -16 puan düşer"*, *"Görevi 5 SP'lik alt parçalara bölmek riski ~14 puan azaltır"*, *"PR dosya sayısını 10'un altına çekmek ve günlük rebase yapmak entegrasyon riskini -%35 iyileştirir"*) ne kadar kazanım sağlayacağını reçete halinde listeler.

---

#### 🔀 5. PR, Kod İnceleme & CI/CD Analitiği (`Sekme 5`)
Kod birleştirme, statik analiz, güvenlik taraması ve geliştirici iş yükü darboğazlarını inceleyen mühendislik analitiği modülüdür:
- **4 PR & CI/CD Özet Metrik Kartı:**
  1. *Toplam İncelenen PR Sayısı*
  2. *Merge Çakışması (Conflict) Oranı (%)* ve çakışmalı PR adedi
  3. *Ortalama Birleştirme Süresi (`time_to_merge_hours`)* ve *İlk İncelemeye Kadar Geçen Süre (`time_to_first_review_hours`)*
  4. *CI/CD Pipeline Kırılma Oranı (%)* ve ortalama kod inceleme turu (`review_iteration_count`)
- **⚔️ Merge Conflict Durumuna Göre PR Risk ve Birleştirme Süresi Grafiği:** Temiz PR'lar ile çakışmalı (`Merge Conflict`) PR'ların ortalama birleştirme sürelerini (saat) ve PR risk skorlarını karşılaştıran grafik.
- **🛡️ Güvenlik Taraması (SAST/CVE) & SonarQube Kalite Kapısı Dağılımı:** PR'ların güvenlik taraması sonuçlarına (`Passed`, `Warning`, `Critical Vulnerability` vb.) ve risk sınıflarına göre dağılım grafiği.
- **👨‍💻 Geliştirici / Sorumlu Bazlı Görev ve PR Risk Yükü Matrisi:** Her bir geliştiricinin (`assignee`) üzerindeki *Toplam Görev*, *Açık Görev*, *Kritik/Yüksek Riskli Görev Sayısı*, *Ortalama Erken Risk Skoru*, *Gecikme Oranı (%)* ve *Toplam PR Conflict Sayısı* değerlerini sıralayarak iş yükü dengesizliklerini ortaya koyan tablo.

---

#### 🏛️ 6. Büyük Veri (5V), Veracity & Canlı Akış (`Sekme 6`)
Büyük Veri mimarisinin **Veracity (Veri Doğruluğu / Kalitesi)** ve **Velocity (Gerçek Zamanlı Olay Akışı)** boyutlarını canlı olarak izleyen modüldür:
- **🔍 4V - Veracity (Görev Veri Kalitesi & Kusur Dağılımı Tablosu):** Task veri setindeki `data_quality_flag` etiketlerinin (`Clean`, `Missing_Estimate`, `Outlier_Effort`, `Clock_Skew_Corrected` vb.) kayıt sayısı ve yüzdesel dağılımı.
- **🔍 4V - Veracity (PR Veri Kalitesi & Kusur Dağılımı Tablosu):** PR veri setindeki `data_quality_flag` etiketlerinin (`Clean`, `Outlier_Diff_Size` vb.) kayıt sayısı ve yüzdesel dağılımı.
- **⚡ 2V - Velocity: Gerçek Zamanlı CI/CD & PR Olay Akışı Radarı (`velocity_event_stream.jsonl`):**
  - *3 Canlı Akış Metriği:* Okunan canlı akış olayı sayısı, anlık kritik risk sıçraması (`instant_risk_delta > +15 puan`) yaşanan olay sayısı ve ortalama anlık risk değişimi.
  - *Streaming Event Radar Tablosu:* Akan olayları *Olay ID, Zaman Damgası, Olay Türü (`CI_PIPELINE_FAILED`, `MERGE_CONFLICT_DETECTED`, `SCOPE_CREEP_ADDED`, `BLOCKER_FLAGGED` vb.), Proje, Bileşen, Görev/PR ID, Geliştirici, Anlık Risk Deltası (`+puan`)* ve *XAI Akış Uyarısı* sütunlarıyla listeler.

---

#### 📥 7. Aksiyon Planı & Rapor İndir (`Sekme 7`)
Seçili filtrelere göre erken müdahale gerektiren yüksek riskli görevlerin ve XAI karar destek önerilerinin yönetim toplantıları, Sprint Planning veya Jira/DevOps entegrasyonlarında kullanılmak üzere dışa aktarıldığı modüldür:
- **📊 CSV Rapor İndirme (`xai_erken_risk_karar_destek_raporu.csv`):** UTF-8 BOM (`utf-8-sig`) kodlamasıyla Excel ve BI araçlarıyla tam uyumlu tablo çıktısı.
- **📄 JSON Rapor İndirme (`xai_erken_risk_karar_destek_raporu.json`):** Jira, GitHub Issues, GitLab veya kurumsal webhook/API entegrasyonları için yapılandırılmış JSON çıktısı.
- **Önizleme Tablosu:** İndirilecek rapordaki en yüksek riskli görevlerin (*Görev ID, Başlık, Proje, Bileşen, Durum, Öncelik, Sorumlu, Hedef Teslim, Bileşik Erken Risk Skoru, Risk Sınıfı, Erken Gecikme Olasılığı, Blocker, PR Conflict, CI Hata, XAI Birincil Risk Faktörleri* ve *Önerilen Karar Destek Aksiyonu*) canlı önizlemesi.

---

## 6. Vercel Dağıtımı (Serverless & Edge CDN Mimarisi)

Proje, hem yerel **Streamlit (`dashboard.py`)** hem de **Vercel Serverless + Web (`public/` + `api/index.py`)** mimarisiyle %100 uyumlu olacak şekilde yapılandırılmıştır:

- [`vercel.json`](file:///c:/karışık/bigdata_/vercel.json): `/api/*` isteklerini Python FastAPI sunucusuna ([`api/index.py`](file:///c:/karışık/bigdata_/api/index.py)), ana arayüzü ise [`public/index.html`](file:///c:/karışık/bigdata_/public/index.html) ve [`public/app.js`](file:///c:/karışık/bigdata_/public/app.js) dosyalarına yönlendirir.
- **Büyük Dosya Yükleme Desteği (Client-Side Parquet / CSV / JSONL Motoru):** Vercel Serverless fonksiyonlarının 4.5 MB payload limitine takılmadan, tarayıcı üzerinde doğrudan `.parquet` (`hyparquet`), `.csv` (`PapaParse`) ve `.jsonl` formatlarında yüz binlerce satırlık **Task ve PR** dosyalarını yükleyip analiz edebilirsiniz.
- **Hazır Örneklem (`public/sample_data.json`):** 1 GB'lık yerel `data/` klasörü [`.vercelignore`](file:///c:/karışık/bigdata_/.vercelignore) ve [`.gitignore`](file:///c:/karışık/bigdata_/.gitignore) ile hariç tutulmuş, Vercel üzerinde açılır açılmaz çalışması için `public/sample_data.json` (500 Task + 938 PR + 120 canlı akış olayı) eklenmiştir.

### Vercel'e Yükleme Adımları:
1. Projeyi GitHub deponuza gönderin (`git add . && git commit -m "Vercel XAI Dashboard" && git push`) ve [vercel.com/new](https://vercel.com/new) üzerinden depoyu içe aktarıp **Deploy** butonuna basın.
2. Veya terminalden Vercel CLI ile doğrudan yayınlayın:
   ```powershell
   npx vercel --prod
   ```


