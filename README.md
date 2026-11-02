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

### A. Sadeleştirilmiş Sol Menü (Sidebar) & 4 Özet KPI Kartı
- **Hızlı Filtreler:** Tek seçimli temiz açılır listelerle *Görev Durumu* (`Tüm Görevler`, `Sadece Açık Görevler`, `Sadece Tamamlanan Görevler`), *Proje*, *Risk Seviyesi* (`Critical`, `High`, `Medium`, `Low`) ve *Yazılım Modülü (`Component`)* filtreleme.
- **🧹 Otomatik Veri Temizleme (Veracity Toggle):** Tek tıkla eksik efor tahminlerini (`Missing_Estimate`) doldurma ve aykırı değerleri (`Outlier_Effort` / `Outlier_Diff_Size`) düzeltme.
- **📂 Başka Veri Seti Yükle (Daraltılabilir Panel):** Ekranı kalabalıklaştırmamak için açılır-kapanır kutu içinde `.parquet`, `.csv` ve `.jsonl` formatlarında Task ve PR dosyası yükleme.
- **📥 Hızlı Rapor İndir:** Sol menünün altından doğrudan `.CSV` ve `.JSON` formatında karar destek raporunu dışa aktarma.
- **4 Renk Kodlu KPI Kartı:**
  1. **📋 İncelenen Görev & PR:** Filtrelenen görev ve bağlı Pull Request sayısı.
  2. **🚨 Acil Müdahale Bekleyen:** Henüz tamamlanmamış (`status != Done`) ve yüksek riskli (`>= 50`) açık görev sayısı.
  3. **🎯 Ortalama Risk Skoru:** Görev planlama (`%62`) ve PR entegrasyon (`%38`) ağırlıklı bileşik risk ortalaması (`0-100`).
  4. **⏳ Teslimat Gecikme Oranı:** Geciken görevlerin yüzdesi ve ortalama gün sapması.

---

### B. Dashboard'da Yer Alan 6 Ana Sekme ve İçerik Detayları

#### 📡 1. Erken Uyarı & Genel Bakış (`Sekme 1`)
- **🚨 Acil Müdahale Bekleyen Görevler Tablosu:** Yatay kaydırma karmaşasını önlemek için en kritik 7 sütunla (*Görev, Proje & Modül, Sorumlu & Teslim, Erken Risk Skoru, Neden Riskli? (XAI Özeti), Önerilen Müdahale, İncele*) özetlenmiştir. Satıra tıklandığında doğrudan 2. sekmedeki görev müfettişini açar.
- **📊 Projelere Göre Risk Dağılımı & 📈 Sprint Bazlı Risk Eğilimi:** Yan yana iki net grafikle proje bazlı risk yükünü ve sprint dönemlerindeki gecikme trendini gösterir.
- **🔥 Proje x Yazılım Modülü Risk Isı Haritası (Heatmap):** Ana ekranı boğmamak için isteğe bağlı açılır (`details` / `expander`) panel içinde sunulur.

#### 🔍 2. Görev & PR Müfettişi — Neden Riskli? (`Sekme 2`)
Seçilen veya ID ile aranan (`TASK-100001` vb.) tek bir görevin neden riskli olduğunu ve ne yapılması gerektiğini adım adım açıklar:
- **1. Neden Bu Görev Riskli? (XAI Teşhisi)** ve **2. Ne Yapılmalı? (Karar Destek Önerisi)** kutuları.
- **📊 Risk Puanını Artıran ve Azaltan Faktörler:** Faktörlerin risk puanına olan yerel katkılarını kırmızı (`+` risk artırıcı) ve yeşil (`-` risk azaltıcı) barlarla gösterir.
- **📋 Görev Planlama Özeti & Bağlı PR Kartları:** Göreve bağlı PR'ları tek satırlık özet rozetlerle listeler; uzun `git diff` kod yamaları varsayılan olarak gizlidir ve tıklandığında açılır.

#### 🎛️ 3. What-If (Müdahale) Karar Simülatörü (`Sekme 3`)
- **Hızlı Senaryo Butonları:** *"✨ Blocker & Engelleri Sıfırla"*, *"✂️ Görevi Küçült (5 SP & PR Küçült)"* ve *"🔧 Conflict & CI Hatalarını Çöz"* butonlarıyla tek tıkla müdahale testi.
- **Canlı Sonuç Kartları & Reçete:** Sayfanın en üstünde *Yeni Bileşik Risk Skoru*, *Yeni Gecikme Olasılığı*, *Tahmini Gecikme Süresi* ve *Simülasyon Karar Reçetesi* anlık olarak güncellenir.
- **3 Sütunlu Kontroller:** *1️⃣ Görev Kapsamı & Planlama*, *2️⃣ Süreç Engelleri (Darboğazlar)* ve *3️⃣ PR & CI/CD Kalitesi* kaydırıcıları.

#### 🔀 4. PR & Ekip İş Yükü Analizi (`Sekme 4`)
- **4 PR Özet Kartı:** Toplam PR, Merge Çakışması Oranı, Ortalama Birleştirme Süresi ve CI/CD Hata Oranı.
- **⚔️ Merge Çakışmasının Süreye Etkisi & 🛡️ Güvenlik Taraması (SAST/CVE) Grafikleri.**
- **👨‍💻 Geliştirici İş Yükü ve Risk Dağılımı Tablosu:** Ekip üyelerinin üzerindeki açık görev, yüksek riskli iş ve PR conflict yükünü karşılaştırır.

#### 🤖 5. Yapay Zeka (XAI) & Veri Kalitesi (`Sekme 5`)
- **ML Model Performansı & Küresel XAI:** `RandomForestClassifier` modelinin **ROC-AUC**, **Accuracy**, **F1**, **Precision**, **Recall** skorları, **Küresel XAI Öznitelik Önemi Grafiği** ve **Confusion Matrix** özeti.
- **🧹 5V Veracity & ⚡ 5V Velocity:** Task ve PR veri kalitesi (`data_quality_flag`) dağılım tabloları ve açılır panelde canlı olay akışı (`velocity_event_stream.jsonl`) kayıtları.

#### 📥 6. Aksiyon Planı & Rapor İndir (`Sekme 6`)
- **🎯 Önerilen Müdahale Aksiyonlarına Göre Görev Özeti:** Hangi karar destek aksiyonunun kaç açık ve toplam görev için önerildiğini ve ortalama risk skorunu gruplayarak gösterir.
- **📊 CSV ve 📄 JSON Rapor İndirme:** Seçili filtrelere göre en yüksek riskli görevlerin ve XAI aksiyon önerilerinin tam listesini yönetim toplantıları veya Jira/DevOps entegrasyonu için dışa aktarır ve ilk 50 kaydın önizlemesini sunar.

---

## 6. Vercel Dağıtımı (Serverless & Edge CDN Mimarisi)

Proje, hem yerel **Streamlit (`dashboard.py`)** hem de **Vercel Serverless + Web (`public/` + `api/index.py`)** mimarisiyle %100 uyumlu olacak şekilde yapılandırılmıştır:

- [`vercel.json`](file:///c:/karışık/bigdata_/vercel.json): `/api/*` isteklerini Python FastAPI sunucusuna ([`api/index.py`](file:///c:/karışık/bigdata_/api/index.py)), ana arayüzü ise [`public/index.html`](file:///c:/karışık/bigdata_/public/index.html) ve [`public/app.js`](file:///c:/karışık/bigdata_/public/app.js) dosyalarına yönlendirir.
- **Varsayılan `data/` Veri Seti Desteği:** Sistem hem Streamlit (`dashboard.py`) hem de Web (`public/app.js`) arayüzünde varsayılan olarak doğrudan `data/` klasöründeki gerçek veri setlerini (`data/tasks_dataset.parquet` — **80.000 Görev**, `data/pull_requests_dataset.parquet` — **151.105 PR** ve `data/velocity_event_stream.jsonl` — **10.000 akış olayı**) yükler.
- **Büyük Dosya Yükleme Desteği (Client-Side Parquet / CSV / JSONL Motoru):** Tarayıcı üzerinde doğrudan `.parquet` (`hyparquet`), `.csv` (`PapaParse`) ve `.jsonl` formatlarında yüz binlerce satırlık **Task ve PR** dosyalarını yükleyip analiz edebilirsiniz.

### Vercel'e Yükleme Adımları:
1. Projeyi GitHub deponuza gönderin (`git add . && git commit -m "Vercel XAI Dashboard" && git push`) ve [vercel.com/new](https://vercel.com/new) üzerinden depoyu içe aktarıp **Deploy** butonuna basın.
2. Veya terminalden Vercel CLI ile doğrudan yayınlayın:
   ```powershell
   npx vercel --prod
   ```


