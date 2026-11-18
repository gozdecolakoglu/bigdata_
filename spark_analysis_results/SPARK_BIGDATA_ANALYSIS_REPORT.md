# Apache Spark Büyük Veri Analiz Raporu
**Proje:** Yazılım Projelerinde Teslimat Risklerinin Büyük Veri ile Erken Tespiti  
**Tarih:** 2026-11-17 21:46:49  
**Kullanılan Motor:** Spark-Native Vectorized Engine  

---

## 1. 5V Büyük Veri ve Format Kıyaslama (Benchmark)
* **Orijinal CSV Boyutu:** 1117.71 MB (1.09 GB)
* **Snappy Parquet Boyutu:** 72.76 MB
* **Depolama Tasarrufu:** %93.49 (Sütun bazlı sıkıştırma)
* **Okuma Hız Farkı:** Parquet, CSV'ye göre **129.7x kat daha hızlı** işlendi!
* **Analiz Edilen Toplam Kayıt:** 567,195 satır

---

## 2. Dağıtık Spark Sorgu Sonuçları: Proje Risk Dağılımı
| Proje Kodu ve Adı | Toplam Görev | Ort. Risk Skoru | Kritik Görevler | Gecikme Oranı (%) | Ort. Efor Sapması (%) |
|---|---|---|---|---|---|
| PRJ-ECOM - Yeni Nesil E-Ticaret Mikroservis Dönüşümü | 37,192 | 52.82 | 7469 | %61.25 | %56.3 |
| PRJ-FIN - FinTech Ödeme ve Açık Bankacılık Platformu | 37,423 | 52.67 | 7401 | %61.32 | %56.15 |
| PRJ-CRM - Müşteri 360 ve Gerçek Zamanlı Kampanya Motoru | 37,518 | 52.6 | 7401 | %61.28 | %55.94 |
| PRJ-HEALTH - Telesağlık ve Elektronik Hasta Kayıt Sistemi | 37,796 | 52.57 | 7549 | %61.21 | %55.96 |
| PRJ-LOG - Akıllı Lojistik Rota Optimizasyonu ve Filo Takibi | 37,434 | 52.56 | 7333 | %61.33 | %55.99 |
| PRJ-AI - Kurumsal Üretken Yapay Zeka ve Analitik Asistanı | 37,620 | 52.51 | 7442 | %60.91 | %55.89 |
| PRJ-CLOUD - Çoklu Bulut (Multi-Cloud) Kubernetes Altyapısı | 37,511 | 52.44 | 7371 | %60.79 | %55.72 |
| PRJ-SEC - Sıfır Güven (Zero-Trust) Kimlik ve Erişim Yönetimi | 37,506 | 52.41 | 7270 | %60.84 | %55.67 |

---

## 3. Geliştirici Darboğaz ve Yük Analizi (Top 5)
| Geliştirici | Atanan Görev | Ort. Risk | Geciken Görev | Gecikme Oranı (%) | Karşılaşılan Blocker |
|---|---|---|---|---|---|
| baris.guler | 10118 | 52.91 | 6327 | %62.53 | 4907 |
| hande.ucar | 10014 | 52.9 | 6227 | %62.18 | 4828 |
| caner.ozturk | 10137 | 52.9 | 6256 | %61.71 | 4795 |
| doruk.kaplan | 9893 | 52.52 | 6091 | %61.57 | 4767 |
| selin.aydın | 9973 | 52.3 | 6138 | %61.55 | 4710 |

---

## 4. Büyük Veri Mimarisi ve Spark Dağıtık İşleme Çıkarımları
1. **Columnar Projection & Predicate Pushdown:** Parquet formatı sayesinde Spark sorguları tüm diski taramak yerine sadece sorgulanan sütunları ve blok metaverilerini okumuştur.
2. **Windowing & Distributed Aggregations:** Proje bazlı en riskli görev sıralamaları (Spark Window API) O(N log N) yerine dağıtık partisyonlar üzerinde paralel olarak O(N/P log(N/P)) karmaşıklığında hesaplanmıştır.
3. **Erken Risk İkazı:** `PRJ-FIN` ve `PRJ-ECOM` projelerinde gecikme oranlarının %30'u aştığı tespit edilmiş ve karar destek kuralları tetiklenmiştir.
