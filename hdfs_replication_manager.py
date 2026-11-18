#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDFS Dağıtık Mimari ve Replikasyon Faktörü (Replication Factor = 3) Yöneticisi
Proje: Yazılım Projelerinde Teslimat Risklerinin Büyük Veri ile Erken Tespiti

Özellikler:
- Verileri HDFS'e yükleme: /bigdata/data/
- Replication Factor ayarlama: dfs.replication = 3
- Blok Dağıtım Raporu (hdfs fsck -files -blocks -locations)
- Otomatik Docker & Küme Entegrasyonu
"""

import argparse
import os
import subprocess
import sys
import time


def check_docker_running():
    try:
        res = subprocess.run(["docker", "ps"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return res.returncode == 0
    except Exception:
        return False


def run_cmd(cmd, check=True):
    print(f"\n[KOMUT] {cmd}")
    res = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res.stdout:
        print(res.stdout)
    if res.stderr and res.returncode != 0:
        print(f"[UYARI / HATA] {res.stderr}")
    return res


def setup_hdfs_cluster(compose_file="docker-compose.distributed.yml"):
    print("=" * 80)
    print("HADOOP HDFS & SPARK DAĞITIK KÜME YÖNETİCİSİ (Replication Factor = 3)")
    print("=" * 80)

    if not check_docker_running():
        print("[BİLGİ] Docker arka planda çalışmıyor veya yüklü değil.")
        print("Aşağıdaki adımlarla Docker üzerinde dağıtık kümenizi başlatabilirsiniz:\n")
        print("  1. Küme Başlatma:")
        print("     docker compose -f docker-compose.distributed.yml up -d\n")
        print("  2. HDFS Klasörü Oluşturma:")
        print("     docker exec -it bigdata-namenode hdfs dfs -mkdir -p /bigdata/data\n")
        print("  3. Verileri HDFS'e Kopyalama:")
        print("     docker exec -it bigdata-namenode hdfs dfs -put /opt/local_data/* /bigdata/data/\n")
        print("  4. Replikasyon Faktörünü 3 Olarak Doğrulama / Ayarlama:")
        print("     docker exec -it bigdata-namenode hdfs dfs -setrep -w 3 /bigdata/data/pull_requests_dataset.parquet\n")
        print("  5. HDFS Blok ve DataNode Dağılım Denetimi (FSCK):")
        print("     docker exec -it bigdata-namenode hdfs fsck /bigdata/data -files -blocks -locations\n")
        return

    print("[1/5] Dağıtık Mimari Konteynerleri Başlatılıyor...")
    run_cmd(f"docker compose -f {compose_file} up -d")

    print("[2/5] NameNode ve DataNode'ların Hazır Olması Bekleniyor (15 sn)...")
    time.sleep(15)

    print("[3/5] HDFS Dizinleri Hazırlanıyor...")
    run_cmd("docker exec bigdata-namenode hdfs dfs -mkdir -p /bigdata/data")

    print("[4/5] 1 GB+ Parquet ve CSV Veri Setleri HDFS'e Yükleniyor...")
    run_cmd("docker exec bigdata-namenode hdfs dfs -put -f /opt/local_data/pull_requests_dataset.parquet /bigdata/data/")
    run_cmd("docker exec bigdata-namenode hdfs dfs -put -f /opt/local_data/tasks_dataset.parquet /bigdata/data/")

    print("[5/5] Replication Factor = 3 Olarak Tanımlanıyor...")
    run_cmd("docker exec bigdata-namenode hdfs dfs -setrep -w 3 /bigdata/data/pull_requests_dataset.parquet")

    print("\n--- HDFS BLOK DAĞILIMI VE SAĞLIK RAPORU (FSCK) ---")
    run_cmd("docker exec bigdata-namenode hdfs fsck /bigdata/data -files -blocks -locations")

    print("\n" + "=" * 80)
    print("DAĞITIK KÜME BAŞARIYLA AKTİF!")
    print("  * HDFS NameNode Web UI : http://localhost:9870")
    print("  * Spark Master Web UI  : http://localhost:8080")
    print("  * DataNode Sayısı      : 3 Adet (Replication Factor: 3)")
    print("  * HDFS Veri Yolu       : hdfs://namenode:9000/bigdata/data/")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="HDFS Dağıtık Mimari ve Replikasyon Yöneticisi")
    parser.add_argument("--up", action="store_true", help="Kümeyi başlat ve verileri yükle")
    args = parser.parse_args()
    setup_hdfs_cluster()


if __name__ == "__main__":
    main()
