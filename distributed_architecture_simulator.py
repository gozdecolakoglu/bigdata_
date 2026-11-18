#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDFS Dağıtık Mimari ve Replikasyon Faktörü (Replication Factor = 3) Simülatörü
Proje: Yazılım Projelerinde Teslimat Risklerinin Büyük Veri ile Erken Tespiti

Özellikler:
1. 128 MB HDFS Blok Bölme (Block Splitting) Simülasyonu.
2. Replication Factor = 3 Dağıtım Matrisi (DataNode-1, DataNode-2, DataNode-3).
3. Hata Toleransı & Otomatik İyileşme (Fault Tolerance & Self-Healing Replication).
4. Canlı Sunum ve Hoca Savunması için İnteraktif Terminal Arayüzü.
"""

import math
import os
import random
import time


def simulate_hdfs_cluster(file_size_mb: float = 1050.0, block_size_mb: int = 128, replication_factor: int = 3):
    num_blocks = math.ceil(file_size_mb / block_size_mb)
    datanodes = ["DataNode-1 (192.168.1.101)", "DataNode-2 (192.168.1.102)", "DataNode-3 (192.168.1.103)"]

    print("=" * 85)
    print(" HADOOP DISTRIBUTED FILE SYSTEM (HDFS) MİMARİSİ VE REPLİKASYON SİMÜLATÖRÜ")
    print("=" * 85)
    print(f" Dosya Adı           : pull_requests_dataset.csv (Büyük Veri Hacmi)")
    print(f" Toplam Dosya Boyutu : {file_size_mb:.2f} MB ({file_size_mb/1024:.2f} GB)")
    print(f" HDFS Blok Boyutu    : {block_size_mb} MB (dfs.blocksize)")
    print(f" Oluşan Blok Sayısı  : {num_blocks} Blok (Blk_1001 .. Blk_{1000 + num_blocks})")
    print(f" Replikasyon Faktörü : {replication_factor} (dfs.replication = 3)")
    print(f" Toplam Fiziksel Alan: {file_size_mb * replication_factor:.2f} MB ({file_size_mb * replication_factor / 1024:.2f} GB)")
    print("=" * 85)

    print("\n--- 1. AŞAMA: 128 MB BLOK BÖLME VE 3 REPLİKA DAĞITIM MATRİSİ ---")
    print(f"{'Blok ID':<12} | {'Boyut':<10} | {'Replication Factor':<20} | {'Atanan Düğümler (DataNodes)'}")
    print("-" * 85)

    block_map = {}
    for i in range(1, num_blocks + 1):
        blk_id = f"Blk_{1000 + i}"
        cur_sz = block_size_mb if i < num_blocks else round(file_size_mb - (num_blocks - 1) * block_size_mb, 1)
        # Her blok tam 3 replika olarak 3 DataNode'a kopyalanır
        assigned_nodes = list(datanodes)
        block_map[blk_id] = {
            "size_mb": cur_sz,
            "replicas": assigned_nodes
        }
        nodes_str = " -> ".join([n.split()[0] for n in assigned_nodes])
        print(f"{blk_id:<12} | {cur_sz:>6.1f} MB | {replication_factor} Replikasyon (Tam) | {nodes_str}")

    print("-" * 85)
    print(">>> Durum: Tüm bloklar 3 bağımsız DataNode üzerine dağıtıldı. Sıfır veri kaybı garantisi.")

    # Simülasyon 2: Arıza Toleransı Testi
    print("\n--- 2. AŞAMA: DAĞITIK ARIZA TOLERANSI (FAULT TOLERANCE) CANLI TESTİ ---")
    crashed_node = "DataNode-2 (192.168.1.102)"
    print(f"[UYARI] Senaryo: Donanım arızası nedeniyle '{crashed_node}' çöktü (OFFLINE)!")
    print("  -> NameNode Heartbeat kontrolü: DataNode-2 yanıt vermiyor (Dead Node tespit edildi).")
    print("  -> Spark Okuma İsteği: Spark Master, DataNode-1 ve DataNode-3 üzerindeki replikalara yönlendirildi.")
    print("  -> Veri Kaybı: %0.00 (Tüm {0} blok DataNode-1 ve DataNode-3'ten eksiksiz okundu!).".format(num_blocks))

    print("\n--- 3. AŞAMA: HDFS KENDİNİ ONARMA (SELF-HEALING REPLICATION) ---")
    print("  -> NameNode Eksik Replikaları Tespit Etti: (Under-Replicated Blocks: {0})".format(num_blocks))
    print("  -> NameNode yeni yedek düğüm devreye alıyor: DataNode-4 (192.168.1.104)")
    print("  -> Bloklar DataNode-1'den DataNode-4'e paralel olarak kopyalanıyor...")
    time.sleep(1)
    print("  -> [BAŞARILI] Replikasyon Faktörü tekrar 3/3 seviyesine getirildi.")
    print("=" * 85)


def main():
    # Gerçek dosya boyutu varsa onu al
    target_csv = "data/pull_requests_dataset.csv"
    sz_mb = 1050.0
    if os.path.exists(target_csv):
        sz_mb = os.path.getsize(target_csv) / (1024 * 1024)
    simulate_hdfs_cluster(file_size_mb=sz_mb)


if __name__ == "__main__":
    main()
