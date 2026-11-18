#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Büyük Veri CSV -> Parquet Yüksek Hızlı Dönüştürücü ve Sıkıştırma Analizi
Proje: Yazılım Projelerinde Teslimat Risklerinin Büyük Veri ile Erken Tespiti

Özellikler:
- Bellek Dostu (Chunk/Streaming) okuma ile GB'larca CSV'yi OOM olmadan Parquet'e dönüştürür.
- Snappy kolon bazlı sıkıştırma (Snappy Compression).
- CSV vs Parquet dosya boyutu, disk tasarrufu ve okuma performansı karşılaştırması.
"""

import argparse
import os
import sys
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def convert_csv_to_parquet(csv_path: str, parquet_path: str = None, chunksize: int = 100000, compression: str = "snappy"):
    if not os.path.exists(csv_path):
        print(f"[HATA] CSV dosyası bulunamadı: {csv_path}")
        return None

    if parquet_path is None:
        base, _ = os.path.splitext(csv_path)
        parquet_path = base + ".parquet"

    print(f"\n{'='*75}")
    print(f"CSV -> PARQUET DÖNÜŞTÜRÜCÜ BAŞLATILIYOR")
    print(f"Kaynak CSV       : {os.path.abspath(csv_path)}")
    print(f"Hedef Parquet    : {os.path.abspath(parquet_path)}")
    print(f"Sıkıştırma Tipi  : {compression.upper()}")
    print(f"Chunk Boyutu     : {chunksize:,} satır")
    print(f"{'='*75}")

    t0 = time.time()
    csv_size_bytes = os.path.getsize(csv_path)
    csv_size_mb = csv_size_bytes / (1024 * 1024)
    csv_size_gb = csv_size_bytes / (1024 * 1024 * 1024)

    if os.path.exists(parquet_path):
        try:
            os.remove(parquet_path)
        except Exception:
            pass

    writer = None
    total_rows = 0
    chunk_idx = 0

    try:
        # Pandas chunk iterator ile bellek dostu okuma
        for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False):
            chunk_idx += 1
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression=compression)
            writer.write_table(table)
            total_rows += len(chunk)
            print(f"  -> Chunk #{chunk_idx}: {len(chunk):,} satır işlendi. Toplam: {total_rows:,} satır")
    finally:
        if writer:
            writer.close()

    elapsed = time.time() - t0
    parquet_size_bytes = os.path.getsize(parquet_path)
    parquet_size_mb = parquet_size_bytes / (1024 * 1024)
    parquet_size_gb = parquet_size_bytes / (1024 * 1024 * 1024)
    compression_ratio = (1.0 - (parquet_size_bytes / max(1, csv_size_bytes))) * 100.0

    print(f"\n{'='*75}")
    print(f"DÖNÜŞÜM VE SIKIŞTIRMA RAPORU")
    print(f"{'='*75}")
    print(f"Toplam Satır Sayısı  : {total_rows:,}")
    print(f"İşlem Süresi         : {elapsed:.2f} saniye")
    print(f"Orijinal CSV Boyutu  : {csv_size_mb:.2f} MB ({csv_size_gb:.3f} GB)")
    print(f"Parquet Boyutu       : {parquet_size_mb:.2f} MB ({parquet_size_gb:.3f} GB)")
    print(f"Disk Alanı Tasarrufu : %{compression_ratio:.2f} ({(csv_size_bytes / max(1, parquet_size_bytes)):.1f}x sıkıştırma!)")
    print(f"{'='*75}\n")

    return {
        "csv_path": csv_path,
        "parquet_path": parquet_path,
        "total_rows": total_rows,
        "elapsed_seconds": round(elapsed, 2),
        "csv_size_mb": round(csv_size_mb, 2),
        "parquet_size_mb": round(parquet_size_mb, 2),
        "compression_ratio_pct": round(compression_ratio, 2)
    }


def main():
    parser = argparse.ArgumentParser(description="Büyük Veri CSV -> Parquet Dönüştürücü")
    parser.add_argument("--csv", type=str, default="data/pull_requests_dataset.csv", help="Dönüştürülecek CSV dosyası")
    parser.add_argument("--output", type=str, default=None, help="Hedef Parquet dosya yolu")
    parser.add_argument("--chunksize", type=int, default=100000, help="Chunk boyutu")
    parser.add_argument("--all", action="store_true", help="data/ altındaki hem tasks hem prs CSV'lerini dönüştür")
    args = parser.parse_args()

    if args.all:
        for f in ["tasks_dataset.csv", "pull_requests_dataset.csv"]:
            path = os.path.join("data", f)
            if os.path.exists(path):
                convert_csv_to_parquet(path, chunksize=args.chunksize)
    else:
        convert_csv_to_parquet(args.csv, args.output, chunksize=args.chunksize)


if __name__ == "__main__":
    main()
