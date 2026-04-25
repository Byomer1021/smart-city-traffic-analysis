"""
═══════════════════════════════════════════════════════════════
  Veri Dönüştürme Yardımcısı (Data Conversion Helper)
═══════════════════════════════════════════════════════════════
  Farklı formatlardaki ham veriyi pipeline'ın beklediği
  standart formata dönüştürür.

  Kullanım:
    python convert_data.py --input raw_data.csv --output formatted.csv --mapping mapping.json

  Mapping JSON Örneği:
  {
    "pickup_datetime": "BINIS_TARIH",
    "dropoff_datetime": "INIS_TARIH",
    "PULocationID": "BINIS_BOLGE_ID",
    "DOLocationID": "INIS_BOLGE_ID",
    "trip_distance": "MESAFE_KM",
    "fare_amount": "UCRET",
    "passenger_count": "YOLCU_SAYISI"
  }
"""

import pandas as pd
import json
import argparse
import sys
import os

# Pipeline'ın beklediği zorunlu sütunlar
REQUIRED_COLUMNS = {
    "pickup_datetime":       {"type": "datetime", "description": "Biniş zamanı"},
    "dropoff_datetime":      {"type": "datetime", "description": "İniş zamanı"},
    "PULocationID":          {"type": "int",      "description": "Biniş bölge ID"},
    "DOLocationID":          {"type": "int",      "description": "İniş bölge ID"},
    "trip_distance":         {"type": "float",    "description": "Mesafe (km)"},
    "fare_amount":           {"type": "float",    "description": "Ücret"},
    "passenger_count":       {"type": "int",      "description": "Yolcu sayısı"},
    "trip_duration_minutes": {"type": "float",    "description": "Süre (dakika)"},
}


def validate_data(df: pd.DataFrame) -> dict:
    """Verinin pipeline'a uygunluğunu kontrol eder."""
    report = {"status": "OK", "errors": [], "warnings": [], "stats": {}}

    # Zorunlu sütun kontrolü
    for col, info in REQUIRED_COLUMNS.items():
        if col not in df.columns:
            # trip_duration_minutes otomatik hesaplanabilir
            if col == "trip_duration_minutes":
                if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
                    report["warnings"].append(
                        f"'{col}' sütunu yok ama pickup/dropoff'tan hesaplanacak."
                    )
                    continue
            report["errors"].append(f"ZORUNLU sütun eksik: '{col}' ({info['description']})")
            report["status"] = "FAIL"

    # Veri kalitesi kontrolleri
    if report["status"] == "OK":
        report["stats"]["total_rows"] = len(df)
        report["stats"]["unique_PU"] = df["PULocationID"].nunique()
        report["stats"]["unique_DO"] = df["DOLocationID"].nunique()

        zero_dist = (df["trip_distance"] <= 0).sum()
        if zero_dist > 0:
            pct = zero_dist / len(df) * 100
            report["warnings"].append(
                f"{zero_dist:,} satırda trip_distance <= 0 ({pct:.1f}%) — ETL'de filtrelenecek."
            )

        if "trip_duration_minutes" in df.columns:
            zero_dur = (df["trip_duration_minutes"] <= 0).sum()
            if zero_dur > 0:
                pct = zero_dur / len(df) * 100
                report["warnings"].append(
                    f"{zero_dur:,} satırda trip_duration <= 0 ({pct:.1f}%) — ETL'de filtrelenecek."
                )

    return report


def convert_data(input_path: str, output_path: str, mapping: dict = None):
    """Ham veriyi pipeline formatına dönüştürür."""
    print(f"[1/4] Veri okunuyor: {input_path}")

    # Format algılama
    ext = os.path.splitext(input_path)[1].lower()
    if ext == ".parquet":
        df = pd.read_parquet(input_path)
    elif ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        df = pd.read_csv(input_path, sep=sep)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(input_path)
    elif ext == ".json":
        df = pd.read_json(input_path)
    else:
        print(f"[HATA] Desteklenmeyen format: {ext}")
        sys.exit(1)

    print(f"       {len(df):,} satır, {len(df.columns)} sütun okundu.")
    print(f"       Sütunlar: {list(df.columns)}")

    # Sütun eşleştirmesi (mapping varsa)
    if mapping:
        print(f"\n[2/4] Sütun eşleştirmesi uygulanıyor...")
        rename_map = {}
        for target_col, source_col in mapping.items():
            if source_col in df.columns:
                rename_map[source_col] = target_col
                print(f"       {source_col} → {target_col}")
            else:
                print(f"       [!] '{source_col}' kaynak veride bulunamadı, atlanıyor.")
        df = df.rename(columns=rename_map)
    else:
        print(f"\n[2/4] Mapping yok — sütun isimleri olduğu gibi kullanılacak.")

    # Süre hesaplama (yoksa)
    if "trip_duration_minutes" not in df.columns:
        if "pickup_datetime" in df.columns and "dropoff_datetime" in df.columns:
            print(f"\n[3/4] trip_duration_minutes hesaplanıyor...")
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
            df["trip_duration_minutes"] = (
                df["dropoff_datetime"] - df["pickup_datetime"]
            ).dt.total_seconds() / 60
        else:
            print(f"\n[3/4] [!] trip_duration_minutes hesaplanamıyor (tarih sütunları yok)")
    else:
        print(f"\n[3/4] trip_duration_minutes zaten mevcut.")

    # Doğrulama
    print(f"\n[4/4] Veri doğrulanıyor...")
    report = validate_data(df)

    if report["errors"]:
        print(f"\n  ❌ HATALAR:")
        for e in report["errors"]:
            print(f"     • {e}")

    if report["warnings"]:
        print(f"\n  ⚠ UYARILAR:")
        for w in report["warnings"]:
            print(f"     • {w}")

    if report["stats"]:
        print(f"\n  📊 İSTATİSTİKLER:")
        for k, v in report["stats"].items():
            print(f"     • {k}: {v:,}" if isinstance(v, int) else f"     • {k}: {v}")

    if report["status"] == "OK":
        # Kaydet
        out_ext = os.path.splitext(output_path)[1].lower()
        if out_ext == ".parquet":
            df.to_parquet(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)

        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"\n  ✅ Dönüştürülmüş veri kaydedildi: {output_path} ({size_mb:.1f} MB)")
    else:
        print(f"\n  ❌ Doğrulama başarısız — düzeltip tekrar dene.")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Veri Dönüştürme Yardımcısı")
    parser.add_argument("--input", required=True, help="Ham veri dosyası")
    parser.add_argument("--output", required=True, help="Çıktı dosyası")
    parser.add_argument("--mapping", default=None,
                        help="Sütun eşleştirme JSON dosyası (opsiyonel)")
    parser.add_argument("--validate-only", action="store_true",
                        help="Sadece doğrulama yap, dönüştürme yapma")
    args = parser.parse_args()

    mapping = None
    if args.mapping:
        with open(args.mapping, "r") as f:
            mapping = json.load(f)

    if args.validate_only:
        ext = os.path.splitext(args.input)[1].lower()
        if ext == ".parquet":
            df = pd.read_parquet(args.input)
        else:
            df = pd.read_csv(args.input)
        report = validate_data(df)
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        convert_data(args.input, args.output, mapping)
