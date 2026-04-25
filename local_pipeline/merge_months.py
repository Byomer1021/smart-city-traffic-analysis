"""
NYC aylık dönüştürülmüş dosyaları tek dosyada birleştirir.

Örnek:
  python local_pipeline/merge_months.py \
    --input-glob data/nyc/formatted/nyc_trips_2023_*.parquet \
    --output data/nyc/formatted/nyc_trips_2023_all.parquet
"""

from pathlib import Path
import argparse
import glob
import pandas as pd


REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
    "passenger_count",
]


def read_any(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".parquet":
        return pd.read_parquet(path)
    if ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        return pd.read_csv(path, sep=sep)
    raise ValueError(f"Desteklenmeyen dosya tipi: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Aylık dosyaları tek dosyada birleştir")
    parser.add_argument("--input-glob", required=True, help="Giriş dosyaları glob pattern")
    parser.add_argument("--output", required=True, help="Çıkış dosyası (.parquet/.csv)")
    args = parser.parse_args()

    files = sorted([Path(p) for p in glob.glob(args.input_glob)])
    if not files:
        raise FileNotFoundError(f"Pattern için dosya bulunamadı: {args.input_glob}")

    print(f"[INFO] Birleştirilecek dosya sayısı: {len(files)}")

    frames = []
    for file_path in files:
        print(f"  - Okunuyor: {file_path}")
        df = read_any(file_path)
        missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
        if missing:
            raise ValueError(f"{file_path} dosyasında eksik sütunlar: {missing}")
        frames.append(df[REQUIRED_COLUMNS].copy())

    merged = pd.concat(frames, axis=0, ignore_index=True)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.suffix.lower() == ".parquet":
        merged.to_parquet(output_path, index=False)
    else:
        merged.to_csv(output_path, index=False)

    print(f"[OK] Birleşik dosya yazıldı: {output_path}")
    print(f"     Toplam satır: {len(merged):,}")


if __name__ == "__main__":
    main()
