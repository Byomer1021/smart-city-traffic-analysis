"""
Auto City Config Builder

Veri setinden (CSV/Parquet) şehir konfigürasyonu türetir:
- zones: benzersiz PU/DO ID'leri ve isimleri
- hotspots: toplam akışa göre en üst yüzde (varsayılan %10)
- secondary_hotspots: sıradaki yüzde (varsayılan %15)
- transport_hubs: opsiyonel olarak zones CSV'deki hub=1 alanından alınır
- coordinates: zones CSV'de lat/lon varsa eklenir

Kullanım örneği:
  python local_pipeline/auto_config_builder.py \
    --input data_formatted.csv \
    --city-name "MyCity" --country "Country" \
    --zones-csv zones_meta.csv \
    --output config/mycity_auto.json

zones_meta.csv beklenen başlıklar: id,name,lat,lon,hub (hub opsiyonel; 1/0)

Not: Bu script bir başlangıç üretir; koordinat ve isim kalitesi giriş verisine bağlıdır.
"""

import json
import argparse
from pathlib import Path
import pandas as pd

REQUIRED_COLS = [
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "trip_duration_minutes",
    "fare_amount",
    "passenger_count",
]


def load_data(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".parquet":
        return pd.read_parquet(path)
    elif ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        return pd.read_csv(path, sep=sep)
    else:
        raise ValueError(f"Desteklenmeyen format: {ext}")


def ensure_columns(df: pd.DataFrame):
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Eksik sütunlar: {missing}")


def load_zones_meta(path: Path | None):
    if not path:
        return None
    ext = path.suffix.lower()
    if ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        df = pd.read_csv(path, sep=sep)
    elif ext == ".parquet":
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"Zones meta formatı desteklenmiyor: {ext}")
    return df


def build_config(df: pd.DataFrame,
                 zones_meta: pd.DataFrame | None,
                 city_name: str,
                 country: str,
                 hotspot_pct: float = 0.10,
                 secondary_pct: float = 0.15,
                 currency: str = "USD",
                 base_fare: float = 3.5,
                 per_km_fare: float = 2.5,
                 timezone: str = "UTC"):
    # Zone listesi
    zone_ids = pd.Index(pd.unique(pd.concat([df["PULocationID"], df["DOLocationID"]]))).astype(int)

    # Zone isimleri ve koordinatlar
    zones = {}
    coordinates = {}
    transport_hubs = []
    if zones_meta is not None:
        meta = zones_meta.copy()
        meta["id"] = meta["id"].astype(int)
        meta = meta.set_index("id")
        for zid in zone_ids:
            row = meta.loc[zid] if zid in meta.index else None
            name = row["name"] if row is not None and pd.notna(row.get("name")) else f"Zone-{zid}"
            zones[zid] = str(name)
            if row is not None:
                lat = row.get("lat")
                lon = row.get("lon")
                if pd.notna(lat) and pd.notna(lon):
                    coordinates[zid] = [float(lat), float(lon)]
                hub_flag = row.get("hub")
                if pd.notna(hub_flag) and hub_flag == 1:
                    transport_hubs.append(int(zid))
    else:
        for zid in zone_ids:
            zones[zid] = f"Zone-{zid}"

    # Akış skorları
    inbound = df.groupby("DOLocationID")["trip_distance"].count()
    outbound = df.groupby("PULocationID")["trip_distance"].count()
    flow = inbound.add(outbound, fill_value=0)
    flow = flow.reindex(zone_ids, fill_value=0)
    flow_sorted = flow.sort_values(ascending=False)

    def top_ids(series, pct):
        if len(series) == 0:
            return []
        k = max(1, int(len(series) * pct))
        return list(series.head(k).index.astype(int))

    hotspots = top_ids(flow_sorted, hotspot_pct)
    secondary = top_ids(flow_sorted.iloc[len(hotspots):], secondary_pct)

    config = {
        "name": city_name,
        "country": country,
        "zones": {str(k): v for k, v in zones.items()},
        "hotspots": hotspots,
        "secondary_hotspots": secondary,
        "transport_hubs": transport_hubs,
        "coordinates": {str(k): v for k, v in coordinates.items()},
        "currency": currency,
        "base_fare": base_fare,
        "per_km_fare": per_km_fare,
        "timezone": timezone,
    }
    return config


def main():
    ap = argparse.ArgumentParser(description="Veriden şehir konfigürasyonu üretir")
    ap.add_argument("--input", required=True, help="Dönüştürülmüş veri (CSV/Parquet)")
    ap.add_argument("--zones-csv", default=None, help="Zone meta (id,name,lat,lon,hub)")
    ap.add_argument("--city-name", required=True, help="Şehir adı")
    ap.add_argument("--country", default="", help="Ülke")
    ap.add_argument("--hotspot-pct", type=float, default=0.10, help="Hotspot üst dilimi (0-1)")
    ap.add_argument("--secondary-pct", type=float, default=0.15, help="İkinci dilim (0-1)")
    ap.add_argument("--currency", default="USD")
    ap.add_argument("--base-fare", type=float, default=3.5)
    ap.add_argument("--per-km-fare", type=float, default=2.5)
    ap.add_argument("--timezone", default="UTC")
    ap.add_argument("--output", default="config/auto_city.json", help="Çıktı JSON yolu")
    args = ap.parse_args()

    input_path = Path(args.input)
    zones_path = Path(args.zones_csv) if args.zones_csv else None
    output_path = Path(args.output)

    df = load_data(input_path)
    ensure_columns(df)
    zones_meta = load_zones_meta(zones_path)

    config = build_config(
        df=df,
        zones_meta=zones_meta,
        city_name=args.city_name,
        country=args.country,
        hotspot_pct=args.hotspot_pct,
        secondary_pct=args.secondary_pct,
        currency=args.currency,
        base_fare=args.base_fare,
        per_km_fare=args.per_km_fare,
        timezone=args.timezone,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"[OK] Konfigürasyon yazıldı: {output_path}")
    print(f"Zones: {len(config['zones'])} | Hotspot: {len(config['hotspots'])} | Secondary: {len(config['secondary_hotspots'])} | Hubs: {len(config['transport_hubs'])}")


if __name__ == "__main__":
    main()
