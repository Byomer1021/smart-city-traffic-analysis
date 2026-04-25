"""
==============================================================================
  Generic Sentetik Trafik Verisi Üretici
==============================================================================
  Herhangi bir şehir konfigürasyonu ile çalışır. CityConfig'deki bölge
  ağırlıklarını kullanarak gerçekçi trafik paterni oluşturur.
==============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.city_config import CityConfig, get_city_config


def generate_trip_data(
    config: CityConfig,
    num_trips: int = 2_000_000,
    year: int = 2023,
    anomaly_rate: float = 0.03,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Şehir konfigürasyonuna göre sentetik yolculuk verisi üretir.

    Parameters:
        config: Şehir konfigürasyonu (CityConfig nesnesi)
        num_trips: Üretilecek yolculuk sayısı
        year: Simülasyon yılı
        anomaly_rate: Anomali oranı (0-1 arası)
        seed: Rastgelelik tohumu

    Returns:
        DataFrame: Yolculuk kayıtları
    """
    np.random.seed(seed)
    print(f"[INFO] {config.name} için {num_trips:,} sentetik yolculuk üretiliyor...")

    all_zones = list(config.zones.keys())
    n_zones = len(all_zones)

    # =========================================================================
    # Bölge ağırlıklarını config'den al
    # =========================================================================
    weights_pickup = np.array([config.get_zone_weight(z) for z in all_zones])
    weights_dropoff = weights_pickup.copy()

    # Normalize
    weights_pickup /= weights_pickup.sum()
    weights_dropoff /= weights_dropoff.sum()

    # =========================================================================
    # Yolculuk verisi üret
    # =========================================================================
    pickup_zones = np.random.choice(all_zones, size=num_trips, p=weights_pickup)
    dropoff_zones = np.random.choice(all_zones, size=num_trips, p=weights_dropoff)

    # Aynı bölge içi yolculuklar (%5)
    same_zone_mask = np.random.random(num_trips) < 0.05
    dropoff_zones[same_zone_mask] = pickup_zones[same_zone_mask]

    # Tarih aralığı
    start_date = datetime(year, 1, 1)
    random_days = np.random.randint(0, 365, size=num_trips)

    # Saat dağılımı (şehirden bağımsız genel trafik paterni)
    hour_probs = np.array([
        0.01, 0.005, 0.005, 0.005, 0.005, 0.01,   # 00-05: gece
        0.03, 0.06, 0.08, 0.07, 0.06, 0.06,        # 06-11: sabah rush
        0.06, 0.05, 0.05, 0.05, 0.06, 0.07,        # 12-17: öğlen-akşam
        0.08, 0.07, 0.06, 0.05, 0.03, 0.02         # 18-23: akşam rush
    ])
    hour_probs /= hour_probs.sum()
    random_hours = np.random.choice(range(24), size=num_trips, p=hour_probs)
    random_minutes = np.random.randint(0, 60, size=num_trips)

    pickup_datetimes = [
        start_date + timedelta(days=int(d), hours=int(h), minutes=int(m))
        for d, h, m in zip(random_days, random_hours, random_minutes)
    ]

    # Yolculuk süresi (lognormal)
    trip_durations = np.random.lognormal(mean=2.5, sigma=0.7, size=num_trips)
    trip_durations = np.clip(trip_durations, 1, 180)

    # Mesafe (süreyle korelasyonlu)
    trip_distances = trip_durations * np.random.uniform(0.1, 0.4, size=num_trips)
    trip_distances = np.clip(trip_distances, 0.1, 50)

    # Ücret (şehir config'ine göre)
    fare_amounts = (
        config.base_fare +
        (trip_distances * config.per_km_fare) +
        (trip_durations / 60 * config.per_km_fare * 0.2)
    )
    fare_amounts += np.random.normal(0, config.base_fare * 0.1, size=num_trips)
    fare_amounts = np.clip(fare_amounts, config.base_fare, config.base_fare * 100)

    # Yolcu sayısı
    passenger_counts = np.random.choice(
        [1, 2, 3, 4], size=num_trips, p=[0.65, 0.20, 0.10, 0.05]
    )

    # =========================================================================
    # Anomaliler ekle
    # =========================================================================
    num_anomalies = int(num_trips * anomaly_rate)
    anomaly_idx = np.random.choice(num_trips, size=num_anomalies, replace=False)
    half = num_anomalies // 2
    trip_distances[anomaly_idx[:half]] = 0.0
    trip_durations[anomaly_idx[half:]] = 0.0

    # =========================================================================
    # DataFrame
    # =========================================================================
    df = pd.DataFrame({
        "pickup_datetime": pickup_datetimes,
        "dropoff_datetime": [
            dt + timedelta(minutes=float(dur))
            for dt, dur in zip(pickup_datetimes, trip_durations)
        ],
        "passenger_count": passenger_counts,
        "trip_distance": np.round(trip_distances, 2),
        "PULocationID": pickup_zones,
        "DOLocationID": dropoff_zones,
        "fare_amount": np.round(fare_amounts, 2),
        "trip_duration_minutes": np.round(trip_durations, 2),
    })

    print(f"[OK] {len(df):,} yolculuk kaydı üretildi ({config.name})")
    print(f"     Bölge sayısı: {df['PULocationID'].nunique()}")
    print(f"     Anomali: {num_anomalies:,} ({anomaly_rate*100:.0f}%)")
    print(f"     Para birimi: {config.currency}")

    return df


def save_data(df: pd.DataFrame, output_dir: str, city_name: str) -> str:
    """Veriyi CSV olarak kaydet."""
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{city_name.lower().replace(' ', '_')}_trips.csv"
    path = os.path.join(output_dir, filename)
    df.to_csv(path, index=False)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"[OK] Veri kaydedildi: {path} ({size_mb:.1f} MB)")
    return path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=str, default="istanbul", help="Şehir adı")
    parser.add_argument("--trips", type=int, default=2_000_000, help="Yolculuk sayısı")
    args = parser.parse_args()

    config = get_city_config(args.city)
    print(config.summary())

    df = generate_trip_data(config, num_trips=args.trips)
    save_data(df, "../results", config.name)
