# Veri Formatı Gereksinimleri

Pipeline zorunlu sütunlar:
- `pickup_datetime` (datetime)
- `dropoff_datetime` (datetime)
- `PULocationID` (int)
- `DOLocationID` (int)
- `trip_distance` (float)
- `trip_duration_minutes` (float)
- `fare_amount` (float)
- `passenger_count` (int)

Desteklenen giriş dosyaları: CSV, Parquet (önerilen), JSON, Excel (önce CSV'ye çevir). Ayrıntılar için üst seviye README'ye bakın.
