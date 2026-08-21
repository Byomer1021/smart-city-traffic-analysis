# Akıllı Şehir Trafik Darboğazı Tespiti ve Simülasyonu

**Çizge (Graph) Analitiği ile Şehir Trafik Ağı Analizi**

---

## Proje Hakkında

Bu proje, bir şehrin yolculuk verilerini devasa bir ağ (graph) olarak modelleyerek, **PageRank algoritması** ile en kritik darboğaz noktalarını tespit eder ve bu noktaların kapanması durumunda trafik akışına etkisini simüle eder.

Proje iki modda çalışır:

| Mod | Motor | Veri Boyutu | Nerede Çalışır |
|-----|-------|-------------|----------------|
| **Lokal Demo** | Pandas + NetworkX | < 10M satır | Laptop / PC |
| **Bulut Üretim** | PySpark + GraphFrames | 10M+ satır (Big Data) | AWS EMR / Google Dataproc |

---

## Dokümantasyon

| Belge | İçerik |
|---|---|
| [docs/FINAL_REPORT.md](docs/FINAL_REPORT.md) | Final teknik rapor — yöntem, sonuçlar, bulgular, kısıtlar |
| [docs/MODULES.md](docs/MODULES.md) | Modül referansı — her scriptin girdi/çıktısı ve parametreleri |
| [docs/VERIFICATION.md](docs/VERIFICATION.md) | Sonuçların ham veriyle doğrulanması ve tespit edilen tutarsızlıklar |
| [docs/weekly/README.md](docs/weekly/README.md) | Haftalık ilerleme raporu |
| [docs/DATA_FORMAT.md](docs/DATA_FORMAT.md) | Veri format gereksinimleri |

---

## Ana Sonuçlar — NYC TLC 2023

NYC TLC Yellow Taxi 2023 veri setinin tamamı (38.310.226 kayıt) işlenerek
elde edilmiştir. Tüm sayılar ham veriden yeniden hesaplanarak doğrulanmıştır
([VERIFICATION.md](docs/VERIFICATION.md)).

| Metrik | Değer |
|---|---|
| İşlenen ham kayıt | 38.310.226 |
| Temizleme sonrası | 37.401.069 (%97,63) |
| Çizge | 258 düğüm, 9.990 kenar, yoğunluk 0,1507 |
| Grafa giren yolculuk | 35.420.777 |
| En kritik bölge | Upper East Side North (PageRank 0,02573) |
| En yüksek köprü rolü | East Harlem South (betweenness 1,00 — PageRank'ta 17.) |
| 5 düğüm silme etkisi | %34,1 akış kaybı |
| Yapısal kırılma | JFK Airport çıkarılınca ağ 1 → 16 bileşene bölünüyor |
| Çalışma süresi | 40,7 sn (AWS EC2 t3.xlarge, 16 GB RAM) |

Şekiller: [results/new_york_city/](results/new_york_city/)

---

## Klasör Yapısı

```
smart-city-traffic-analysis/
│
├── README.md                              ← Bu dosya
├── requirements.txt                       ← Python bağımlılıkları
│
├── config/                                ← Şehir Konfigürasyonları
│   └── city_config.py                     ← İstanbul, NYC, Ankara tanımları
│                                             + JSON'dan yükleme desteği
│
├── setup_and_run.sh                       ← EC2 uçtan uca kurulum (8 adım)
│
├── local_pipeline/                        ← Lokal Demo (Pandas + NetworkX)
│   ├── traffic_analysis_generic.py        ← Tam analiz pipeline'ı
│   ├── export_map_data.py                 ← Harita için JSON üretici
│   ├── generate_data.py                   ← Sentetik veri üretici
│   ├── convert_data.py                    ← Ham şema → pipeline şeması
│   ├── merge_months.py                    ← Aylık dosyaları birleştirme
│   ├── build_nyc_centroids.py             ← Shapefile → bölge koordinatları
│   ├── auto_config_builder.py             ← Veriden otomatik şehir config'i
│   ├── gnn_node2vec.py                    ← Node2Vec gömme (GNN eklentisi)
│   └── gnn_visualize.py                   ← t-SNE gömme görselleştirmesi
│
├── spark_pipeline/                        ← Bulut Üretim (PySpark + GraphFrames)
│   └── spark_traffic_pipeline.py          ← Tam dağıtık pipeline
│                                             (ETL → Graph → PageRank →
│                                              Topluluk → Simülasyon →
│                                              Görselleştirme → JSON)
│
├── visualization/                         ← İnteraktif Harita
│   ├── viewer.html                        ← Leaflet + OSM görüntüleyici
│   └── istanbul_traffic_map.jsx           ← (yer tutucu — uygulanmadı)
│
├── results/                               ← Çıktılar
│   └── new_york_city/                     ← NYC TLC 2023 gerçek koşusu
│       ├── 01_dashboard.png               ← Ana dashboard
│       ├── 02_network_topology.png        ← Ağ topolojisi
│       ├── 03_simulation_detail.png       ← Simülasyon detayları
│       ├── 04_centrality_heatmap.png      ← Merkezilik karşılaştırması
│       ├── 05_distributions.png           ← Dağılım grafikleri
│       ├── 06_geo_map.png                 ← Coğrafi harita
│       └── map_data.json                  ← İnteraktif harita verisi
│
└── docs/                                  ← Dokümantasyon
    ├── FINAL_REPORT.md                    ← Final teknik rapor
    ├── MODULES.md                         ← Modül referansı
    ├── VERIFICATION.md                    ← Sonuçların doğrulanması
    ├── DATA_FORMAT.md                     ← Veri format gereksinimleri
    └── weekly/README.md                   ← Haftalık ilerleme raporu
```

> `results/` klasörü genel olarak `.gitignore` kapsamındadır; yalnızca NYC
> gerçek koşusunun figürleri ve `map_data.json` dosyası, rapordaki şekillerin
> kaynağıyla birlikte gelmesi için versiyon kontrolüne alınmıştır. Diğer
> şehirlerin çıktıları pipeline çalıştırıldığında yerel olarak üretilir.

---

## Hızlı Başlangıç

### 1. Bağımlılıkları Kur

```bash
pip install -r requirements.txt
```

> **Windows'ta:** Scriptler konsola Unicode kutu karakterleri basar. Çalıştırmadan
> önce `set PYTHONUTF8=1` yapın, aksi hâlde `cp1254` kod sayfasında
> `UnicodeEncodeError` alırsınız.

### 2. Gerçek NYC Verisiyle Çalıştır (rapordaki sonuçlar)

Boş bir Ubuntu sunucusunda tek komutla — veri indirme, dönüştürme, birleştirme
ve analiz dâhil:

```bash
./setup_and_run.sh          # script başında MONTHS=12 yapın
```

Veri zaten hazırsa doğrudan:

```bash
python local_pipeline/traffic_analysis_generic.py \
    --city nyc --data data/nyc/formatted/nyc_trips_2023_all.parquet --top-k 5

python local_pipeline/export_map_data.py \
    --city nyc --data data/nyc/formatted/nyc_trips_2023_all.parquet
```

Beklenen çıktı: 258 düğüm, 9.990 kenar, 35.420.777 yolculuk, %34,1 akış kaybı,
`1 → 16` bileşen. Ayrıntı: [docs/VERIFICATION.md](docs/VERIFICATION.md).

### 3. Sentetik Veri ile Çalıştır (Sıfır Hazırlık)

```bash
# İstanbul analizi (sentetik veri otomatik üretilir)
cd local_pipeline
python traffic_analysis_generic.py --city istanbul

# NYC analizi
python traffic_analysis_generic.py --city nyc

# Ankara analizi (daha küçük veri seti)
python traffic_analysis_generic.py --city ankara --trips 500000
```

### 4. Sonuçlara Bak

Çalıştırma bittiğinde `results/<şehir>/` klasöründe 6 görselleştirme PNG dosyası ve terminal çıktısında detaylı rapor oluşur.

---

## Kullanım Kılavuzu

### A. Lokal Pipeline (Laptop)

**Ne zaman kullan:** Veri seti 10 milyon satırdan küçükse, bulut hesabın yoksa, hızlı prototipleme yapıyorsan.

```bash
cd local_pipeline

# Adım 1: Analiz çalıştır
python traffic_analysis_generic.py --city istanbul --trips 2000000

# Adım 2: Harita verisi üret
python export_map_data.py --city istanbul

# Parametreler:
#   --city      : istanbul / nyc / ankara (veya JSON config)
#   --trips     : Sentetik yolculuk sayısı (varsayılan: 2,000,000)
#   --top-k     : Simülasyonda çıkarılacak düğüm sayısı (varsayılan: 5)
#   --config    : Harici JSON config dosyası (opsiyonel)
```

**Pipeline adımları:**

```
CSV/Parquet Okuma → Anomali Temizleme → Bölge Gruplama
→ Çizge Oluşturma → PageRank → Merkezilik Hesaplama
→ Topluluk Tespiti → Darboğaz Simülasyonu → Görselleştirme
```

### B. Spark Pipeline (Bulut — Big Data)

**Ne zaman kullan:** Veri 10M+ satır, gerçek TLC/İBB verisi kullanıyorsan, dağıtık küme gerekiyorsa.

#### AWS EMR

```bash
# 1. Küme oluştur
aws emr create-cluster \
    --name "Traffic-Bottleneck" \
    --release-label emr-7.0.0 \
    --applications Name=Spark \
    --instance-type m5.xlarge \
    --instance-count 5

# 2. Veriyi S3'e yükle
aws s3 cp my_data/ s3://my-bucket/taxi-data/ --recursive

# 3. Pipeline çalıştır
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
    --num-executors 10 \
    --executor-memory 8g \
    --executor-cores 4 \
    spark_pipeline/spark_traffic_pipeline.py \
    --city istanbul \
    --input s3://my-bucket/taxi-data/*.parquet \
    --output s3://my-bucket/results/
```

#### Google Dataproc

```bash
# 1. Küme oluştur
gcloud dataproc clusters create traffic-cluster \
    --region=us-central1 \
    --num-workers=4 \
    --worker-machine-type=n1-standard-4

# 2. Pipeline çalıştır
gcloud dataproc jobs submit pyspark \
    --cluster=traffic-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/graphframes-0.8.3.jar \
    spark_pipeline/spark_traffic_pipeline.py \
    -- --city istanbul \
       --input gs://my-bucket/taxi-data/*.parquet \
       --output gs://my-bucket/results/
```

### C. İnteraktif Harita

`visualization/viewer.html` bağımlılıksız, tek dosyalık bir Leaflet +
OpenStreetMap görüntüleyicisidir. `map_data.json` okur; düğümleri PageRank'a
göre boyutlandırır, bölge tipine göre renklendirir (kırmızı hotspot / mavi hub /
yeşil normal) ve en yoğun rotaları çizer.

`fetch` `file://` üzerinden çalışmadığı için basit bir HTTP sunucusu gerekir:

```bash
python -m http.server 8000
# tarayıcı: http://localhost:8000/visualization/viewer.html
```

Hangi sonucun gösterileceği dosya içindeki `mapDataPath` sabitiyle belirlenir.
NYC gerçek koşusu için: `/results/new_york_city/map_data.json`

> `visualization/istanbul_traffic_map.jsx` şu an yalnızca bir **yer tutucudur** —
> çalışan bir React bileşeni içermez. Sunumdaki interaktif harita `viewer.html`
> üzerinden gösterilmiştir.

---

## Kendi Verini Bağlama Rehberi

### Veri Format Gereksinimleri

Pipeline'ın çalışması için verinizde şu **zorunlu sütunlar** olmalıdır:

| Sütun Adı | Tip | Açıklama | Örnek |
|-----------|-----|----------|-------|
| `PULocationID` | Integer | Biniş bölgesi ID | 5 |
| `DOLocationID` | Integer | İniş bölgesi ID | 13 |
| `trip_distance` | Float | Yolculuk mesafesi (km/mil) | 3.7 |
| `trip_duration_minutes` | Float | Yolculuk süresi (dakika) | 18.5 |
| `fare_amount` | Float | Ücret | 45.00 |
| `passenger_count` | Integer | Yolcu sayısı | 2 |
| `pickup_datetime` | Datetime | Biniş zamanı | 2023-06-15 08:30:00 |
| `dropoff_datetime` | Datetime | İniş zamanı | 2023-06-15 08:48:30 |

### Desteklenen Dosya Formatları

| Format | Lokal Pipeline | Spark Pipeline | Not |
|--------|---------------|----------------|-----|
| **CSV** | Doğrudan okur | `spark.read.csv()` ile | Yavaş, büyük veri için uygun değil |
| **Parquet** | `pyarrow` gerekli | Doğrudan okur (önerilen) | En hızlı, sıkıştırmalı |
| **JSON** | `pd.read_json()` ile | `spark.read.json()` ile | Parquet'e dönüştürülmesi önerilir |
| **Excel** | `openpyxl` gerekli | Desteklenmez | Önce CSV'ye çevir |

### Senaryo 1: NYC TLC Gerçek Verisini Bağlama

```bash
# 1. NYC Open Data'dan Parquet indir
#    https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

# 2. Sütun isimlerini kontrol et (NYC TLC zaten uyumlu!)
#    NYC TLC Parquet dosyaları şu sütunlara sahip:
#    tpep_pickup_datetime, tpep_dropoff_datetime,
#    PULocationID, DOLocationID, trip_distance, fare_amount,
#    passenger_count
#
#    NOT: trip_duration_minutes sütunu yok, pipeline hesaplar

# 3. Çalıştır
python traffic_analysis_generic.py --city nyc
# Spark ile:
spark-submit ... --input ./yellow_tripdata_2023-*.parquet --output ./results/
```

### Senaryo 2: İBB (İstanbul) Gerçek Verisini Bağlama

İBB Açık Veri Portalı: https://data.ibb.gov.tr/

```python
# İBB verisini pipeline formatına dönüştüren örnek script
import pandas as pd

# İBB'den indirilen CSV
ibb_df = pd.read_csv("ibb_taxi_data.csv")

# Sütun eşleştirmesi yap
df = pd.DataFrame({
    "pickup_datetime": pd.to_datetime(ibb_df["BINIS_TARIH"]),
    "dropoff_datetime": pd.to_datetime(ibb_df["INIS_TARIH"]),
    "PULocationID": ibb_df["BINIS_BOLGE_ID"],      # Bölge ID
    "DOLocationID": ibb_df["INIS_BOLGE_ID"],        # Bölge ID
    "trip_distance": ibb_df["MESAFE_KM"],
    "fare_amount": ibb_df["UCRET"],
    "passenger_count": ibb_df.get("YOLCU_SAYISI", 1),
    "trip_duration_minutes": (
        pd.to_datetime(ibb_df["INIS_TARIH"]) -
        pd.to_datetime(ibb_df["BINIS_TARIH"])
    ).dt.total_seconds() / 60,
})

# Kaydet
df.to_csv("istanbul_trips_formatted.csv", index=False)
# veya Parquet (Spark için)
df.to_parquet("istanbul_trips_formatted.parquet", index=False)
```

### Senaryo 3: Tamamen Farklı Bir Şehir (Örn. Londra, Berlin)

İki adım gerekli: (1) şehir config'i tanımla, (2) veriyi formatla.

**Adım 1: Şehir Config Oluştur**

Yöntem A — Python'da:

```python
# config/city_config.py dosyasına ekle:
def create_london_config():
    return CityConfig(
        name="London",
        country="UK",
        zones={
            1: "Westminster", 2: "City of London",
            3: "Camden", 4: "Islington",
            # ... tüm bölgeler
        },
        hotspots=[1, 2, 3],             # Yoğun bölge ID'leri
        secondary_hotspots=[4, 5, 6],
        transport_hubs=[20, 21],         # Heathrow, Kings Cross vb.
        coordinates={                    # Opsiyonel (harita için)
            1: (51.5007, -0.1246),
            2: (51.5155, -0.0922),
            # ...
        },
        currency="GBP",
        base_fare=3.80,
        per_km_fare=1.50,
    )

# Registry'ye kaydet:
CITY_REGISTRY["london"] = create_london_config
```

Yöntem B — JSON dosyası ile (kod değiştirmeden):

```json
{
    "name": "London",
    "country": "UK",
    "zones": {
        "1": "Westminster",
        "2": "City of London",
        "3": "Camden"
    },
    "hotspots": [1, 2],
    "secondary_hotspots": [3],
    "transport_hubs": [20, 21],
    "coordinates": {
        "1": [51.5007, -0.1246],
        "2": [51.5155, -0.0922]
    },
    "currency": "GBP",
    "base_fare": 3.80,
    "per_km_fare": 1.50
}
```

```bash
# JSON config ile çalıştır
python traffic_analysis_generic.py --config london.json
```

**Adım 2: Veriyi Formatla**

Yukarıdaki zorunlu sütun tablosundaki isimlere uygun hale getir. Önemli olan `PULocationID` ve `DOLocationID` sütunlarının config'deki zone ID'lerle eşleşmesi.

### Veri Bağlama Kontrol Listesi

Verini bağlamadan önce şu kontrolleri yap:

```
[ ] Zorunlu 8 sütun mevcut mu?
[ ] PULocationID ve DOLocationID integer mi?
[ ] Bu ID'ler city_config'deki zone ID'lerle eşleşiyor mu?
[ ] trip_distance > 0 olan kayıtlar var mı?
[ ] trip_duration_minutes > 0 olan kayıtlar var mı?
[ ] pickup_datetime parse edilebilir formatta mı?
[ ] Veri boyutu:
    [ ] < 10M satır → Lokal pipeline yeterli
    [ ] > 10M satır → Spark pipeline kullan
[ ] (Opsiyonel) Bölge koordinatları config'e eklendi mi? (harita için)
```

### Sık Yapılan Hatalar

| Hata | Çözüm |
|------|-------|
| `KeyError: 'PULocationID'` | Sütun isimlerini kontrol et, gerekirse rename yap |
| `Zone-123` şeklinde isimsiz bölgeler | Config'deki zones dict'ine eksik ID'leri ekle |
| Haritada düğümler görünmüyor | Coordinates dict'ine lat/lon ekle |
| Tüm PageRank skorları eşit | Minimum trip eşiğini düşür (varsayılan: 50) |
| Memory error (lokal) | `--trips` azalt veya Spark pipeline'a geç |

---

## Metodoloji Özeti

### Matematiksel Model

**PageRank formülü:**

```
PR(v) = (1 - α) / N + α × Σ [PR(u) / out_degree(u)]
                              u ∈ in_neighbors(v)
```

- `α = 0.85` (damping factor)
- `N` = toplam düğüm sayısı
- Yüksek skor = o bölge kapandığında zincirleme etki büyük

**Darboğaz simülasyonu:** En yüksek PageRank'lı düğümü çıkar → Tüm kenarlarını sil → PageRank'ı yeniden hesapla → Akış kaybını ölç. Bu döngüyü top-k düğüm için tekrarla.

### Pandas/NetworkX → PySpark/GraphFrames Dönüşüm Tablosu

| İşlem | Pandas/NetworkX | PySpark/GraphFrames |
|-------|-----------------|---------------------|
| Veri Okuma | `pd.read_csv()` | `spark.read.parquet()` |
| Filtreleme | `df[df.col > 0]` | `df.filter(F.col("c") > 0)` |
| Gruplama | `df.groupby().agg()` | `df.groupBy().agg()` |
| Çizge | `nx.DiGraph()` | `GraphFrame(vertices, edges)` |
| PageRank | `nx.pagerank(G)` | `graph.pageRank()` |
| Bağlantılılık | `nx.connected_components()` | `graph.connectedComponents()` |
| Topluluk | `nx.community.louvain()` | `graph.labelPropagation()` |
| Düğüm Silme | `G.remove_node(n)` | `v.filter(col("id") != n)` |
| Sonuç Toplama | Zaten lokalde | `.toPandas()` (küçük veri!) |

---

## Kaynakça

1. Leskovec, J., Rajaraman, A., & Ullman, J. D. (2020). *Mining of Massive Datasets*. Cambridge University Press.
2. Page, L., Brin, S., Motwani, R., & Winograd, T. (1999). *The PageRank Citation Ranking*.
3. Apache Spark GraphX Programming Guide. https://spark.apache.org/docs/latest/graphx-programming-guide.html
4. NYC Taxi and Limousine Commission Trip Record Data. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
5. İBB Açık Veri Portalı. https://data.ibb.gov.tr/

---

## Lisans

Kod [MIT lisansı](LICENSE) ile yayımlanmıştır — Ömer Can Atlı, Kamil Duru (2026).

Analiz edilen NYC TLC yolculuk verisi New York Şehri tarafından kamu malı
olarak yayımlanmaktadır ve bu lisansın kapsamı dışındadır:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
