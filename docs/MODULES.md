# Modül Referansı

Bu belge projedeki her modülün ne yaptığını, hangi girdiyi alıp hangi çıktıyı
ürettiğini ve komut satırı parametrelerini anlatır. Kod okumadan pipeline'ı
anlamak ve çalıştırmak için tasarlandı.

Sayısal sonuçlar için [FINAL_REPORT.md](FINAL_REPORT.md), sonuçların ham veriyle
doğrulanması için [VERIFICATION.md](VERIFICATION.md) dosyalarına bakın.

---

## Genel Akış

```
                    ┌──────────────────────────┐
   NYC TLC Parquet  │  convert_data.py         │  şema eşleme
   (aylık, 12 ay)   │  merge_months.py         │  tek dosyada birleştirme
                    └────────────┬─────────────┘
                                 │
   taxi_zones.shp   ┌────────────▼─────────────┐
   ──────────────▶  │  build_nyc_centroids.py  │  bölge koordinatları
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │  city_config.py          │  bölge adları, ağırlıklar,
                    │  (veya auto_config_      │  koordinatlar
                    │   builder.py)            │
                    └────────────┬─────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
┌───────▼──────────┐   ┌─────────▼──────────┐   ┌─────────▼──────────┐
│ traffic_analysis │   │ export_map_data.py │   │ spark_traffic_     │
│ _generic.py      │   │                    │   │ pipeline.py        │
│ (lokal, NetworkX)│   │ (harita JSON)      │   │ (dağıtık, Spark)   │
└───────┬──────────┘   └─────────┬──────────┘   └─────────┬──────────┘
        │                        │                        │
   6 adet PNG              map_data.json            PNG + JSON (küme)
                                 │
                    ┌────────────▼─────────────┐
                    │  viewer.html             │  Leaflet haritası
                    │  gnn_node2vec.py         │  128-boyutlu gömme
                    │  gnn_visualize.py        │  t-SNE görselleştirme
                    └──────────────────────────┘
```

---

## `config/city_config.py`

Pipeline'ı şehirden bağımsız kılan konfigürasyon katmanı. Analiz kodu hiçbir
yerde "İstanbul" ya da "NYC" bilmez; sadece bir `CityConfig` nesnesi alır.

### `CityConfig` (dataclass)

| Alan | Tip | Açıklama |
|---|---|---|
| `name`, `country` | `str` | Şehir ve ülke adı |
| `zones` | `Dict[int, str]` | `{bölge_id: bölge_adı}` |
| `hotspots` | `List[int]` | Yüksek yoğunluklu bölge ID'leri |
| `secondary_hotspots` | `List[int]` | Orta yoğunluklu bölgeler |
| `transport_hubs` | `List[int]` | Havalimanı, otogar, terminal |
| `coordinates` | `Dict[int, Tuple[float, float]]` | `{bölge_id: (enlem, boylam)}` |
| `hotspot_weight` … `default_weight` | `float` | Sentetik veri üretiminde bölge çekim ağırlıkları |
| `currency`, `base_fare`, `per_km_fare` | | Ücret hesabı |
| `timezone` | `str` | Zaman dilimi |

**Önemli:** `hotspots`, `secondary_hotspots` ve ağırlık alanları yalnızca
*sentetik* veri üretimini etkiler. Gerçek veriyle çalışırken graf tamamen
veriden kurulur; bu listeler sonucu değiştirmez. Yalnızca `map_data.json`
içindeki `zone_type` etiketini (harita renklendirmesi) belirlerler.

`coordinates` ise gerçek veride de kritiktir: boşsa coğrafi harita üretilmez
ve `export_map_data.py` koordinatsız kenarların tamamını atar.

### Kayıtlı şehirler

| Şehir | Bölge | Hotspot | İkincil | Hub | Koordinat | Para |
|---|---|---|---|---|---|---|
| `istanbul` | 92 | 23 | 19 | 11 | 92 | TRY |
| `nyc` | 265 | 20 | 20 | 5 | 263 | USD |
| `ankara` | 20 | 5 | 7 | 2 | 20 | TRY |

NYC konfigürasyonu iki dosyadan beslenir; ikisi de yoksa koda gömülü yedek
listeye düşer:

- `load_nyc_zones_from_lookup()` → `data/nyc/taxi_zone_lookup.csv` (265 bölge adı)
- `load_nyc_coordinates_from_centroids()` → `data/nyc/nyc_zone_centroids.csv` (263 centroid)

NYC `hotspots` listesi 2023 gerçek akış verisinden (PU+DO toplamı) türetilmiş
ilk 20 bölgedir; `secondary_hotspots` sıradaki 20 bölgedir.

### Fonksiyonlar

| Fonksiyon | Açıklama |
|---|---|
| `get_city_config(name)` | İsimden config döndürür (`istanbul` / `nyc` / `ankara`) |
| `list_cities()` | Kayıtlı şehir anahtarları |
| `load_config_from_json(path)` | Kod değiştirmeden JSON'dan şehir yükler |

Yeni şehir eklemek için ya `CITY_REGISTRY`'ye bir fabrika fonksiyonu ekleyin,
ya da JSON yazıp `--config` ile geçin. Örnek: `config/example_city_london.json`.

---

## `local_pipeline/traffic_analysis_generic.py`

Ana analiz pipeline'ı. Altı adımı sırayla çalıştırır ve 6 PNG üretir.

```bash
python local_pipeline/traffic_analysis_generic.py --city nyc \
    --data data/nyc/formatted/nyc_trips_2023_all.parquet --top-k 5
```

| Parametre | Varsayılan | Açıklama |
|---|---|---|
| `--city` | `istanbul` | `istanbul` / `nyc` / `ankara` |
| `--config` | — | Harici JSON config (`--city` yerine) |
| `--data` | — | Gerçek veri dosyası (CSV veya Parquet). Verilmezse sentetik üretilir |
| `--trips` | `2_000_000` | Sentetik yolculuk sayısı (`--data` yoksa) |
| `--top-k` | `5` | Simülasyonda çıkarılacak düğüm sayısı |

### Adımlar

| # | Fonksiyon | Ne yapar |
|---|---|---|
| 1 | `etl_pipeline()` | CSV/Parquet okur, anomali temizler, OD çiftlerine indirger |
| 2 | `build_graph()` | Yönlü ağırlıklı `nx.DiGraph` kurar |
| 3 | `run_pagerank()` | `nx.pagerank(alpha=0.85, max_iter=100, weight="weight")` |
| 3.5 | `compute_centrality()` | Betweenness + in/out-degree merkeziliği |
| 4 | `detect_communities()` | SCC, WCC ve Louvain (`seed=42`) |
| 5 | `simulate_bottleneck()` | Top-k düğümü sırayla siler, akış/kenar/bileşen ölçer |
| 6 | `create_visualizations()` | 6 PNG üretir |

### ETL filtreleri

Sırayla uygulanır:

1. `trip_distance > 0`
2. `trip_duration_minutes > 0`
3. `trip_duration_minutes <= 300`
4. Aynı bölge içi **ve** `trip_distance < 0.1` olan kayıtlar atılır
5. Gruplama sonrası: `PULocationID != DOLocationID` (öz-döngü atılır)
6. Gruplama sonrası: `trip_count >= 50` (gürültü eşiği)

> **Dikkat:** Lokal pipeline `fare_amount` ve `passenger_count` üzerinde filtre
> uygulamaz; Spark pipeline uygular. Bu, iki motorun farklı graf üretmesine yol
> açar — ayrıntı için [VERIFICATION.md](VERIFICATION.md).

### Betweenness hakkında

`compute_centrality()` 1000 düğüme kadar **tam** betweenness hesaplar
(deterministik). Daha büyük graflarda `k=500, seed=42` ile örnekleme yapar.

Önceden `k=min(100, N)` ile ve `seed` verilmeden çağrılıyordu; bu, aynı graf
üzerinde bile her koşuda farklı sonuç üretiyordu. Detay: [VERIFICATION.md](VERIFICATION.md).

### Üretilen görseller

| Dosya | İçerik |
|---|---|
| `01_dashboard.png` | 4 panel: top-15 PageRank, simülasyon eğrisi, PR-vs-betweenness, topluluk dağılımı |
| `02_network_topology.png` | Top-40 düğümün ağ topolojisi (koordinat varsa coğrafi, yoksa spring layout) |
| `03_simulation_detail.png` | Kalan kapasite, ağ parçalanması, ortalama en kısa yol |
| `04_centrality_heatmap.png` | Top-20 bölge × 4 merkezilik metriği, normalize |
| `05_distributions.png` | Kenar ağırlığı (log ölçek) ve derece dağılımı |
| `06_geo_map.png` | Coğrafi harita — **yalnızca config'de 5'ten fazla koordinat varsa** |

Çıktılar `results/<şehir-slug>/` altına yazılır (`İstanbul` → `istanbul`,
`New York City` → `new_york_city`).

---

## `local_pipeline/export_map_data.py`

Aynı analizi çalıştırıp sonucu interaktif harita için tek bir JSON'a yazar.

```bash
python local_pipeline/export_map_data.py --city nyc \
    --data data/nyc/formatted/nyc_trips_2023_all.parquet
```

| Parametre | Varsayılan | Açıklama |
|---|---|---|
| `--city` | `istanbul` | Şehir anahtarı |
| `--data` | — | Gerçek veri dosyası (CSV/Parquet) |
| `--trips` | `2_000_000` | Sentetik yolculuk sayısı (`--data` yoksa) |

### `map_data.json` şeması

| Anahtar | İçerik |
|---|---|
| `city` | Ad, ülke, para birimi, harita merkezi (`center_lat` / `center_lon`), zoom |
| `nodes` | Her bölge: `id`, `name`, `lat`, `lon`, `pagerank(_norm)`, `betweenness(_norm)`, `in_flow`, `out_flow`, `community`, `zone_type`, `rank` |
| `edges` | En yoğun 300 rota: `src`, `dst`, `weight(_norm)`, `avg_duration`, uç nokta ad/koordinatları |
| `simulation` | Her adımda silinen düğüm ve kalan akış/kenar yüzdesi |
| `communities` | Topluluk boyutları, örnek üyeler, ortalama PageRank |
| `stats` | `total_nodes`, `total_edges`, `total_flow`, `density`, `n_communities` |

`zone_type` değeri `hotspot` / `hub` / `normal` olur ve config'deki listelerden
belirlenir; haritadaki renk kodlaması bunu kullanır.

> **Dikkat:** Kenar listesi, iki ucundan birinin koordinatı `0` olan rotaları
> atlar. Config'de koordinat yoksa `edges` **boş** çıkar.

---

## `local_pipeline/generate_data.py`

Şehir konfigürasyonundan sentetik yolculuk verisi üretir. Gerçek veri olmadan
pipeline'ı uçtan uca denemek için.

`generate_trip_data(config, num_trips=2_000_000, year=2023, anomaly_rate=0.03, seed=42)`

- Bölge seçimi `config.get_zone_weight()` ağırlıklarıyla yapılır
- Yolculukların %5'i aynı bölge içi
- Saat dağılımı sabit bir günlük trafik profilinden (sabah 08 ve akşam 18 zirveli)
- Süre: `lognormal(mean=2.5, sigma=0.7)`, 1–180 dk arasına kırpılır
- Mesafe: süreyle korelasyonlu, 0.1–50 arası
- Ücret: `base_fare + mesafe × per_km_fare + süre payı`, üstüne gürültü
- `anomaly_rate` oranında kayda sıfır mesafe veya sıfır süre enjekte edilir
  (ETL'in temizleme adımını sınamak için)

`seed=42` sabit olduğundan aynı parametrelerle aynı veri üretilir.

---

## `local_pipeline/convert_data.py`

Herhangi bir ham veri şemasını pipeline'ın beklediği 8 sütuna dönüştürür.

```bash
python local_pipeline/convert_data.py \
    --input data/nyc/yellow_tripdata_2023-01.parquet \
    --output data/nyc/formatted/nyc_trips_2023_01.parquet \
    --mapping config/sample_mapping_nyc_tlc.json
```

| Parametre | Açıklama |
|---|---|
| `--input` | Ham dosya (CSV / TSV / Parquet / JSON / Excel) |
| `--output` | Çıktı (uzantıya göre CSV veya Parquet) |
| `--mapping` | `{hedef_sütun: kaynak_sütun}` eşleme JSON'u (opsiyonel) |
| `--validate-only` | Dönüştürme yapmaz, sadece doğrulama raporu basar |

Hazır eşlemeler: `config/sample_mapping_nyc_tlc.json`, `config/sample_mapping_ibb.json`.

`trip_duration_minutes` kaynakta yoksa `dropoff - pickup` farkından hesaplanır.
Zorunlu bir sütun eksikse çıkış kodu 1 ile durur.

### Zorunlu sütunlar

`pickup_datetime`, `dropoff_datetime`, `PULocationID`, `DOLocationID`,
`trip_distance`, `trip_duration_minutes`, `fare_amount`, `passenger_count`

---

## `local_pipeline/merge_months.py`

Aylık dosyaları tek dosyada birleştirir. Analiz pipeline'ı tek bir dosya yolu
aldığı için 12 aylık NYC verisinde gereklidir.

```bash
python local_pipeline/merge_months.py \
    --input-glob "data/nyc/formatted/nyc_trips_2023_[0-1][0-9].parquet" \
    --output data/nyc/formatted/nyc_trips_2023_all.parquet
```

Her dosyada 8 zorunlu sütunu doğrular, eksikse hata verir.

> Glob deseninde `nyc_trips_2023_*.parquet` kullanırsanız daha önce üretilmiş
> `_all.parquet` dosyası da eşleşir ve veri iki katına çıkar. Yukarıdaki
> `[0-1][0-9]` deseni yalnızca ay dosyalarını seçer.

---

## `local_pipeline/build_nyc_centroids.py`

NYC TLC shapefile'ından bölge merkez koordinatlarını üretir.

```bash
python local_pipeline/build_nyc_centroids.py --download
```

| Parametre | Varsayılan |
|---|---|
| `--download` | Kapalı — verilirse `taxi_zones.zip` indirir |
| `--zip-url` | TLC CDN adresi |
| `--zip-path` | `data/nyc/taxi_zones.zip` |
| `--extract-dir` | `data/nyc/taxi_zones` |
| `--output` | `data/nyc/nyc_zone_centroids.csv` |

Centroid'i `EPSG:2263` (NAD83 / New York Long Island) projeksiyonunda hesaplar,
sonra `EPSG:4326`'ya çevirir — WGS84'te doğrudan centroid almak hatalı sonuç
verir. **`geopandas` gerektirir** (`requirements.txt`'te opsiyonel olarak
yorumlu; üretilmiş CSV zaten repoda mevcut).

Çıktı sütunları: `LocationID`, `zone`, `borough`, `centroid_lat`,
`centroid_lon` — 263 satır.

---

## `local_pipeline/auto_config_builder.py`

Elinizde bölge listesi olmayan bir veri setinden otomatik `CityConfig` JSON'u
türetir.

```bash
python local_pipeline/auto_config_builder.py \
    --input data_formatted.csv --city-name "London" --country "UK" \
    --zones-csv zones_meta.csv --output config/london_auto.json
```

- `zones`: veride görülen benzersiz PU/DO ID'leri
- `hotspots`: toplam akışa göre en üst `--hotspot-pct` dilim (varsayılan %10)
- `secondary_hotspots`: sıradaki `--secondary-pct` dilim (varsayılan %15)
- `transport_hubs` ve `coordinates`: `--zones-csv` verilirse oradan

`zones_meta.csv` başlıkları: `id,name,lat,lon,hub` (`hub` opsiyonel, 1/0).
Verilmezse bölgeler `Zone-<id>` diye adlandırılır ve koordinat üretilmez.

---

## `local_pipeline/gnn_node2vec.py`

Graf üzerinde Node2Vec gömmesi eğitir — "hangi bölgeler birbirine benziyor"
sorusunu cevaplar. PageRank "kritik mi" der, Node2Vec "neye benziyor" der.

```bash
python local_pipeline/gnn_node2vec.py \
    --input results/new_york_city/map_data.json \
    --output results/new_york_city/embeddings.npz
```

| Parametre | Varsayılan | Açıklama |
|---|---|---|
| `--dim` | `128` | Gömme boyutu |
| `--walks` | `10` | Düğüm başına yürüyüş sayısı |
| `--length` | `80` | Yürüyüş uzunluğu |
| `--p` | `1.0` | Return parametresi (düşük → BFS benzeri) |
| `--q` | `1.0` | In-out parametresi (düşük → DFS benzeri) |

`p = q = 1` DeepWalk'a denk gelir. Çıktılar: `.npz` (düğümler, gömme matrisi,
kosinüs benzerlik matrisi) ve `_summary.json` (top-10 bölgenin en benzer 5
komşusu).

> **Girdisi `map_data.json`'dır**, ham veri değil. Yani önce
> `export_map_data.py` çalıştırılmalıdır. Ayrıca yalnızca JSON'daki **en yoğun
> 300 kenar** üzerinden graf kurar — tam graf değil. Bu, benzerlik sonuçlarını
> yüksek hacimli koridorlara doğru yanlı hale getirir.

`node2vec` ve `gensim` gerektirir (`requirements.txt`'te tanımlı).

---

## `local_pipeline/gnn_visualize.py`

128 boyutlu gömmeleri t-SNE ile 2 boyuta indirip iki panelli grafik çizer:
solda topluluk renklendirmesi, sağda PageRank kritiklik skoru.

```bash
python local_pipeline/gnn_visualize.py \
    --embeddings results/new_york_city/embeddings.npz \
    --map-data   results/new_york_city/map_data.json \
    --output     results/new_york_city/07_gnn_embeddings.png
```

`--perplexity` varsayılan 30; düğüm sayısı azsa otomatik olarak `n-1`'e
düşürülür. `scikit-learn` gerektirir.

---

## `spark_pipeline/spark_traffic_pipeline.py`

Lokal pipeline'ın PySpark + GraphFrames karşılığı. 10M+ satırlık veri setleri
ve dağıtık küme için.

```bash
spark-submit --master yarn --deploy-mode cluster \
  --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \
  spark_pipeline/spark_traffic_pipeline.py \
  --city nyc --input s3://bucket/data/*.parquet --output s3://bucket/results/
```

| Parametre | Varsayılan | Açıklama |
|---|---|---|
| `--city` | `istanbul` | Şehir adı |
| `--input` | **zorunlu** | Parquet yolu (S3 / GCS / HDFS / lokal) |
| `--output` | **zorunlu** | Çıktı dizini |
| `--reset-prob` | `0.15` | PageRank reset olasılığı (= `1 - alpha`) |
| `--max-iter` | `20` | PageRank iterasyon üst sınırı |
| `--top-k` | `5` | Simülasyonda silinecek düğüm |

Lokal karşılıklar:

| İşlem | Lokal | Dağıtık |
|---|---|---|
| Okuma | `pd.read_parquet()` | `spark.read.parquet()` |
| Filtre | `df[df.col > 0]` | `df.filter(F.col("c") > 0)` |
| Gruplama | `df.groupby().agg()` | `df.groupBy().agg()` |
| Çizge | `nx.DiGraph()` | `GraphFrame(vertices, edges)` |
| PageRank | `nx.pagerank()` | `graph.pageRank()` |
| Topluluk | `nx.community.louvain_communities()` | `graph.labelPropagation()` |
| Bileşen | `nx.connected_components()` | `graph.connectedComponents()` |

> **İki motor birebir aynı sonucu vermez.** Spark ETL'i ek olarak
> `fare_amount > 0` ve `passenger_count > 0` filtreler; topluluk tespiti Louvain
> yerine Label Propagation kullanır. NYC verisinde bu fark 9.990 kenar yerine
> 9.183 kenar demektir. Ayrıntı: [VERIFICATION.md](VERIFICATION.md).

---

## `index.html`

İnteraktif harita uygulaması. Depo kökündedir çünkü GitHub Pages'in servis
ettiği sayfa budur: https://byomer1021.github.io/smart-city-traffic-analysis/

Bağımlılığı yalnızca Leaflet CDN'idir; başka hiçbir kütüphane kullanmaz.
`results/new_york_city/map_data.json` dosyasını **göreli** yolla okur — mutlak
yol kullanılmamalıdır, çünkü GitHub Pages proje siteleri
`/<repo-adı>/` alt yolunda servis edilir ve baştaki `/` bağlantıyı kırar.

| Bölüm | İşlev |
|---|---|
| İstatistik paneli | Bölge, rota, yolculuk ve topluluk sayısı |
| Rota kaydırıcısı | Çizilen rota sayısı (0 – JSON'daki kenar sayısı) |
| Bölge kaydırıcısı | Yalnızca PageRank sıralamasında ilk N bölgeyi göster |
| Topluluk anahtarı | Renklendirmeyi topluluk ↔ bölge tipi arasında değiştirir |
| Top-15 listesi | Tıklanınca haritada o bölgeye uçar; çıkarılan bölgeler üstü çizili görünür |
| Simülasyon paneli | Düğümleri sırayla çıkarır; kalan akış, kalan rota, bileşen sayısı ve kalan bölge sayısını günceller |

Simülasyon paneli `map_data.json` içindeki `simulation` dizisini kullanır.
`num_components` ve `nodes_remaining` alanları **tam çizge** üzerinde
hesaplanır; JSON'a yalnızca en yoğun 300 kenar yazıldığı için bu değerler
istemci tarafında doğru hesaplanamaz.

Yerelde çalıştırmak için (`fetch` `file://` üzerinden çalışmaz):

```bash
python -m http.server 8000
# tarayıcı: http://localhost:8000/
```

## `visualization/viewer.html`

Artık `index.html`'e yönlendiren bir yer tutucu. Eski bağlantılar ve
dokümantasyon referansları kırılmasın diye duruyor.

Önceki hâli bağımsız bir görüntüleyiciydi ancak `.gitignore` kapsamındaki
`results/all_cities/map_data_combined.json` dosyasını okuduğu için depoyu
klonlayan herkes 404 alıyordu.

## `visualization/istanbul_traffic_map.jsx`

**Şu an yalnızca yer tutucu** — iki satırlık yorumdan ibaret, çalışan bir React
bileşeni içermiyor. Sunumdaki interaktif harita demosu için `index.html`
kullanılmalıdır.

---

## `setup_and_run.sh`

Sıfırdan bir Ubuntu sunucusunda (AWS EC2'de test edildi) uçtan uca kurulum ve
çalıştırma. 8 adım:

| Adım | Ne yapar |
|---|---|
| 1 | Sistem paketleri (`git`, `python3-venv`, `unzip`, …) |
| 2 | Repo klonu + venv + `requirements.txt` |
| 3 | NYC TLC Parquet indirme (`MONTHS` kadar ay) |
| 4 | `build_nyc_centroids.py --download` |
| 5 | `convert_data.py` ile aylık şema dönüşümü |
| 6 | `merge_months.py` ile birleştirme |
| 7 | `traffic_analysis_generic.py --data <birleşik>` |
| 8 | `export_map_data.py --data <birleşik>` |

Script başındaki değişkenler ayarlanabilir: `YEAR`, `MONTHS`, `CITY`, `TOP_K`.
Varsayılan `MONTHS=4`; rapordaki 12 aylık sonuç için `MONTHS=12` yapın.

---

## Bilinen kısıtlar

| Konu | Durum |
|---|---|
| `istanbul_traffic_map.jsx` | Yer tutucu, uygulanmadı |
| Zamansal analiz | `pickup_datetime` okunuyor ama saat/gün bazlı segmentasyon yapılmıyor |
| Lokal ↔ Spark filtre farkı | İki motor farklı graf üretir (yukarıda) |
| Node2Vec girdisi | Tam graf değil, `map_data.json`'daki en yoğun 300 kenar |
| Windows konsolu | Scriptler `█` / `═` gibi karakterler basar; `cp1254` konsolda `UnicodeEncodeError` verir. Çözüm: `set PYTHONUTF8=1` (veya `PYTHONIOENCODING=utf-8`) |
