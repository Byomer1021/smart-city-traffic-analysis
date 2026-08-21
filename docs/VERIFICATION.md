# Sonuçların Doğrulanması

Bu belge, midterm raporunda ve final sunumunda bildirilen sayıların ham veriyle
yeniden hesaplanarak denetlenmesini kayda geçirir. Amaç iki soruyu kesin olarak
cevaplamak:

1. Bildirilen sonuçlar bu repodaki kodla yeniden üretilebiliyor mu?
2. Üretilemiyorsa fark nereden geliyor?

**Doğrulama tarihi:** 21 Ağustos 2026
**Veri kaynağı:** `data/nyc/yellow_tripdata_2023-{01..12}.parquet` — NYC TLC
Yellow Taxi, 2023 tam yılı, doğrudan TLC CDN'inden indirilmiş orijinal dosyalar.

---

## 1. Yöntem

Doğrulama iki bağımsız yoldan yapıldı:

**(a) Pipeline'dan bağımsız yeniden hesaplama.** 12 aylık Parquet dosyası
ayrı bir betikle tek tek okundu, `traffic_analysis_generic.py` içindeki ETL
filtreleri elle yeniden uygulandı ve OD çiftleri aylık olarak toplanıp
birleştirildi. Bu, pipeline kodunda bir hata olsa bile yakalanmasını sağlar.

**(b) Pipeline'ın kendisinin çalıştırılması.** Aynı veri
`merge_months.py` ile tek Parquet'e birleştirilip
`traffic_analysis_generic.py --city nyc --data ... --top-k 5` koşuldu.

İki yol da aynı sonucu verdi.

---

## 2. Midterm raporu — tamamı doğrulandı

Midterm raporundaki her sayı, beşinci ondalık basamağa kadar yeniden üretildi.

| Midterm raporu | Yeniden hesaplanan | Durum |
|---|---|---|
| Ham kayıt: 38.310.226 | 38.310.226 | ✅ |
| Temizlenen: 909.157 (%2,4) | 909.157 (%2,37) | ✅ |
| Kalan: 37.401.069 (%97,6) | 37.401.069 (%97,63) | ✅ |
| Düğüm / kenar: 258 / 9.990 | 258 / 9.990 | ✅ |
| Graftaki yolculuk: 35.420.777 | 35.420.777 | ✅ |
| Yoğunluk: 0,1507 | 0,1507 | ✅ |
| Topluluk: 3 (Louvain) | 3 — 182 / 44 / 32 | ✅ |
| Çalışma süresi: 40,7 sn (t3.xlarge) | 34,0 sn (yerel, 12 çekirdek) | ✅ tutarlı |

### PageRank ilk 5

| Sıra | Bölge | Midterm | Yeniden hesaplanan |
|---|---|---|---|
| 1 | Upper East Side North | 0,02573 | 0,025732 ✅ |
| 2 | Upper East Side South | 0,02382 | 0,023821 ✅ |
| 3 | Midtown Center | 0,02345 | 0,023445 ✅ |
| 4 | JFK Airport | 0,02260 | 0,022602 ✅ |
| 5 | Murray Hill | 0,01918 | 0,019183 ✅ |

### Darboğaz simülasyonu

| | Midterm | Yeniden hesaplanan |
|---|---|---|
| 1. düğüm sonrası akış kaybı | %8,2 | %8,2 ✅ |
| 5 düğüm sonrası kümülatif kayıp | %34,1 | %34,1 ✅ |
| Bileşen sayısı | 1 → 16 | 1 → 16 ✅ |
| Parçalanmanın olduğu adım | 4 (JFK Airport) | 4 (JFK Airport) ✅ |

**Sonuç:** Midterm raporu güvenilirdir ve bu repodan uçtan uca yeniden
üretilebilir.

---

## 3. Final sunumu — düzeltilmesi gereken sayılar

Sunumdaki bazı değerler hiçbir filtre kombinasyonuyla yeniden üretilemedi.

| Sunum | Gerçek değer | Not |
|---|---|---|
| 35,9M yolculuk | **35.420.777** (35,4M) | Aşağıdaki tarama bakınız |
| 10.368 rota | **9.990** | Hiçbir eşik/filtre kombinasyonu 10.368 vermiyor |
| 10 GB Parquet (Snappy) | **607 MB** ham, 810 MB dönüştürülmüş | Diskteki gerçek boyut |
| Motor: PySpark + GraphFrames | Sayılar **Pandas + NetworkX** çıktısı | §5 |
| "NetworkX ✗ OOM (16 GB yetmez)" | 16 GB'lik makinede 34 sn'de tamamlandı | §5 |
| Topluluk: Label Propagation | Bildirilen 182/44/32 **Louvain** sonucu | §5 |
| JFK betweenness = 1,00 (1. sıra) | **0,96 — 2. sıra** | §4 |
| Kapak: "BIG DATA ANALYTICS · 2025" | Proposal Mart 2026, midterm Nisan 2026 | Yıl hatası |

### 10.368 kenar araması

Kenar eşiği ve öz-döngü politikası tarandı; hiçbiri 10.368 üretmiyor:

| Eşik | Öz-döngü atılmış | Öz-döngü dahil |
|---|---|---|
| ≥ 40 | 11.030 | 11.202 |
| ≥ 45 | 10.471 | 10.641 |
| **≥ 50 (kodda kullanılan)** | **9.990** | 10.154 |
| ≥ 55 | 9.569 | 9.729 |
| ≥ 60 | 9.215 | 9.372 |

### 35,9M yolculuk araması

| Filtre varyantı | Kalan satır | Kenar | Graftaki akış |
|---|---|---|---|
| Yalnız `distance > 0` | 37.536.769 | 10.014 | 35.453.922 |
| `distance > 0` + `dur > 0` | 37.533.491 | 9.999 | 35.451.332 |
| **Kodda kullanılan ETL** | **37.401.069** | **9.990** | **35.420.777** |
| + `fare > 0` | 37.080.492 | 9.924 | 35.135.474 |
| + `passenger > 0` | 35.819.388 | 9.243 | 33.907.204 |

35,9M'ye en yakın değer `passenger_count > 0` filtresi sonrası kalan **satır**
sayısıdır (35.819.388). Ancak bu, grafa giren yolculuk sayısı değildir ve o
varyant 9.243 kenar üretir — sunumdaki 10.368 ile de uyuşmaz.

**Öneri:** Sunumda 35,9M yerine **35,4M**, 10.368 yerine **9.990**, 10 GB
yerine **~600 MB** yazılmalı. Bu değerler hem midterm raporuyla hem de
repodaki kodla tutarlıdır.

---

## 4. Betweenness: bulunan ve düzeltilen hata

### Belirti

Midterm raporu JFK Airport için normalize betweenness **0,74**, final sunumu
**1,00 (1. sıra)** diyor. Aynı veri, aynı graf, iki farklı sayı.

### Sebep

`compute_centrality()` şöyle çağırıyordu:

```python
nx.betweenness_centrality(G, weight="weight", k=min(100, G.number_of_nodes()))
```

258 düğümlü grafta `k=100`, betweenness'ın **rastgele örneklemeli yaklaşık**
hesaplanması demektir. `seed` parametresi verilmediği için her koşuda farklı
100 kaynak düğüm seçilir.

### Kanıt

Aynı graf üzerinde, düzeltmeden önce dört ardışık koşu:

| Koşu | JFK (norm.) | East Harlem South (norm.) | 1. sıra |
|---|---|---|---|
| 1 | 1,00 | 0,77 | JFK Airport |
| 2 | 0,82 | 1,00 | East Harlem South |
| 3 | 0,95 | 1,00 | East Harlem South |
| 4 | 0,93 | 1,00 | East Harlem South |

JFK'nin ham skoru koşular arasında 0,0645–0,0765 bandında, yaklaşık %18
oynuyordu. Midterm ile sunum arasındaki fark bir hesaplama hatası değil,
aynı kararsız yaklaşımdan alınmış iki farklı çekilişti.

### Düzeltme

1000 düğüme kadar tam (deterministik) hesap; üstünde sabit `seed` ile örnekleme:

```python
n = G.number_of_nodes()
if n <= 1000:
    betweenness = nx.betweenness_centrality(G, weight="weight")
else:
    betweenness = nx.betweenness_centrality(G, weight="weight", k=500, seed=42)
```

Düzeltme sonrası üç ardışık koşu birebir aynı sonucu verdi
(JFK = 0,064050, East Harlem South = 0,066492).

### Düzeltilmiş betweenness sıralaması

| Sıra | Bölge | Betweenness | Norm. | PageRank sırası |
|---|---|---|---|---|
| 1 | East Harlem South | 0,06649 | 1,00 | 17 |
| 2 | JFK Airport | 0,06405 | 0,96 | 4 |
| 3 | LaGuardia Airport | 0,04567 | 0,69 | 12 |
| 4 | Jamaica | 0,03754 | 0,56 | 65 |
| 5 | East New York | 0,03689 | 0,55 | 51 |

### Bulguya etkisi

Sunumdaki "JFK Paradoksu" başlığı **düzeltilmiş haliyle de geçerli**, ama
ifadesi değişmeli:

- JFK PageRank'ta 4., betweenness'ta **2.** (1. değil). Aradaki fark hâlâ
  belirgin ve köprü rolünü destekliyor.
- Paradoksun **en uç örneği aslında East Harlem South**: PageRank'ta 17.,
  betweenness'ta 1. Hacim düşük, geçiş rolü en yüksek. Bu, "PageRank tek başına
  yetmez" tezini JFK'den daha güçlü anlatıyor.
- JFK için **en sağlam kanıt betweenness değil, simülasyondur**: JFK
  çıkarıldığında ağ 1 bileşenden 16 bileşene düşüyor. Bu ölçüm deterministiktir,
  örneklemeden etkilenmez ve her koşuda aynıdır.

**Öneri:** Sunumdaki manşeti betweenness sayısı yerine simülasyon sonucuna
dayandırın. "JFK çıkarıldığında ağ 16 parçaya bölünüyor" cümlesi hem daha
çarpıcı hem de tamamen tekrar üretilebilir.

---

## 5. Motor uyuşmazlığı: NetworkX mi, GraphFrames mi?

Sunum sonuçların PySpark + GraphFrames ile üretildiğini söylüyor. Kanıtlar
aksini gösteriyor:

1. **Sayılar lokal pipeline'ınkiyle birebir aynı.** 258 düğüm / 9.990 kenar /
   35.420.777 yolculuk / 0,1507 yoğunluk — hepsi `traffic_analysis_generic.py`
   çıktısı.

2. **Spark pipeline farklı bir graf üretir.** `spark_traffic_pipeline.py`
   ETL'inde lokal sürümde bulunmayan iki filtre var:
   `fare_amount > 0` ve `passenger_count > 0`. Bu filtrelerle graf
   **258 düğüm / 9.183 kenar / 33.629.117 yolculuk / 0,1385 yoğunluk** olur.
   Bildirilen sayılar bunlar değil.

3. **Topluluk algoritması uyuşmuyor.** Sunum "Label Propagation" diyor, ama
   bildirilen 182/44/32 bölünmesi midterm'in Louvain sonucuyla aynı. İki farklı
   algoritmanın birebir aynı bölünmeyi vermesi beklenmez.

4. **OOM iddiası tutmuyor.** Sunum NetworkX'in 35,9M satırda 16 GB RAM ile
   patladığını söylüyor. Aynı iş 16 GB'lik bir makinede 34 saniyede tamamlandı.
   Midterm'in kendisi de "40,7 saniyede bitti" diyor.

**Öneri:** Sunumda anlatı şöyle düzeltilmeli: analiz **Pandas + NetworkX ile
AWS EC2 t3.xlarge üzerinde** çalıştırıldı; PySpark + GraphFrames pipeline'ı
daha büyük veri setleri için **yazıldı ve repoda mevcut**, ancak bildirilen
sonuçları üreten motor değil. Bu, projenin değerini düşürmez — 38,3M kayıt tek
düğümde 34 saniyede işlenmişse bu kendi başına anlamlı bir mühendislik
sonucudur ve dürüst anlatım savunması çok daha kolaydır.

---

## 6. Küçük tutarsızlıklar

| Konu | Durum |
|---|---|
| Bölge sayısı | Proposal 263, midterm "265 zone with centroid coordinates from shapefiles". Gerçek: `taxi_zone_lookup.csv` 265 satır, shapefile centroid'i 263 satır. Midterm cümlesi iki kaynağı birleştirmiş. Grafa giren düğüm sayısı 258. |
| "2,5 GB on disk" (midterm) | Ham Parquet 607 MB. Ham + dönüştürülmüş + birleşik sayılırsa ~2,2 GB, o hâlde savunulabilir. |
| Sunum slayt 10 "CUMULATIVE LOSS" | −%8,2 / −%7,4 / −%5,2 değerleri kümülatif değil, 1./3./5. adımların tek tek düşüşleri. Toplam ~%34 doğru, başlık yanlış. |
| Louvain sürüm duyarlılığı | `seed=42` verilmesine rağmen sonuç NetworkX sürümüne ve düğüm ekleme sırasına duyarlı. Pipeline üzerinden 182/44/32, bağımsız betikte 181/46/31 çıktı. Toplam 258 her ikisinde de doğru. |
| Node2Vec sonuçları | Sunum slayt 13'teki benzerlik skorları (LaGuardia 0,88, Penn Station 0,71, Times Sq 0,65) ölçülmüş değil — slaytta "Expected Answer (sanity check)" olarak işaretli. Repoda `embeddings.npz` yok. Jüri çıktı isterse önce `gnn_node2vec.py` çalıştırılmalı. |

---

## 7. Yeniden üretim

```bash
# 1) Bağımlılıklar
pip install -r requirements.txt

# 2) 12 aylık TLC verisini indir ve dönüştür
#    (setup_and_run.sh MONTHS=12 ile bunu otomatik yapar)
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  python local_pipeline/convert_data.py \
    --input  data/nyc/yellow_tripdata_2023-$m.parquet \
    --output data/nyc/formatted/nyc_trips_2023_$m.parquet \
    --mapping config/sample_mapping_nyc_tlc.json
done

# 3) Birleştir
python local_pipeline/merge_months.py \
  --input-glob "data/nyc/formatted/nyc_trips_2023_[0-1][0-9].parquet" \
  --output data/nyc/formatted/nyc_trips_2023_all.parquet

# 4) Analiz
python local_pipeline/traffic_analysis_generic.py \
  --city nyc --data data/nyc/formatted/nyc_trips_2023_all.parquet --top-k 5

# 5) Harita verisi
python local_pipeline/export_map_data.py \
  --city nyc --data data/nyc/formatted/nyc_trips_2023_all.parquet
```

Windows'ta çalıştırıyorsanız önce `set PYTHONUTF8=1` yapın; scriptler konsola
Unicode kutu karakterleri basıyor ve `cp1254` kod sayfasında hata veriyor.

Beklenen çıktı: 258 düğüm, 9.990 kenar, 35.420.777 yolculuk, yoğunluk 0,1507,
3 topluluk (182/44/32), ilk düğümde %8,2 akış kaybı, beş düğümde %34,1 ve
JFK Airport çıkarıldığında 1 → 16 bileşen.
