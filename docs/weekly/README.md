# Proje Günlüğü — Haftalık İlerleme

CSE458 — Introduction to Big Data Analytics · 2026 Bahar
Ömer Can Atlı, Kamil Duru

---

## Bu belge hakkında

Proje dönem sonunda tamamlandı. Bu günlük, geriye dönük olarak — git geçmişi,
dosya zaman damgaları, teslim edilmiş belgeler ve sonuçların yeniden
çalıştırılarak doğrulanmasıyla — derlenmiştir.

Hafta numaraları ve tarihler iki sabit noktadan türetilmiştir:

| Sabit nokta | Kaynak |
|---|---|
| Proposal teslimi — Mart 2026 | Proje önerisi kapak sayfası |
| Midterm raporu — Nisan 2026 | Ara rapor kapak sayfası |
| NYC veri hattı + EC2 scripti — 25 Nisan 2026 | git `a7ff801`, `32d55c9` |
| Node2Vec eklentisi — 9 Mayıs 2026 | git `d774edc` |
| "Node2Vec = Hafta 12-13 ders konusu" | Final sunumu, slayt 13 |

Son iki satır birlikte okunduğunda dönemin 16 Şubat 2026 haftasında başladığı
sonucu çıkmaktadır. **Ders takvimi farklıysa hafta numaraları kaydırılmalıdır**
— belgedeki tek teyit edilmemiş varsayım budur; tarihler ve içerik doğrudur.

---

## Özet tablo

| Hafta | Tarih | Aşama | Ana çıktı |
|---|---|---|---|
| 1–2 | 16 Şubat – 27 Şubat | Konu belirleme, literatür taraması | Problem çerçevesi |
| 3–4 | 2 Mart – 13 Mart | Veri kaynağı ve teknoloji seçimi | Mimari kararlar |
| 5–6 | 16 Mart – 27 Mart | **Proposal** | 5 sayfalık proje önerisi |
| 7–8 | 30 Mart – 10 Nisan | Lokal prototip | Çalışan pipeline + config katmanı |
| 9 | 13 Nisan – 17 Nisan | **Midterm raporu** | 14 sayfalık ara rapor |
| 10 | 20 Nisan – 24 Nisan | Gerçek veri hattı, EC2 dağıtımı | 38,3M kayıt işlendi |
| 11 | 27 Nisan – 1 Mayıs | Görselleştirme ve harita katmanı | 6 figür + interaktif harita |
| 12–13 | 4 Mayıs – 15 Mayıs | Node2Vec / GNN eklentisi | Gömme modülleri |
| 14 | 18 Mayıs – 22 Mayıs | **Final sunumu** | 15 slayt |
| — | 21 Ağustos 2026 | Doğrulama ve dokümantasyon | Bu belge + 3 doküman |

---

## Çalışma biçimi

Proje boyunca **danışman yönlendirmesi alınmamıştır.** Konu seçimi, veri
kaynağı, teknoloji yığını, mimari değişiklikler ve analiz yöntemleri dâhil tüm
kararlar ekip tarafından verilmiştir.

**İş bölümü:**

| | |
|---|---|
| **Ömer Can Atlı** | Kod tabanının tamamı. Depodaki commit'lerin hepsi bu hesaptan (`a7ff801`, `32d55c9`, `d774edc`): config katmanı, ETL, çizge analizi, simülasyon, görselleştirme, veri dönüştürme araçları ve GNN eklentisi. |
| **Kamil Duru** | Sunucu kurulumu ve sunucu üzerinde çalıştırma testleri. AWS EC2 örneğinin ayağa kaldırılması, ortam hazırlığı ve pipeline'ın gerçek veriyle uçtan uca koşturulması. Raporlama tarafında ara rapor derlemesi. |

---

## Hafta 1–2 · Konu belirleme ve literatür taraması

**Tarih:** 16 Şubat – 27 Şubat 2026

Projenin çıkış sorusu şuydu: mevcut trafik yönetim sistemleri noktasal sensör
ölçümlerine dayanıyor ve bir kavşağın *kendi başına* ne kadar yoğun olduğunu
söylüyor. Peki bir bölgenin ağın *bütünü* içindeki kritikliği nasıl ölçülür?

Bu ayrım projenin tüm yapısını belirledi. Kritiklik iki farklı kaynaktan
gelebilir:

- **Hacim** — çok yolculuğun başladığı/bittiği yer olması
- **Konum** — ağın başka türlü bağlanamayan iki parçası arasında köprü olması

Sensör verisi yalnızca birincisini yakalar. İkincisinin çizge analitiğiyle
ölçülebileceği fikri, projenin temel tezi hâline geldi.

**Taranan kaynaklar** (proposal'daki literatür bölümüne temel oldu):

| Kaynak | Katkısı |
|---|---|
| Leskovec, Rajaraman & Ullman (2020) | Seyrek çizgelerde iteratif algoritmalar; matematiksel zemin |
| Brin & Page (1998) | PageRank'ın özgün tanımı ve α = 0,85 seçimi |
| Wang vd. (2019) | Hareketlilik verisinde PageRank tabanlı sıralamanın gerçek tıkanıklıkla örtüşmesi |
| Zaharia vd. (2016) | Spark RDD soyutlaması, iteratif iş yüklerinde bellek içi avantaj |
| Xin vd. (2013) | GraphX, vertex-cut bölümleme, dağıtık PageRank |
| Zheng vd. (2014) | Kentsel hesaplama; çizge temsillerinin skaler sensöre üstünlüğü |

---

## Hafta 3–4 · Veri kaynağı ve teknoloji seçimi

**Tarih:** 2 Mart – 13 Mart 2026

### Veri seti kararı

NYC TLC Trip Record Data üç ölçüte göre seçildi:

| Ölçüt | Gerekçe |
|---|---|
| **Hacim** | Yılda ~38M kayıt — "big data" iddiasını gerçekten sınayacak ölçek |
| **Çeşitlilik** | Her kayıtta biniş/iniş zamanı, bölge ID'leri, süre, mesafe, yolcu sayısı |
| **Doğruluk** | Resmî olarak raporlanmış ve önceden anonimleştirilmiş — mahremiyet sorunu yok |

Ayrıca TLC, 263 bölge ID'sini adlandırılmış mahalle ve ilçelere eşleyen bir
shapefile yayımlıyor; bu, sonuçların anlamlı etiketlenmesini mümkün kıldı.

**İstanbul ikincil hedef olarak tutuldu.** İBB Açık Veri portalı için bir şema
eşleme dosyası (`config/sample_mapping_ibb.json`) ve tam bir İstanbul
konfigürasyonu (92 bölge, köprü ve tünel geçişleri dâhil) hazırlandı; ancak
dönem içinde gerçek İBB verisi bağlanmadı. Altyapı hazır durumdadır.

### Format kararı

Apache Parquet seçildi: sütunlu düzen, sıkıştırma ve predicate pushdown
desteği. Spark iş yüklerinde bu üçü doğrudan okuma süresine yansıyor. Ham CSV
karşılaştırması bunu doğruladı — aynı 12 aylık veri Parquet olarak 607 MB,
birleştirilmiş CSV olarak 2,9 GB yer kapladı.

### Hesaplama kararı (sonradan değişti)

Başlangıç planı Apache Spark + GraphX'ti; AWS EMR veya GCP Dataproc üzerinde
otomatik ölçeklenen worker node'larla. Konulan başarı ölçütü: 5 düğümlü
kümede 30M+ kayıtlık pipeline'ın 30 dakikanın altında tamamlanması.

Bu karar 10. haftada değişti — gerekçesi orada.

---

## Hafta 5–6 · Proposal

**Tarih:** 16 Mart – 27 Mart 2026 · **Çıktı:** Proje önerisi (5 sayfa)

Teslim edilen öneri: problem tanımı, dört başlıkta literatür taraması, veri
kaynağı ve sistem mimarisi, ETL tasarımı, çizge kurulumu, analitik teknikler
(PageRank, bağlı bileşenler, darboğaz simülasyonu), değerlendirme ölçütleri ve
beklenen çıktılar.

**Belirlenen üç hedef:**

1. Her bölgeyi trafiğe yapısal katkısına göre sıralamak
2. Bağlı bileşen analiziyle doğal trafik kümelerini bulmak
3. Kesinti senaryolarını simüle edip trafiğin nasıl yeniden dağıldığını ölçmek

**Doğrulama planı:** Tespit edilen darboğazlar, bilinen NYC tıkanıklık
noktalarıyla (Midtown Manhattan, JFK erişim yolları, Holland Tunnel yaklaşımı)
karşılaştırılacak; ayrıca modelin yalnızca en yüksek hacimli bölgeleri değil,
bariz olmayan yüksek kritiklikli düğümleri de bulup bulmadığına bakılacaktı.

Bu ikinci ölçüt sonradan projenin en değerli bulgusunu üretti — bkz. Hafta 14.

---

## Hafta 7–8 · Lokal prototip

**Tarih:** 30 Mart – 10 Nisan 2026

Gerçek veriyi indirmeden önce çalışan bir pipeline kurma yaklaşımı benimsendi.

### Şehirden bağımsız konfigürasyon katmanı

`config/city_config.py` — analiz kodunun hiçbir yerde şehir adı bilmemesi
tasarım kararıydı. Kod yalnızca bir `CityConfig` nesnesi alır: bölge adları,
ağırlıklar, koordinatlar, para birimi, ücret parametreleri.

Bu karar sonraki her şeyi kolaylaştırdı — NYC'ye geçiş, Ankara'nın referans
olarak eklenmesi ve `--config` ile koda dokunmadan JSON'dan şehir yüklenmesi
hep bu katman sayesinde mümkün oldu.

Hazırlanan konfigürasyonlar:

| Şehir | Bölge | Not |
|---|---|---|
| İstanbul | 92 | İlçe/semt bazlı; köprü girişleri, Avrasya Tüneli, metrobüs aktarma ve havalimanları ayrı düğüm olarak modellendi |
| NYC | 265 | TLC bölge listesinden |
| Ankara | 20 | Küçük ölçekli referans/test seti |

### Sentetik veri üreteci

`local_pipeline/generate_data.py` — gerçek veri olmadan pipeline'ı uçtan uca
sınamak için. Bölge çekim ağırlıkları, günlük saat profili (sabah 08 ve akşam
18 zirveli), lognormal süre dağılımı ve mesafe-süre korelasyonu içerir.

Tasarımın kritik detayı: üretici kasıtlı olarak **%3 oranında anomali enjekte
eder** — sıfır mesafe ve sıfır süre kayıtları. Bunun amacı ETL'in temizleme
adımını sınamaktı; gerçek veriye geçildiğinde filtrelerin doğru çalıştığından
emin olunabildi.

### Ana pipeline

`local_pipeline/traffic_analysis_generic.py` — altı adım: ETL → çizge →
PageRank → merkezilik → topluluk tespiti → darboğaz simülasyonu →
görselleştirme. Her adım bağımsız fonksiyon olarak yazıldı, böylece tek tek
test edilebildi.

**Alınan tasarım kararı:** Prototip Pandas + NetworkX ile yazıldı, Spark
sonraya bırakıldı. Amaç algoritmayı küçük veride doğrulamak, sonra ölçeklemekti.
Bu karar 10. haftada beklenmedik bir sonuca yol açtı.

---

## Hafta 9 · Midterm raporu

**Tarih:** 13 Nisan – 17 Nisan 2026 · **Çıktı:** Ara rapor (14 sayfa)

Rapor, veri hattının tam NYC TLC 2023 veri seti üzerinde uçtan uca
çalıştırıldığını belgeledi.

**Bildirilen sonuçlar** — tamamı 21 Ağustos 2026'da ham veriden yeniden
hesaplanarak doğrulanmıştır ([VERIFICATION.md](../VERIFICATION.md)):

| Metrik | Değer |
|---|---|
| İşlenen ham kayıt | 38.310.226 |
| Temizlenen | 909.157 (%2,37) |
| Düğüm / kenar | 258 / 9.990 |
| Grafa giren yolculuk | 35.420.777 |
| Yoğunluk | 0,1507 |
| Topluluk | 3 (Louvain: 182 / 44 / 32) |
| En kritik bölge | Upper East Side North (PageRank 0,02573) |
| 5 düğüm silme etkisi | %34,1 akış kaybı, 1 → 16 bileşen |
| Çalışma süresi | 40,7 sn (EC2 t3.xlarge) |

**Sonraki adımlar listesi:** zamansal analiz, PySpark/GraphFrames dağıtımı,
interaktif harita, çok şehirli karşılaştırma, bileşik dayanıklılık skoru.

---

## Hafta 10 · Gerçek veri hattı ve bulut dağıtımı

**Tarih:** 20 Nisan – 24 Nisan 2026 · **Kanıt:** git `a7ff801`, `32d55c9`

Projenin teknik olarak en yoğun dönemi. Dosya zaman damgaları 25 Nisan günü
**13:13–14:33 arasında kesintisiz bir seans** gösteriyor:

| Saat | İş |
|---|---|
| 13:13–13:29 | 12 aylık TLC Parquet dosyasının indirilmesi (607 MB) |
| 13:19 | `taxi_zone_lookup.csv` — 265 bölge adı |
| 13:51 | `build_nyc_centroids.py`; shapefile'dan 263 centroid üretimi |
| 13:59–14:00 | `convert_data.py` ile 12 ayın şema dönüşümü |
| 14:15 | `merge_months.py` |
| 14:23 | Birleşik dosya üretildi |
| 14:33 | `setup_and_run.sh` — 8 adımlı EC2 kurulum scripti |

### Çözülen üç teknik problem

**1. TLC şeması pipeline şemasıyla uyuşmuyor.** TLC dosyalarında
`tpep_pickup_datetime` / `tpep_dropoff_datetime` var, `trip_duration_minutes`
hiç yok. Çözüm sabit kodlama değil, `convert_data.py` oldu: bir eşleme JSON'u
alıp şemayı çeviriyor ve süreyi biniş/iniş farkından hesaplıyor. Böylece aynı
analiz kodu farklı şehirlerin verisiyle çalışabiliyor.

**2. Bölge koordinatları veri setinde yok.** `taxi_zone_lookup.csv` yalnızca ad
ve ilçe içeriyor; harita ve coğrafi görselleştirme için enlem/boylam gerekiyordu.
Koordinatlar TLC shapefile'ından üretildi.

Buradaki asıl detay projeksiyon seçimi: centroid `EPSG:2263` (NAD83 / New York
Long Island) projeksiyonunda hesaplanıp sonra `EPSG:4326`'ya çevriliyor.
WGS84 üzerinde doğrudan centroid almak, enlem ve boylam derecelerinin eşit
uzunlukta olmaması nedeniyle kaymalı sonuç verir.

**3. Pipeline tek dosya yolu alıyor, veri 12 parçalı.** `merge_months.py`
yazıldı; her dosyada 8 zorunlu sütunu doğrulayıp birleştiriyor.

### Mimari kararın değişmesi

Proposal'da hedeflenen AWS EMR yönetilen Spark kümesi yerine tek bir
**EC2 t3.xlarge** örneği (4 vCPU, 16 GB RAM, Ubuntu 24.04) kullanıldı ve analiz
Pandas + NetworkX ile çalıştırıldı.

Gerekçe ölçümdü: 38,3 milyon kaydın tek düğümde **40,7 saniyede** işlenebildiği
görüldü. Bu ölçekte küme kurulumunun maliyeti ve karmaşıklığı haklı
çıkmıyordu. Toplam bulut maliyeti 1 doların altında kaldı.

Sunucunun ayağa kaldırılması, ortam hazırlığı ve uçtan uca koşturma testleri
Kamil Duru tarafından yapıldı.

Bu, projenin en savunulabilir mühendislik kararlarından biridir: "büyük veri"
etiketinin otomatik olarak dağıtık altyapı gerektirmediğini ölçümle gösteriyor.
PySpark + GraphFrames pipeline'ı yine de yazıldı ve repoda duruyor — daha büyük
veri setleri için hazır, ancak bildirilen sonuçları üreten motor değil.

---

## Hafta 11 · Görselleştirme ve harita katmanı

**Tarih:** 27 Nisan – 1 Mayıs 2026

Sayısal sonuçlar hazırdı; bu hafta onları okunabilir hâle getirmeye ayrıldı.

### Altı statik görselleştirme

| Figür | Hangi soruyu cevaplıyor |
|---|---|
| `01_dashboard.png` | Dört panelde genel tablo: kritik bölgeler, simülasyon, PR-betweenness ilişkisi, topluluklar |
| `02_network_topology.png` | Ağ nasıl görünüyor? Havalimanları neden izole? |
| `03_simulation_detail.png` | Düğüm silindikçe kapasite, parçalanma ve verimlilik nasıl değişiyor? |
| `04_centrality_heatmap.png` | Dört metrik aynı bölgeler için ne diyor? Nerede ayrışıyorlar? |
| `05_distributions.png` | Ağ güç yasası yapısında mı? |
| `06_geo_map.png` | Topluluklar gerçek coğrafyayla örtüşüyor mu? |

Dashboard'daki **PageRank vs Betweenness** dağılım grafiği özellikle önemliydi:
projenin ana tezini — hacmin kritikliğe eşit olmadığını — tek bakışta gösteren
panel bu.

### Harita katmanı

`export_map_data.py` analiz sonucunu tek bir `map_data.json`'a yazar: düğümler
(PageRank, betweenness, akış, topluluk, tip), en yoğun 300 rota, simülasyon
adımları, topluluk özetleri ve genel istatistikler. Analiz ile sunum katmanının
ayrılması, haritanın pipeline'dan bağımsız geliştirilmesini sağladı.

`viewer.html` — Leaflet + OpenStreetMap tabanlı, **bağımlılıksız tek dosyalık**
görüntüleyici. Düğümler PageRank'a göre boyutlanır, bölge tipine göre renklenir
(kırmızı hotspot / mavi hub / yeşil normal). Ticari harita servisi yerine
Leaflet + OSM tercih edildi: ücretsiz, API anahtarı gerektirmiyor ve tek HTML
dosyasıyla çalışıyor.

`visualization/istanbul_traffic_map.jsx` React bileşeni olarak açıldı ancak
**doldurulmadı** — `viewer.html` ihtiyacı karşıladığı için gerek kalmadı.
Sunumdaki interaktif harita demosu `viewer.html` üzerinden gösterildi.

---

## Hafta 12–13 · Node2Vec / GNN eklentisi

**Tarih:** 4 Mayıs – 15 Mayıs 2026 · **Kanıt:** git `d774edc` (9 Mayıs)

Dersin 12-13. hafta konusu olan çizge gömme yöntemleri projeye uygulandı.

**`gnn_node2vec.py`** — her bölge için 128 boyutlu vektör üretir. Yöntem
(Grover & Leskovec, 2016): her düğümden yanlı rastgele yürüyüşler (10 yürüyüş ×
80 adım) → yürüyüşler "cümle", düğümler "kelime" → Word2Vec Skip-gram ile gömme
→ kosinüs benzerliğiyle bölge benzerlik matrisi.

**`gnn_visualize.py`** — 128 boyutlu gömmeleri t-SNE ile 2 boyuta indirip iki
panelli grafik çizer: solda topluluk renklendirmesi, sağda PageRank kritikliği.

**Kavramsal katkı:** PageRank "bu bölge kritik mi" sorusunu cevaplar; Node2Vec
"bu bölge hangi bölgelere benziyor" sorusunu ekler. İkisi farklı bilgi verir ve
birlikte kullanıldığında bölge tipolojisi çıkarılabilir.

**Dönem içinde açık kalan iş:** Modüller yazıldı ve çalışabilir durumdaydı,
ancak kaydedilmiş bir sonuç üretmediler. Final sunumunun 13. slaytındaki
benzerlik skorları slaytta da "Expected Answer (sanity check)" olarak
işaretlenmiş beklenti değerleridir, ölçüm değildi.

21 Ağustos 2026'da çalıştırıldı ve iki hata çıktı: modül tam graf yerine
`map_data.json`'daki 300 kenarı kullandığı için düğümlerin %84'ü izoleydi ve
gömmeler çöküyordu; ayrıca `node2vec` kütüphanesi düğüm kimliklerini float'a
çevirdiği için sözlük araması başarısız oluyordu. İkisi de giderildi, sonuçlar
aşağıdaki turda kayda geçti.

---

## Hafta 14 · Final sunumu

**Tarih:** 18 Mayıs – 22 Mayıs 2026 · **Çıktı:** 15 slaytlık sunum

**Kapsam:** problem ve matematiksel temel, sistem mimarisi, veri, ETL/MapReduce
paradigması, genel sonuçlar, dört panelli dashboard, coğrafi topluluk yapısı,
canlı harita demosu, darboğaz simülasyonu, merkezilik karşılaştırması, ağ
topolojisi, Node2Vec eklentisi, kabul edilen ödünler ve üç ana bulgu.

**Sunulan üç ana bulgu:**

1. Manhattan çekirdeğinde ikili hub — Upper East Side North ve Midtown Center
2. Hacim ile kritikliğin ayrışması — JFK PageRank'ta 4., köprü rolünde çok daha
   yukarıda
3. Üç doğal coğrafi topluluk — algoritma şehri tanımadan gerçek bölgeleri buldu

Proposal'da konulan "bariz olmayan yüksek kritiklikli düğümleri bulabiliyor
mu" ölçütü burada karşılığını buldu: **East Harlem South**, PageRank
sıralamasında 17. olmasına rağmen en yüksek betweenness merkeziliğine sahip.
Hacmi ortalama, geçiş rolü en yüksek — tam olarak sensör tabanlı sistemlerin
göremeyeceği türden bir kritiklik.

> **Sunumda düzeltilmesi gereken sayılar var.** Ham veriyle doğrulanamayan
> değerler: 35,9M yolculuk (gerçek: 35,4M), 10.368 rota (gerçek: 9.990), 10 GB
> Parquet (gerçek: 607 MB) ve motorun PySpark + GraphFrames olarak sunulması
> (gerçekte Pandas + NetworkX). Ayrıca "JFK betweenness = 1,00, 1. sıra"
> iddiası kararsız bir hesaplamadan geliyordu; düzeltilmiş değer 0,96 ve
> 2. sıra. Tam liste: [VERIFICATION.md](../VERIFICATION.md).

---

## Doğrulama ve dokümantasyon turu

**Tarih:** 21 Ağustos 2026

Dönem sonrası yapılan denetim ve toparlama çalışması.

**1. Bildirilen tüm sayılar ham veriyle yeniden hesaplandı.** 12 aylık Parquet
dosyası pipeline'dan bağımsız bir betikle yeniden işlendi, sonra pipeline'ın
kendisi çalıştırıldı. Midterm raporunun her sayısı beşinci ondalığa kadar
doğrulandı. Final sunumundaki dört sayı ve motor iddiası doğrulanamadı.

**2. Betweenness kararsızlığı bulundu ve düzeltildi.** `compute_centrality()`
fonksiyonu `k = min(100, N)` örneklemesiyle ve sabit tohum verilmeden
çalışıyordu — yani rastgele örneklemeli bir yaklaşım. Aynı graf üzerinde
koşudan koşuya farklı sonuç üretiyordu: JFK'nin normalize değeri 0,82–1,00
arasında oynuyor, 1. sıra JFK ile East Harlem South arasında değişiyordu.
Ara rapor ile final sunumu arasındaki farkın (0,74 → 1,00) kaynağı buydu.
1000 düğüme kadar tam hesap yapacak şekilde değiştirildi; sonuç artık
deterministik.

**3. `setup_and_run.sh` uçtan uca çalışır hâle getirildi.** Script 7. adımda
`--data` bayrağını geçiyordu ama bayrak çalışma kopyasından silinmişti;
8. adımda `export_map_data.py --data` çağrılıyordu ama o bayrak hiç
tanımlanmamıştı. Her ikisi de giderildi.

**4. NYC gerçek koşusu tekrarlandı ve çıktıları repoya alındı.** Altı figür ve
`map_data.json` artık `results/new_york_city/` altında versiyon kontrolünde —
rapordaki şekiller kaynağıyla birlikte geliyor.

**5. Güvenlik:** Çalışma dizininde şifrelenmemiş bir RSA private key
(`traffic-key.pem`) `.gitignore` kapsamı dışında duruyordu. Eklendi; depo
geçmişine hiç girmemiştir.

**6. Dokümantasyon yazıldı:** [MODULES.md](../MODULES.md),
[FINAL_REPORT.md](../FINAL_REPORT.md), [VERIFICATION.md](../VERIFICATION.md)
ve bu belge.

**7. İnteraktif harita yapıldı ve yayınlandı.** `visualization/viewer.html`
depoyu klonlayan herkes için 404 veriyordu — `.gitignore` kapsamındaki bir
dosyayı okuyordu. Yerine sunumda gösterilen arayüzü gerçekten uygulayan bir
harita yazıldı: istatistik paneli, rota ve bölge filtreleri, topluluk
renklendirme anahtarı, tıklanabilir top-15 listesi ve **adım adım darboğaz
simülasyonu**. JFK çıkarıldığında bileşen sayacının 1'den 16'ya sıçraması artık
canlı izlenebiliyor. GitHub Pages'te yayında:
https://byomer1021.github.io/smart-city-traffic-analysis/

**8. Node2Vec çalıştırıldı.** Modüle `--data` bayrağı eklendi (tam graf
üzerinde eğitim) ve düğüm kimliği tip hatası giderildi. Sonuç, sunumdaki
beklentiyi doğrulamadı: JFK'ye en benzer bölgeler havalimanları değil, Queens'in
dış mahalleleri çıktı (Ozone Park 0,842); LaGuardia 258 bölge içinde 13. sırada.
Node2Vec işlevsel değil konumsal benzerlik ölçtüğü için bu tutarlıdır ve
parçalanma bulgusunu bağımsız olarak destekler. Eksik olan
`07_gnn_embeddings.png` figürü üretildi.
