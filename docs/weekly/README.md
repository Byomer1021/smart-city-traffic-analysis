# Haftalık İlerleme Raporu

CSE458 — Introduction to Big Data Analytics · 2026 Bahar
Ömer Can Atlı, Kamil Duru

---

## Bu belgenin durumu

Aşağıdaki kayıtların bir kısmı **kanıta dayalıdır** (git geçmişi, dosya
zaman damgaları, teslim edilmiş belgeler ve yeniden çalıştırılıp doğrulanmış
sonuçlar). Bir kısmı ise **doldurulmayı beklemektedir** — bunlar
`[DOLDURULACAK]` ile işaretlidir.

Hafta numaraları ve tarihler, elimizdeki sabit noktalardan geriye doğru
türetilmiştir ve **teyit edilmelidir**:

| Sabit nokta | Kaynak |
|---|---|
| Proposal teslimi — Mart 2026 | `Ömer Can ATLI-Kamil DURU.pdf` kapak sayfası |
| Midterm raporu — Nisan 2026 | `midterm_report.pdf` kapak sayfası |
| NYC veri hattı + EC2 scripti — 25 Nisan 2026 | git commit `a7ff801`, `32d55c9` |
| Node2Vec eklentisi — 9 Mayıs 2026 | git commit `d774edc` |
| "Node2Vec = Hafta 12-13 ders konusu" | Final sunumu, slayt 13 |

Son iki satır birlikte okunduğunda dönemin 16 Şubat 2026 haftasında başladığı
sonucu çıkmaktadır. Ders takvimi farklıysa aşağıdaki hafta numaraları
kaydırılmalıdır.

---

## Özet tablo

| Hafta | Tarih | Aşama | Durum |
|---|---|---|---|
| 1–2 | 16 Şubat – 27 Şubat | Konu belirleme, literatür taraması | [DOLDURULACAK] |
| 3–4 | 2 Mart – 13 Mart | Veri kaynağı araştırması, teknoloji seçimi | [DOLDURULACAK] |
| 5–6 | 16 Mart – 27 Mart | **Proposal yazımı ve teslimi** | ✅ Teslim edildi |
| 7–8 | 30 Mart – 10 Nisan | Lokal prototip: sentetik veri + PageRank | ✅ Kodda mevcut |
| 9 | 13 Nisan – 17 Nisan | **Midterm raporu** | ✅ Teslim edildi |
| 10 | 20 Nisan – 24 Nisan | NYC gerçek verisine geçiş, EC2 dağıtımı | ✅ Doğrulandı |
| 11 | 27 Nisan – 1 Mayıs | Görselleştirme ve harita katmanı | [DOLDURULACAK] |
| 12–13 | 4 Mayıs – 15 Mayıs | **Node2Vec / GNN eklentisi** | ✅ Kodda mevcut |
| 14 | 18 Mayıs – 22 Mayıs | **Final sunumu** | ✅ Sunuldu |
| — | 21 Ağustos 2026 | Doğrulama ve dokümantasyon turu | ✅ Bu tur |

---

## Hafta 1–2 · Konu belirleme ve literatür taraması

**Tarih:** 16 Şubat – 27 Şubat 2026

[DOLDURULACAK] — Konunun nasıl seçildiği, değerlendirilen alternatif konular,
okunan ilk kaynaklar.

Proposal'daki literatür taramasından geriye bakıldığında bu dönemde
incelenen ana kaynaklar şunlardır:

- Leskovec, Rajaraman & Ullman (2020) — seyrek çizgelerde iteratif algoritmalar
- Brin & Page (1998) — PageRank'ın özgün tanımı
- Zaharia vd. (2016) — Spark RDD soyutlaması
- Xin vd. (2013) — GraphX ve vertex-cut bölümleme
- Zheng vd. (2014) — kentsel hesaplama derlemesi
- Wang vd. (2019) — hareketlilik verisinde PageRank tabanlı sıralama

---

## Hafta 3–4 · Veri kaynağı ve teknoloji seçimi

**Tarih:** 2 Mart – 13 Mart 2026

[DOLDURULACAK] — Hangi veri setleri değerlendirildi, NYC TLC neden seçildi,
İBB Açık Veri portalı neden ikincil kaldı.

Proposal'dan çıkarılabilen kararlar:

- **Veri:** NYC TLC Trip Record Data — yılda ~30-40M kayıt, kamuya açık,
  anonimleştirilmiş, kentsel analitikte standart kıyaslama seti
- **Format:** Apache Parquet — sütunlu düzen, sıkıştırma, predicate pushdown
- **Depolama:** bulut nesne depolama (S3/GCS), yıl-ay bazlı bölümleme
- **Hesaplama:** Apache Spark + GraphX, AWS EMR veya GCP Dataproc üzerinde

> **Not:** Bu son karar dönem içinde değişmiştir. Gerçekleşen mimari için
> Hafta 10 kaydına bakınız.

---

## Hafta 5–6 · Proposal

**Tarih:** 16 Mart – 27 Mart 2026 · **Çıktı:** Proje önerisi (5 sayfa)

Teslim edilen öneri şunları kapsıyordu: problem tanımı, dört başlıkta literatür
taraması, veri kaynağı ve sistem mimarisi, ETL tasarımı, çizge kurulumu,
analitik teknikler (PageRank, bağlı bileşenler, darboğaz simülasyonu),
değerlendirme ölçütleri ve beklenen çıktılar.

**Belirlenen hedefler:**

- Her bölgeyi trafiğe yapısal katkısına göre sıralamak
- Bağlı bileşen analiziyle doğal trafik kümelerini bulmak
- Kesinti senaryolarını simüle edip trafiğin nasıl yeniden dağıldığını ölçmek

**Konulan başarı ölçütü:** 5 düğümlü Spark kümesinde 30M+ kayıtlık pipeline'ın
30 dakikanın altında tamamlanması.

---

## Hafta 7–8 · Lokal prototip

**Tarih:** 30 Mart – 10 Nisan 2026

[DOLDURULACAK] — Bu haftalarda karşılaşılan somut problemler ve çözümleri.

Kod tabanından çıkarılabilen çalışma:

- `config/city_config.py` — şehirden bağımsız konfigürasyon katmanı. Analiz
  kodunun hiçbir yerde şehir adı bilmemesi, sonraki çok şehirli genişlemeyi
  mümkün kıldı.
- `local_pipeline/generate_data.py` — sentetik veri üreteci. Gerçek veri
  indirilmeden pipeline'ın uçtan uca denenebilmesi için. Bölge ağırlıkları,
  günlük saat profili, lognormal süre dağılımı ve kasıtlı anomali enjeksiyonu
  (%3) içerir — anomali enjeksiyonu ETL temizleme adımını sınamak içindir.
- `local_pipeline/traffic_analysis_generic.py` — altı adımlı ana pipeline:
  ETL → çizge → PageRank → merkezilik → topluluk → simülasyon → görselleştirme.
- İstanbul konfigürasyonu (92 bölge, köprü ve tünel geçişleri dâhil) ve
  Ankara konfigürasyonu (20 bölge) referans olarak eklendi.

**Alınan tasarım kararı:** Prototip Pandas + NetworkX ile yazıldı, Spark
sonraya bırakıldı. Bu karar sonradan belirleyici oldu — bkz. Hafta 10.

---

## Hafta 9 · Midterm raporu

**Tarih:** 13 Nisan – 17 Nisan 2026 · **Çıktı:** Ara rapor (14 sayfa)

Rapor, veri hattının tam NYC TLC 2023 veri seti üzerinde uçtan uca
çalıştırıldığını belgeledi.

**Bildirilen sonuçlar** (tamamı 21 Ağustos 2026'da yeniden hesaplanarak
doğrulanmıştır — bkz. [VERIFICATION.md](../VERIFICATION.md)):

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

**Rapordaki sonraki adımlar listesi:** zamansal analiz, PySpark/GraphFrames
dağıtımı, interaktif harita, çok şehirli karşılaştırma, bileşik dayanıklılık
skoru.

---

## Hafta 10 · Gerçek veri hattı ve bulut dağıtımı

**Tarih:** 20 Nisan – 24 Nisan 2026 · **Kanıt:** git `a7ff801`, `32d55c9` (25 Nisan)

Bu hafta projenin teknik olarak en yoğun dönemidir. Dosya zaman damgaları
25 Nisan günü 13:13–14:33 arasında kesintisiz bir çalışma seansı göstermektedir.

**Yapılan işler:**

| Saat | İş |
|---|---|
| 13:13–13:29 | 12 aylık TLC Parquet dosyasının indirilmesi (607 MB) |
| 13:19 | `taxi_zone_lookup.csv` — 265 bölge adı |
| 13:51 | `build_nyc_centroids.py` yazıldı; shapefile'dan 263 centroid üretildi |
| 13:59–14:00 | `convert_data.py` ile 12 ayın şema dönüşümü |
| 14:15 | `merge_months.py` yazıldı |
| 14:23 | Birleşik dosya üretildi (2,9 GB CSV) |
| 14:33 | `setup_and_run.sh` — 8 adımlı EC2 kurulum scripti |

**Çözülen teknik problemler:**

1. **TLC şeması pipeline şemasıyla uyuşmuyor.** TLC dosyalarında
   `tpep_pickup_datetime` / `tpep_dropoff_datetime` var, `trip_duration_minutes`
   yok. `convert_data.py` bir eşleme JSON'u alarak şemayı çeviriyor ve süreyi
   biniş/iniş farkından hesaplıyor. Bu modül sayesinde aynı analiz kodu farklı
   şehirlerin verisiyle çalışabiliyor.

2. **Bölge koordinatları veri setinde yok.** `taxi_zone_lookup.csv` yalnızca ad
   ve ilçe içeriyor. Koordinatlar TLC shapefile'ından üretildi. Centroid hesabı
   `EPSG:2263` projeksiyonunda yapıldı, sonra `EPSG:4326`'ya çevrildi — WGS84
   üzerinde doğrudan centroid almak kaymalı sonuç verir.

3. **Pipeline tek dosya yolu alıyor, veri 12 parçalı.** `merge_months.py`
   yazıldı; her dosyada 8 zorunlu sütunu doğrulayıp birleştiriyor.

**Mimari kararın değişmesi.** Proposal'da hedeflenen AWS EMR yönetilen Spark
kümesi yerine tek bir **EC2 t3.xlarge** (4 vCPU, 16 GB RAM) örneği kullanıldı ve
analiz Pandas + NetworkX ile çalıştırıldı. Gerekçe: 38,3M kaydın tek düğümde
40 saniyenin altında işlenebildiği görüldü; küme maliyeti ve kurulum karmaşıklığı
bu ölçekte haklı çıkmıyordu. Toplam bulut maliyeti 1 doların altında kaldı.

Bu, projenin en savunulabilir mühendislik kararlarından biridir ve raporda
böyle sunulmalıdır.

---

## Hafta 11 · Görselleştirme ve harita katmanı

**Tarih:** 27 Nisan – 1 Mayıs 2026

[DOLDURULACAK] — Bu haftanın çalışma detayları.

Kod tabanından çıkarılabilen çıktılar:

- `local_pipeline/export_map_data.py` — analiz sonucunu tek bir
  `map_data.json` dosyasına yazar: düğümler (PageRank, betweenness, akış,
  topluluk, tip), en yoğun 300 rota, simülasyon adımları, topluluk özetleri
  ve genel istatistikler.
- `visualization/viewer.html` — Leaflet + OpenStreetMap tabanlı, bağımlılıksız
  tek dosyalık harita görüntüleyici. Düğümler PageRank'a göre boyutlanır,
  bölge tipine göre renklenir (hotspot / hub / normal).
- Altı adet matplotlib görselleştirmesi: dashboard, ağ topolojisi, simülasyon
  detayı, merkezilik ısı haritası, dağılımlar, coğrafi harita.

> `visualization/istanbul_traffic_map.jsx` bu dönemde açılmış ancak
> **doldurulmamıştır** — hâlâ iki satırlık yer tutucudur. Sunumdaki interaktif
> harita `viewer.html` üzerinden gösterilmiştir.

---

## Hafta 12–13 · Node2Vec / GNN eklentisi

**Tarih:** 4 Mayıs – 15 Mayıs 2026 · **Kanıt:** git `d774edc` (9 Mayıs)

Dersin 12-13. hafta konusu olan çizge gömme yöntemleri projeye uygulandı.

**Eklenen modüller:**

- `local_pipeline/gnn_node2vec.py` — her bölge için 128 boyutlu vektör üretir.
  Yöntem (Grover & Leskovec, 2016): her düğümden yanlı rastgele yürüyüşler
  (10 yürüyüş × 80 adım) → yürüyüşler "cümle", düğümler "kelime" → Word2Vec
  Skip-gram ile gömme → kosinüs benzerliğiyle bölge benzerlik matrisi.
- `local_pipeline/gnn_visualize.py` — 128 boyutlu gömmeleri t-SNE ile 2 boyuta
  indirip iki panelli grafik çizer.

**Kavramsal katkı:** PageRank "bu bölge kritik mi" sorusunu cevaplar; Node2Vec
"bu bölge hangi bölgelere benziyor" sorusunu ekler. İkisi farklı bilgi verir.

**Açık kalan iş:** Bu modüller çalıştırılmış bir sonuç üretmemiştir — repoda
`embeddings.npz` yoktur. Final sunumunun 13. slaytındaki benzerlik skorları
(LaGuardia 0,88, Penn Station 0,71) ölçüm değil, slaytta da "Expected Answer
(sanity check)" olarak işaretlenmiş beklenti değerleridir. Ayrıca modül tam
graf yerine `map_data.json` içindeki en yoğun 300 kenarı kullanır.

---

## Hafta 14 · Final sunumu

**Tarih:** 18 Mayıs – 22 Mayıs 2026 · **Çıktı:** 15 slaytlık sunum

**Kapsam:** problem ve matematiksel temel, sistem mimarisi, veri, ETL/MapReduce
paradigması, genel sonuçlar, dört panelli dashboard, coğrafi topluluk yapısı,
canlı harita demosu, darboğaz simülasyonu, merkezilik karşılaştırması, ağ
topolojisi, Node2Vec eklentisi, kabul edilen ödünler ve üç ana bulgu.

**Sunulan üç ana bulgu:**

1. Manhattan çekirdeğinde ikili hub — Upper East Side North ve Midtown Center
2. "JFK Paradoksu" — PageRank'ta 4., betweenness'ta 1. sıra
3. Üç doğal coğrafi topluluk — algoritma şehri tanımadan gerçek bölgeleri buldu

> **Düzeltme gerektiren noktalar.** Sunumdaki bazı sayılar ham veriyle
> doğrulanamamıştır: 35,9M yolculuk (gerçek: 35,4M), 10.368 rota (gerçek:
> 9.990), 10 GB Parquet (gerçek: 607 MB) ve motorun PySpark + GraphFrames
> olarak sunulması (gerçekte Pandas + NetworkX). Ayrıca "JFK betweenness = 1,00,
> 1. sıra" iddiası kararsız bir hesaplamadan gelmektedir; düzeltilmiş değer
> 0,96 ve 2. sıradır. Tam liste ve düzeltme önerileri:
> [VERIFICATION.md](../VERIFICATION.md).

---

## Doğrulama ve dokümantasyon turu

**Tarih:** 21 Ağustos 2026

Dönem sonrası yapılan denetim ve toparlama çalışması.

**Yapılanlar:**

1. **Bildirilen tüm sayılar ham veriyle yeniden hesaplandı.** Midterm
   raporunun her sayısı beşinci ondalığa kadar doğrulandı. Final sunumundaki
   dört sayı ve motor iddiası doğrulanamadı.

2. **Betweenness kararsızlığı bulundu ve düzeltildi.** `compute_centrality()`
   fonksiyonu `k = min(100, N)` örneklemesiyle ve sabit tohum verilmeden
   çalışıyordu; aynı graf üzerinde koşudan koşuya farklı sonuç üretiyordu.
   Midterm ile sunum arasındaki JFK farkının (0,74 → 1,00) kaynağı buydu.
   1000 düğüme kadar tam hesap yapacak şekilde değiştirildi; sonuç artık
   deterministiktir.

3. **`setup_and_run.sh` uçtan uca çalışır hâle getirildi.** Script 7. adımda
   `--data` bayrağını geçiyordu ama bayrak çalışma kopyasından silinmişti;
   8. adımda `export_map_data.py --data` çağrılıyordu ama o bayrak hiç
   tanımlanmamıştı. Her ikisi de giderildi.

4. **NYC gerçek koşusu tekrarlandı ve çıktıları repoya alındı.** Altı figür ve
   `map_data.json`, `results/new_york_city/` altında artık versiyon kontrolünde.
   Böylece rapordaki şekiller kaynağıyla birlikte geliyor.

5. **Güvenlik:** Çalışma dizininde şifrelenmemiş bir RSA private key
   (`traffic-key.pem`) `.gitignore` kapsamı dışında duruyordu. `.gitignore`'a
   eklendi. Depo geçmişine hiç girmemiştir. Bu anahtar EC2 örneğine aitse
   döndürülmesi önerilir.

6. **Dokümantasyon yazıldı:** [MODULES.md](../MODULES.md),
   [FINAL_REPORT.md](../FINAL_REPORT.md), [VERIFICATION.md](../VERIFICATION.md)
   ve bu belge.

---

## Doldurulmayı bekleyen bölümler

Aşağıdaki başlıklar için ders notları, haftalık toplantı kayıtları veya
kişisel notlar gerekiyor:

- [ ] Hafta 1–2 · Konu seçim süreci ve değerlendirilen alternatifler
- [ ] Hafta 3–4 · Veri seti ve teknoloji karşılaştırması, karar gerekçeleri
- [ ] Hafta 7–8 · Prototip aşamasında karşılaşılan somut hatalar ve çözümleri
- [ ] Hafta 11 · Görselleştirme kararları, denenip vazgeçilen yaklaşımlar
- [ ] Tüm haftalar · İki kişi arasındaki iş bölümü
- [ ] Tüm haftalar · Danışman geri bildirimleri ve bunların projeye etkisi
- [ ] Ders takviminin teyidi (dönem başlangıç tarihi, hafta numaralandırması)
