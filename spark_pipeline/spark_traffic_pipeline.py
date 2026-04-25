"""
╔══════════════════════════════════════════════════════════════════════╗
║  Akıllı Şehir Trafik Darboğazı Tespiti — PySpark + GraphFrames    ║
║  Full Production Pipeline (Pandas/NetworkX → Spark/GraphFrames)    ║
╚══════════════════════════════════════════════════════════════════════╝

Bu script, lokal Pandas/NetworkX pipeline'ının birebir PySpark/GraphFrames
dönüşümüdür. Her adım aşağıdaki dönüşüm tablosuna göre yapılmıştır:

┌─────────────────────┬──────────────────────┬───────────────────────────┐
│ İŞLEM               │ ÖNCE (Lokal)         │ SONRA (Dağıtık)           │
├─────────────────────┼──────────────────────┼───────────────────────────┤
│ Veri Okuma          │ pd.read_csv()        │ spark.read.parquet()      │
│ Filtreleme          │ df[df["col"] > 0]    │ df.filter(F.col() > 0)   │
│ Gruplama            │ df.groupby().agg()   │ df.groupBy().agg()       │
│ Çizge Oluşturma     │ nx.DiGraph()         │ GraphFrame(v, e)         │
│ PageRank            │ nx.pagerank()        │ graph.pageRank()         │
│ Connected Comp.     │ nx.connected_comp()  │ graph.connectedComp()    │
│ Topluluk Tespiti    │ nx.louvain_comm()    │ graph.labelPropagation() │
│ Görselleştirme      │ matplotlib (lokal)   │ .toPandas() → matplotlib │
└─────────────────────┴──────────────────────┴───────────────────────────┘

Çalıştırma:
  # AWS EMR
  spark-submit \\
      --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \\
      --master yarn --deploy-mode cluster \\
      --num-executors 10 --executor-memory 8g --executor-cores 4 \\
      spark_traffic_pipeline.py \\
      --city istanbul \\
      --input s3://bucket/istanbul-taxi/*.parquet \\
      --output s3://bucket/results/

  # Google Dataproc
  gcloud dataproc jobs submit pyspark \\
      --cluster=traffic-cluster --region=us-central1 \\
      --jars=gs://spark-lib/graphframes-0.8.3.jar \\
      spark_traffic_pipeline.py \\
      -- --city istanbul \\
         --input gs://bucket/istanbul-taxi/*.parquet \\
         --output gs://bucket/results/

  # Lokal Test (standalone mode)
  spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 \\
      spark_traffic_pipeline.py \\
      --city istanbul --input ./data/*.parquet --output ./results/
"""

import argparse
import logging
import time
import json
import os
from typing import Dict, List, Tuple, Optional

# ═══════════════════════════════════════════════════════════════════════
# PySpark İmportları
# ═══════════════════════════════════════════════════════════════════════
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, FloatType, LongType
)

# GraphFrames — Spark'ın dağıtık çizge kütüphanesi
# (spark-submit --packages ile yüklenmeli)
from graphframes import GraphFrame

# Görselleştirme (driver node'da çalışır)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

# ═══════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SparkTrafficPipeline")

# ═══════════════════════════════════════════════════════════════════════
# RENK PALETİ (Görselleştirme)
# ═══════════════════════════════════════════════════════════════════════
COLORS = {
    "primary": "#1a73e8", "danger": "#dc3545", "warning": "#ffc107",
    "success": "#28a745", "dark": "#2c3e50", "light_bg": "#f8f9fa",
    "grid": "#e9ecef",
}


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ŞEHİR KONFİGÜRASYONU (city_config.py'den taşındı)               ║
# ╚══════════════════════════════════════════════════════════════════════╝
# Spark ortamında harici modül importu sorunlu olabileceği için
# config'ler bu dosyada inline tanımlı. Üretimde ayrı paket olarak
# dağıtılabilir (--py-files city_config.py).

CITY_CONFIGS = {
    "istanbul": {
        "name": "İstanbul", "country": "Türkiye", "currency": "TRY",
        "zones": {
            1: "Fatih / Sultanahmet", 2: "Fatih / Eminönü",
            3: "Fatih / Aksaray", 4: "Fatih / Laleli",
            5: "Beyoğlu / Taksim", 6: "Beyoğlu / Galata",
            7: "Beyoğlu / Karaköy", 8: "Beyoğlu / İstiklal Cad.",
            9: "Beşiktaş / Beşiktaş Merkez", 10: "Beşiktaş / Levent",
            11: "Beşiktaş / Etiler", 12: "Beşiktaş / Bebek",
            13: "Şişli / Mecidiyeköy", 14: "Şişli / Nişantaşı",
            15: "Şişli / Maslak", 16: "Şişli / Fulya",
            17: "Kağıthane / Cendere", 18: "Kağıthane / Merkez",
            19: "Eyüpsultan / Eyüp Merkez", 20: "Eyüpsultan / Alibeyköy",
            21: "Sarıyer / İstinye", 22: "Sarıyer / Maslak",
            23: "Sarıyer / Tarabya", 24: "Sarıyer / Rumelihisarı",
            25: "Bakırköy / Bakırköy Merkez", 26: "Bakırköy / Ataköy",
            27: "Bakırköy / Yeşilköy", 28: "Bahçelievler / Merkez",
            29: "Bahçelievler / Yenibosna", 30: "Bağcılar / Merkez",
            31: "Bağcılar / Güneşli", 32: "Esenler / Merkez",
            33: "Güngören / Merkez", 34: "Zeytinburnu / Merkez",
            35: "Bayrampaşa / Merkez", 36: "Küçükçekmece / Halkalı",
            37: "Küçükçekmece / Atakent", 38: "Avcılar / Merkez",
            39: "Avcılar / Ambarlı", 40: "Başakşehir / Merkez",
            41: "Başakşehir / İkitelli", 42: "Esenyurt / Merkez",
            43: "Beylikdüzü / Merkez", 44: "Büyükçekmece / Merkez",
            45: "Arnavutköy / Merkez", 46: "Sultangazi / Merkez",
            47: "Gaziosmanpaşa / Merkez",
            50: "Kadıköy / Kadıköy Merkez", 51: "Kadıköy / Moda",
            52: "Kadıköy / Bostancı", 53: "Kadıköy / Fenerbahçe",
            54: "Kadıköy / Kozyatağı", 55: "Üsküdar / Üsküdar Merkez",
            56: "Üsküdar / Çengelköy", 57: "Üsküdar / Kısıklı",
            58: "Ataşehir / Ataşehir Merkez", 59: "Ataşehir / İçerenköy",
            60: "Ataşehir / Ünalan", 61: "Maltepe / Merkez",
            62: "Maltepe / Cevizli", 63: "Kartal / Merkez",
            64: "Kartal / Soğanlık", 65: "Pendik / Merkez",
            66: "Pendik / Kurtköy", 67: "Tuzla / Merkez",
            68: "Sultanbeyli / Merkez", 69: "Sancaktepe / Merkez",
            70: "Ümraniye / Ümraniye Merkez", 71: "Ümraniye / Çakmak",
            72: "Beykoz / Merkez", 73: "Beykoz / Kavacık",
            74: "Çekmeköy / Merkez", 75: "Şile / Merkez",
            80: "İstanbul Havalimanı (IST)", 81: "Sabiha Gökçen (SAW)",
            82: "Marmaray / Sirkeci", 83: "Marmaray / Üsküdar",
            84: "Yenikapı İDO", 85: "Kadıköy İDO",
            86: "Harem Otogarı", 87: "Esenler Otogarı",
            88: "Söğütlüçeşme Marmaray", 89: "Mecidiyeköy Metrobüs",
            90: "Zincirlikuyu Metrobüs",
            91: "15 Temmuz Köprüsü (Avrupa)", 92: "15 Temmuz Köprüsü (Anadolu)",
            93: "FSM Köprüsü (Avrupa)", 94: "FSM Köprüsü (Anadolu)",
            95: "Yavuz Sultan Selim (Avrupa)", 96: "Yavuz Sultan Selim (Anadolu)",
            97: "Avrasya Tüneli (Avrupa)", 98: "Avrasya Tüneli (Anadolu)",
        },
        "coordinates": {
            1: (41.0082, 28.9784), 2: (41.0166, 28.9696),
            3: (41.0097, 28.9509), 4: (41.0104, 28.9558),
            5: (41.0370, 28.9850), 6: (41.0256, 28.9741),
            7: (41.0215, 28.9730), 8: (41.0337, 28.9784),
            9: (41.0430, 29.0070), 10: (41.0810, 29.0100),
            11: (41.0801, 29.0301), 12: (41.0730, 29.0440),
            13: (41.0625, 28.9910), 14: (41.0480, 28.9930),
            15: (41.1110, 29.0200), 16: (41.0550, 29.0100),
            17: (41.0780, 28.9720), 18: (41.0750, 28.9650),
            19: (41.0480, 28.9340), 20: (41.0650, 28.9450),
            21: (41.1080, 29.0570), 22: (41.1100, 29.0200),
            23: (41.1320, 29.0580), 24: (41.0850, 29.0550),
            25: (40.9810, 28.8770), 26: (40.9680, 28.8550),
            27: (40.9580, 28.8200), 28: (41.0000, 28.8630),
            29: (40.9920, 28.8350), 30: (41.0360, 28.8560),
            31: (41.0200, 28.8700), 32: (41.0430, 28.8760),
            33: (41.0150, 28.8880), 34: (41.0050, 28.9100),
            35: (41.0420, 28.9120), 36: (41.0210, 28.7870),
            37: (41.0300, 28.7650), 38: (40.9800, 28.7220),
            39: (40.9700, 28.6930), 40: (41.0920, 28.8000),
            41: (41.0600, 28.8200), 42: (41.0320, 28.6830),
            43: (41.0050, 28.6400), 44: (41.0240, 28.5880),
            45: (41.1850, 28.7400), 46: (41.1050, 28.8670),
            47: (41.0650, 28.9170),
            50: (40.9905, 29.0290), 51: (40.9850, 29.0250),
            52: (40.9590, 29.0670), 53: (40.9710, 29.0370),
            54: (40.9770, 29.0700), 55: (41.0250, 29.0153),
            56: (41.0470, 29.0530), 57: (41.0350, 29.0400),
            58: (40.9900, 29.1100), 59: (40.9730, 29.1000),
            60: (40.9960, 29.0900), 61: (40.9350, 29.1300),
            62: (40.9200, 29.1400), 63: (40.8900, 29.1900),
            64: (40.8850, 29.2000), 65: (40.8750, 29.2500),
            66: (40.9050, 29.3050), 67: (40.8200, 29.3000),
            68: (40.9650, 29.2700), 69: (41.0000, 29.2300),
            70: (41.0280, 29.0900), 71: (41.0200, 29.1000),
            72: (41.0900, 29.1000), 73: (41.1050, 29.0750),
            74: (41.0400, 29.1800), 75: (41.1760, 29.6120),
            80: (41.2600, 28.7420), 81: (40.8986, 29.3092),
            82: (41.0160, 28.9690), 83: (41.0250, 29.0160),
            84: (41.0050, 28.9520), 85: (40.9920, 29.0240),
            86: (41.0050, 29.0250), 87: (41.0420, 28.8840),
            88: (40.9920, 29.0350), 89: (41.0630, 28.9900),
            90: (41.0670, 29.0100),
            91: (41.0450, 29.0200), 92: (41.0500, 29.0350),
            93: (41.0900, 29.0500), 94: (41.0950, 29.0650),
            95: (41.2050, 29.1200), 96: (41.2100, 29.1350),
            97: (41.0100, 28.9580), 98: (41.0030, 29.0200),
        },
        "hotspots": [5, 6, 7, 8, 9, 10, 13, 14, 1, 2, 3, 4, 50, 51, 55,
                     91, 92, 93, 94, 97, 98, 89, 90],
        "transport_hubs": [80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90],
    },
    "nyc": {
        "name": "New York City", "country": "USA", "currency": "USD",
        "zones": {
            231: "Times Sq/Theatre", 161: "Midtown North",
            162: "Midtown South", 163: "Midtown East",
            164: "Midtown West", 170: "Murray Hill",
            186: "Penn Station", 234: "Union Sq",
            236: "Upper East N", 237: "Upper East S",
            238: "Upper West N", 239: "Upper West S",
            132: "JFK Airport", 138: "LaGuardia Airport",
        },
        "coordinates": {
            231: (40.7580, -73.9855), 161: (40.7648, -73.9775),
            162: (40.7505, -73.9847), 163: (40.7547, -73.9696),
            164: (40.7590, -73.9892), 170: (40.7484, -73.9760),
            186: (40.7506, -73.9935), 234: (40.7359, -73.9911),
            236: (40.7773, -73.9558), 237: (40.7695, -73.9596),
            238: (40.7870, -73.9754), 239: (40.7800, -73.9793),
            132: (40.6413, -73.7781), 138: (40.7769, -73.8740),
        },
        "hotspots": [231, 161, 162, 163, 164, 170, 186, 234, 236, 237, 238, 239],
        "transport_hubs": [132, 138],
    },
}


def get_zone_name(city: str, zone_id: int) -> str:
    """Config'den bölge adı döndürür."""
    zones = CITY_CONFIGS.get(city, {}).get("zones", {})
    return zones.get(zone_id, f"Zone-{zone_id}")


def get_coordinates(city: str, zone_id: int) -> Optional[Tuple[float, float]]:
    """Config'den koordinat döndürür."""
    return CITY_CONFIGS.get(city, {}).get("coordinates", {}).get(zone_id)


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 0: SPARK OTURUMU                                             ║
# ╚══════════════════════════════════════════════════════════════════════╝
def create_spark_session(app_name: str = "TrafficBottleneckAnalysis") -> SparkSession:
    """
    Optimize edilmiş Spark oturumu.

    Üretim ayarları:
    ┌───────────────────────────────────────┬───────────────────────────┐
    │ Parametre                             │ Değer                     │
    ├───────────────────────────────────────┼───────────────────────────┤
    │ spark.sql.adaptive.enabled            │ true (AQE aktif)          │
    │ spark.sql.shuffle.partitions          │ 200 (büyük veri için)     │
    │ spark.serializer                      │ KryoSerializer            │
    │ spark.sql.parquet.compression.codec   │ snappy                    │
    │ spark.graphx.pregel.checkpointInterval│ 10                        │
    └───────────────────────────────────────┴───────────────────────────┘
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Bellek ve Paralelizm
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", "4")
        .config("spark.default.parallelism", "100")
        # AQE (Adaptive Query Execution) — Spark 3.x
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Shuffle & Sıkıştırma
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.mergeSchema", "false")
        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        # GraphX Checkpoint
        .config("spark.graphx.pregel.checkpointInterval", "10")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/spark-graphframes-checkpoints")
    logger.info(f"Spark {spark.version} oturumu oluşturuldu")
    return spark


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 1: VERİ OKUMA VE TEMİZLEME (ETL — MapReduce Paradigması)   ║
# ╚══════════════════════════════════════════════════════════════════════╝
def etl_pipeline(spark: SparkSession, input_path: str) -> Tuple[DataFrame, int]:
    """
    Dönüşüm Tablosu:
    ┌───────────────────────────┬──────────────────────────────────────┐
    │ Pandas (Önce)             │ PySpark (Sonra)                      │
    ├───────────────────────────┼──────────────────────────────────────┤
    │ pd.read_csv(path)         │ spark.read.parquet(path)             │
    │ df[df["col"] > 0]        │ df.filter(F.col("col") > 0)         │
    │ df.groupby(cols).agg()   │ df.groupBy(cols).agg()              │
    │ df[col1 != col2]         │ df.filter(F.col(c1) != F.col(c2))   │
    │ len(df)                  │ df.count()                           │
    └───────────────────────────┴──────────────────────────────────────┘
    """
    logger.info("═" * 60)
    logger.info("ADIM 1: ETL PIPELINE (Dağıtık MapReduce)")
    logger.info("═" * 60)

    t0 = time.time()

    # ──────────────────────────────────────────────────────────────────
    # MAP AŞAMASI: Parquet Okuma + Column Pruning + Predicate Pushdown
    # ──────────────────────────────────────────────────────────────────
    #
    # Pandas eşdeğeri:
    #   df = pd.read_csv(path, usecols=["pickup_datetime", ...])
    #
    # Spark avantajı: Parquet'in sütun tabanlı formatı sayesinde sadece
    # ihtiyaç duyulan sütunlar diskten okunur (Column Pruning). Ayrıca
    # filter koşulları doğrudan depolama katmanına itilir (Pushdown).
    # ──────────────────────────────────────────────────────────────────
    logger.info(f"  [MAP] Parquet okunuyor: {input_path}")

    raw_df = (
        spark.read.parquet(input_path)
        .select(
            F.col("pickup_datetime").cast("timestamp"),
            F.col("dropoff_datetime").cast("timestamp"),
            F.col("trip_distance").cast(DoubleType()),
            F.col("PULocationID").cast(IntegerType()),
            F.col("DOLocationID").cast(IntegerType()),
            F.col("fare_amount").cast(DoubleType()),
            F.col("passenger_count").cast(IntegerType()),
            F.col("trip_duration_minutes").cast(DoubleType()),
        )
    )

    raw_count = raw_df.count()
    logger.info(f"  Ham kayıt sayısı: {raw_count:,}")

    # ──────────────────────────────────────────────────────────────────
    # FILTER AŞAMASI: Anomali Temizleme
    # ──────────────────────────────────────────────────────────────────
    #
    # Pandas eşdeğeri:
    #   df = df[df["trip_distance"] > 0]
    #   df = df[df["trip_duration_minutes"] > 0]
    #   df = df[~((df["PU"] == df["DO"]) & (df["trip_distance"] < 0.1))]
    #
    # Spark: Lazy evaluation — tüm filter'lar tek bir DAG'da birleştirilir
    # ve Catalyst optimizer tarafından fiziksel plana dönüştürülür.
    # ──────────────────────────────────────────────────────────────────
    logger.info("  [FILTER] Anomaliler filtreleniyor...")

    clean_df = (
        raw_df
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_duration_minutes") > 0)
        .filter(F.col("trip_duration_minutes") <= 300)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        # Self-loop ultra-short trip filtresi
        .filter(
            ~(
                (F.col("PULocationID") == F.col("DOLocationID")) &
                (F.col("trip_distance") < 0.1)
            )
        )
    )

    clean_count = clean_df.count()
    removed = raw_count - clean_count
    logger.info(f"  Temizlenen: {removed:,} ({removed/max(raw_count,1)*100:.1f}%)")
    logger.info(f"  Kalan: {clean_count:,}")

    # ──────────────────────────────────────────────────────────────────
    # REDUCE AŞAMASI: GroupBy + Aggregation
    # ──────────────────────────────────────────────────────────────────
    #
    # Pandas eşdeğeri:
    #   edge_df = df.groupby(["PULocationID", "DOLocationID"]).agg(
    #       trip_count=("trip_distance", "count"),
    #       avg_duration=("trip_duration_minutes", "mean"),
    #       ...
    #   )
    #
    # Spark: Shuffle-based aggregation. AQE (Adaptive Query Execution)
    # otomatik olarak partition sayısını ve join stratejisini optimize eder.
    # ──────────────────────────────────────────────────────────────────
    logger.info("  [REDUCE] Bölge çiftleri gruplanıyor...")

    edge_df = (
        clean_df
        # Self-loop kenarları çıkar
        .filter(F.col("PULocationID") != F.col("DOLocationID"))
        # GroupBy + Aggregation (MapReduce'un Reduce aşaması)
        .groupBy("PULocationID", "DOLocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_minutes").alias("avg_duration"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("fare_amount").alias("total_fare"),
            F.avg("fare_amount").alias("avg_fare"),
        )
        # Minimum yolculuk eşiği (gürültü azaltma)
        .filter(F.col("trip_count") >= 50)
        # Tekrar kullanılacağı için cache'le
        .cache()
    )

    edge_count = edge_df.count()  # Cache'i tetikle (action)
    elapsed = time.time() - t0

    logger.info(f"  ─ ETL tamamlandı ({elapsed:.1f}s)")
    logger.info(f"  ─ Benzersiz rota (kenar): {edge_count:,}")
    logger.info(f"  ─ Throughput: {raw_count/max(elapsed,0.1):,.0f} kayıt/s")
    logger.info(f"  ─ Latency: {elapsed:.2f}s")

    return edge_df, raw_count


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 2: ÇİZGE İNŞASI (GraphFrame Construction)                   ║
# ╚══════════════════════════════════════════════════════════════════════╝
def build_graph(spark: SparkSession, edge_df: DataFrame, city: str) -> GraphFrame:
    """
    Dönüşüm:
    ┌──────────────────────────────┬──────────────────────────────────────┐
    │ NetworkX (Önce)              │ GraphFrame (Sonra)                   │
    ├──────────────────────────────┼──────────────────────────────────────┤
    │ G = nx.DiGraph()             │ graph = GraphFrame(vertices, edges)  │
    │ G.add_node(id, name=...)     │ vertices = DataFrame("id", "name")  │
    │ G.add_edge(u, v, weight=...) │ edges = DataFrame("src","dst",...)   │
    │ G.number_of_nodes()          │ graph.vertices.count()              │
    │ G.number_of_edges()          │ graph.edges.count()                 │
    └──────────────────────────────┴──────────────────────────────────────┘
    """
    logger.info("═" * 60)
    logger.info("ADIM 2: ÇİZGE İNŞASI (GraphFrame)")
    logger.info("═" * 60)

    zones = CITY_CONFIGS.get(city, {}).get("zones", {})
    coords = CITY_CONFIGS.get(city, {}).get("coordinates", {})

    # ── Vertices (Düğümler) DataFrame ──
    # NetworkX: for n in all_nodes: G.add_node(n, name=get_zone_name(n))
    # Spark: Union ile tüm benzersiz düğümleri topla
    pu_nodes = edge_df.select(F.col("PULocationID").alias("id")).distinct()
    do_nodes = edge_df.select(F.col("DOLocationID").alias("id")).distinct()
    vertex_ids = pu_nodes.union(do_nodes).distinct()

    # Zone isimlerini ve koordinatları broadcast join ile ekle
    zone_data = [
        (int(k), v, coords.get(k, (0.0, 0.0))[0], coords.get(k, (0.0, 0.0))[1])
        for k, v in zones.items()
    ]
    zone_schema = StructType([
        StructField("zone_id", IntegerType(), False),
        StructField("zone_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ])
    zone_df = spark.createDataFrame(zone_data, schema=zone_schema)

    # Left join ile isim ve koordinat ekle
    vertices = (
        vertex_ids
        .join(F.broadcast(zone_df), vertex_ids.id == zone_df.zone_id, "left")
        .select(
            F.col("id"),
            F.coalesce(
                F.col("zone_name"),
                F.concat(F.lit("Zone-"), F.col("id"))
            ).alias("name"),
            F.coalesce(F.col("latitude"), F.lit(0.0)).alias("latitude"),
            F.coalesce(F.col("longitude"), F.lit(0.0)).alias("longitude"),
        )
    )

    # ── Edges (Kenarlar) DataFrame ──
    # NetworkX: G.add_edge(u, v, weight=trip_count, ...)
    # GraphFrame: src, dst sütunları zorunlu
    edges = edge_df.select(
        F.col("PULocationID").alias("src"),
        F.col("DOLocationID").alias("dst"),
        F.col("trip_count").cast(LongType()).alias("weight"),
        F.col("avg_duration"),
        F.col("avg_distance"),
        F.col("total_fare"),
    )

    # ── GraphFrame Oluştur ──
    graph = GraphFrame(vertices, edges)

    v_count = graph.vertices.count()
    e_count = graph.edges.count()
    total_flow = graph.edges.agg(F.sum("weight")).collect()[0][0]

    logger.info(f"  Düğüm: {v_count} | Kenar: {e_count}")
    logger.info(f"  Toplam akış: {total_flow:,}")

    return graph


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 3: PAGERANK ANALİZİ (Dağıtık)                                ║
# ╚══════════════════════════════════════════════════════════════════════╝
def run_pagerank(graph: GraphFrame, reset_prob: float = 0.15,
                 max_iter: int = 20) -> DataFrame:
    """
    Dönüşüm:
    ┌──────────────────────────────────────┬────────────────────────────────────┐
    │ NetworkX (Önce)                      │ GraphFrame (Sonra)                 │
    ├──────────────────────────────────────┼────────────────────────────────────┤
    │ scores = nx.pagerank(                │ results = graph.pageRank(          │
    │     G, alpha=0.85,                   │     resetProbability=0.15,         │
    │     max_iter=100,                    │     maxIter=20                     │
    │     weight="weight"                  │ )                                  │
    │ )                                    │                                    │
    │ sorted(scores.items(), ...)          │ results.vertices                   │
    │                                      │     .orderBy(desc("pagerank"))     │
    └──────────────────────────────────────┴────────────────────────────────────┘

    PageRank formülü (her iki implementasyonda aynı):
        PR(v) = (1 - d)/N + d × Σ [PR(u) / out_degree(u)]
                                   u ∈ in_neighbors(v)
    """
    logger.info("═" * 60)
    logger.info("ADIM 3: PAGERANK (Dağıtık — GraphFrame)")
    logger.info("═" * 60)

    t0 = time.time()

    # ── GraphFrame PageRank ──
    # NetworkX tek makinede seri çalışır
    # GraphFrame Pregel tabanlı BSP (Bulk Synchronous Parallel) modelde
    # tüm executor'lar üzerinde paralel çalışır
    pr_results = graph.pageRank(
        resetProbability=reset_prob,
        maxIter=max_iter
    )

    # Sonuçları sırala
    pagerank_df = (
        pr_results.vertices
        .select("id", "name", "latitude", "longitude", "pagerank")
        .orderBy(F.desc("pagerank"))
        .cache()
    )

    elapsed = time.time() - t0
    logger.info(f"  PageRank tamamlandı ({elapsed:.1f}s)")
    logger.info(f"  Reset probability: {reset_prob} | Max iter: {max_iter}")

    # Top 15'i logla
    logger.info(f"\n  {'Sıra':<5}{'ID':<8}{'Bölge':<35}{'PageRank':<12}")
    logger.info("  " + "─" * 60)
    for i, row in enumerate(pagerank_df.take(15), 1):
        logger.info(f"  {i:<5}{row['id']:<8}{row['name']:<35}{row['pagerank']:.6f}")

    return pagerank_df


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 4: TOPLULUK TESPİTİ (Dağıtık)                                ║
# ╚══════════════════════════════════════════════════════════════════════╝
def detect_communities(graph: GraphFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Dönüşüm:
    ┌─────────────────────────────────────┬──────────────────────────────────────┐
    │ NetworkX (Önce)                     │ GraphFrame (Sonra)                    │
    ├─────────────────────────────────────┼──────────────────────────────────────┤
    │ nx.strongly_connected_components(G) │ graph.connectedComponents()          │
    │ nx.community.louvain_communities()  │ graph.labelPropagation(maxIter=5)    │
    └─────────────────────────────────────┴──────────────────────────────────────┘

    NOT: GraphFrame'in connectedComponents() zayıf bağlantılılık kullanır.
    labelPropagation Louvain'e alternatif olarak dağıtık topluluk tespiti yapar.
    """
    logger.info("═" * 60)
    logger.info("ADIM 4: TOPLULUK TESPİTİ (Dağıtık)")
    logger.info("═" * 60)

    t0 = time.time()

    # ── Connected Components ──
    # NetworkX: list(nx.weakly_connected_components(G))
    # GraphFrame: Pregel tabanlı paralel BFS
    cc_df = graph.connectedComponents()
    cc_stats = (
        cc_df.groupBy("component")
        .agg(F.count("*").alias("size"))
        .orderBy(F.desc("size"))
    )
    n_components = cc_stats.count()
    logger.info(f"  Connected Components: {n_components} bileşen ({time.time()-t0:.1f}s)")
    cc_stats.show(5, truncate=False)

    # ── Label Propagation ──
    # NetworkX: nx.community.louvain_communities(G_undirected)
    # GraphFrame: Label Propagation Algorithm (LPA) — dağıtık Louvain alternatifi
    t1 = time.time()
    lpa_df = graph.labelPropagation(maxIter=5)
    lpa_stats = (
        lpa_df.groupBy("label")
        .agg(F.count("*").alias("size"))
        .orderBy(F.desc("size"))
    )
    n_communities = lpa_stats.count()
    logger.info(f"  Label Propagation: {n_communities} topluluk ({time.time()-t1:.1f}s)")
    lpa_stats.show(5, truncate=False)

    return cc_df, lpa_df


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 5: DARBOĞAZ SİMÜLASYONU (Dağıtık)                           ║
# ╚══════════════════════════════════════════════════════════════════════╝
def simulate_bottleneck(
    spark: SparkSession,
    graph: GraphFrame,
    pagerank_df: DataFrame,
    top_k: int = 5
) -> List[dict]:
    """
    Dönüşüm:
    ┌──────────────────────────────────────┬─────────────────────────────────────────┐
    │ NetworkX (Önce)                      │ GraphFrame (Sonra)                       │
    ├──────────────────────────────────────┼─────────────────────────────────────────┤
    │ G_sim = G.copy()                     │ current_v = graph.vertices               │
    │ G_sim.remove_node(node)              │ current_v = v.filter(col("id") != node)  │
    │                                      │ current_e = e.filter(                    │
    │                                      │   (col("src") != node) &                 │
    │                                      │   (col("dst") != node)                   │
    │                                      │ )                                        │
    │ nx.pagerank(G_sim, ...)              │ GraphFrame(v, e).pageRank(...)            │
    │ sum(d["weight"] for ...)             │ edges.agg(F.sum("weight"))               │
    └──────────────────────────────────────┴─────────────────────────────────────────┘
    """
    logger.info("═" * 60)
    logger.info("ADIM 5: DARBOĞAZ SİMÜLASYONU (Dağıtık)")
    logger.info("═" * 60)

    # En kritik düğümleri al (driver'a küçük veri çek)
    top_rows = pagerank_df.select("id", "name").take(top_k)
    top_nodes = [(row["id"], row["name"]) for row in top_rows]

    # Orijinal metrikleri hesapla
    original_flow = graph.edges.agg(F.sum("weight")).collect()[0][0]
    original_edges = graph.edges.count()
    original_nodes = graph.vertices.count()

    logger.info(f"  Orijinal: {original_nodes} düğüm, {original_edges} kenar, "
                f"{original_flow:,} akış")

    # Simülasyon — her iterasyonda düğüm çıkar, yeni çizge oluştur
    current_vertices = graph.vertices
    current_edges = graph.edges
    results = []

    logger.info(f"\n  {'Çıkarılan':<35}{'Akış%':<10}{'Kenar%':<10}{'YeniTop3'}")
    logger.info("  " + "─" * 75)

    for node_id, node_name in top_nodes:

        # ── Düğüm Çıkarma (Node Removal) ──
        # NetworkX: G_sim.remove_node(node)
        # Spark: filter ile düğüm ve ilişkili kenarları çıkar
        current_vertices = current_vertices.filter(F.col("id") != node_id)
        current_edges = current_edges.filter(
            (F.col("src") != node_id) & (F.col("dst") != node_id)
        )

        # Metrikleri hesapla
        remaining_flow_row = current_edges.agg(F.sum("weight")).collect()[0][0]
        remaining_flow = remaining_flow_row if remaining_flow_row else 0
        remaining_edges = current_edges.count()

        flow_pct = (remaining_flow / original_flow) * 100
        edge_pct = (remaining_edges / original_edges) * 100

        # ── Yeni PageRank (Dağıtık Yeniden Hesaplama) ──
        # NetworkX: nx.pagerank(G_sim)
        # Spark: Yeni GraphFrame oluştur + pageRank çalıştır
        new_graph = GraphFrame(current_vertices, current_edges)
        new_pr = new_graph.pageRank(resetProbability=0.15, maxIter=10)
        new_top3 = new_pr.vertices.orderBy(F.desc("pagerank")).take(3)
        new_top3_names = [r["name"] for r in new_top3]

        results.append({
            "step": len(results) + 1,
            "removed_id": node_id,
            "removed_name": node_name,
            "remaining_flow_pct": round(flow_pct, 2),
            "remaining_edge_pct": round(edge_pct, 2),
            "new_top3": new_top3_names,
        })

        logger.info(f"  {node_name:<35}{flow_pct:>6.1f}%   {edge_pct:>6.1f}%   "
                     f"{', '.join(new_top3_names[:2])}")

    return results


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 6: SONUÇLARI DRIVER'A TOPLA + GÖRSELLEŞTİRME               ║
# ╚══════════════════════════════════════════════════════════════════════╝
def collect_and_visualize(
    pagerank_df: DataFrame,
    lpa_df: DataFrame,
    graph: GraphFrame,
    simulation: List[dict],
    city: str,
    output_dir: str,
):
    """
    Dağıtık Spark DataFrame'lerinden KÜÇÜK, özetlenmiş verileri driver'a
    toplayarak matplotlib ile görselleştirir.

    KRİTİK: .toPandas() ve .collect() SADECE küçük, aggregate edilmiş
    veriler için kullanılmalıdır. Ham milyonlarca satırı asla driver'a
    çekmeyin — OOM (Out of Memory) hatasına neden olur.
    """
    logger.info("═" * 60)
    logger.info("ADIM 6: GÖRSELLEŞTİRME (Driver Node)")
    logger.info("═" * 60)

    import seaborn as sns
    import numpy as np

    os.makedirs(output_dir, exist_ok=True)

    # ── Küçük veri setlerini driver'a topla ──
    # Sadece top 20 düğüm + topluluk istatistikleri
    pr_pandas = pagerank_df.limit(20).toPandas()

    community_stats = (
        lpa_df.groupBy("label")
        .agg(F.count("*").alias("size"))
        .orderBy(F.desc("size"))
        .toPandas()
    )

    # Kenar ağırlık istatistikleri (histogram için sample)
    edge_weights = (
        graph.edges
        .select("weight")
        .sample(fraction=min(1.0, 10000 / max(graph.edges.count(), 1)))
        .toPandas()
    )

    # Derece dağılımı
    degree_df = (
        graph.degrees
        .toPandas()
    )

    # ══════════════════════════════════════════════════════════════════
    # VIZ 1: Ana Dashboard
    # ══════════════════════════════════════════════════════════════════
    fig = plt.figure(figsize=(20, 16))
    fig.suptitle(
        f"Trafik Darboğazı Analizi — {CITY_CONFIGS[city]['name']}",
        fontsize=18, fontweight="bold", y=0.98, color=COLORS["dark"]
    )
    gs = GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.3)

    # Panel 1: Top 15 PageRank
    ax1 = fig.add_subplot(gs[0, 0])
    top15 = pr_pandas.head(15)
    colors = [COLORS["danger"] if i < 3 else COLORS["warning"] if i < 7
              else COLORS["primary"] for i in range(len(top15))]
    bars = ax1.barh(range(len(top15)), top15["pagerank"], color=colors, edgecolor="white")
    ax1.set_yticks(range(len(top15)))
    ax1.set_yticklabels(top15["name"].tolist(), fontsize=7)
    ax1.invert_yaxis()
    ax1.set_xlabel("PageRank Skoru")
    ax1.set_title("En Kritik 15 Bölge (PageRank)", fontsize=12, fontweight="bold")
    ax1.set_facecolor(COLORS["light_bg"])
    ax1.grid(axis="x", alpha=0.3)
    for bar, v in zip(bars, top15["pagerank"]):
        ax1.text(bar.get_width() + 0.0005, bar.get_y() + bar.get_height()/2,
                 f"{v:.4f}", va="center", fontsize=6)
    legend_elements = [
        mpatches.Patch(facecolor=COLORS["danger"], label="Kritik (Top 3)"),
        mpatches.Patch(facecolor=COLORS["warning"], label="Yüksek (4-7)"),
        mpatches.Patch(facecolor=COLORS["primary"], label="Orta (8-15)"),
    ]
    ax1.legend(handles=legend_elements, loc="lower right", fontsize=7)

    # Panel 2: Simülasyon
    ax2 = fig.add_subplot(gs[0, 1])
    sim_flows = [100.0] + [s["remaining_flow_pct"] for s in simulation]
    sim_edges = [100.0] + [s["remaining_edge_pct"] for s in simulation]
    sim_labels = ["Başlangıç"] + [s["removed_name"] for s in simulation]
    steps = range(len(sim_flows))

    ax2.plot(steps, sim_flows, "o-", color=COLORS["danger"], lw=2, ms=8, label="Akış %")
    ax2.plot(steps, sim_edges, "s--", color=COLORS["primary"], lw=2, ms=6, label="Kenar %")
    ax2.fill_between(steps, sim_flows, alpha=0.1, color=COLORS["danger"])
    ax2.set_xticks(list(steps))
    ax2.set_xticklabels(sim_labels, rotation=45, ha="right", fontsize=6)
    ax2.set_ylabel("Yüzde (%)")
    ax2.set_title("Darboğaz Simülasyonu", fontsize=12, fontweight="bold")
    ax2.legend(fontsize=8)
    ax2.set_facecolor(COLORS["light_bg"])
    ax2.grid(alpha=0.3)
    ax2.set_ylim(0, 105)
    for i in range(1, len(sim_flows)):
        drop = sim_flows[i-1] - sim_flows[i]
        if drop > 0:
            ax2.annotate(f"-{drop:.1f}%", xy=(i, sim_flows[i]),
                        xytext=(i, sim_flows[i]+3), fontsize=7, ha="center",
                        color=COLORS["danger"], fontweight="bold")

    # Panel 3: Topluluk Dağılımı
    ax3 = fig.add_subplot(gs[1, 0])
    n_comm = len(community_stats)
    comm_colors = plt.cm.Set3(np.linspace(0, 1, n_comm))
    wedges, _, _ = ax3.pie(
        community_stats["size"], labels=None,
        autopct=lambda p: f"{p:.1f}%" if p > 5 else "",
        colors=comm_colors, startangle=90, pctdistance=0.75,
        wedgeprops=dict(edgecolor="white", linewidth=2)
    )
    ax3.add_patch(plt.Circle((0, 0), 0.55, fc="white"))
    ax3.text(0, 0, f"{n_comm}\nTopluluk", ha="center", va="center",
             fontsize=14, fontweight="bold", color=COLORS["dark"])
    ax3.set_title("Trafik Toplulukları (Label Propagation)", fontsize=12, fontweight="bold")
    leg = [f"T{i+1} ({s} bölge)" for i, s in enumerate(community_stats["size"].head(8))]
    ax3.legend(wedges[:8], leg, loc="center left", bbox_to_anchor=(0.85, 0.5), fontsize=7)

    # Panel 4: Derece + Ağırlık Dağılımı
    ax4 = fig.add_subplot(gs[1, 1])
    if not degree_df.empty:
        ax4.hist(degree_df["degree"], bins=30, color=COLORS["success"],
                 alpha=0.7, edgecolor="white")
        ax4.axvline(degree_df["degree"].mean(), color=COLORS["danger"], ls="--",
                    label=f"Ort: {degree_df['degree'].mean():.1f}")
    ax4.set_xlabel("Derece (Bağlantı Sayısı)")
    ax4.set_ylabel("Frekans")
    ax4.set_title("Düğüm Derece Dağılımı", fontsize=12, fontweight="bold")
    ax4.legend()
    ax4.grid(alpha=0.3)
    ax4.set_facecolor(COLORS["light_bg"])

    plt.savefig(os.path.join(output_dir, "01_dashboard.png"), dpi=150, facecolor="white")
    plt.close()
    logger.info("  [OK] 01_dashboard.png")

    # ══════════════════════════════════════════════════════════════════
    # VIZ 2: Merkezilik Heatmap (sadece PageRank + Degree — dağıtık)
    # ══════════════════════════════════════════════════════════════════
    fig, ax = plt.subplots(figsize=(14, 8))
    # Degree bilgisini PageRank ile birleştir
    if not degree_df.empty:
        merged = pr_pandas.head(20).merge(degree_df, on="id", how="left")
        merged = merged.fillna(0)
        # Normalize
        for col_name in ["pagerank", "degree"]:
            mx = merged[col_name].max()
            if mx > 0:
                merged[col_name + "_norm"] = merged[col_name] / mx
            else:
                merged[col_name + "_norm"] = 0

        heat_data = merged[["pagerank_norm", "degree_norm"]].copy()
        heat_data.columns = ["PageRank", "Degree"]
        heat_data.index = merged["name"]

        sns.heatmap(heat_data, annot=True, fmt=".2f", cmap="YlOrRd",
                    linewidths=0.5, ax=ax, cbar_kws={"label": "Normalize (0-1)"})
    ax.set_title(f"Merkezilik Karşılaştırması — {CITY_CONFIGS[city]['name']}",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "02_centrality_heatmap.png"), dpi=150, facecolor="white")
    plt.close()
    logger.info("  [OK] 02_centrality_heatmap.png")


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 7: SONUÇLARI HARİTA İÇİN JSON OLARAK DIŞA AKTAR             ║
# ╚══════════════════════════════════════════════════════════════════════╝
def export_map_data(
    pagerank_df: DataFrame,
    lpa_df: DataFrame,
    graph: GraphFrame,
    simulation: List[dict],
    city: str,
    output_dir: str,
):
    """
    İnteraktif harita görselleştirmesi için tüm analiz sonuçlarını
    JSON formatında dışa aktarır. Bu JSON, React/Leaflet haritası
    tarafından okunacaktır.

    Çıktı yapısı:
    {
        "city": { "name": "İstanbul", ... },
        "nodes": [ { "id": 5, "name": "Taksim", "lat": 41.037, "lon": 28.985,
                     "pagerank": 0.025, "community": 1 }, ... ],
        "edges": [ { "src": 5, "dst": 13, "weight": 12500,
                     "src_lat": ..., "src_lon": ..., "dst_lat": ..., "dst_lon": ... }, ... ],
        "simulation": [ ... ],
        "top_bottlenecks": [ ... ]
    }
    """
    logger.info("═" * 60)
    logger.info("ADIM 7: HARİTA VERİSİ DIŞA AKTARIMI (JSON)")
    logger.info("═" * 60)

    coords = CITY_CONFIGS.get(city, {}).get("coordinates", {})
    city_info = CITY_CONFIGS.get(city, {})

    # ── Düğüm verisi (PageRank + Topluluk) ──
    # İki Spark DF'yi join'le, sonra küçük veriyi driver'a çek
    node_data = (
        pagerank_df
        .join(
            lpa_df.select(F.col("id").alias("lpa_id"), F.col("label").alias("community")),
            pagerank_df.id == F.col("lpa_id"),
            "left"
        )
        .select("id", "name", "latitude", "longitude", "pagerank", "community")
        .orderBy(F.desc("pagerank"))
        .toPandas()
    )

    nodes = []
    for _, row in node_data.iterrows():
        nodes.append({
            "id": int(row["id"]),
            "name": row["name"],
            "lat": float(row["latitude"]),
            "lon": float(row["longitude"]),
            "pagerank": round(float(row["pagerank"]), 6),
            "community": int(row["community"]) if row["community"] else 0,
        })

    # ── Kenar verisi (Top 200 en yoğun rota) ──
    top_edges_df = (
        graph.edges
        .orderBy(F.desc("weight"))
        .limit(200)
        .toPandas()
    )

    edges = []
    for _, row in top_edges_df.iterrows():
        src_coord = coords.get(int(row["src"]), (0, 0))
        dst_coord = coords.get(int(row["dst"]), (0, 0))
        edges.append({
            "src": int(row["src"]),
            "dst": int(row["dst"]),
            "weight": int(row["weight"]),
            "src_lat": src_coord[0], "src_lon": src_coord[1],
            "dst_lat": dst_coord[0], "dst_lon": dst_coord[1],
        })

    # ── Top darboğazlar ──
    top_bottlenecks = []
    for n in nodes[:10]:
        if n["lat"] != 0 and n["lon"] != 0:
            top_bottlenecks.append(n)

    # ── JSON dosyasını oluştur ──
    map_data = {
        "city": {
            "name": city_info.get("name", city),
            "country": city_info.get("country", ""),
            "center_lat": sum(n["lat"] for n in nodes if n["lat"] != 0) / max(len([n for n in nodes if n["lat"] != 0]), 1),
            "center_lon": sum(n["lon"] for n in nodes if n["lon"] != 0) / max(len([n for n in nodes if n["lon"] != 0]), 1),
        },
        "nodes": nodes,
        "edges": edges,
        "simulation": simulation,
        "top_bottlenecks": top_bottlenecks,
        "stats": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "total_flow": sum(e["weight"] for e in edges),
        },
    }

    json_path = os.path.join(output_dir, "map_data.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(map_data, f, ensure_ascii=False, indent=2)

    logger.info(f"  [OK] Harita verisi: {json_path}")
    logger.info(f"       Düğüm: {len(nodes)} | Kenar: {len(edges)}")

    return map_data


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  ADIM 8: SONUÇLARI PARQUET OLARAK KAYDET                           ║
# ╚══════════════════════════════════════════════════════════════════════╝
def save_results(
    spark: SparkSession,
    pagerank_df: DataFrame,
    lpa_df: DataFrame,
    simulation: List[dict],
    output_dir: str,
):
    """
    Sonuçları hem Parquet (büyük veri ekosistemi) hem CSV (kolay erişim)
    formatında kaydeder.
    """
    logger.info("═" * 60)
    logger.info("ADIM 8: SONUÇLARI KAYDET")
    logger.info("═" * 60)

    # PageRank sonuçları — Parquet
    pr_path = os.path.join(output_dir, "pagerank_results")
    pagerank_df.write.mode("overwrite").parquet(pr_path)
    logger.info(f"  [OK] PageRank → {pr_path}")

    # Topluluk sonuçları — Parquet
    lpa_path = os.path.join(output_dir, "community_results")
    lpa_df.write.mode("overwrite").parquet(lpa_path)
    logger.info(f"  [OK] Communities → {lpa_path}")

    # Simülasyon — JSON
    sim_path = os.path.join(output_dir, "simulation_results.json")
    with open(sim_path, "w") as f:
        json.dump(simulation, f, indent=2)
    logger.info(f"  [OK] Simulation → {sim_path}")

    # Özet CSV (driver'da küçük veri)
    csv_path = os.path.join(output_dir, "pagerank_summary.csv")
    pagerank_df.limit(50).toPandas().to_csv(csv_path, index=False)
    logger.info(f"  [OK] Summary CSV → {csv_path}")


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  MAIN — Tam Pipeline Orkestratörü                                  ║
# ╚══════════════════════════════════════════════════════════════════════╝
def main():
    parser = argparse.ArgumentParser(
        description="Akıllı Şehir Trafik Darboğazı — PySpark/GraphFrames Pipeline"
    )
    parser.add_argument("--city", type=str, default="istanbul",
                        help="Şehir adı (istanbul/nyc)")
    parser.add_argument("--input", type=str, required=True,
                        help="Parquet veri yolu (S3/GCS/HDFS/lokal)")
    parser.add_argument("--output", type=str, required=True,
                        help="Çıktı dizini yolu")
    parser.add_argument("--reset-prob", type=float, default=0.15,
                        help="PageRank reset probability")
    parser.add_argument("--max-iter", type=int, default=20,
                        help="PageRank max iterasyon")
    parser.add_argument("--top-k", type=int, default=5,
                        help="Simülasyonda çıkarılacak düğüm sayısı")
    args = parser.parse_args()

    total_start = time.time()

    city_name = CITY_CONFIGS.get(args.city, {}).get("name", args.city)
    logger.info(f"\n{'█' * 60}")
    logger.info(f"  TRAFİK DARBOĞAZI ANALİZİ — {city_name.upper()}")
    logger.info(f"  Motor: Apache Spark + GraphFrames (Dağıtık)")
    logger.info(f"{'█' * 60}")

    # Pipeline
    spark = create_spark_session()

    edge_df, raw_count = etl_pipeline(spark, args.input)
    graph = build_graph(spark, edge_df, args.city)
    pagerank_df = run_pagerank(graph, args.reset_prob, args.max_iter)
    cc_df, lpa_df = detect_communities(graph)
    simulation = simulate_bottleneck(spark, graph, pagerank_df, args.top_k)

    # Görselleştirme (driver node'da)
    collect_and_visualize(pagerank_df, lpa_df, graph, simulation, args.city, args.output)

    # Harita verisi
    map_data = export_map_data(pagerank_df, lpa_df, graph, simulation, args.city, args.output)

    # Kaydet
    save_results(spark, pagerank_df, lpa_df, simulation, args.output)

    total_elapsed = time.time() - total_start
    logger.info(f"\n{'═' * 60}")
    logger.info(f"  PIPELINE TAMAMLANDI")
    logger.info(f"  Toplam süre: {total_elapsed:.1f}s")
    logger.info(f"  İşlenen kayıt: {raw_count:,}")
    logger.info(f"  Throughput: {raw_count/max(total_elapsed,0.1):,.0f} kayıt/s")
    logger.info(f"{'═' * 60}")

    spark.stop()


if __name__ == "__main__":
    main()
