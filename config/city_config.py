"""
==============================================================================
  Şehir Konfigürasyon Sistemi (City Configuration)
==============================================================================
  Her şehir için bölge tanımları, ağırlıklar ve koordinatları tek bir
  config dosyasında tutarak pipeline'ı şehirden bağımsız hale getirir.

  Yeni şehir eklemek için:
    1. Bu dosyaya yeni bir CityConfig sınıfı ekle
    2. CITY_REGISTRY'ye kaydet
    3. Pipeline'ı --city parametresiyle çalıştır
==============================================================================
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
import json
import os
import csv


@dataclass
class CityConfig:
    """Şehir konfigürasyonu için temel sınıf."""

    name: str                                    # Şehir adı
    country: str                                 # Ülke
    zones: Dict[int, str]                        # {zone_id: zone_name}
    hotspots: List[int]                          # Yüksek yoğunluklu bölge ID'leri
    secondary_hotspots: List[int]                # Orta yoğunluklu bölge ID'leri
    transport_hubs: List[int]                    # Havalimanı, terminal vb.
    coordinates: Dict[int, Tuple[float, float]]  # {zone_id: (lat, lon)} — görselleştirme için
    hotspot_weight: float = 25.0                 # Hotspot trafik ağırlığı
    secondary_weight: float = 12.0               # İkincil bölge ağırlığı
    hub_weight: float = 15.0                     # Ulaşım merkezi ağırlığı
    default_weight: float = 1.0                  # Varsayılan ağırlık
    currency: str = "USD"                        # Para birimi (ücret hesabı için)
    base_fare: float = 3.50                      # Temel ücret
    per_km_fare: float = 2.50                    # Km başı ücret
    timezone: str = "UTC"                        # Zaman dilimi

    def get_zone_name(self, zone_id: int) -> str:
        return self.zones.get(zone_id, f"Bölge-{zone_id}")

    def get_zone_weight(self, zone_id: int) -> float:
        if zone_id in self.hotspots:
            return self.hotspot_weight
        elif zone_id in self.transport_hubs:
            return self.hub_weight
        elif zone_id in self.secondary_hotspots:
            return self.secondary_weight
        return self.default_weight

    def summary(self) -> str:
        return (
            f"Şehir: {self.name}, {self.country}\n"
            f"Toplam bölge: {len(self.zones)}\n"
            f"Hotspot: {len(self.hotspots)} | İkincil: {len(self.secondary_hotspots)} "
            f"| Ulaşım merkezi: {len(self.transport_hubs)}"
        )


# =============================================================================
# İSTANBUL KONFİGÜRASYONU
# =============================================================================
def create_istanbul_config() -> CityConfig:
    """
    İstanbul ilçe/semt bazlı trafik bölgeleri.
    Gerçek projede İBB Açık Veri portalından alınabilir:
    https://data.ibb.gov.tr/
    """

    zones = {
        # === Avrupa Yakası — Merkez ===
        1: "Fatih / Sultanahmet",
        2: "Fatih / Eminönü",
        3: "Fatih / Aksaray",
        4: "Fatih / Laleli",
        5: "Beyoğlu / Taksim",
        6: "Beyoğlu / Galata",
        7: "Beyoğlu / Karaköy",
        8: "Beyoğlu / İstiklal Cad.",
        9: "Beşiktaş / Beşiktaş Merkez",
        10: "Beşiktaş / Levent",
        11: "Beşiktaş / Etiler",
        12: "Beşiktaş / Bebek",
        13: "Şişli / Mecidiyeköy",
        14: "Şişli / Nişantaşı",
        15: "Şişli / Maslak",
        16: "Şişli / Fulya",
        17: "Kağıthane / Cendere",
        18: "Kağıthane / Merkez",
        19: "Eyüpsultan / Eyüp Merkez",
        20: "Eyüpsultan / Alibeyköy",
        21: "Sarıyer / İstinye",
        22: "Sarıyer / Maslak (Sarıyer)",
        23: "Sarıyer / Tarabya",
        24: "Sarıyer / Rumelihisarı",

        # === Avrupa Yakası — Batı ===
        25: "Bakırköy / Bakırköy Merkez",
        26: "Bakırköy / Ataköy",
        27: "Bakırköy / Yeşilköy",
        28: "Bahçelievler / Bahçelievler Merkez",
        29: "Bahçelievler / Yenibosna",
        30: "Bağcılar / Bağcılar Merkez",
        31: "Bağcılar / Güneşli",
        32: "Esenler / Esenler Merkez",
        33: "Güngören / Güngören Merkez",
        34: "Zeytinburnu / Zeytinburnu Merkez",
        35: "Bayrampaşa / Bayrampaşa Merkez",
        36: "Küçükçekmece / Halkalı",
        37: "Küçükçekmece / Atakent",
        38: "Avcılar / Avcılar Merkez",
        39: "Avcılar / Ambarlı",
        40: "Başakşehir / Başakşehir Merkez",
        41: "Başakşehir / İkitelli",
        42: "Esenyurt / Esenyurt Merkez",
        43: "Beylikdüzü / Beylikdüzü Merkez",
        44: "Büyükçekmece / Büyükçekmece Merkez",
        45: "Arnavutköy / Arnavutköy Merkez",
        46: "Sultangazi / Sultangazi Merkez",
        47: "Gaziosmanpaşa / Gaziosmanpaşa Merkez",

        # === Anadolu Yakası — Merkez ===
        50: "Kadıköy / Kadıköy Merkez",
        51: "Kadıköy / Moda",
        52: "Kadıköy / Bostancı",
        53: "Kadıköy / Fenerbahçe",
        54: "Kadıköy / Kozyatağı",
        55: "Üsküdar / Üsküdar Merkez",
        56: "Üsküdar / Çengelköy",
        57: "Üsküdar / Kısıklı",
        58: "Ataşehir / Ataşehir Merkez",
        59: "Ataşehir / İçerenköy",
        60: "Ataşehir / Ünalan",
        61: "Maltepe / Maltepe Merkez",
        62: "Maltepe / Cevizli",
        63: "Kartal / Kartal Merkez",
        64: "Kartal / Soğanlık",
        65: "Pendik / Pendik Merkez",
        66: "Pendik / Kurtköy",
        67: "Tuzla / Tuzla Merkez",
        68: "Sultanbeyli / Sultanbeyli Merkez",
        69: "Sancaktepe / Sancaktepe Merkez",
        70: "Ümraniye / Ümraniye Merkez",
        71: "Ümraniye / Çakmak",
        72: "Beykoz / Beykoz Merkez",
        73: "Beykoz / Kavacık",
        74: "Çekmeköy / Çekmeköy Merkez",
        75: "Şile / Şile Merkez",

        # === Ulaşım Merkezleri ===
        80: "İstanbul Havalimanı (IST)",
        81: "Sabiha Gökçen Havalimanı (SAW)",
        82: "Marmaray / Sirkeci",
        83: "Marmaray / Üsküdar",
        84: "Yenikapı İDO Terminali",
        85: "Kadıköy İDO Terminali",
        86: "Harem Otogarı",
        87: "Esenler Otogarı (Büyük İstanbul Otogarı)",
        88: "Söğütlüçeşme Marmaray",
        89: "Mecidiyeköy Metrobüs Aktarma",
        90: "Zincirlikuyu Metrobüs Aktarma",

        # === Köprü / Geçiş Noktaları ===
        91: "15 Temmuz Şehitler Köprüsü (Avrupa Giriş)",
        92: "15 Temmuz Şehitler Köprüsü (Anadolu Giriş)",
        93: "FSM Köprüsü (Avrupa Giriş)",
        94: "FSM Köprüsü (Anadolu Giriş)",
        95: "Yavuz Sultan Selim Köprüsü (Avrupa Giriş)",
        96: "Yavuz Sultan Selim Köprüsü (Anadolu Giriş)",
        97: "Avrasya Tüneli (Avrupa Giriş)",
        98: "Avrasya Tüneli (Anadolu Giriş)",
    }

    # Yüksek trafik bölgeleri (darboğaz adayları)
    hotspots = [
        5, 6, 7, 8,        # Beyoğlu / Taksim bölgesi
        9, 10, 13, 14,      # Beşiktaş / Şişli merkez
        1, 2, 3, 4,         # Fatih / Tarihi yarımada
        50, 51, 55,         # Kadıköy / Üsküdar
        91, 92, 93, 94,     # Köprü girişleri
        97, 98,             # Avrasya Tüneli
        89, 90,             # Metrobüs aktarma noktaları
    ]

    secondary_hotspots = [
        15, 16, 17,         # Maslak / Fulya / Cendere
        25, 26, 28, 30, 31, # Bakırköy / Bahçelievler / Bağcılar
        52, 54, 58, 60,     # Bostancı / Kozyatağı / Ataşehir
        70, 71, 73,         # Ümraniye / Kavacık
        34, 35,             # Zeytinburnu / Bayrampaşa
        42, 40,             # Esenyurt / Başakşehir
    ]

    transport_hubs = [
        80, 81,             # Havalimanları
        82, 83, 84, 85,     # Marmaray / İDO
        86, 87,             # Otogarlar
        88, 89, 90,         # Marmaray / Metrobüs
    ]

    # Yaklaşık koordinatlar (görselleştirme için)
    coordinates = {
        1: (41.0082, 28.9784), 2: (41.0166, 28.9696), 3: (41.0097, 28.9509),
        4: (41.0104, 28.9558), 5: (41.0370, 28.9850), 6: (41.0256, 28.9741),
        7: (41.0215, 28.9730), 8: (41.0337, 28.9784), 9: (41.0430, 29.0070),
        10: (41.0810, 29.0100), 11: (41.0801, 29.0301), 12: (41.0730, 29.0440),
        13: (41.0625, 28.9910), 14: (41.0480, 28.9930), 15: (41.1110, 29.0200),
        16: (41.0550, 29.0100), 17: (41.0780, 28.9720), 18: (41.0750, 28.9650),
        19: (41.0480, 28.9340), 20: (41.0650, 28.9450), 21: (41.1080, 29.0570),
        22: (41.1100, 29.0200), 23: (41.1320, 29.0580), 24: (41.0850, 29.0550),
        25: (40.9810, 28.8770), 26: (40.9680, 28.8550), 27: (40.9580, 28.8200),
        28: (41.0000, 28.8630), 29: (40.9920, 28.8350), 30: (41.0360, 28.8560),
        31: (41.0200, 28.8700), 32: (41.0430, 28.8760), 33: (41.0150, 28.8880),
        34: (41.0050, 28.9100), 35: (41.0420, 28.9120), 36: (41.0210, 28.7870),
        37: (41.0300, 28.7650), 38: (40.9800, 28.7220), 39: (40.9700, 28.6930),
        40: (41.0920, 28.8000), 41: (41.0600, 28.8200), 42: (41.0320, 28.6830),
        43: (41.0050, 28.6400), 44: (41.0240, 28.5880), 45: (41.1850, 28.7400),
        46: (41.1050, 28.8670), 47: (41.0650, 28.9170),
        50: (40.9905, 29.0290), 51: (40.9850, 29.0250), 52: (40.9590, 29.0670),
        53: (40.9710, 29.0370), 54: (40.9770, 29.0700), 55: (41.0250, 29.0153),
        56: (41.0470, 29.0530), 57: (41.0350, 29.0400), 58: (40.9900, 29.1100),
        59: (40.9730, 29.1000), 60: (40.9960, 29.0900), 61: (40.9350, 29.1300),
        62: (40.9200, 29.1400), 63: (40.8900, 29.1900), 64: (40.8850, 29.2000),
        65: (40.8750, 29.2500), 66: (40.9050, 29.3050), 67: (40.8200, 29.3000),
        68: (40.9650, 29.2700), 69: (41.0000, 29.2300), 70: (41.0280, 29.0900),
        71: (41.0200, 29.1000), 72: (41.0900, 29.1000), 73: (41.1050, 29.0750),
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
    }

    return CityConfig(
        name="İstanbul",
        country="Türkiye",
        zones=zones,
        hotspots=hotspots,
        secondary_hotspots=secondary_hotspots,
        transport_hubs=transport_hubs,
        coordinates=coordinates,
        hotspot_weight=25.0,
        secondary_weight=10.0,
        hub_weight=20.0,     # İstanbul'da havalimanı/köprü çok kritik
        default_weight=1.0,
        currency="TRY",
        base_fare=35.0,      # İstanbul taksimetre açılış
        per_km_fare=22.0,    # Km başı ücret (yaklaşık)
        timezone="Europe/Istanbul",
    )


def load_nyc_zones_from_lookup() -> Dict[int, str]:
    """NYC TLC taxi_zone_lookup.csv dosyasından zone adlarını yükler."""
    lookup_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "data",
        "nyc",
        "taxi_zone_lookup.csv",
    )
    if not os.path.exists(lookup_path):
        return {}

    zones: Dict[int, str] = {}
    with open(lookup_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                location_id = int(row.get("LocationID", "").strip())
            except (TypeError, ValueError, AttributeError):
                continue

            zone_name = (row.get("Zone") or "").strip()
            if zone_name:
                zones[location_id] = zone_name

    return zones


def load_nyc_coordinates_from_centroids() -> Dict[int, Tuple[float, float]]:
    """NYC centroid CSV dosyasından zone koordinatlarını yükler."""
    centroids_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "data",
        "nyc",
        "nyc_zone_centroids.csv",
    )
    if not os.path.exists(centroids_path):
        return {}

    coordinates: Dict[int, Tuple[float, float]] = {}
    with open(centroids_path, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                location_id = int(str(row.get("LocationID", "")).strip())
                lat = float(str(row.get("centroid_lat", "")).strip())
                lon = float(str(row.get("centroid_lon", "")).strip())
            except (TypeError, ValueError, AttributeError):
                continue

            coordinates[location_id] = (lat, lon)

    return coordinates


# =============================================================================
# NYC KONFİGÜRASYONU
# =============================================================================
def create_nyc_config() -> CityConfig:
    """NYC konfigürasyonu — mevcut verinin yapılandırılmış hali."""

    fallback_zones = {
        231: "Times Sq/Theatre District", 161: "Midtown North",
        162: "Midtown South", 163: "Midtown East", 164: "Midtown West",
        170: "Murray Hill", 186: "Penn Station/Madison Sq",
        234: "Union Sq", 236: "Upper East Side North",
        237: "Upper East Side South", 238: "Upper West Side North",
        239: "Upper West Side South", 230: "Sutton Place North",
        229: "Sutton Place South", 100: "Garment District",
        90: "Flatiron", 107: "Gramercy", 113: "Greenwich Village N",
        114: "Greenwich Village S", 48: "Clinton East", 50: "Clinton West",
        68: "East Chelsea", 79: "East Village",
        87: "Financial District N", 88: "Financial District S",
        125: "Hudson Sq", 137: "Lenox Hill East", 140: "Lenox Hill West",
        141: "Lincoln Square E", 142: "Lincoln Square W",
        143: "Little Italy/NoLiTa", 148: "Lower East Side",
        151: "Manhattan Valley", 153: "Meatpacking/W Village",
        158: "Midtown Center", 202: "Roosevelt Island",
        209: "Seaport", 211: "SoHo", 224: "Stuy Town/PCV",
        232: "TriBeCa/Civic Center", 233: "UN/Turtle Bay South",
        243: "Washington Heights S", 244: "Washington Heights N",
        246: "West Chelsea/Hudson Yards", 249: "West Village",
        261: "World Trade Center", 262: "Yorkville East", 263: "Yorkville West",
        132: "JFK Airport", 138: "LaGuardia Airport",
        25: "Brooklyn Heights", 76: "Fort Greene", 21: "Boerum Hill",
        37: "Cobble Hill", 33: "Carroll Gardens", 181: "Park Slope",
        189: "Prospect Heights", 54: "Downtown Brooklyn/MetroTech",
        97: "Greenpoint", 248: "Williamsburg (North Side)",
        255: "Williamsburg (South Side)",
        2: "Astoria", 101: "Jackson Heights", 144: "LIC/Queens Plaza",
    }

    zones = load_nyc_zones_from_lookup() or fallback_zones

    # 2023 yellow taxi gerçek akış verisinden (PU+DO) türetilmiş ilk 20 bölge
    hotspots = [
        237, 236, 161, 132, 230, 162, 142, 170, 186, 239,
        163, 68, 48, 234, 141, 138, 164, 79, 238, 107,
    ]

    # Akış sıralamasında hotspot sonrası gelen 20 bölge
    secondary_hotspots = [
        229, 140, 263, 249, 246, 100, 90, 231, 43, 262,
        233, 113, 143, 137, 114, 148, 264, 158, 144, 50,
    ]

    # Ulaşım merkezi (havaalanı + ana istasyon/liman)
    transport_hubs = [1, 132, 138, 186, 209]

    # fallback: taxi_zone_lookup.csv centroid içermez; bilinen kritik bölgeler için yaklaşık koordinatlar
    fallback_coordinates = {
        231: (40.7580, -73.9855), 161: (40.7648, -73.9775),
        162: (40.7505, -73.9847), 163: (40.7547, -73.9696),
        164: (40.7590, -73.9892), 170: (40.7484, -73.9760),
        186: (40.7506, -73.9935), 234: (40.7359, -73.9911),
        236: (40.7773, -73.9558), 237: (40.7695, -73.9596),
        238: (40.7870, -73.9754), 239: (40.7800, -73.9793),
        132: (40.6413, -73.7781), 138: (40.7769, -73.8740),
        1: (40.6895, -74.1745), 209: (40.7079, -74.0016),
    }
    coordinates = load_nyc_coordinates_from_centroids() or fallback_coordinates

    return CityConfig(
        name="New York City",
        country="USA",
        zones=zones,
        hotspots=hotspots,
        secondary_hotspots=secondary_hotspots,
        transport_hubs=transport_hubs,
        coordinates=coordinates,
        currency="USD",
        base_fare=3.50,
        per_km_fare=2.50,
        timezone="America/New_York",
    )


# =============================================================================
# ANKARA KONFİGÜRASYONU (Ek Örnek — Kolayca Genişletilebilir)
# =============================================================================
def create_ankara_config() -> CityConfig:
    """Ankara örnek konfigürasyonu — genişletilebilir."""

    zones = {
        1: "Kızılay", 2: "Ulus", 3: "Sıhhiye", 4: "Çankaya",
        5: "Tunalı Hilmi", 6: "Bahçelievler", 7: "Emek",
        8: "Dikmen", 9: "Eryaman", 10: "Batıkent",
        11: "Keçiören Merkez", 12: "Etlik", 13: "Mamak Merkez",
        14: "Sincan", 15: "Esenboğa Havalimanı",
        16: "AŞTİ (Otogar)", 17: "Yenimahalle", 18: "Çayyolu",
        19: "Bilkent", 20: "ODTÜ",
    }

    hotspots = [1, 2, 3, 4, 5]
    secondary_hotspots = [6, 7, 8, 11, 12, 17, 18]
    transport_hubs = [15, 16]
    coordinates = {
        1: (39.9208, 32.8541),   # Kızılay
        2: (39.9413, 32.8573),   # Ulus
        3: (39.9310, 32.8580),   # Sıhhiye
        4: (39.9020, 32.8630),   # Çankaya
        5: (39.9045, 32.8596),   # Tunalı Hilmi
        6: (39.9242, 32.8274),   # Bahçelievler
        7: (39.9192, 32.8171),   # Emek
        8: (39.8732, 32.8456),   # Dikmen
        9: (39.9840, 32.6580),   # Eryaman
        10: (39.9676, 32.7222),  # Batıkent
        11: (40.0036, 32.8622),  # Keçiören Merkez
        12: (39.9702, 32.8390),  # Etlik
        13: (39.9250, 32.9056),  # Mamak Merkez
        14: (39.9583, 32.5833),  # Sincan
        15: (40.1281, 32.9951),  # Esenboğa Havalimanı
        16: (39.9370, 32.8130),  # AŞTİ (Otogar)
        17: (39.9647, 32.8021),  # Yenimahalle
        18: (39.8890, 32.6850),  # Çayyolu
        19: (39.8700, 32.7500),  # Bilkent
        20: (39.8914, 32.7836),  # ODTÜ
    }

    return CityConfig(
        name="Ankara",
        country="Türkiye",
        zones=zones,
        hotspots=hotspots,
        secondary_hotspots=secondary_hotspots,
        transport_hubs=transport_hubs,
        coordinates=coordinates,
        currency="TRY",
        base_fare=35.0,
        per_km_fare=22.0,
        timezone="Europe/Istanbul",
    )


# =============================================================================
# ŞEHİR KAYIT DEFTERİ (City Registry)
# =============================================================================
CITY_REGISTRY = {
    "istanbul": create_istanbul_config,
    "nyc": create_nyc_config,
    "ankara": create_ankara_config,
}


def get_city_config(city_name: str) -> CityConfig:
    """İsimle şehir konfigürasyonu döndürür."""
    key = city_name.lower().strip()
    if key not in CITY_REGISTRY:
        available = ", ".join(CITY_REGISTRY.keys())
        raise ValueError(f"Bilinmeyen şehir: '{city_name}'. Mevcut şehirler: {available}")
    return CITY_REGISTRY[key]()


def list_cities() -> List[str]:
    """Kayıtlı şehirlerin listesini döndürür."""
    return list(CITY_REGISTRY.keys())


# =============================================================================
# JSON'dan yükleme (harici konfigürasyon desteği)
# =============================================================================
def load_config_from_json(json_path: str) -> CityConfig:
    """
    JSON dosyasından şehir konfigürasyonu yükler.
    Bu sayede kod değiştirmeden yeni şehir eklenebilir.

    Örnek JSON:
    {
        "name": "Londra",
        "country": "UK",
        "zones": {"1": "Westminster", "2": "City of London", ...},
        "hotspots": [1, 2, 3],
        "secondary_hotspots": [4, 5],
        "transport_hubs": [10, 11],
        "coordinates": {"1": [51.5007, -0.1246], ...},
        "currency": "GBP",
        "base_fare": 3.80,
        "per_km_fare": 1.50
    }
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Zone ID'leri int'e çevir
    zones = {int(k): v for k, v in data["zones"].items()}
    coords = {int(k): tuple(v) for k, v in data.get("coordinates", {}).items()}

    return CityConfig(
        name=data["name"],
        country=data["country"],
        zones=zones,
        hotspots=data["hotspots"],
        secondary_hotspots=data.get("secondary_hotspots", []),
        transport_hubs=data.get("transport_hubs", []),
        coordinates=coords,
        currency=data.get("currency", "USD"),
        base_fare=data.get("base_fare", 3.50),
        per_km_fare=data.get("per_km_fare", 2.50),
        timezone=data.get("timezone", "UTC"),
    )


if __name__ == "__main__":
    # Test: tüm şehirleri yükle ve özetlerini göster
    for city_key in list_cities():
        config = get_city_config(city_key)
        print(f"\n{'='*50}")
        print(config.summary())
        print(f"Para birimi: {config.currency}")
        print(f"Açılış ücreti: {config.base_fare} {config.currency}")
