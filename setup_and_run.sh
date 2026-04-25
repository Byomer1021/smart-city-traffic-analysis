#!/usr/bin/env bash
# ╔══════════════════════════════════════════════════════════════════════╗
# ║   Smart City Traffic Analysis — AWS EC2 Kurulum & Çalıştırma      ║
# ║   Ubuntu 22.04 LTS üzerinde test edilmiştir                        ║
# ║                                                                      ║
# ║   Kullanım:                                                          ║
# ║     chmod +x setup_and_run.sh                                        ║
# ║     ./setup_and_run.sh                                               ║
# ╚══════════════════════════════════════════════════════════════════════╝

set -euo pipefail   # hata olursa dur, tanımsız değişkende dur

# ══════════════════════════════════════════════════════════════════════
# PARAMETRELER — ihtiyaca göre değiştir
# ══════════════════════════════════════════════════════════════════════
REPO_URL="https://github.com/Byomer1021/smart-city-traffic-analysis.git"
REPO_DIR="smart-city-traffic-analysis"
YEAR=2023
MONTHS=4          # 1-12 arası; kaç aylık veri indirilsin
CITY="nyc"
TOP_K=5
LOG_FILE="run_$(date +%Y%m%d_%H%M%S).log"

# ══════════════════════════════════════════════════════════════════════
# RENKLER & YARDIMCI FONKSİYONLAR
# ══════════════════════════════════════════════════════════════════════
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()   { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*" | tee -a "$LOG_FILE"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"                | tee -a "$LOG_FILE"; }
warn()  { echo -e "${YELLOW}[UYARI]${NC} $*"            | tee -a "$LOG_FILE"; }
die()   { echo -e "${RED}[HATA]${NC} $*"                | tee -a "$LOG_FILE"; exit 1; }

step() {
  echo "" | tee -a "$LOG_FILE"
  echo -e "${BOLD}${CYAN}══════════════════════════════════════${NC}" | tee -a "$LOG_FILE"
  echo -e "${BOLD}  ADIM $*${NC}" | tee -a "$LOG_FILE"
  echo -e "${BOLD}${CYAN}══════════════════════════════════════${NC}" | tee -a "$LOG_FILE"
}

timer_start() { _T0=$(date +%s); }
timer_end()   { echo -e "  ${YELLOW}Süre: $(($(date +%s)-_T0))s${NC}" | tee -a "$LOG_FILE"; }

# ══════════════════════════════════════════════════════════════════════
# BAŞLANGIÇ
# ══════════════════════════════════════════════════════════════════════
echo -e "${BOLD}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Smart City Traffic Bottleneck Analysis                ║"
echo "║   AWS EC2 Kurulum Scripti                               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo "Log dosyası: $LOG_FILE"
echo "Parametreler: YEAR=$YEAR | MONTHS=$MONTHS | CITY=$CITY | TOP_K=$TOP_K"
echo ""

# ══════════════════════════════════════════════════════════════════════
# ADIM 1 — SUNUCU HAZIRLIK
# ══════════════════════════════════════════════════════════════════════
step "1/8 — Sistem Paketleri Kurulumu"
timer_start

log "apt güncelleniyor..."
sudo apt-get update -qq

log "Gerekli paketler kuruluyor..."
sudo apt-get install -y -qq \
  git \
  python3-pip \
  python3-venv \
  unzip \
  curl \
  htop \
  tree

ok "Sistem paketleri kuruldu."
timer_end

# ══════════════════════════════════════════════════════════════════════
# ADIM 2 — REPO + PYTHON ORTAMI
# ══════════════════════════════════════════════════════════════════════
step "2/8 — Repo & Python Ortamı"
timer_start

if [ -d "$REPO_DIR" ]; then
  warn "Repo zaten mevcut, güncelleniyor..."
  cd "$REPO_DIR" && git pull && cd ..
else
  log "Repo klonlanıyor: $REPO_URL"
  git clone "$REPO_URL" "$REPO_DIR"
fi

cd "$REPO_DIR"
log "Virtual environment oluşturuluyor..."
python3 -m venv .venv

log "Paketler kuruluyor (requirements.txt)..."
source .venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q
pip install pyarrow geopandas -q

ok "Python ortamı hazır."
python3 --version
pip show pandas | grep Version
timer_end

# ══════════════════════════════════════════════════════════════════════
# ADIM 3 — NYC VERİSİ İNDİR
# ══════════════════════════════════════════════════════════════════════
step "3/8 — NYC Parquet Verisi İndirme ($MONTHS ay)"
timer_start

mkdir -p data/nyc

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

for m in $(seq -w 1 "$MONTHS"); do
  FNAME="yellow_tripdata_${YEAR}-${m}.parquet"
  DEST="data/nyc/$FNAME"

  if [ -f "$DEST" ]; then
    warn "$FNAME zaten mevcut, atlanıyor."
    continue
  fi

  log "İndiriliyor: $FNAME"
  curl -L --progress-bar \
       --retry 3 \
       --retry-delay 5 \
       "$BASE_URL/$FNAME" \
       -o "$DEST" || die "$FNAME indirilemedi."

  SIZE=$(du -h "$DEST" | cut -f1)
  ok "$FNAME indirildi ($SIZE)"
done

# Zone lookup tablosu
if [ ! -f "data/nyc/taxi_zone_lookup.csv" ]; then
  log "taxi_zone_lookup.csv indiriliyor..."
  curl -L "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" \
       -o data/nyc/taxi_zone_lookup.csv
  ok "taxi_zone_lookup.csv indirildi."
fi

log "İndirilen dosyalar:"
ls -lh data/nyc/*.parquet
timer_end

# ══════════════════════════════════════════════════════════════════════
# ADIM 4 — ZONE COORDİNAT (CENTROID) ÜRETİMİ
# ══════════════════════════════════════════════════════════════════════
step "4/8 — Zone Centroid Üretimi"
timer_start

if [ -f "data/nyc/nyc_zone_centroids.csv" ]; then
  warn "Centroid dosyası zaten mevcut, atlanıyor."
else
  log "Shapefile indiriliyor ve centroid hesaplanıyor..."
  python3 local_pipeline/build_nyc_centroids.py --download \
    || die "build_nyc_centroids.py başarısız oldu."
  ok "Centroid CSV oluşturuldu: data/nyc/nyc_zone_centroids.csv"
fi

ZONE_COUNT=$(wc -l < data/nyc/nyc_zone_centroids.csv)
log "Toplam zone: $((ZONE_COUNT - 1))"
timer_end

# ══════════════════════════════════════════════════════════════════════
# ADIM 5 — VERİ DÖNÜŞÜMÜ (TLC → Pipeline Formatı)
# ══════════════════════════════════════════════════════════════════════
step "5/8 — TLC → Pipeline Format Dönüşümü"
timer_start

mkdir -p data/nyc/formatted

for m in $(seq -w 1 "$MONTHS"); do
  INPUT="data/nyc/yellow_tripdata_${YEAR}-${m}.parquet"
  OUTPUT="data/nyc/formatted/nyc_trips_${YEAR}_${m}.parquet"
  MAPPING="config/sample_mapping_nyc_tlc.json"

  if [ -f "$OUTPUT" ]; then
    warn "$(basename $OUTPUT) zaten mevcut, atlanıyor."
    continue
  fi

  log "Dönüştürülüyor: $(basename $INPUT)"
  python3 local_pipeline/convert_data.py \
    --input  "$INPUT" \
    --output "$OUTPUT" \
    --mapping "$MAPPING" \
    || die "convert_data.py başarısız: $INPUT"

  ok "$(basename $OUTPUT) oluşturuldu."
done

timer_end

# ══════════════════════════════════════════════════════════════════════
# ADIM 6 — AYLIK DOSYALARI BİRLEŞTİR
# ══════════════════════════════════════════════════════════════════════
step "6/8 — Aylık Dosyaları Birleştir"
timer_start

MERGED="data/nyc/formatted/nyc_trips_${YEAR}_all.parquet"

if [ -f "$MERGED" ]; then
  warn "Birleşik dosya zaten mevcut: $MERGED"
else
  log "$MONTHS aylık dosya birleştiriliyor..."
  python3 local_pipeline/merge_months.py \
    --input-glob "data/nyc/formatted/nyc_trips_${YEAR}_*.parquet" \
    --output "$MERGED" \
    || die "merge_months.py başarısız oldu."

  SIZE=$(du -h "$MERGED" | cut -f1)
  ok "Birleşik dosya oluşturuldu: $MERGED ($SIZE)"
fi

# Satır sayısı kontrol
ROW_COUNT=$(python3 -c "
import pandas as pd
df = pd.read_parquet('$MERGED', columns=['PULocationID'])
print(f'{len(df):,}')
")
log "Toplam satır: $ROW_COUNT"
timer_end

# ══════════════════════════════════════════════════════════════════════
# ADIM 7 — ANALİZ ÇALIŞTIR
# ══════════════════════════════════════════════════════════════════════
step "7/8 — Trafik Analizi Çalıştır"
timer_start

log "PageRank + Topluluk Tespiti + Simülasyon başlıyor..."
log "Bu adım büyük veriyle 10-30 dk sürebilir..."

python3 local_pipeline/traffic_analysis_generic.py \
  --city   "$CITY" \
  --data   "$MERGED" \
  --top-k  "$TOP_K" \
  || die "traffic_analysis_generic.py başarısız oldu."

ok "Analiz tamamlandı."
log "Sonuçlar: results/${CITY}/"
ls -lh results/${CITY}/*.png 2>/dev/null || warn "PNG bulunamadı."
timer_end

# ══════════════════════════════════════════════════════════════════════
# ADIM 8 — HARİTA JSON ÜRETİMİ
# ══════════════════════════════════════════════════════════════════════
step "8/8 — İnteraktif Harita Verisi Üret"
timer_start

log "map_data.json oluşturuluyor..."
python3 local_pipeline/export_map_data.py \
  --city  "$CITY" \
  --data  "$MERGED" \
  || die "export_map_data.py başarısız oldu."

MAP_JSON="results/${CITY}/map_data.json"
if [ -f "$MAP_JSON" ]; then
  SIZE=$(du -h "$MAP_JSON" | cut -f1)
  ok "Harita verisi hazır: $MAP_JSON ($SIZE)"
fi

timer_end

# ══════════════════════════════════════════════════════════════════════
# ÖZET
# ══════════════════════════════════════════════════════════════════════
echo ""
echo -e "${BOLD}${GREEN}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              TÜM ADIMLAR TAMAMLANDI ✓                   ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${BOLD}Sonuçlar:${NC}"
echo "  📊 Görseller  : results/${CITY}/"
echo "  🗺️  Harita JSON: results/${CITY}/map_data.json"
echo "  📋 Log        : $LOG_FILE"
echo ""

echo -e "${BOLD}Oluşturulan dosyalar:${NC}"
find results/${CITY} -type f | sort | while read f; do
  SIZE=$(du -h "$f" | cut -f1)
  echo "  $(basename $f) — $SIZE"
done

echo ""
echo -e "${YELLOW}Sonraki adım — Dosyaları S3'e yükle:${NC}"
echo "  aws s3 cp results/${CITY}/ s3://BUCKET_ADIN/results/${CITY}/ --recursive"
echo "  aws s3 cp data/nyc/formatted/${MERGED##*/} s3://BUCKET_ADIN/data/"
