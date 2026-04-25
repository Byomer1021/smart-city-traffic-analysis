"""
NYC Taxi Zone centroid üretici.

Akış:
1) taxi_zones.zip indirir (opsiyonel)
2) zip'i çıkarır
3) taxi_zones.shp dosyasını okur
4) EPSG:4326'ya çevirip centroid hesaplar
5) data/nyc/nyc_zone_centroids.csv üretir

Kullanım:
  python local_pipeline/build_nyc_centroids.py --download
"""

from pathlib import Path
import argparse
import zipfile
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser(description="Build NYC zone centroid CSV from taxi_zones shapefile")
    parser.add_argument("--download", action="store_true", help="Download taxi_zones.zip before processing")
    parser.add_argument(
        "--zip-url",
        default="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
        help="Source URL for taxi_zones.zip",
    )
    parser.add_argument(
        "--zip-path",
        default="data/nyc/taxi_zones.zip",
        help="Local path for zip file",
    )
    parser.add_argument(
        "--extract-dir",
        default="data/nyc/taxi_zones",
        help="Extraction folder",
    )
    parser.add_argument(
        "--output",
        default="data/nyc/nyc_zone_centroids.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    zip_path = Path(args.zip_path)
    extract_dir = Path(args.extract_dir)
    output_path = Path(args.output)

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.download:
        print(f"[INFO] Downloading {args.zip_url} -> {zip_path}")
        urllib.request.urlretrieve(args.zip_url, zip_path)

    if not zip_path.exists():
        raise FileNotFoundError(f"Zip not found: {zip_path}. Use --download or place zip manually.")

    print(f"[INFO] Extracting {zip_path} -> {extract_dir}")
    with zipfile.ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(extract_dir)

    shp_candidates = list(extract_dir.rglob("taxi_zones.shp"))
    if not shp_candidates:
        raise FileNotFoundError(f"taxi_zones.shp not found under: {extract_dir}")
    shp_path = shp_candidates[0]

    try:
        import geopandas as gpd
    except Exception as exc:
        raise RuntimeError(
            "geopandas not installed. Run: pip install geopandas"
        ) from exc

    print(f"[INFO] Reading shapefile: {shp_path}")
    gdf = gpd.read_file(shp_path)

    # Centroid'i projected CRS'te hesapla (WGS84'te centroid uyarısı verir)
    gdf_projected = gdf.to_crs("EPSG:2263")  # NAD83 / New York Long Island (ftUS)
    centroid_geom = gdf_projected.geometry.centroid
    centroid_wgs84 = gpd.GeoSeries(centroid_geom, crs="EPSG:2263").to_crs("EPSG:4326")

    centroids = gdf.copy()
    centroids["centroid_lat"] = centroid_wgs84.y
    centroids["centroid_lon"] = centroid_wgs84.x

    out_df = centroids[["LocationID", "zone", "borough", "centroid_lat", "centroid_lon"]]
    out_df.to_csv(output_path, index=False)

    print(f"[OK] Centroids written: {output_path}")
    print(f"     Total zones: {len(out_df)}")


if __name__ == "__main__":
    main()
