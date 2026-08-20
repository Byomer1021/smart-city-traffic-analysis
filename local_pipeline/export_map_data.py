"""
Lokal pipeline sonuçlarından interaktif harita için JSON üretir.
Spark pipeline çalıştırılamayan ortamlarda kullanılır.
"""
import sys, os, json, time, unicodedata
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import numpy as np
import networkx as nx
from config.city_config import get_city_config
from local_pipeline.generate_data import generate_trip_data, save_data


def slugify(name: str) -> str:
    """ASCII slug for folder/file names (drops accents, spaces→underscore)."""
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_name.lower().replace(" ", "_")


def read_trip_data(input_path) -> pd.DataFrame:
    """CSV veya Parquet okur; tarih sutunlarini normalize eder."""
    if str(input_path).lower().endswith(".parquet"):
        df = pd.read_parquet(input_path)
        for col in ("pickup_datetime", "dropoff_datetime"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    return pd.read_csv(input_path, parse_dates=["pickup_datetime", "dropoff_datetime"])


def main(city_name="istanbul", num_trips=2_000_000, data_path=None):
    config = get_city_config(city_name)
    print(f"[INFO] {config.name} analizi başlıyor...")

    city_slug = slugify(config.name)
    RESULTS_DIR = PROJECT_ROOT / "results" / city_slug
    os.makedirs(RESULTS_DIR, exist_ok=True)
    data_file = RESULTS_DIR / f"{city_slug}_trips.csv"

    if data_path:
        provided_path = Path(data_path)
        if not provided_path.is_absolute():
            provided_path = PROJECT_ROOT / provided_path
        if not provided_path.exists():
            raise FileNotFoundError(f"Veri dosyasi bulunamadi: {provided_path}")
        data_file = provided_path
        print(f"[INFO] Harici veri kullanilacak: {data_file}")
    elif not os.path.exists(data_file):
        df = generate_trip_data(config, num_trips=num_trips)
        save_data(df, RESULTS_DIR, city_slug)

    # ETL
    df = read_trip_data(data_file)
    df = df[df["trip_distance"] > 0]
    df = df[df["trip_duration_minutes"] > 0]
    df = df[df["trip_duration_minutes"] <= 300]
    same = (df["PULocationID"] == df["DOLocationID"]) & (df["trip_distance"] < 0.1)
    df = df[~same]

    edge_df = (
        df.groupby(["PULocationID", "DOLocationID"])
        .agg(trip_count=("trip_distance", "count"),
             avg_duration=("trip_duration_minutes", "mean"),
             avg_distance=("trip_distance", "mean"),
             total_fare=("fare_amount", "sum"))
        .reset_index()
    )
    edge_df = edge_df[edge_df["PULocationID"] != edge_df["DOLocationID"]]
    edge_df = edge_df[edge_df["trip_count"] >= 50]

    # Graph
    G = nx.DiGraph()
    all_nodes = set(edge_df["PULocationID"]) | set(edge_df["DOLocationID"])
    for n in all_nodes:
        G.add_node(n, name=config.get_zone_name(n))
    for _, row in edge_df.iterrows():
        G.add_edge(row["PULocationID"], row["DOLocationID"],
                   weight=row["trip_count"], avg_duration=row["avg_duration"])

    # PageRank
    scores = nx.pagerank(G, alpha=0.85, max_iter=100, weight="weight")

    # Communities
    G_und = G.to_undirected()
    communities = nx.community.louvain_communities(G_und, weight="weight", seed=42)
    node_comm = {}
    for i, comm in enumerate(communities):
        for n in comm:
            node_comm[n] = i

    # Betweenness
    # Betweenness: k ornekleme parametresi kaldirildi. Onceden
    # k=min(100, N) ve seed=None kullaniliyordu; bu, ayni graf uzerinde bile
    # her kosuda farkli skor uretiyordu (NYC grafinda JFK'nin normalize degeri
    # kosudan kosuya 0.82-1.00 arasi oynadi, 1. sira JFK ile East Harlem South
    # arasinda degisti). Orta boy graflarda tam hesap saniyeler suruyor ve
    # deterministik. Cok buyuk graflarda ornekleme sart olur; o durumda seed
    # sabitlenir ki sonuc tekrar uretilebilsin.
    if G.number_of_nodes() <= 1000:
        betweenness = nx.betweenness_centrality(G, weight="weight")
    else:
        betweenness = nx.betweenness_centrality(G, weight="weight", k=500, seed=42)
    in_degree = dict(G.in_degree(weight="weight"))
    out_degree = dict(G.out_degree(weight="weight"))

    # Simulation
    sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top5 = [n for n, _ in sorted_nodes[:5]]
    original_flow = sum(d["weight"] for _, _, d in G.edges(data=True))
    original_edges = G.number_of_edges()
    G_sim = G.copy()
    simulation = []
    for node in top5:
        name = config.get_zone_name(node)
        G_sim.remove_node(node)
        cur_flow = sum(d["weight"] for _, _, d in G_sim.edges(data=True))
        simulation.append({
            "step": len(simulation) + 1,
            "removed_id": int(node),
            "removed_name": name,
            "remaining_flow_pct": round((cur_flow / original_flow) * 100, 2),
            "remaining_edge_pct": round((G_sim.number_of_edges() / original_edges) * 100, 2),
        })

    # Build JSON
    nodes = []
    max_pr = max(scores.values())
    max_bw = max(betweenness.values()) if betweenness else 1
    max_in = max(in_degree.values()) if in_degree else 1
    max_out = max(out_degree.values()) if out_degree else 1

    for n in sorted(all_nodes):
        coord = config.coordinates.get(n, (0, 0))
        pr = scores.get(n, 0)
        bw = betweenness.get(n, 0)
        ind = in_degree.get(n, 0)
        outd = out_degree.get(n, 0)
        is_hotspot = n in config.hotspots
        is_hub = n in config.transport_hubs
        zone_type = "hotspot" if is_hotspot else ("hub" if is_hub else "normal")

        nodes.append({
            "id": int(n),
            "name": config.get_zone_name(n),
            "lat": coord[0],
            "lon": coord[1],
            "pagerank": round(pr, 6),
            "pagerank_norm": round(pr / max_pr, 4) if max_pr > 0 else 0,
            "betweenness": round(bw, 6),
            "betweenness_norm": round(bw / max_bw, 4) if max_bw > 0 else 0,
            "in_flow": int(ind),
            "out_flow": int(outd),
            "in_flow_norm": round(ind / max_in, 4) if max_in > 0 else 0,
            "community": node_comm.get(n, 0),
            "zone_type": zone_type,
            "rank": sorted([nn for nn, _ in sorted_nodes]).index(n) + 1 if n in scores else 999,
        })

    # Rank'ı düzelt
    pr_sorted = sorted(nodes, key=lambda x: x["pagerank"], reverse=True)
    for i, node in enumerate(pr_sorted):
        node["rank"] = i + 1

    # Top edges (en yoğun 300 rota)
    edge_list = []
    sorted_edges = sorted(G.edges(data=True), key=lambda x: x[2]["weight"], reverse=True)
    max_edge_w = sorted_edges[0][2]["weight"] if sorted_edges else 1

    for u, v, d in sorted_edges[:300]:
        src_c = config.coordinates.get(u, (0, 0))
        dst_c = config.coordinates.get(v, (0, 0))
        if src_c[0] == 0 or dst_c[0] == 0:
            continue
        edge_list.append({
            "src": int(u), "dst": int(v),
            "weight": int(d["weight"]),
            "weight_norm": round(d["weight"] / max_edge_w, 4),
            "avg_duration": round(d.get("avg_duration", 0), 1),
            "src_name": config.get_zone_name(u),
            "dst_name": config.get_zone_name(v),
            "src_lat": src_c[0], "src_lon": src_c[1],
            "dst_lat": dst_c[0], "dst_lon": dst_c[1],
        })

    # Community summaries
    comm_summaries = []
    for i, comm in enumerate(sorted(communities, key=len, reverse=True)):
        members = [config.get_zone_name(n) for n in sorted(comm)[:5]]
        comm_summaries.append({
            "id": i,
            "size": len(comm),
            "top_members": members,
            "avg_pagerank": round(np.mean([scores.get(n, 0) for n in comm]), 6),
        })

    # Valid nodes for centering
    valid_nodes = [n for n in nodes if n["lat"] != 0 and n["lon"] != 0]
    center_lat = np.mean([n["lat"] for n in valid_nodes]) if valid_nodes else 41.0
    center_lon = np.mean([n["lon"] for n in valid_nodes]) if valid_nodes else 29.0

    map_data = {
        "city": {
            "name": config.name,
            "country": config.country,
            "currency": config.currency,
            "center_lat": round(center_lat, 4),
            "center_lon": round(center_lon, 4),
            "zoom": 11,
        },
        "nodes": pr_sorted,
        "edges": edge_list,
        "simulation": simulation,
        "communities": comm_summaries,
        "stats": {
            "total_nodes": G.number_of_nodes(),
            "total_edges": G.number_of_edges(),
            "total_flow": int(original_flow),
            "density": round(nx.density(G), 4),
            "n_communities": len(communities),
        },
    }

    json_path = os.path.join(RESULTS_DIR, "map_data.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(map_data, f, ensure_ascii=False, indent=2)

    print(f"[OK] map_data.json → {json_path}")
    print(f"     Düğüm: {len(nodes)} | Kenar: {len(edge_list)} | Topluluk: {len(comm_summaries)}")
    return map_data

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", default="istanbul")
    parser.add_argument("--trips", type=int, default=2_000_000)
    parser.add_argument("--data", default=None,
                        help="Gercek veri dosyasi (CSV/Parquet) - verilirse sentetik uretim atlanir")
    args = parser.parse_args()
    main(args.city, args.trips, args.data)
