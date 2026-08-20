"""
==============================================================================
  Generic Akıllı Şehir Trafik Darboğazı Tespiti Pipeline
==============================================================================
  Herhangi bir şehir konfigürasyonu ile çalışır.
  Kullanım:
    python traffic_analysis_generic.py --city istanbul
    python traffic_analysis_generic.py --city nyc
    python traffic_analysis_generic.py --city ankara --trips 500000
    python traffic_analysis_generic.py --config custom_city.json
==============================================================================
"""

import pandas as pd
import numpy as np
import networkx as nx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import seaborn as sns
import warnings
import os
import time
import argparse
import unicodedata
import sys
from pathlib import Path

warnings.filterwarnings("ignore")
plt.rcParams["font.family"] = "DejaVu Sans"
plt.rcParams["figure.dpi"] = 150
plt.rcParams["savefig.bbox"] = "tight"

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.city_config import CityConfig, get_city_config, load_config_from_json
from local_pipeline.generate_data import generate_trip_data, save_data

COLORS = {
    "primary": "#1a73e8", "danger": "#dc3545", "warning": "#ffc107",
    "success": "#28a745", "dark": "#2c3e50", "light_bg": "#f8f9fa",
    "grid": "#e9ecef",
}


def slugify(name: str) -> str:
    """ASCII slug for folder/file names (drops accents, spaces→underscore)."""
    normalized = unicodedata.normalize("NFKD", name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_name.lower().replace(" ", "_")


# =============================================================================
# ADIM 1: ETL
# =============================================================================
def etl_pipeline(input_path: str, config: CityConfig) -> pd.DataFrame:
    """Config-agnostik ETL pipeline."""
    print("=" * 70)
    print(f"  ADIM 1: VERİ TEMİZLEME — {config.name}")
    print("=" * 70)

    t0 = time.time()
    input_lower = str(input_path).lower()
    if input_lower.endswith(".parquet"):
        df = pd.read_parquet(input_path)
        if "pickup_datetime" in df.columns:
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
        if "dropoff_datetime" in df.columns:
            df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    else:
        df = pd.read_csv(input_path, parse_dates=["pickup_datetime", "dropoff_datetime"])
    print(f"  Ham kayıt: {len(df):,}")

    before = len(df)
    df = df[df["trip_distance"] > 0]
    df = df[df["trip_duration_minutes"] > 0]
    df = df[df["trip_duration_minutes"] <= 300]

    same_zone = (df["PULocationID"] == df["DOLocationID"])
    ultra_short = df["trip_distance"] < 0.1
    df = df[~(same_zone & ultra_short)]

    removed = before - len(df)
    print(f"  Temizlenen: {removed:,} ({removed/before*100:.1f}%)")

    edge_df = (
        df.groupby(["PULocationID", "DOLocationID"])
        .agg(
            trip_count=("trip_distance", "count"),
            avg_duration=("trip_duration_minutes", "mean"),
            avg_distance=("trip_distance", "mean"),
            total_fare=("fare_amount", "sum"),
        )
        .reset_index()
    )
    edge_df = edge_df[edge_df["PULocationID"] != edge_df["DOLocationID"]]
    edge_df = edge_df[edge_df["trip_count"] >= 50]

    print(f"  Kenar sayısı: {len(edge_df):,} ({time.time()-t0:.1f}s)")
    return edge_df


# =============================================================================
# ADIM 2: ÇİZGE İNŞASI
# =============================================================================
def build_graph(edge_df: pd.DataFrame, config: CityConfig) -> nx.DiGraph:
    print(f"\n  ADIM 2: ÇİZGE İNŞASI — {config.name}")
    print("=" * 70)

    G = nx.DiGraph()
    all_nodes = set(edge_df["PULocationID"]) | set(edge_df["DOLocationID"])

    for node in all_nodes:
        G.add_node(node, name=config.get_zone_name(node))

    for _, row in edge_df.iterrows():
        G.add_edge(
            row["PULocationID"], row["DOLocationID"],
            weight=row["trip_count"],
            avg_duration=row["avg_duration"],
            avg_distance=row["avg_distance"],
        )

    print(f"  Düğüm: {G.number_of_nodes()} | Kenar: {G.number_of_edges()}")
    print(f"  Yoğunluk: {nx.density(G):.4f}")
    return G


# =============================================================================
# ADIM 3: PAGERANK
# =============================================================================
def run_pagerank(G: nx.DiGraph, config: CityConfig, alpha=0.85) -> dict:
    print(f"\n  ADIM 3: PAGERANK — {config.name}")
    print("=" * 70)

    t0 = time.time()
    scores = nx.pagerank(G, alpha=alpha, max_iter=100, weight="weight")
    sorted_pr = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    print(f"  {'Sıra':<5} {'ID':<8} {'Bölge':<35} {'Skor':<12}")
    print("  " + "-" * 60)
    for rank, (node, score) in enumerate(sorted_pr[:15], 1):
        print(f"  {rank:<5} {node:<8} {config.get_zone_name(node):<35} {score:.6f}")

    print(f"  ({time.time()-t0:.3f}s)")
    return scores


# =============================================================================
# ADIM 3.5: MERKEZİLİK
# =============================================================================
def compute_centrality(G: nx.DiGraph) -> dict:
    print("\n  [EK] Merkezilik metrikleri...")
    # Betweenness: k ornekleme parametresi kaldirildi. Onceden
    # k=min(100, N) ve seed=None kullaniliyordu; bu, ayni graf uzerinde bile
    # her kosuda farkli skor uretiyordu (NYC grafinda JFK'nin normalize degeri
    # kosudan kosuya 0.82-1.00 arasi oynadi, 1. sira JFK ile East Harlem South
    # arasinda degisti). Orta boy graflarda tam hesap saniyeler suruyor ve
    # deterministik. Cok buyuk graflarda ornekleme sart olur; o durumda seed
    # sabitlenir ki sonuc tekrar uretilebilsin.
    n = G.number_of_nodes()
    if n <= 1000:
        betweenness = nx.betweenness_centrality(G, weight="weight")
    else:
        betweenness = nx.betweenness_centrality(G, weight="weight", k=500, seed=42)

    return {
        "betweenness": betweenness,
        "in_degree": nx.in_degree_centrality(G),
        "out_degree": nx.out_degree_centrality(G),
    }


# =============================================================================
# ADIM 4: TOPLULUK TESPİTİ
# =============================================================================
def detect_communities(G: nx.DiGraph, config: CityConfig) -> dict:
    print(f"\n  ADIM 4: TOPLULUK TESPİTİ — {config.name}")
    print("=" * 70)

    scc = list(nx.strongly_connected_components(G))
    wcc = list(nx.weakly_connected_components(G))
    print(f"  SCC: {len(scc)} | WCC: {len(wcc)}")

    G_und = G.to_undirected()
    communities = nx.community.louvain_communities(G_und, weight="weight", seed=42)
    print(f"  Louvain topluluk: {len(communities)}")

    for i, comm in enumerate(sorted(communities, key=len, reverse=True)[:5]):
        names = [config.get_zone_name(n) for n in list(comm)[:3]]
        print(f"    {i+1}. {len(comm)} bölge → {', '.join(names)}...")

    return {"strongly_connected": scc, "weakly_connected": wcc, "louvain": communities}


# =============================================================================
# ADIM 5: SİMÜLASYON
# =============================================================================
def simulate_bottleneck(G: nx.DiGraph, scores: dict, config: CityConfig, top_k=5) -> dict:
    print(f"\n  ADIM 5: DARBOĞAZ SİMÜLASYONU — {config.name}")
    print("=" * 70)

    sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_nodes = [n for n, _ in sorted_nodes[:top_k]]

    original_flow = sum(d["weight"] for _, _, d in G.edges(data=True))
    original_edges = G.number_of_edges()
    original_nodes = G.number_of_nodes()

    G_und = G.to_undirected()
    largest_cc = max(nx.connected_components(G_und), key=len)
    try:
        orig_avg_path = nx.average_shortest_path_length(G_und.subgraph(largest_cc))
    except:
        orig_avg_path = float("inf")

    results = {
        "removed_nodes": [],
        "remaining_flow_pct": [100.0],
        "remaining_edges_pct": [100.0],
        "avg_path_length": [orig_avg_path],
        "num_components": [len(list(nx.weakly_connected_components(G)))],
    }

    G_sim = G.copy()
    for node in top_nodes:
        name = config.get_zone_name(node)
        G_sim.remove_node(node)

        cur_flow = sum(d["weight"] for _, _, d in G_sim.edges(data=True))
        flow_pct = (cur_flow / original_flow) * 100
        edge_pct = (G_sim.number_of_edges() / original_edges) * 100
        n_wcc = len(list(nx.weakly_connected_components(G_sim)))

        G_sim_und = G_sim.to_undirected()
        if G_sim_und.number_of_nodes() > 1:
            lcc = max(nx.connected_components(G_sim_und), key=len)
            try:
                avg_path = nx.average_shortest_path_length(G_sim_und.subgraph(lcc))
            except:
                avg_path = float("inf")
        else:
            avg_path = float("inf")

        results["removed_nodes"].append((node, name))
        results["remaining_flow_pct"].append(flow_pct)
        results["remaining_edges_pct"].append(edge_pct)
        results["avg_path_length"].append(avg_path)
        results["num_components"].append(n_wcc)

        print(f"  [-] {name:<35} Akış: {flow_pct:.1f}%  Kenar: {edge_pct:.1f}%  Bileşen: {n_wcc}")

    return results


# =============================================================================
# ADIM 6: GÖRSELLEŞTİRME
# =============================================================================
def create_visualizations(G, scores, centrality, communities, simulation, config, output_dir):
    print(f"\n  ADIM 6: GÖRSELLEŞTİRME — {config.name}")
    print("=" * 70)
    os.makedirs(output_dir, exist_ok=True)

    # --- VIZ 1: Dashboard ---
    fig = plt.figure(figsize=(20, 16))
    fig.suptitle(
        f"Trafik Darboğazı Analizi — {config.name}",
        fontsize=18, fontweight="bold", y=0.98, color=COLORS["dark"]
    )
    gs = GridSpec(2, 2, figure=fig, hspace=0.3, wspace=0.3)

    # Panel 1: Top 15 PageRank
    ax1 = fig.add_subplot(gs[0, 0])
    sorted_pr = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:15]
    names = [config.get_zone_name(n) for n, _ in sorted_pr]
    vals = [s for _, s in sorted_pr]
    colors = [COLORS["danger"] if i < 3 else COLORS["warning"] if i < 7 else COLORS["primary"]
              for i in range(len(vals))]

    bars = ax1.barh(range(len(names)), vals, color=colors, edgecolor="white")
    ax1.set_yticks(range(len(names)))
    ax1.set_yticklabels(names, fontsize=7)
    ax1.invert_yaxis()
    ax1.set_xlabel("PageRank Skoru")
    ax1.set_title("En Kritik 15 Bölge", fontsize=12, fontweight="bold")
    ax1.set_facecolor(COLORS["light_bg"])
    ax1.grid(axis="x", alpha=0.3)
    for bar, v in zip(bars, vals):
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
    steps = range(len(simulation["remaining_flow_pct"]))
    labels = ["Başlangıç"] + [n for _, n in simulation["removed_nodes"]]

    ax2.plot(steps, simulation["remaining_flow_pct"], "o-", color=COLORS["danger"], lw=2, ms=8, label="Akış %")
    ax2.plot(steps, simulation["remaining_edges_pct"], "s--", color=COLORS["primary"], lw=2, ms=6, label="Kenar %")
    ax2.fill_between(steps, simulation["remaining_flow_pct"], alpha=0.1, color=COLORS["danger"])
    ax2.set_xticks(list(steps))
    ax2.set_xticklabels(labels, rotation=45, ha="right", fontsize=6)
    ax2.set_ylabel("Yüzde (%)")
    ax2.set_title("Darboğaz Simülasyonu", fontsize=12, fontweight="bold")
    ax2.legend(fontsize=8)
    ax2.set_facecolor(COLORS["light_bg"])
    ax2.grid(alpha=0.3)
    ax2.set_ylim(0, 105)

    for i, (flow, name) in enumerate(zip(simulation["remaining_flow_pct"][1:],
                                          [n for _, n in simulation["removed_nodes"]])):
        prev = simulation["remaining_flow_pct"][i]
        drop = prev - flow
        if drop > 0:
            ax2.annotate(f"-{drop:.1f}%", xy=(i+1, flow), xytext=(i+1, flow+3),
                        fontsize=7, ha="center", color=COLORS["danger"], fontweight="bold")

    # Panel 3: Scatter
    ax3 = fig.add_subplot(gs[1, 0])
    common = set(scores.keys()) & set(centrality["betweenness"].keys())
    pr_v = [scores[n] for n in common]
    bw_v = [centrality["betweenness"][n] for n in common]
    in_d = [centrality["in_degree"].get(n, 0) for n in common]
    sizes = [max(d * 2000, 20) for d in in_d]

    sc = ax3.scatter(pr_v, bw_v, s=sizes, c=pr_v, cmap="YlOrRd", alpha=0.7, edgecolors="white")
    top5 = sorted(common, key=lambda n: scores[n], reverse=True)[:5]
    for n in top5:
        ax3.annotate(config.get_zone_name(n), xy=(scores[n], centrality["betweenness"][n]),
                     xytext=(5, 5), textcoords="offset points", fontsize=6, fontweight="bold",
                     arrowprops=dict(arrowstyle="-", color="gray", lw=0.5))
    ax3.set_xlabel("PageRank")
    ax3.set_ylabel("Betweenness Centrality")
    ax3.set_title("PageRank vs Betweenness", fontsize=12, fontweight="bold")
    ax3.set_facecolor(COLORS["light_bg"])
    ax3.grid(alpha=0.3)
    plt.colorbar(sc, ax=ax3, label="PageRank", shrink=0.8)

    # Panel 4: Topluluk
    ax4 = fig.add_subplot(gs[1, 1])
    comm_sizes = sorted([len(c) for c in communities["louvain"]], reverse=True)
    comm_colors = plt.cm.Set3(np.linspace(0, 1, len(comm_sizes)))
    wedges, _, _ = ax4.pie(comm_sizes, labels=None, autopct=lambda p: f"{p:.1f}%" if p > 5 else "",
                            colors=comm_colors, startangle=90, pctdistance=0.75,
                            wedgeprops=dict(edgecolor="white", linewidth=2))
    ax4.add_patch(plt.Circle((0, 0), 0.55, fc="white"))
    ax4.text(0, 0, f"{len(communities['louvain'])}\nTopluluk", ha="center", va="center",
             fontsize=14, fontweight="bold", color=COLORS["dark"])
    ax4.set_title(f"Trafik Toplulukları — {config.name}", fontsize=12, fontweight="bold")
    leg_labels = [f"T{i+1} ({s} bölge)" for i, s in enumerate(comm_sizes[:8])]
    ax4.legend(wedges[:8], leg_labels, loc="center left", bbox_to_anchor=(0.85, 0.5), fontsize=7)

    plt.savefig(os.path.join(output_dir, "01_dashboard.png"), dpi=150, facecolor="white")
    plt.close()
    print("  [OK] 01_dashboard.png")

    # --- VIZ 2: Ağ Topolojisi ---
    fig, ax = plt.subplots(1, 1, figsize=(18, 14))
    fig.suptitle(f"{config.name} Trafik Ağı Topolojisi", fontsize=16, fontweight="bold")

    top_n = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:40]
    top_set = {n for n, _ in top_n}
    G_sub = G.subgraph(top_set).copy()

    # Koordinat varsa kullan, yoksa spring layout
    if config.coordinates and any(n in config.coordinates for n in G_sub.nodes()):
        pos = {}
        for n in G_sub.nodes():
            if n in config.coordinates:
                lat, lon = config.coordinates[n]
                pos[n] = (lon, lat)  # matplotlib x=lon, y=lat
            else:
                # Rastgele konumla
                pos[n] = (np.random.normal(29.0, 0.05), np.random.normal(41.0, 0.05))
    else:
        pos = nx.spring_layout(G_sub, k=2.5, iterations=80, weight="weight", seed=42)

    node_sizes = [scores.get(n, 0) * 30000 for n in G_sub.nodes()]

    # Topluluk renkleri
    node_comm = {}
    for i, comm in enumerate(communities["louvain"]):
        for n in comm:
            node_comm[n] = i
    node_colors = [node_comm.get(n, 0) for n in G_sub.nodes()]

    edge_ws = [G_sub[u][v]["weight"] for u, v in G_sub.edges()]
    max_w = max(edge_ws) if edge_ws else 1
    edge_widths = [0.5 + (w / max_w) * 3 for w in edge_ws]
    edge_alphas = [0.1 + (w / max_w) * 0.4 for w in edge_ws]

    for (u, v), width, alpha in zip(G_sub.edges(), edge_widths, edge_alphas):
        ax.annotate("", xy=pos[v], xytext=pos[u],
                    arrowprops=dict(arrowstyle="->", color="gray", alpha=alpha, lw=width,
                                   connectionstyle="arc3,rad=0.1"))

    nx.draw_networkx_nodes(G_sub, pos, ax=ax, node_size=node_sizes, node_color=node_colors,
                           cmap=plt.cm.Set2, alpha=0.85, edgecolors="white", linewidths=1.5)

    top15 = {n for n, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:15]}
    labels = {n: config.get_zone_name(n) for n in G_sub.nodes() if n in top15}
    nx.draw_networkx_labels(G_sub, pos, labels, ax=ax, font_size=6, font_weight="bold")

    ax.set_facecolor("#f0f2f5")
    ax.axis("off")

    plt.savefig(os.path.join(output_dir, "02_network_topology.png"), dpi=150, facecolor="white")
    plt.close()
    print("  [OK] 02_network_topology.png")

    # --- VIZ 3: Simülasyon Detay ---
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(f"Darboğaz Simülasyonu — {config.name}", fontsize=14, fontweight="bold", y=1.02)
    labels = ["Başlangıç"] + [n for _, n in simulation["removed_nodes"]]

    ax = axes[0]
    ax.bar([s-0.15 for s in steps], simulation["remaining_flow_pct"], 0.3, color=COLORS["danger"], alpha=0.8, label="Akış")
    ax.bar([s+0.15 for s in steps], simulation["remaining_edges_pct"], 0.3, color=COLORS["primary"], alpha=0.8, label="Kenar")
    ax.set_xticks(list(steps)); ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=6)
    ax.set_ylabel("%"); ax.set_title("Kalan Kapasite"); ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3)

    ax = axes[1]
    ax.plot(steps, simulation["num_components"], "D-", color=COLORS["warning"], lw=2, ms=10)
    ax.fill_between(steps, simulation["num_components"], alpha=0.2, color=COLORS["warning"])
    ax.set_xticks(list(steps)); ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=6)
    ax.set_ylabel("Bileşen"); ax.set_title("Ağ Parçalanması"); ax.grid(alpha=0.3)

    ax = axes[2]
    paths = [p if p != float("inf") else None for p in simulation["avg_path_length"]]
    vs = [(s, p) for s, p in zip(steps, paths) if p is not None]
    if vs:
        ax.plot([x[0] for x in vs], [x[1] for x in vs], "o-", color=COLORS["success"], lw=2, ms=8)
    ax.set_xticks(list(steps)); ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=6)
    ax.set_ylabel("Ort. En Kısa Yol"); ax.set_title("Ağ Verimliliği"); ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "03_simulation_detail.png"), dpi=150, facecolor="white")
    plt.close()
    print("  [OK] 03_simulation_detail.png")

    # --- VIZ 4: Merkezilik Heatmap ---
    fig, ax = plt.subplots(figsize=(14, 8))
    top20 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:20]
    top20_nodes = [n for n, _ in top20]
    top20_names = [config.get_zone_name(n) for n in top20_nodes]

    metrics = {
        "PageRank": [scores.get(n, 0) for n in top20_nodes],
        "Betweenness": [centrality["betweenness"].get(n, 0) for n in top20_nodes],
        "In-Degree": [centrality["in_degree"].get(n, 0) for n in top20_nodes],
        "Out-Degree": [centrality["out_degree"].get(n, 0) for n in top20_nodes],
    }
    for k in metrics:
        arr = np.array(metrics[k])
        mx = arr.max()
        metrics[k] = (arr / mx).tolist() if mx > 0 else arr.tolist()

    sns.heatmap(pd.DataFrame(metrics, index=top20_names), annot=True, fmt=".2f",
                cmap="YlOrRd", linewidths=0.5, ax=ax, cbar_kws={"label": "Normalize (0-1)"})
    ax.set_title(f"Merkezilik Karşılaştırması — {config.name}", fontsize=13, fontweight="bold")

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "04_centrality_heatmap.png"), dpi=150, facecolor="white")
    plt.close()
    print("  [OK] 04_centrality_heatmap.png")

    # --- VIZ 5: Dağılımlar ---
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(f"Ağ İstatistikleri — {config.name}", fontsize=14, fontweight="bold", y=1.02)

    weights = [d["weight"] for _, _, d in G.edges(data=True)]
    ax1.hist(weights, bins=50, color=COLORS["primary"], alpha=0.7, edgecolor="white")
    ax1.set_xlabel("Yolculuk Sayısı"); ax1.set_ylabel("Frekans")
    ax1.set_title("Kenar Ağırlık Dağılımı"); ax1.set_yscale("log"); ax1.grid(alpha=0.3)
    ax1.axvline(np.median(weights), color=COLORS["danger"], ls="--", label=f"Medyan: {np.median(weights):,.0f}")
    ax1.legend()

    degrees = [d for _, d in G.degree()]
    ax2.hist(degrees, bins=30, color=COLORS["success"], alpha=0.7, edgecolor="white")
    ax2.set_xlabel("Derece"); ax2.set_ylabel("Frekans"); ax2.set_title("Derece Dağılımı"); ax2.grid(alpha=0.3)
    ax2.axvline(np.mean(degrees), color=COLORS["danger"], ls="--", label=f"Ort: {np.mean(degrees):.1f}")
    ax2.legend()

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "05_distributions.png"), dpi=150, facecolor="white")
    plt.close()
    print("  [OK] 05_distributions.png")

    # --- VIZ 6 (BONUS): Coğrafi harita (koordinat varsa) ---
    if config.coordinates and len(config.coordinates) > 5:
        create_geo_visualization(G, scores, communities, config, output_dir)


def create_geo_visualization(G, scores, communities, config, output_dir):
    """Coğrafi koordinatlar üzerinde çizge görselleştirmesi."""
    fig, ax = plt.subplots(figsize=(16, 12))
    fig.suptitle(f"{config.name} — Coğrafi Trafik Haritası", fontsize=16, fontweight="bold")

    # Topluluk renkleri
    node_comm = {}
    for i, comm in enumerate(communities["louvain"]):
        for n in comm:
            node_comm[n] = i
    cmap = plt.cm.Set2
    n_comms = max(node_comm.values()) + 1 if node_comm else 1

    # Düğümleri çiz
    for node in G.nodes():
        if node not in config.coordinates:
            continue
        lat, lon = config.coordinates[node]
        pr = scores.get(node, 0)
        size = max(pr * 50000, 30)
        comm_id = node_comm.get(node, 0)
        color = cmap(comm_id / max(n_comms, 1))

        ax.scatter(lon, lat, s=size, c=[color], alpha=0.7, edgecolors="white", linewidths=1, zorder=3)

        # Sadece yüksek PR skorlu düğümleri etiketle
        sorted_pr = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top10_ids = {n for n, _ in sorted_pr[:10]}
        if node in top10_ids:
            ax.annotate(config.get_zone_name(node), xy=(lon, lat),
                       xytext=(5, 5), textcoords="offset points",
                       fontsize=6, fontweight="bold",
                       bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.8))

    # Kenarları çiz (top ağırlıklı)
    edge_data = [(u, v, G[u][v]["weight"]) for u, v in G.edges()
                 if u in config.coordinates and v in config.coordinates]
    edge_data.sort(key=lambda x: x[2], reverse=True)
    max_w = edge_data[0][2] if edge_data else 1

    for u, v, w in edge_data[:200]:  # En yoğun 200 kenar
        lat1, lon1 = config.coordinates[u]
        lat2, lon2 = config.coordinates[v]
        alpha = 0.05 + (w / max_w) * 0.3
        width = 0.3 + (w / max_w) * 2
        ax.annotate("", xy=(lon2, lat2), xytext=(lon1, lat1),
                    arrowprops=dict(arrowstyle="->", color="gray", alpha=alpha, lw=width))

    ax.set_xlabel("Boylam (Longitude)")
    ax.set_ylabel("Enlem (Latitude)")
    ax.set_facecolor("#f0f2f5")
    ax.grid(alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "06_geo_map.png"), dpi=150, facecolor="white")
    plt.close()
    print("  [OK] 06_geo_map.png (Coğrafi harita)")


# =============================================================================
# SONUÇ RAPORU
# =============================================================================
def print_report(G, scores, communities, simulation, config):
    top5 = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"""
{'='*70}
  SONUÇ RAPORU — {config.name}
{'='*70}
  Ağ: {G.number_of_nodes()} düğüm, {G.number_of_edges()} kenar
  Toplam yolculuk: {sum(d['weight'] for _,_,d in G.edges(data=True)):,}

  En Kritik 5 Bölge:
    1. {config.get_zone_name(top5[0][0]):<35} ({top5[0][1]:.5f})
    2. {config.get_zone_name(top5[1][0]):<35} ({top5[1][1]:.5f})
    3. {config.get_zone_name(top5[2][0]):<35} ({top5[2][1]:.5f})
    4. {config.get_zone_name(top5[3][0]):<35} ({top5[3][1]:.5f})
    5. {config.get_zone_name(top5[4][0]):<35} ({top5[4][1]:.5f})

  Simülasyon:
    İlk düğüm çıkarıldığında akış kaybı: {100 - simulation['remaining_flow_pct'][1]:.1f}%
    5 düğüm çıkarıldığında akış kaybı:   {100 - simulation['remaining_flow_pct'][-1]:.1f}%
    Ağ parçalanması: {simulation['num_components'][0]} → {simulation['num_components'][-1]} bileşen

  Topluluk: {len(communities['louvain'])} adet
""")


# =============================================================================
# MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Generic Trafik Darboğazı Analizi")
    parser.add_argument("--city", type=str, default="istanbul", help="Şehir (istanbul/nyc/ankara)")
    parser.add_argument("--config", type=str, default=None, help="JSON config dosyası (opsiyonel)")
    parser.add_argument("--data", type=str, default=None, help="Gerçek veri dosyası (CSV/Parquet)")
    parser.add_argument("--trips", type=int, default=2_000_000, help="Sentetik yolculuk sayısı")
    parser.add_argument("--top-k", type=int, default=5, help="Simülasyonda çıkarılacak düğüm")
    args = parser.parse_args()

    # Config yükle
    if args.config:
        config = load_config_from_json(args.config)
    else:
        config = get_city_config(args.city)

    print(f"\n{'█'*70}")
    print(f"  TRAFİK DARBOĞAZI ANALİZİ — {config.name.upper()}")
    print(f"{'█'*70}")
    print(config.summary())

    city_slug = slugify(config.name)
    RESULTS_DIR = ROOT_DIR / "results" / city_slug
    os.makedirs(RESULTS_DIR, exist_ok=True)
    data_file = RESULTS_DIR / f"{city_slug}_trips.csv"

    if args.data:
        provided_path = Path(args.data)
        if not provided_path.is_absolute():
            provided_path = ROOT_DIR / provided_path
        if not provided_path.exists():
            raise FileNotFoundError(f"Veri dosyası bulunamadı: {provided_path}")
        data_file = provided_path
        print(f"\n[INFO] Harici veri kullanılacak: {data_file}")
    elif not os.path.exists(data_file):
        print(f"\n[!] Veri üretiliyor: {config.name}...")
        df = generate_trip_data(config, num_trips=args.trips)
        save_data(df, RESULTS_DIR, city_slug)

    t0 = time.time()

    edge_df = etl_pipeline(data_file, config)
    G = build_graph(edge_df, config)
    scores = run_pagerank(G, config)
    cent = compute_centrality(G)
    comms = detect_communities(G, config)
    sim = simulate_bottleneck(G, scores, config, top_k=args.top_k)
    create_visualizations(G, scores, cent, comms, sim, config, RESULTS_DIR)
    print_report(G, scores, comms, sim, config)

    print(f"  Toplam süre: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
