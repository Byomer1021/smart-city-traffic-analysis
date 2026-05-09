"""
═══════════════════════════════════════════════════════════════════════
  GNN Visualization: Embedding Görselleştirmesi
═══════════════════════════════════════════════════════════════════════
  128 boyutlu embedding'leri t-SNE ile 2 boyuta düşürür ve
  scatter plot olarak çizer. Benzer trafik karakterine sahip bölgeler
  haritada birbirine yakın görünür.

  Kullanım:
    python gnn_visualize.py \
      --embeddings results/<city>/embeddings.npz \
      --map-data   results/<city>/map_data.json \
      --output     results/<city>/07_gnn_embeddings.png
═══════════════════════════════════════════════════════════════════════
"""
import argparse
import json
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE


COLORS = {
    "primary": "#1a73e8", "danger": "#dc3545", "warning": "#ffc107",
    "success": "#28a745", "dark":   "#2c3e50", "bg":     "#f8f9fa",
    "muted":   "#94A3B8",
}


def visualize(embeddings_path: str, map_data_path: str, output_path: str,
              perplexity: int = 30, top_label_n: int = 15):
    print("═" * 65)
    print("  Node2Vec Embeddings → t-SNE Görselleştirmesi")
    print("═" * 65)

    # Embedding'leri yükle
    print(f"[1/4] Embedding'ler yükleniyor: {embeddings_path}")
    npz = np.load(embeddings_path, allow_pickle=True)
    nodes = list(npz["nodes"])
    embeddings = npz["embeddings"]
    print(f"      Düğüm: {len(nodes)}, Boyut: {embeddings.shape[1]}")

    # Map metadata'sını yükle
    print(f"[2/4] Map metadata yükleniyor: {map_data_path}")
    with open(map_data_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    meta = {n["id"]: n for n in data["nodes"]}
    city_name = data["city"]["name"]

    # t-SNE 2D
    print(f"[3/4] t-SNE 2D projeksiyonu (perplexity={perplexity})...")
    perp = min(perplexity, len(nodes) - 1)
    tsne = TSNE(n_components=2, perplexity=perp, random_state=42,
                init="pca", learning_rate="auto")
    coords_2d = tsne.fit_transform(embeddings)
    print(f"      Tamamlandı")

    # Görselleştirme — 2 panel
    print(f"[4/4] Plot oluşturuluyor...")
    fig, axes = plt.subplots(1, 2, figsize=(18, 9))
    fig.suptitle(
        f"Graph Neural Network — Node Embeddings ({city_name})",
        fontsize=16, fontweight="bold", color=COLORS["dark"], y=0.98,
    )

    # PANEL 1: Topluluğa göre renk
    ax = axes[0]
    communities = np.array([meta[n].get("community", 0) for n in nodes])
    n_comm = max(communities) + 1
    cmap = plt.cm.Set2

    for c in range(n_comm):
        mask = communities == c
        ax.scatter(coords_2d[mask, 0], coords_2d[mask, 1],
                   c=[cmap(c / max(n_comm, 1))], s=80,
                   alpha=0.75, edgecolors="white", linewidths=1,
                   label=f"Topluluk {c + 1} ({mask.sum()} bölge)")

    # Top-N etiket
    ranks = np.array([meta[n].get("rank", 999) for n in nodes])
    top_idx = np.argsort(ranks)[:top_label_n]
    for i in top_idx:
        ax.annotate(
            meta[nodes[i]].get("name", str(nodes[i])),
            xy=(coords_2d[i, 0], coords_2d[i, 1]),
            xytext=(6, 6), textcoords="offset points",
            fontsize=8, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.25", facecolor="white",
                      edgecolor="gray", alpha=0.85, linewidth=0.5),
        )

    ax.set_title("Topluluk yapısına göre renklendirme",
                 fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("t-SNE Boyut 1")
    ax.set_ylabel("t-SNE Boyut 2")
    ax.legend(loc="best", fontsize=9, framealpha=0.9)
    ax.grid(alpha=0.2)
    ax.set_facecolor(COLORS["bg"])

    # PANEL 2: PageRank'a göre renk (kritiklik)
    ax = axes[1]
    pageranks = np.array([meta[n].get("pagerank", 0) for n in nodes])
    sizes = 30 + (pageranks / pageranks.max()) * 250

    sc = ax.scatter(coords_2d[:, 0], coords_2d[:, 1],
                    c=pageranks, s=sizes, cmap="YlOrRd",
                    alpha=0.8, edgecolors="white", linewidths=1)

    for i in top_idx[:10]:
        ax.annotate(
            f"#{meta[nodes[i]].get('rank', '?')}",
            xy=(coords_2d[i, 0], coords_2d[i, 1]),
            ha="center", va="center", fontsize=8, fontweight="bold",
            color=COLORS["dark"],
        )

    ax.set_title("PageRank kritiklik skoru (boyut + renk)",
                 fontsize=13, fontweight="bold", pad=10)
    ax.set_xlabel("t-SNE Boyut 1")
    ax.set_ylabel("t-SNE Boyut 2")
    plt.colorbar(sc, ax=ax, label="PageRank", shrink=0.8)
    ax.grid(alpha=0.2)
    ax.set_facecolor(COLORS["bg"])

    # Yorum kutusu
    fig.text(
        0.5, 0.02,
        "Node2Vec → her bölge için 128-dim vektör → t-SNE ile 2D'ye düşürüldü.\n"
        "Aynı renkte ve birbirine yakın noktalar → benzer trafik patern'ine sahip bölgeler.",
        ha="center", fontsize=10, style="italic", color=COLORS["muted"],
    )

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()

    size_kb = os.path.getsize(output_path) / 1024
    print(f"      [OK] Kaydedildi: {output_path} ({size_kb:.0f} KB)")
    print("═" * 65)


def main():
    parser = argparse.ArgumentParser(description="t-SNE visualization of node embeddings")
    parser.add_argument("--embeddings", required=True, help=".npz embedding dosyası")
    parser.add_argument("--map-data", required=True, help="map_data.json")
    parser.add_argument("--output", required=True, help="Çıktı PNG dosyası")
    parser.add_argument("--perplexity", type=int, default=30, help="t-SNE perplexity")
    args = parser.parse_args()

    visualize(args.embeddings, args.map_data, args.output, args.perplexity)


if __name__ == "__main__":
    main()
