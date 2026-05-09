"""
═══════════════════════════════════════════════════════════════════════
  GNN Extension: Node2Vec Embeddings
═══════════════════════════════════════════════════════════════════════
  Mevcut trafik graf'ından her bölge için 128 boyutlu vektör üretir.
  Bu vektörlerle "hangi bölgeler birbirine benziyor" sorusunu cevaplar.

  Algoritma (Node2Vec — Grover & Leskovec 2016):
  1. Her node'dan biased random walk yap (length=80, num_walks=10)
  2. Walk'lar = "cümleler", node'lar = "kelimeler"
  3. Word2Vec (Skip-gram) ile 128-dim embedding öğren
  4. Cosine similarity ile node benzerliklerini hesapla

  Kullanım:
    pip install node2vec gensim networkx
    python gnn_node2vec.py --input results/<city>/map_data.json \
                           --output results/<city>/embeddings.npz
═══════════════════════════════════════════════════════════════════════
"""
import argparse
import json
import os
import time
import numpy as np
import networkx as nx
from node2vec import Node2Vec


def load_graph_from_map_data(json_path: str):
    """map_data.json'dan NetworkX graph oluştur."""
    print(f"[1/5] Map data okunuyor: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    G = nx.DiGraph()
    node_meta = {}

    for n in data["nodes"]:
        G.add_node(n["id"])
        node_meta[n["id"]] = {
            "name":      n["name"],
            "lat":       n.get("lat", 0),
            "lon":       n.get("lon", 0),
            "pagerank":  n.get("pagerank", 0),
            "community": n.get("community", 0),
            "rank":      n.get("rank", 999),
            "type":      n.get("zone_type", "normal"),
        }

    for e in data["edges"]:
        if "src" in e and "dst" in e:
            G.add_edge(e["src"], e["dst"], weight=e.get("weight", 1))

    print(f"      Düğüm: {G.number_of_nodes()} | Kenar: {G.number_of_edges()}")
    return G, node_meta, data["city"]


def train_node2vec(G: nx.DiGraph,
                   dimensions: int = 128,
                   walk_length: int = 80,
                   num_walks: int = 10,
                   p: float = 1.0,
                   q: float = 1.0,
                   workers: int = 4):
    """
    Node2Vec eğitimi.

    Parametre rehberi (Grover & Leskovec 2016):
      p = return parameter   → düşük: aynı node'a geri dönmeye eğilimli (BFS-like)
      q = in-out parameter   → düşük: uzaklara gitmeye eğilimli (DFS-like)
      p=1, q=1 → DeepWalk (uniform random walk)
    """
    print(f"[2/5] Node2Vec eğitimi başlıyor...")
    print(f"      dim={dimensions}, walks={num_walks}, length={walk_length}, p={p}, q={q}")

    t0 = time.time()
    node2vec = Node2Vec(
        G, dimensions=dimensions, walk_length=walk_length,
        num_walks=num_walks, p=p, q=q, workers=workers,
        weight_key="weight", quiet=True,
    )
    model = node2vec.fit(window=10, min_count=1, batch_words=4)
    elapsed = time.time() - t0

    print(f"      Eğitim tamam ({elapsed:.1f}s)")

    # Embedding matrisi oluştur
    nodes = list(G.nodes())
    emb_matrix = np.array([model.wv[str(n)] for n in nodes])
    print(f"      Embedding boyutu: {emb_matrix.shape}")

    return nodes, emb_matrix


def compute_similarity_matrix(embeddings: np.ndarray) -> np.ndarray:
    """Cosine similarity hesapla."""
    print(f"[3/5] Cosine similarity hesaplanıyor...")
    normed = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
    sim_matrix = normed @ normed.T
    return sim_matrix


def find_similar_zones(node_ids, sim_matrix, node_meta, top_n: int = 5):
    """Top PageRank zone'lar için en benzer node'ları bul."""
    print(f"[4/5] En kritik bölgelerin benzerleri hesaplanıyor...")

    # PageRank'a göre sırala
    sorted_by_pr = sorted(node_ids,
                          key=lambda n: node_meta[n]["pagerank"],
                          reverse=True)
    top_10 = sorted_by_pr[:10]

    similarity_results = {}
    for node_id in top_10:
        idx = node_ids.index(node_id)
        sims = sim_matrix[idx]
        # Kendisi hariç en benzerleri al
        sorted_idx = np.argsort(-sims)[1:top_n + 1]

        similar = []
        for sim_idx in sorted_idx:
            similar_id = node_ids[sim_idx]
            similar.append({
                "id": int(similar_id),
                "name": node_meta[similar_id]["name"],
                "similarity": float(sims[sim_idx]),
                "pagerank": node_meta[similar_id]["pagerank"],
            })

        similarity_results[int(node_id)] = {
            "name": node_meta[node_id]["name"],
            "rank": node_meta[node_id]["rank"],
            "similar": similar,
        }

    return similarity_results


def save_results(output_path: str, nodes, embeddings, similarity_matrix,
                 similar_zones, node_meta, city_info):
    """Sonuçları .npz + .json olarak kaydet."""
    print(f"[5/5] Sonuçlar kaydediliyor...")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    # NumPy array'ları .npz dosyasında sakla (kompakt)
    np.savez_compressed(
        output_path,
        nodes=np.array(nodes),
        embeddings=embeddings,
        similarity=similarity_matrix,
    )

    # Okunabilir JSON özeti
    json_path = output_path.replace(".npz", "_summary.json")
    summary = {
        "city": city_info,
        "stats": {
            "num_nodes": len(nodes),
            "embedding_dim": embeddings.shape[1],
        },
        "similar_zones_top10": similar_zones,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"      [OK] Embedding matrisi: {output_path}")
    print(f"      [OK] Özet JSON:          {json_path}")

    # Konsola top-3 örnek
    print(f"\n  ═══ EN KRİTİK 3 BÖLGENİN BENZERLERİ ═══")
    for i, (zone_id, info) in enumerate(list(similar_zones.items())[:3]):
        print(f"\n  #{info['rank']} {info['name']}")
        print(f"     Benzer bölgeler:")
        for s in info["similar"][:3]:
            print(f"       • {s['name']:<35} (sim: {s['similarity']:.3f})")


def main():
    parser = argparse.ArgumentParser(description="Node2Vec graph embeddings")
    parser.add_argument("--input", required=True,
                        help="Map data JSON yolu (örn: results/istanbul/map_data.json)")
    parser.add_argument("--output", required=True,
                        help="Çıktı .npz dosyası (örn: results/istanbul/embeddings.npz)")
    parser.add_argument("--dim", type=int, default=128, help="Embedding boyutu")
    parser.add_argument("--walks", type=int, default=10, help="Düğüm başına walk sayısı")
    parser.add_argument("--length", type=int, default=80, help="Walk uzunluğu")
    parser.add_argument("--p", type=float, default=1.0, help="Return parameter")
    parser.add_argument("--q", type=float, default=1.0, help="In-out parameter")
    args = parser.parse_args()

    print("═" * 65)
    print("  Node2Vec — Graph Embedding Pipeline")
    print("═" * 65)

    G, node_meta, city = load_graph_from_map_data(args.input)
    nodes, embeddings = train_node2vec(
        G, dimensions=args.dim, walk_length=args.length,
        num_walks=args.walks, p=args.p, q=args.q,
    )
    sim_matrix = compute_similarity_matrix(embeddings)
    similar_zones = find_similar_zones(nodes, sim_matrix, node_meta)
    save_results(args.output, nodes, embeddings, sim_matrix,
                 similar_zones, node_meta, city)

    print("\n" + "═" * 65)
    print("  GNN extension tamamlandı.")
    print("═" * 65)


if __name__ == "__main__":
    main()
