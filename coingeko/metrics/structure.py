from __future__ import annotations

import networkx as nx
import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import linkage
from scipy.spatial.distance import squareform
from sklearn.decomposition import PCA


def build_structure_matrix(
    log_returns_wide: pd.DataFrame,
    *,
    min_history_days: int,
    min_coverage_ratio: float,
) -> pd.DataFrame:
    coverage_days = log_returns_wide.notna().sum()
    eligible = coverage_days.loc[coverage_days >= min_history_days].index
    filtered = log_returns_wide.loc[:, eligible]
    if filtered.empty:
        raise ValueError("no assets remain after history filter")

    coverage_ratio = filtered.notna().mean()
    filtered = filtered.loc[:, coverage_ratio >= min_coverage_ratio]
    aligned = filtered.dropna(axis=0, how="any")
    if aligned.shape[0] < 2 or aligned.shape[1] < 2:
        raise ValueError("not enough common samples for structure analysis")

    stdev = aligned.std(ddof=0)
    aligned = aligned.loc[:, stdev > 0]
    if aligned.shape[1] < 2:
        raise ValueError("not enough non-constant assets for structure analysis")
    return aligned


def compute_pca(structure_matrix: pd.DataFrame, *, n_components: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    standardized = (structure_matrix - structure_matrix.mean()) / structure_matrix.std(ddof=0)
    effective_components = min(n_components, standardized.shape[0], standardized.shape[1])
    model = PCA(n_components=effective_components)
    model.fit(standardized)

    summary = pd.DataFrame(
        {
            "component": range(1, effective_components + 1),
            "explained_variance": model.explained_variance_,
            "explained_variance_ratio": model.explained_variance_ratio_,
            "cumulative_ratio": np.cumsum(model.explained_variance_ratio_),
        }
    )

    loadings = pd.DataFrame(
        model.components_.T,
        index=standardized.columns,
        columns=[f"PC{idx}" for idx in range(1, effective_components + 1)],
    )
    loadings = loadings.stack().rename("loading").reset_index()
    loadings.columns = ["coin_id", "component", "loading"]
    loadings["component"] = loadings["component"].str.removeprefix("PC").astype(int)
    return summary, loadings


def compute_clustering_linkage(structure_matrix: pd.DataFrame, *, method: str) -> pd.DataFrame:
    corr = structure_matrix.corr().clip(-1.0, 1.0)
    distance = np.sqrt(0.5 * (1.0 - corr))
    condensed = squareform(distance.values, checks=False)
    linkage_matrix = linkage(condensed, method=method)
    output = pd.DataFrame(linkage_matrix, columns=["left", "right", "distance", "cluster_size"])
    output.insert(0, "cluster_step", range(1, len(output) + 1))
    return output


def compute_centrality(
    pairwise_correlation: pd.DataFrame,
    *,
    coin_ids: list[str],
    corr_threshold: float,
    min_overlap_days: int,
) -> pd.DataFrame:
    graph = nx.Graph()
    graph.add_nodes_from(coin_ids)

    selected = pairwise_correlation.loc[
        (pairwise_correlation["n_obs"] >= min_overlap_days)
        & (pairwise_correlation["pearson_corr"].abs() >= corr_threshold)
    ]
    for row in selected.itertuples(index=False):
        graph.add_edge(
            str(row.coin_id_x),
            str(row.coin_id_y),
            weight=float(abs(row.pearson_corr)),
        )

    degree = nx.degree_centrality(graph)
    betweenness = nx.betweenness_centrality(graph, weight=None)
    try:
        eigenvector = nx.eigenvector_centrality(graph, weight="weight", max_iter=1_000)
    except Exception:
        eigenvector = {coin_id: 0.0 for coin_id in graph.nodes}

    graph_type = f"pearson_abs_gte_{corr_threshold:g}"
    rows = []
    for coin_id in coin_ids:
        rows.append(
            {
                "coin_id": coin_id,
                "degree_centrality": float(degree.get(coin_id, 0.0)),
                "betweenness_centrality": float(betweenness.get(coin_id, 0.0)),
                "eigenvector_centrality": float(eigenvector.get(coin_id, 0.0)),
                "graph_type": graph_type,
            }
        )

    return pd.DataFrame(rows).sort_values(["degree_centrality", "coin_id"], ascending=[False, True]).reset_index(
        drop=True
    )
