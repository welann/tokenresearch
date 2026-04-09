from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

from .config import DEFAULT_CORR_THRESHOLD
from .io import ensure_parent, read_asset_metadata, read_prepared_prices, read_wide_panel
from .panel import build_market_return


PAIR_KEY_SEPARATOR = "__"
DEFAULT_TOP_PAIR_COUNT = 36
DEFAULT_HEATMAP_COIN_COUNT = 18


def pair_key(coin_id_x: str, coin_id_y: str) -> str:
    left, right = sorted((str(coin_id_x), str(coin_id_y)))
    return f"{left}{PAIR_KEY_SEPARATOR}{right}"


def optional_frame(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (np.floating, float)):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    if isinstance(value, (np.integer, int)):
        return int(value)
    if value is pd.NA:
        return None
    if pd.isna(value):
        return None
    return value


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(_json_ready(payload), handle, ensure_ascii=False, indent=2)


def _lookup_metadata(asset_metadata: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {
        str(row.coin_id): {
            "coinId": str(row.coin_id),
            "symbol": str(row.coin_symbol),
            "name": str(row.coin_name),
            "marketCapRank": int(row.market_cap_rank),
        }
        for row in asset_metadata.itertuples(index=False)
    }


def _window_return(price_wide: pd.DataFrame, window: int) -> pd.Series:
    return price_wide.iloc[-1].div(price_wide.shift(window).iloc[-1]).sub(1.0)


def _annualized_vol(returns_wide: pd.DataFrame, window: int) -> pd.Series:
    return returns_wide.rolling(window=window, min_periods=window).std().iloc[-1].mul(np.sqrt(365.0))


def _latest_drawdown(price_wide: pd.DataFrame) -> pd.Series:
    running_peak = price_wide.cummax()
    return price_wide.iloc[-1].div(running_peak.iloc[-1]).sub(1.0)


def _series_records(frame: pd.DataFrame, *, value_columns: list[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        payload = {"date": pd.Timestamp(row.date_utc).strftime("%Y-%m-%d")}
        for column in value_columns:
            payload[column] = getattr(row, column)
        records.append(payload)
    return records


def _coin_summary_index(
    asset_metadata: pd.DataFrame,
    prepared_prices: pd.DataFrame,
    price_wide: pd.DataFrame,
    returns_wide: pd.DataFrame,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    latest_rows = (
        prepared_prices.sort_values(["coin_id", "date_utc"])
        .groupby("coin_id", sort=False)
        .tail(1)
        .set_index("coin_id")
    )
    ret_7d = _window_return(price_wide, 7)
    ret_30d = _window_return(price_wide, 30)
    ret_90d = _window_return(price_wide, 90)
    vol_30d = _annualized_vol(returns_wide, 30)
    latest_drawdown = _latest_drawdown(price_wide)

    summaries: list[dict[str, Any]] = []
    summary_by_coin: dict[str, dict[str, Any]] = {}
    for row in asset_metadata.itertuples(index=False):
        coin_id = str(row.coin_id)
        latest = latest_rows.loc[coin_id]
        summary = {
            "coinId": coin_id,
            "symbol": str(row.coin_symbol).upper(),
            "name": str(row.coin_name),
            "marketCapRank": int(row.market_cap_rank),
            "latestDate": pd.Timestamp(latest["date_utc"]).strftime("%Y-%m-%d"),
            "price": float(latest["price"]),
            "marketCap": float(latest["market_cap"]),
            "volume": float(latest["total_volume"]),
            "return7d": ret_7d.get(coin_id),
            "return30d": ret_30d.get(coin_id),
            "return90d": ret_90d.get(coin_id),
            "vol30d": vol_30d.get(coin_id),
            "latestDrawdown": latest_drawdown.get(coin_id),
        }
        summaries.append(summary)
        summary_by_coin[coin_id] = summary

    return summaries, summary_by_coin


def build_overview_payload(
    asset_metadata: pd.DataFrame,
    prepared_prices: pd.DataFrame,
    price_wide: pd.DataFrame,
    market_cap_wide: pd.DataFrame,
    returns_wide: pd.DataFrame,
    pairwise_correlation: pd.DataFrame,
    coin_summaries: list[dict[str, Any]],
    top_pair_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    latest_by_coin = prepared_prices.groupby("coin_id", sort=False).tail(1)
    latest_market_cap = float(latest_by_coin["market_cap"].sum())
    latest_volume = float(latest_by_coin["total_volume"].sum())
    latest_date = pd.Timestamp(prepared_prices["date_utc"].max()).strftime("%Y-%m-%d")
    start_date = pd.Timestamp(prepared_prices["date_utc"].min()).strftime("%Y-%m-%d")

    market_returns = build_market_return(
        returns_wide,
        market_cap_wide,
        asset_metadata,
        market_proxy="cap_weighted",
    )
    market_returns["market_index"] = np.exp(market_returns["market_return"].fillna(0.0).cumsum()) * 100.0
    market_index_series = [
        {
            "date": pd.Timestamp(row.date_utc).strftime("%Y-%m-%d"),
            "marketIndex": row.market_index,
            "marketReturn": row.market_return,
        }
        for row in market_returns.itertuples(index=False)
    ]

    market_return_30d = float(market_returns["market_index"].iloc[-1] / market_returns["market_index"].iloc[-31] - 1.0)
    breadth_30d = float(
        pd.Series([item["return30d"] for item in coin_summaries], dtype=float).gt(0).mean()
    )

    top_market_caps = sorted(coin_summaries, key=lambda item: item["marketCap"], reverse=True)[:12]
    top_returns = sorted(
        [item for item in coin_summaries if item["return30d"] is not None],
        key=lambda item: item["return30d"],
        reverse=True,
    )[:12]
    top_volatility = sorted(
        [item for item in coin_summaries if item["vol30d"] is not None],
        key=lambda item: item["vol30d"],
        reverse=True,
    )[:12]

    rank_lookup = asset_metadata.set_index("coin_id")["market_cap_rank"].to_dict()
    candidate_pairs = pairwise_correlation.copy()
    candidate_pairs["pairKey"] = candidate_pairs.apply(
        lambda row: pair_key(row["coin_id_x"], row["coin_id_y"]),
        axis=1,
    )
    candidate_pairs["absCorr"] = candidate_pairs["pearson_corr"].abs()
    candidate_pairs["rankScore"] = candidate_pairs["coin_id_x"].map(rank_lookup).fillna(999) + candidate_pairs[
        "coin_id_y"
    ].map(rank_lookup).fillna(999)
    candidate_pairs = candidate_pairs.loc[candidate_pairs["n_obs"] >= 180]
    candidate_pairs = candidate_pairs.sort_values(["rankScore", "absCorr"], ascending=[True, False])
    featured_pairs = []
    for row in candidate_pairs.itertuples(index=False):
        if len(featured_pairs) >= top_pair_count:
            break
        featured_pairs.append(
            {
                "pairKey": row.pairKey,
                "coinIdX": row.coin_id_x,
                "coinIdY": row.coin_id_y,
                "nObs": int(row.n_obs),
                "pearsonCorr": float(row.pearson_corr),
                "absCorr": float(row.absCorr),
            }
        )

    overview = {
        "summary": {
            "assetCount": int(asset_metadata["coin_id"].nunique()),
            "dateStart": start_date,
            "dateEnd": latest_date,
            "latestMarketCap": latest_market_cap,
            "latestVolume": latest_volume,
            "marketReturn30d": market_return_30d,
            "breadth30d": breadth_30d,
        },
        "marketIndexSeries": market_index_series,
        "featuredCoins": {
            "leadersByMarketCap": top_market_caps,
            "leadersByReturn30d": top_returns,
            "leadersByVol30d": top_volatility,
        },
        "featuredPairs": featured_pairs,
    }
    return overview, featured_pairs


def export_coin_details(
    out_dir: Path,
    prepared_prices: pd.DataFrame,
    returns_wide: pd.DataFrame,
    summary_by_coin: dict[str, dict[str, Any]],
) -> None:
    coins_dir = out_dir / "coins"
    write_json(coins_dir / "index.json", list(summary_by_coin.values()))

    for coin_id, coin_frame in prepared_prices.groupby("coin_id", sort=False):
        coin_frame = coin_frame.sort_values("date_utc").copy()
        price_series = coin_frame["price"]
        drawdown = price_series.div(price_series.cummax()).sub(1.0)

        frame = pd.DataFrame(
            {
                "date_utc": coin_frame["date_utc"].values,
                "price": coin_frame["price"].values,
                "marketCap": coin_frame["market_cap"].values,
                "volume": coin_frame["total_volume"].values,
                "logReturn": returns_wide[coin_id].reindex(coin_frame["date_utc"]).values,
                "drawdown": drawdown.values,
            }
        )
        payload = {
            "summary": summary_by_coin[coin_id],
            "series": _series_records(
                frame,
                value_columns=["price", "marketCap", "volume", "logReturn", "drawdown"],
            ),
        }
        write_json(coins_dir / f"{coin_id}.json", payload)


def _pair_relative_strength(series_x: pd.Series, series_y: pd.Series) -> pd.Series:
    aligned = pd.concat([series_x.rename("x"), series_y.rename("y")], axis=1).dropna()
    return (aligned["x"] - aligned["y"]).fillna(0.0).cumsum()


def _pair_ccf(series_x: pd.Series, series_y: pd.Series, max_lag: int = 14) -> list[dict[str, Any]]:
    aligned = pd.concat([series_x.rename("x"), series_y.rename("y")], axis=1).dropna()
    rows: list[dict[str, Any]] = []
    for lag in range(-max_lag, max_lag + 1):
        shifted = aligned["x"].shift(lag)
        joined = pd.concat([shifted.rename("x"), aligned["y"].rename("y")], axis=1).dropna()
        if len(joined) < 2:
            continue
        rows.append({"lag": lag, "value": joined["x"].corr(joined["y"]), "nObs": len(joined)})
    return rows


def export_pair_details(
    out_dir: Path,
    featured_pairs: list[dict[str, Any]],
    metadata_lookup: dict[str, dict[str, Any]],
    returns_wide: pd.DataFrame,
) -> None:
    pairs_dir = out_dir / "pairs"
    write_json(pairs_dir / "index.json", featured_pairs)

    windows = (30, 60, 90)
    for pair in featured_pairs:
        coin_id_x = pair["coinIdX"]
        coin_id_y = pair["coinIdY"]
        aligned = returns_wide[[coin_id_x, coin_id_y]].dropna()
        relative_strength = _pair_relative_strength(returns_wide[coin_id_x], returns_wide[coin_id_y])
        rolling_rows: list[dict[str, Any]] = []
        for window in windows:
            rolling = aligned[coin_id_x].rolling(window=window, min_periods=window).corr(aligned[coin_id_y]).dropna()
            for date_utc, value in rolling.items():
                rolling_rows.append(
                    {
                        "date": pd.Timestamp(date_utc).strftime("%Y-%m-%d"),
                        "window": window,
                        "value": value,
                    }
                )

        relative_strength_rows = [
            {"date": pd.Timestamp(date_utc).strftime("%Y-%m-%d"), "value": value}
            for date_utc, value in relative_strength.items()
        ]
        payload = {
            "summary": {
                **pair,
                "labelX": metadata_lookup[coin_id_x]["name"],
                "labelY": metadata_lookup[coin_id_y]["name"],
                "symbolX": metadata_lookup[coin_id_x]["symbol"].upper(),
                "symbolY": metadata_lookup[coin_id_y]["symbol"].upper(),
            },
            "rollingCorrelation": rolling_rows,
            "relativeStrength": relative_strength_rows,
            "ccf": _pair_ccf(returns_wide[coin_id_x], returns_wide[coin_id_y]),
        }
        write_json(pairs_dir / f"{pair['pairKey']}.json", payload)


def build_structure_payload(
    asset_metadata: pd.DataFrame,
    pairwise_correlation: pd.DataFrame,
    returns_wide: pd.DataFrame,
    *,
    heatmap_coin_count: int,
    corr_threshold: float,
) -> dict[str, Any]:
    top_coins = asset_metadata.sort_values("market_cap_rank").head(heatmap_coin_count)["coin_id"].tolist()
    filtered_returns = returns_wide[top_coins].dropna(axis=0, how="any")
    standardized = (filtered_returns - filtered_returns.mean()) / filtered_returns.std(ddof=0)
    standardized = standardized.loc[:, standardized.std(ddof=0) > 0]

    pca_components = min(5, standardized.shape[0], standardized.shape[1])
    pca = PCA(n_components=pca_components)
    pca.fit(standardized)
    pca_summary = [
        {
            "component": index + 1,
            "explainedVariance": pca.explained_variance_[index],
            "explainedVarianceRatio": pca.explained_variance_ratio_[index],
            "cumulativeRatio": float(np.cumsum(pca.explained_variance_ratio_)[index]),
        }
        for index in range(pca_components)
    ]

    pca_loadings = []
    for component_index in range(pca_components):
        for coin_id, loading in zip(standardized.columns, pca.components_[component_index], strict=False):
            pca_loadings.append(
                {
                    "component": component_index + 1,
                    "coinId": coin_id,
                    "loading": loading,
                }
            )

    pair_lookup = {
        pair_key(row.coin_id_x, row.coin_id_y): float(row.pearson_corr)
        for row in pairwise_correlation.itertuples(index=False)
    }
    heatmap_rows = []
    for coin_id_x in top_coins:
        for coin_id_y in top_coins:
            if coin_id_x == coin_id_y:
                value = 1.0
            else:
                value = pair_lookup.get(pair_key(coin_id_x, coin_id_y))
            heatmap_rows.append({"x": coin_id_x, "y": coin_id_y, "value": value})

    graph = nx.Graph()
    graph.add_nodes_from(asset_metadata["coin_id"].tolist())
    selected_edges = pairwise_correlation.loc[
        (pairwise_correlation["n_obs"] >= 180) & (pairwise_correlation["pearson_corr"].abs() >= corr_threshold)
    ]
    for row in selected_edges.itertuples(index=False):
        graph.add_edge(
            str(row.coin_id_x),
            str(row.coin_id_y),
            weight=float(abs(row.pearson_corr)),
        )

    degree = nx.degree_centrality(graph)
    betweenness = nx.betweenness_centrality(graph)
    try:
        eigenvector = nx.eigenvector_centrality(graph, weight="weight", max_iter=1_000)
    except Exception:
        eigenvector = {coin_id: 0.0 for coin_id in graph.nodes}

    centrality = [
        {
            "coinId": coin_id,
            "degreeCentrality": degree.get(coin_id, 0.0),
            "betweennessCentrality": betweenness.get(coin_id, 0.0),
            "eigenvectorCentrality": eigenvector.get(coin_id, 0.0),
        }
        for coin_id in asset_metadata["coin_id"].tolist()
    ]
    centrality = sorted(centrality, key=lambda item: item["degreeCentrality"], reverse=True)[:30]

    return {
        "heatmap": {
            "coinIds": top_coins,
            "matrix": heatmap_rows,
        },
        "pcaSummary": pca_summary,
        "pcaLoadings": pca_loadings,
        "centrality": centrality,
    }


def export_static_web_data(
    *,
    analysis_dir: str | Path,
    out_dir: str | Path,
    top_pair_count: int = DEFAULT_TOP_PAIR_COUNT,
    heatmap_coin_count: int = DEFAULT_HEATMAP_COIN_COUNT,
    corr_threshold: float = DEFAULT_CORR_THRESHOLD,
) -> Path:
    analysis_dir = Path(analysis_dir)
    out_dir = Path(out_dir)

    asset_metadata = read_asset_metadata(analysis_dir / "asset_metadata.csv")
    prepared_prices = read_prepared_prices(analysis_dir / "prepared_prices.csv")
    price_wide = read_wide_panel(analysis_dir / "price_wide.csv")
    market_cap_wide = read_wide_panel(analysis_dir / "market_cap_wide.csv")
    returns_wide = read_wide_panel(analysis_dir / "returns_wide.csv")
    pairwise_correlation = pd.read_csv(analysis_dir / "pairwise_correlation.csv")
    pairwise_correlation["n_obs"] = pd.to_numeric(pairwise_correlation["n_obs"], errors="coerce")
    pairwise_correlation["pearson_corr"] = pd.to_numeric(pairwise_correlation["pearson_corr"], errors="coerce")

    metadata_lookup = _lookup_metadata(asset_metadata)
    coin_summaries, summary_by_coin = _coin_summary_index(asset_metadata, prepared_prices, price_wide, returns_wide)
    overview, featured_pairs = build_overview_payload(
        asset_metadata,
        prepared_prices,
        price_wide,
        market_cap_wide,
        returns_wide,
        pairwise_correlation,
        coin_summaries,
        top_pair_count,
    )
    structure_payload = build_structure_payload(
        asset_metadata,
        pairwise_correlation,
        returns_wide,
        heatmap_coin_count=heatmap_coin_count,
        corr_threshold=corr_threshold,
    )

    latest_date = pd.Timestamp(prepared_prices["date_utc"].max()).strftime("%Y-%m-%d")
    manifest = {
        "generatedAt": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "analysisDate": latest_date,
        "assetCount": int(asset_metadata["coin_id"].nunique()),
        "featuredCoinIds": [item["coinId"] for item in sorted(coin_summaries, key=lambda row: row["marketCapRank"])[:6]],
        "featuredPairKeys": [item["pairKey"] for item in featured_pairs[:6]],
        "routes": ["overview", "coin", "pair", "structure"],
    }

    write_json(out_dir / "manifest.json", manifest)
    write_json(out_dir / "overview.json", overview)
    export_coin_details(out_dir, prepared_prices, returns_wide, summary_by_coin)
    export_pair_details(out_dir, featured_pairs, metadata_lookup, returns_wide)
    write_json(out_dir / "structure.json", structure_payload)
    return out_dir
