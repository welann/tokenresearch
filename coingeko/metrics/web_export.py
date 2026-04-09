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
DEFAULT_HEATMAP_COIN_COUNT = 40
MAX_EXPORTED_TABLE_ROWS = 120_000


def pair_key(coin_id_x: str, coin_id_y: str) -> str:
    left, right = sorted((str(coin_id_x), str(coin_id_y)))
    return f"{left}{PAIR_KEY_SEPARATOR}{right}"


def optional_frame(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path, low_memory=False)


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


def _frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    output = frame.copy()
    for column in output.columns:
        if pd.api.types.is_datetime64_any_dtype(output[column]):
            output[column] = pd.to_datetime(output[column], utc=True, errors="coerce").dt.strftime("%Y-%m-%d")
    return output.replace({np.nan: None}).to_dict(orient="records")


def _table_ready_frame(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    if not isinstance(output.index, pd.RangeIndex):
        output = output.reset_index()
    return output


def _write_table_dataset(out_dir: Path, dataset_id: str, frame: pd.DataFrame) -> str:
    relative_path = Path("tables") / f"{dataset_id}.json"
    write_json(out_dir / relative_path, _frame_records(_table_ready_frame(frame)))
    return relative_path.as_posix()


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

    summaries.sort(key=lambda item: item["marketCapRank"])
    return summaries, summary_by_coin


def _build_pair_index(
    pairwise_correlation: pd.DataFrame,
    metadata_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in pairwise_correlation.itertuples(index=False):
        meta_x = metadata_lookup[str(row.coin_id_x)]
        meta_y = metadata_lookup[str(row.coin_id_y)]
        rows.append(
            {
                "pairKey": pair_key(str(row.coin_id_x), str(row.coin_id_y)),
                "coinIdX": str(row.coin_id_x),
                "coinIdY": str(row.coin_id_y),
                "labelX": meta_x["name"],
                "labelY": meta_y["name"],
                "symbolX": meta_x["symbol"].upper(),
                "symbolY": meta_y["symbol"].upper(),
                "marketCapRankX": meta_x["marketCapRank"],
                "marketCapRankY": meta_y["marketCapRank"],
                "rankScore": meta_x["marketCapRank"] + meta_y["marketCapRank"],
                "nObs": int(row.n_obs),
                "pearsonCorr": float(row.pearson_corr),
                "absCorr": float(abs(row.pearson_corr)),
            }
        )
    rows.sort(key=lambda item: (item["rankScore"], -item["absCorr"], item["pairKey"]))
    return rows


def _build_source_catalog_entries(
    *,
    analysis_dir: Path,
    exported_tables: dict[str, str],
    source_frames: dict[str, pd.DataFrame | None],
) -> list[dict[str, Any]]:
    registry = [
        {
            "id": "asset_metadata",
            "title": "Asset Metadata",
            "description": "Static asset identities and current ranks.",
            "file": "asset_metadata.csv",
            "viewer": "table",
            "category": "global",
        },
        {
            "id": "prepared_prices",
            "title": "Prepared Prices",
            "description": "Cleaned long-form daily panel. Available in Coin Lab and raw table view.",
            "file": "prepared_prices.csv",
            "viewer": "table",
            "category": "coin",
        },
        {
            "id": "price_wide",
            "title": "Price Panel",
            "description": "Wide daily price panel with one column per asset.",
            "file": "price_wide.csv",
            "viewer": "table",
            "category": "coin",
        },
        {
            "id": "market_cap_wide",
            "title": "Market Cap Panel",
            "description": "Wide daily market-cap panel with one column per asset.",
            "file": "market_cap_wide.csv",
            "viewer": "table",
            "category": "coin",
        },
        {
            "id": "volume_wide",
            "title": "Volume Panel",
            "description": "Wide daily total-volume panel with one column per asset.",
            "file": "volume_wide.csv",
            "viewer": "table",
            "category": "coin",
        },
        {
            "id": "returns_wide",
            "title": "Return Panel",
            "description": "Wide daily log-return panel. Used by Coin Lab, Pair Lab, and table view.",
            "file": "returns_wide.csv",
            "viewer": "table",
            "category": "coin",
        },
        {
            "id": "simple_returns_wide",
            "title": "Simple Return Panel",
            "description": "Wide daily arithmetic return panel with one column per asset.",
            "file": "simple_returns_wide.csv",
            "viewer": "table",
            "category": "coin",
        },
        {
            "id": "pairwise_correlation",
            "title": "Pairwise Correlation",
            "description": "Full Pearson correlation table for all eligible pairs.",
            "file": "pairwise_correlation.csv",
            "viewer": "table",
            "category": "pair",
        },
        {
            "id": "rolling_correlation",
            "title": "Rolling Correlation",
            "description": "Per-pair rolling correlation rows. Computed on demand in Pair Lab.",
            "file": "rolling_correlation.csv",
            "viewer": "pair",
            "category": "pair",
        },
        {
            "id": "market_exposure",
            "title": "Market Exposure",
            "description": "Beta, alpha, R-squared, and residual volatility.",
            "file": "market_exposure.csv",
            "viewer": "table",
            "category": "coin",
        },
        {
            "id": "risk_metrics",
            "title": "Risk Metrics",
            "description": "Rolling realized vol, downside semivariance, and drawdown summaries.",
            "file": "risk_metrics.csv",
            "viewer": "coin",
            "category": "coin",
        },
        {
            "id": "cointegration",
            "title": "Cointegration",
            "description": "Long-run pair relationship tests.",
            "file": "cointegration.csv",
            "viewer": "table",
            "category": "pair",
        },
        {
            "id": "ccf",
            "title": "Cross-Correlation Function",
            "description": "Lead-lag scan across pair lags. Computed in Pair Lab when absent.",
            "file": "ccf.csv",
            "viewer": "pair",
            "category": "pair",
        },
        {
            "id": "granger",
            "title": "Granger Causality",
            "description": "Directed pair test results by lag.",
            "file": "granger.csv",
            "viewer": "table",
            "category": "pair",
        },
        {
            "id": "dcc_garch",
            "title": "DCC-GARCH",
            "description": "Dynamic correlation series for screened pairs.",
            "file": "dcc_garch.csv",
            "viewer": "pair",
            "category": "pair",
        },
        {
            "id": "market_returns",
            "title": "Market Returns",
            "description": "Exported market proxy return series.",
            "file": "market_returns.csv",
            "viewer": "table",
            "category": "global",
        },
        {
            "id": "pca_summary",
            "title": "PCA Summary",
            "description": "Principal component explained variance output.",
            "file": "pca_summary.csv",
            "viewer": "table",
            "category": "structure",
        },
        {
            "id": "pca_loadings",
            "title": "PCA Loadings",
            "description": "Component loadings per asset.",
            "file": "pca_loadings.csv",
            "viewer": "table",
            "category": "structure",
        },
        {
            "id": "clustering_linkage",
            "title": "Clustering Linkage",
            "description": "Hierarchical clustering linkage matrix.",
            "file": "clustering_linkage.csv",
            "viewer": "table",
            "category": "structure",
        },
        {
            "id": "centrality",
            "title": "Centrality",
            "description": "Network centrality rankings across the correlation graph.",
            "file": "centrality.csv",
            "viewer": "table",
            "category": "structure",
        },
    ]

    catalog: list[dict[str, Any]] = []
    for item in registry:
        source_path = analysis_dir / item["file"]
        if not source_path.exists():
            continue
        frame = source_frames.get(item["id"])
        catalog_frame = None if frame is None else _table_ready_frame(frame)
        row_count = None if catalog_frame is None else int(len(catalog_frame))
        columns = [] if catalog_frame is None else list(catalog_frame.columns)
        catalog.append(
            {
                "id": item["id"],
                "title": item["title"],
                "description": item["description"],
                "category": item["category"],
                "viewer": item["viewer"],
                "rowCount": row_count,
                "columns": columns,
                "exportedPath": exported_tables.get(item["id"]),
                "sourceFile": item["file"],
            }
        )
    return catalog


def _build_overview_payload(
    asset_metadata: pd.DataFrame,
    prepared_prices: pd.DataFrame,
    market_cap_wide: pd.DataFrame,
    returns_wide: pd.DataFrame,
) -> dict[str, Any]:
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

    breadth_30d = float(
        _window_return(
            prepared_prices.pivot(index="date_utc", columns="coin_id", values="price").sort_index(),
            30,
        )
        .gt(0)
        .mean()
    )
    market_return_30d = float(market_returns["market_index"].iloc[-1] / market_returns["market_index"].iloc[-31] - 1.0)

    return {
        "summary": {
            "assetCount": int(asset_metadata["coin_id"].nunique()),
            "dateStart": start_date,
            "dateEnd": latest_date,
            "latestMarketCap": latest_market_cap,
            "latestVolume": latest_volume,
            "marketReturn30d": market_return_30d,
            "breadth30d": breadth_30d,
        },
        "marketIndexSeries": [
            {
                "date": pd.Timestamp(row.date_utc).strftime("%Y-%m-%d"),
                "marketIndex": row.market_index,
                "marketReturn": row.market_return,
            }
            for row in market_returns.itertuples(index=False)
        ],
    }


def _build_structure_payload(
    *,
    asset_metadata: pd.DataFrame,
    pairwise_correlation: pd.DataFrame,
    returns_wide: pd.DataFrame,
    pca_summary: pd.DataFrame | None,
    pca_loadings: pd.DataFrame | None,
    centrality: pd.DataFrame | None,
    clustering_linkage: pd.DataFrame | None,
    heatmap_coin_count: int,
    corr_threshold: float,
) -> dict[str, Any]:
    top_coins = asset_metadata.sort_values("market_cap_rank").head(heatmap_coin_count)["coin_id"].tolist()
    heatmap_lookup = {
        pair_key(row.coin_id_x, row.coin_id_y): float(row.pearson_corr)
        for row in pairwise_correlation.itertuples(index=False)
        if row.coin_id_x in top_coins and row.coin_id_y in top_coins
    }
    heatmap_rows = []
    for coin_id_x in top_coins:
        for coin_id_y in top_coins:
            value = 1.0 if coin_id_x == coin_id_y else heatmap_lookup.get(pair_key(coin_id_x, coin_id_y))
            heatmap_rows.append({"x": coin_id_x, "y": coin_id_y, "value": value})

    if pca_summary is None or pca_loadings is None or centrality is None:
        filtered_returns = returns_wide[top_coins].dropna(axis=0, how="any")
        standardized = (filtered_returns - filtered_returns.mean()) / filtered_returns.std(ddof=0)
        standardized = standardized.loc[:, standardized.std(ddof=0) > 0]
        pca_components = min(5, standardized.shape[0], standardized.shape[1])
        pca = PCA(n_components=pca_components)
        pca.fit(standardized)

        pca_summary = pd.DataFrame(
            {
                "component": range(1, pca_components + 1),
                "explained_variance": pca.explained_variance_,
                "explained_variance_ratio": pca.explained_variance_ratio_,
                "cumulative_ratio": np.cumsum(pca.explained_variance_ratio_),
            }
        )

        pca_loadings = pd.DataFrame(
            [
                {
                    "coin_id": coin_id,
                    "component": component_index + 1,
                    "loading": loading,
                }
                for component_index in range(pca_components)
                for coin_id, loading in zip(standardized.columns, pca.components_[component_index], strict=False)
            ]
        )

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
        centrality = pd.DataFrame(
            [
                {
                    "coin_id": coin_id,
                    "degree_centrality": degree.get(coin_id, 0.0),
                    "betweenness_centrality": betweenness.get(coin_id, 0.0),
                    "eigenvector_centrality": eigenvector.get(coin_id, 0.0),
                    "graph_type": f"pearson_abs_gte_{corr_threshold:g}",
                }
                for coin_id in asset_metadata["coin_id"].tolist()
            ]
        )

    return {
        "heatmapUniverse": top_coins,
        "heatmapRows": heatmap_rows,
        "pcaSummary": [
            {
                "component": int(row.component),
                "explainedVariance": float(
                    getattr(row, "explained_variance", getattr(row, "explainedVariance", np.nan))
                ),
                "explainedVarianceRatio": float(
                    getattr(row, "explained_variance_ratio", getattr(row, "explainedVarianceRatio", np.nan))
                ),
                "cumulativeRatio": float(getattr(row, "cumulative_ratio", getattr(row, "cumulativeRatio", np.nan))),
            }
            for row in pca_summary.itertuples(index=False)
        ],
        "pcaLoadings": [
            {
                "coinId": str(getattr(row, "coin_id", getattr(row, "coinId", ""))),
                "component": int(row.component),
                "loading": float(row.loading),
            }
            for row in pca_loadings.itertuples(index=False)
        ],
        "centrality": [
            {
                "coinId": str(getattr(row, "coin_id", getattr(row, "coinId", ""))),
                "degreeCentrality": float(
                    getattr(row, "degree_centrality", getattr(row, "degreeCentrality", np.nan))
                ),
                "betweennessCentrality": float(
                    getattr(row, "betweenness_centrality", getattr(row, "betweennessCentrality", np.nan))
                ),
                "eigenvectorCentrality": float(
                    getattr(row, "eigenvector_centrality", getattr(row, "eigenvectorCentrality", np.nan))
                ),
                "graphType": getattr(row, "graph_type", getattr(row, "graphType", "")),
            }
            for row in centrality.itertuples(index=False)
        ],
        "clusteringLinkage": [] if clustering_linkage is None else _frame_records(clustering_linkage),
    }


def _export_coin_details(
    out_dir: Path,
    prepared_prices: pd.DataFrame,
    returns_wide: pd.DataFrame,
    summary_by_coin: dict[str, dict[str, Any]],
    market_exposure: pd.DataFrame | None,
    risk_metrics: pd.DataFrame | None,
) -> None:
    coins_dir = out_dir / "coins"
    write_json(coins_dir / "index.json", list(summary_by_coin.values()))

    exposure_lookup = None
    if market_exposure is not None:
        exposure_lookup = {
            str(row.coin_id): {
                "beta": float(row.beta),
                "alpha": float(row.alpha),
                "nObs": int(row.n_obs),
                "marketProxy": str(row.market_proxy),
                "rSquared": _json_ready(row.r_squared),
                "adjRSquared": _json_ready(row.adj_r_squared),
                "residualVol": _json_ready(row.residual_vol),
                "residualVolAnnualized": _json_ready(row.residual_vol_annualized),
            }
            for row in market_exposure.itertuples(index=False)
        }

    risk_lookup: dict[str, list[dict[str, Any]]] = {}
    if risk_metrics is not None:
        risk_frame = risk_metrics.copy()
        if "date_utc" in risk_frame.columns:
            risk_frame["date_utc"] = pd.to_datetime(risk_frame["date_utc"], utc=True, errors="coerce")
        for coin_id, frame in risk_frame.groupby("coin_id", sort=False):
            risk_lookup[str(coin_id)] = [
                {
                    "date": None if pd.isna(row.date_utc) else pd.Timestamp(row.date_utc).strftime("%Y-%m-%d"),
                    "window": _json_ready(row.window),
                    "metricScope": str(row.metric_scope),
                    "realizedVol": _json_ready(row.realized_vol),
                    "downsideSemivariance": _json_ready(row.downside_semivariance),
                    "downsideSemivol": _json_ready(row.downside_semivol),
                    "mdd": _json_ready(row.mdd),
                    "mddStartDate": _json_ready(getattr(row, "mdd_start_date", None)),
                    "mddTroughDate": _json_ready(getattr(row, "mdd_trough_date", None)),
                    "latestDrawdown": _json_ready(row.latest_drawdown),
                }
                for row in frame.itertuples(index=False)
            ]

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
                "pointsInDay": coin_frame["points_in_day"].values,
                "logReturn": returns_wide[coin_id].reindex(coin_frame["date_utc"]).values,
                "drawdown": drawdown.values,
            }
        )
        payload = {
            "summary": summary_by_coin[coin_id],
            "series": _series_records(
                frame,
                value_columns=["price", "marketCap", "volume", "pointsInDay", "logReturn", "drawdown"],
            ),
            "exposure": None if exposure_lookup is None else exposure_lookup.get(coin_id),
            "riskRows": risk_lookup.get(coin_id, []),
        }
        write_json(coins_dir / f"{coin_id}.json", payload)


def _export_pair_data(
    out_dir: Path,
    pair_index: list[dict[str, Any]],
    cointegration: pd.DataFrame | None,
    granger: pd.DataFrame | None,
    dcc_garch: pd.DataFrame | None,
) -> None:
    pairs_dir = out_dir / "pairs"
    write_json(pairs_dir / "index.json", pair_index)

    if cointegration is not None and not cointegration.empty:
        frame = cointegration.copy()
        frame["pairKey"] = frame.apply(lambda row: pair_key(row["coin_id_x"], row["coin_id_y"]), axis=1)
        write_json(
            pairs_dir / "cointegration.json",
            _frame_records(frame.rename(columns={"coin_id_x": "coinIdX", "coin_id_y": "coinIdY"})),
        )

    if granger is not None and not granger.empty:
        write_json(
            pairs_dir / "granger.json",
            _frame_records(
                granger.rename(
                    columns={
                        "source_coin_id": "sourceCoinId",
                        "target_coin_id": "targetCoinId",
                    }
                )
            ),
        )

    if dcc_garch is not None and not dcc_garch.empty:
        frame = dcc_garch.copy()
        frame["pairKey"] = frame.apply(lambda row: pair_key(row["coin_id_x"], row["coin_id_y"]), axis=1)
        summary_rows = []
        dcc_dir = pairs_dir / "dcc"
        for pair_value, pair_frame in frame.groupby("pairKey", sort=False):
            pair_frame = pair_frame.sort_values("date_utc")
            summary_rows.append(
                {
                    "pairKey": pair_value,
                    "latestDcc": float(pair_frame.iloc[-1]["dcc_corr"]),
                    "meanDcc": float(pair_frame["dcc_corr"].mean()),
                    "minDcc": float(pair_frame["dcc_corr"].min()),
                    "maxDcc": float(pair_frame["dcc_corr"].max()),
                    "nRows": int(len(pair_frame)),
                }
            )
            write_json(
                dcc_dir / f"{pair_value}.json",
                _frame_records(
                    pair_frame.rename(
                        columns={
                            "date_utc": "date",
                            "coin_id_x": "coinIdX",
                            "coin_id_y": "coinIdY",
                            "dcc_corr": "dccCorr",
                            "garch_spec": "garchSpec",
                            "dcc_alpha": "dccAlpha",
                            "dcc_beta": "dccBeta",
                        }
                    )
                ),
            )
        write_json(pairs_dir / "dcc-summary.json", summary_rows)


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
    simple_returns_wide = read_wide_panel(analysis_dir / "simple_returns_wide.csv")
    volume_wide = read_wide_panel(analysis_dir / "volume_wide.csv")
    pairwise_correlation = pd.read_csv(analysis_dir / "pairwise_correlation.csv")
    pairwise_correlation["n_obs"] = pd.to_numeric(pairwise_correlation["n_obs"], errors="coerce")
    pairwise_correlation["pearson_corr"] = pd.to_numeric(pairwise_correlation["pearson_corr"], errors="coerce")

    market_exposure = optional_frame(analysis_dir / "market_exposure.csv")
    risk_metrics = optional_frame(analysis_dir / "risk_metrics.csv")
    cointegration = optional_frame(analysis_dir / "cointegration.csv")
    granger = optional_frame(analysis_dir / "granger.csv")
    dcc_garch = optional_frame(analysis_dir / "dcc_garch.csv")
    market_returns = optional_frame(analysis_dir / "market_returns.csv")
    pca_summary = optional_frame(analysis_dir / "pca_summary.csv")
    pca_loadings = optional_frame(analysis_dir / "pca_loadings.csv")
    clustering_linkage = optional_frame(analysis_dir / "clustering_linkage.csv")
    centrality = optional_frame(analysis_dir / "centrality.csv")

    metadata_lookup = _lookup_metadata(asset_metadata)
    coin_summaries, summary_by_coin = _coin_summary_index(asset_metadata, prepared_prices, price_wide, returns_wide)
    pair_index = _build_pair_index(pairwise_correlation, metadata_lookup)
    overview = _build_overview_payload(asset_metadata, prepared_prices, market_cap_wide, returns_wide)
    structure_payload = _build_structure_payload(
        asset_metadata=asset_metadata,
        pairwise_correlation=pairwise_correlation,
        returns_wide=returns_wide,
        pca_summary=pca_summary,
        pca_loadings=pca_loadings,
        centrality=centrality,
        clustering_linkage=clustering_linkage,
        heatmap_coin_count=heatmap_coin_count,
        corr_threshold=corr_threshold,
    )

    exported_tables: dict[str, str] = {}
    table_sources: dict[str, pd.DataFrame | None] = {
        "asset_metadata": asset_metadata,
        "prepared_prices": prepared_prices,
        "price_wide": price_wide,
        "market_cap_wide": market_cap_wide,
        "volume_wide": volume_wide,
        "returns_wide": returns_wide,
        "simple_returns_wide": simple_returns_wide,
        "pairwise_correlation": pairwise_correlation.rename(columns={"coin_id_x": "coinIdX", "coin_id_y": "coinIdY"}),
        "market_exposure": market_exposure,
        "cointegration": None
        if cointegration is None
        else cointegration.rename(columns={"coin_id_x": "coinIdX", "coin_id_y": "coinIdY"}),
        "granger": granger,
        "market_returns": market_returns,
        "pca_summary": pca_summary,
        "pca_loadings": pca_loadings,
        "clustering_linkage": clustering_linkage,
        "centrality": centrality,
    }
    for dataset_id, frame in table_sources.items():
        if frame is None or len(frame) > MAX_EXPORTED_TABLE_ROWS:
            continue
        exported_tables[dataset_id] = _write_table_dataset(out_dir, dataset_id, frame)

    source_catalog = _build_source_catalog_entries(
        analysis_dir=analysis_dir,
        exported_tables=exported_tables,
        source_frames={
            **table_sources,
            "rolling_correlation": None,
            "risk_metrics": risk_metrics,
            "ccf": optional_frame(analysis_dir / "ccf.csv"),
            "dcc_garch": dcc_garch,
        },
    )

    _export_coin_details(out_dir, prepared_prices, returns_wide, summary_by_coin, market_exposure, risk_metrics)
    _export_pair_data(out_dir, pair_index, cointegration, granger, dcc_garch)

    latest_date = pd.Timestamp(prepared_prices["date_utc"].max()).strftime("%Y-%m-%d")
    featured_pairs = [item["pairKey"] for item in pair_index[:top_pair_count]]
    manifest = {
        "generatedAt": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "analysisDate": latest_date,
        "assetCount": int(asset_metadata["coin_id"].nunique()),
        "featuredCoinIds": [item["coinId"] for item in coin_summaries[:6]],
        "featuredPairKeys": featured_pairs[:6],
        "routes": ["overview", "coin", "pair", "structure", "sources"],
        "availableSources": [item["id"] for item in source_catalog],
    }

    write_json(out_dir / "manifest.json", manifest)
    write_json(out_dir / "source-catalog.json", source_catalog)
    write_json(out_dir / "overview.json", overview)
    write_json(out_dir / "structure.json", structure_payload)
    return out_dir
