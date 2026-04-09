from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .config import (
    AnalysisPaths,
    DEFAULT_CCF_MAX_LAG,
    DEFAULT_CORR_THRESHOLD,
    DEFAULT_DCC_CORR_THRESHOLD,
    DEFAULT_DCC_MIN_OBS,
    DEFAULT_GRANGER_MAX_LAG,
    DEFAULT_MARKET_PROXY,
    DEFAULT_MIN_COVERAGE_RATIO,
    DEFAULT_MIN_HISTORY_DAYS,
    DEFAULT_MIN_OVERLAP_DAYS,
    DEFAULT_PCA_COMPONENTS,
    DEFAULT_RISK_WINDOWS,
    DEFAULT_ROLLING_WINDOWS,
)
from . import dcc, io, leadlag, market_model, pairwise, panel, risk, structure, web_export
from .utils import parse_int_csv


def _load_pairwise_correlation_if_exists(paths: AnalysisPaths) -> pd.DataFrame | None:
    if not paths.pairwise_correlation.exists():
        return None

    frame = pd.read_csv(paths.pairwise_correlation)
    if frame.empty:
        return frame.reindex(columns=["coin_id_x", "coin_id_y", "n_obs", "pearson_corr"])
    frame["n_obs"] = pd.to_numeric(frame["n_obs"], errors="coerce")
    frame["pearson_corr"] = pd.to_numeric(frame["pearson_corr"], errors="coerce")
    return frame


def _write_or_empty(path: Path, fieldnames: list[str], rows) -> int:
    return io.write_row_dicts(path, fieldnames, rows)


def _select_candidates(
    returns_wide: pd.DataFrame,
    *,
    min_overlap_days: int,
    correlation_df: pd.DataFrame | None,
    abs_corr_threshold: float | None,
    max_pairs: int | None,
) -> list[pairwise.CandidatePair]:
    return pairwise.select_candidate_pairs(
        returns_wide,
        min_overlap_days=min_overlap_days,
        correlation_df=correlation_df,
        abs_corr_threshold=abs_corr_threshold,
        max_pairs=max_pairs,
    )


def cmd_prepare(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    universe = io.load_universe_csv(args.universe_csv)
    daily_prices = io.load_daily_csv(args.daily_csv)
    prepared_long = io.prepare_long_prices(
        universe,
        daily_prices,
        min_history_days=args.min_history_days,
    )
    bundle = panel.build_panel_bundle(prepared_long)
    panel.write_panel_bundle(paths, bundle)
    print(f"prepared {len(bundle.asset_metadata)} assets across {len(bundle.price_wide.index)} dates")
    print(paths.prepared_prices)


def cmd_correlation(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)

    pairwise_corr = pairwise.compute_pearson_correlation(bundle.log_returns_wide, args.min_overlap_days)
    io.write_frame(paths.pairwise_correlation, pairwise_corr)

    candidate_pairs = None
    if args.rolling_candidate_corr_threshold is not None or args.max_pairs is not None:
        candidate_pairs = _select_candidates(
            bundle.log_returns_wide,
            min_overlap_days=args.min_overlap_days,
            correlation_df=pairwise_corr,
            abs_corr_threshold=args.rolling_candidate_corr_threshold,
            max_pairs=args.max_pairs,
        )

    row_count = _write_or_empty(
        paths.rolling_correlation,
        ["date_utc", "coin_id_x", "coin_id_y", "window", "rolling_corr"],
        pairwise.iter_rolling_correlations(
            bundle.log_returns_wide,
            windows=args.windows,
            min_overlap_days=args.min_overlap_days,
            candidate_pairs=candidate_pairs,
        ),
    )
    print(f"wrote {len(pairwise_corr)} pairwise rows -> {paths.pairwise_correlation}")
    print(f"wrote {row_count} rolling rows -> {paths.rolling_correlation}")


def cmd_cointegration(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)
    correlation_df = _load_pairwise_correlation_if_exists(paths)
    candidate_pairs = None
    if args.candidate_corr_threshold is not None or args.max_pairs is not None:
        candidate_pairs = _select_candidates(
            bundle.log_returns_wide,
            min_overlap_days=args.min_overlap_days,
            correlation_df=correlation_df,
            abs_corr_threshold=args.candidate_corr_threshold,
            max_pairs=args.max_pairs,
        )
    output = pairwise.compute_cointegration(
        bundle.price_wide,
        min_overlap_days=args.min_overlap_days,
        use_log_price=not args.level_price,
        candidate_pairs=candidate_pairs,
    )
    io.write_frame(paths.cointegration, output)
    print(f"wrote {len(output)} rows -> {paths.cointegration}")


def cmd_ccf(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)
    correlation_df = _load_pairwise_correlation_if_exists(paths)
    candidates = _select_candidates(
        bundle.log_returns_wide,
        min_overlap_days=args.min_overlap_days,
        correlation_df=correlation_df,
        abs_corr_threshold=args.candidate_corr_threshold,
        max_pairs=args.max_pairs,
    )
    row_count = _write_or_empty(
        paths.ccf,
        ["coin_id_x", "coin_id_y", "lag", "ccf_value", "n_obs"],
        leadlag.iter_ccf_rows(
            bundle.log_returns_wide,
            candidate_pairs=candidates,
            max_lag=args.max_lag,
        ),
    )
    print(f"wrote {row_count} rows across {len(candidates)} candidate pairs -> {paths.ccf}")


def cmd_granger(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)
    correlation_df = _load_pairwise_correlation_if_exists(paths)
    candidates = _select_candidates(
        bundle.log_returns_wide,
        min_overlap_days=args.min_overlap_days,
        correlation_df=correlation_df,
        abs_corr_threshold=args.candidate_corr_threshold,
        max_pairs=args.max_pairs,
    )
    row_count = _write_or_empty(
        paths.granger,
        ["source_coin_id", "target_coin_id", "lag", "test_name", "statistic", "pvalue", "n_obs"],
        leadlag.iter_granger_rows(
            bundle.log_returns_wide,
            candidate_pairs=candidates,
            max_lag=args.max_lag,
            test_name=args.test_name,
        ),
    )
    print(f"wrote {row_count} rows across {len(candidates)} candidate pairs -> {paths.granger}")


def cmd_market_model(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)
    exposure, market_returns = market_model.compute_market_exposure(
        bundle.log_returns_wide,
        bundle.market_cap_wide,
        bundle.asset_metadata,
        market_proxy=args.market_proxy,
        min_history_days=args.min_history_days,
    )
    io.write_frame(paths.market_returns, market_returns)
    io.write_frame(paths.market_exposure, exposure)
    print(f"wrote {len(exposure)} rows -> {paths.market_exposure}")


def cmd_risk(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)
    output = risk.compute_risk_metrics(
        bundle.log_returns_wide,
        bundle.price_wide,
        windows=args.windows,
    )
    io.write_frame(paths.risk_metrics, output)
    print(f"wrote {len(output)} rows -> {paths.risk_metrics}")


def cmd_structure(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)
    structure_matrix = structure.build_structure_matrix(
        bundle.log_returns_wide,
        min_history_days=args.min_history_days,
        min_coverage_ratio=args.min_coverage_ratio,
    )
    pca_summary, pca_loadings = structure.compute_pca(structure_matrix, n_components=args.n_components)
    linkage_frame = structure.compute_clustering_linkage(structure_matrix, method=args.linkage_method)

    correlation_df = _load_pairwise_correlation_if_exists(paths)
    if correlation_df is None:
        correlation_df = pairwise.compute_pearson_correlation(bundle.log_returns_wide, args.min_overlap_days)
    centrality_frame = structure.compute_centrality(
        correlation_df,
        coin_ids=bundle.asset_metadata["coin_id"].tolist(),
        corr_threshold=args.corr_threshold,
        min_overlap_days=args.min_overlap_days,
    )

    io.write_frame(paths.pca_summary, pca_summary)
    io.write_frame(paths.pca_loadings, pca_loadings)
    io.write_frame(paths.clustering_linkage, linkage_frame)
    io.write_frame(paths.centrality, centrality_frame)
    print(f"wrote PCA, clustering, and centrality outputs to {paths.out_dir}")


def cmd_dcc(args: argparse.Namespace) -> None:
    paths = AnalysisPaths.from_out_dir(args.out_dir)
    bundle = panel.load_panel_bundle(paths)
    correlation_df = _load_pairwise_correlation_if_exists(paths)
    candidates = _select_candidates(
        bundle.log_returns_wide,
        min_overlap_days=args.min_overlap_days,
        correlation_df=correlation_df,
        abs_corr_threshold=args.candidate_corr_threshold,
        max_pairs=args.max_pairs,
    )
    standardized_residuals = dcc.fit_standardized_residuals(
        bundle.log_returns_wide,
        candidate_pairs=candidates,
        min_obs=args.min_overlap_days,
    )
    row_count = _write_or_empty(
        paths.dcc_garch,
        ["date_utc", "coin_id_x", "coin_id_y", "dcc_corr", "n_obs", "garch_spec", "dcc_alpha", "dcc_beta"],
        dcc.iter_dcc_rows(
            standardized_residuals,
            candidate_pairs=candidates,
            min_obs=args.min_overlap_days,
        ),
    )
    print(f"wrote {row_count} rows across {len(candidates)} candidate pairs -> {paths.dcc_garch}")


def cmd_web_export(args: argparse.Namespace) -> None:
    out_dir = web_export.export_static_web_data(
        analysis_dir=args.analysis_dir,
        out_dir=args.out_dir,
        top_pair_count=args.top_pairs,
        heatmap_coin_count=args.heatmap_coins,
        corr_threshold=args.corr_threshold,
    )
    print(f"exported static web data -> {out_dir}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CoinGecko metrics research pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="clean raw CSVs and build reusable panels")
    prepare_parser.add_argument("--universe-csv", required=True, help="path to universe Top300 csv")
    prepare_parser.add_argument("--daily-csv", required=True, help="path to daily market chart csv")
    prepare_parser.add_argument("--out-dir", default="analysis_out", help="output directory")
    prepare_parser.add_argument(
        "--min-history-days",
        type=int,
        default=DEFAULT_MIN_HISTORY_DAYS,
        help="minimum valid daily observations per asset",
    )
    prepare_parser.set_defaults(func=cmd_prepare)

    corr_parser = subparsers.add_parser("correlation", help="compute pairwise and rolling correlations")
    corr_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    corr_parser.add_argument(
        "--min-overlap-days",
        type=int,
        default=DEFAULT_MIN_OVERLAP_DAYS,
        help="minimum overlapping observations per pair",
    )
    corr_parser.add_argument(
        "--windows",
        type=parse_int_csv,
        default=DEFAULT_ROLLING_WINDOWS,
        help="comma-separated rolling windows, e.g. 30,60,90",
    )
    corr_parser.add_argument(
        "--rolling-candidate-corr-threshold",
        type=float,
        default=None,
        help="optional abs Pearson threshold for rolling correlation candidate pairs",
    )
    corr_parser.add_argument("--max-pairs", type=int, default=None, help="optional maximum pair count for rolling runs")
    corr_parser.set_defaults(func=cmd_correlation)

    coint_parser = subparsers.add_parser("cointegration", help="compute pairwise cointegration statistics")
    coint_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    coint_parser.add_argument(
        "--min-overlap-days",
        type=int,
        default=DEFAULT_DCC_MIN_OBS,
        help="minimum overlapping observations per pair",
    )
    coint_parser.add_argument(
        "--candidate-corr-threshold",
        type=float,
        default=None,
        help="optional abs Pearson correlation threshold for candidate screening",
    )
    coint_parser.add_argument("--max-pairs", type=int, default=None, help="optional maximum pair count")
    coint_parser.add_argument("--level-price", action="store_true", help="use raw prices instead of log prices")
    coint_parser.set_defaults(func=cmd_cointegration)

    ccf_parser = subparsers.add_parser("ccf", help="compute cross-correlation functions for candidate pairs")
    ccf_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    ccf_parser.add_argument(
        "--min-overlap-days",
        type=int,
        default=DEFAULT_MIN_OVERLAP_DAYS,
        help="minimum overlapping observations per pair",
    )
    ccf_parser.add_argument("--max-lag", type=int, default=DEFAULT_CCF_MAX_LAG, help="maximum absolute lag")
    ccf_parser.add_argument(
        "--candidate-corr-threshold",
        type=float,
        default=DEFAULT_CORR_THRESHOLD,
        help="minimum abs Pearson correlation for candidate screening",
    )
    ccf_parser.add_argument("--max-pairs", type=int, default=None, help="optional maximum pair count")
    ccf_parser.set_defaults(func=cmd_ccf)

    granger_parser = subparsers.add_parser("granger", help="compute Granger causality for candidate pairs")
    granger_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    granger_parser.add_argument(
        "--min-overlap-days",
        type=int,
        default=DEFAULT_DCC_MIN_OBS,
        help="minimum overlapping observations per pair",
    )
    granger_parser.add_argument("--max-lag", type=int, default=DEFAULT_GRANGER_MAX_LAG, help="maximum lag")
    granger_parser.add_argument(
        "--candidate-corr-threshold",
        type=float,
        default=DEFAULT_CORR_THRESHOLD,
        help="minimum abs Pearson correlation for candidate screening",
    )
    granger_parser.add_argument("--max-pairs", type=int, default=None, help="optional maximum pair count")
    granger_parser.add_argument(
        "--test-name",
        default="ssr_ftest",
        help="Granger test statistic to extract from statsmodels",
    )
    granger_parser.set_defaults(func=cmd_granger)

    market_parser = subparsers.add_parser("market-model", help="compute beta, R^2 and residual volatility")
    market_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    market_parser.add_argument(
        "--market-proxy",
        default=DEFAULT_MARKET_PROXY,
        help="cap_weighted, equal_weighted, btc, eth, or a coin_id",
    )
    market_parser.add_argument(
        "--min-history-days",
        type=int,
        default=DEFAULT_MIN_HISTORY_DAYS,
        help="minimum asset observations for regression",
    )
    market_parser.set_defaults(func=cmd_market_model)

    risk_parser = subparsers.add_parser("risk", help="compute rolling risk metrics and drawdown summaries")
    risk_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    risk_parser.add_argument(
        "--windows",
        type=parse_int_csv,
        default=DEFAULT_RISK_WINDOWS,
        help="comma-separated rolling windows, e.g. 7,30,90",
    )
    risk_parser.set_defaults(func=cmd_risk)

    structure_parser = subparsers.add_parser("structure", help="compute PCA, clustering, and network centrality")
    structure_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    structure_parser.add_argument(
        "--min-history-days",
        type=int,
        default=DEFAULT_MIN_HISTORY_DAYS,
        help="minimum asset observations before structure filtering",
    )
    structure_parser.add_argument(
        "--min-overlap-days",
        type=int,
        default=DEFAULT_MIN_OVERLAP_DAYS,
        help="minimum overlapping observations for graph edges",
    )
    structure_parser.add_argument(
        "--min-coverage-ratio",
        type=float,
        default=DEFAULT_MIN_COVERAGE_RATIO,
        help="minimum non-null ratio before using an asset in the structure matrix",
    )
    structure_parser.add_argument(
        "--corr-threshold",
        type=float,
        default=DEFAULT_CORR_THRESHOLD,
        help="abs Pearson threshold for graph edges",
    )
    structure_parser.add_argument(
        "--n-components",
        type=int,
        default=DEFAULT_PCA_COMPONENTS,
        help="maximum number of PCA components",
    )
    structure_parser.add_argument(
        "--linkage-method",
        default="average",
        help="scipy linkage method, e.g. average, complete, ward",
    )
    structure_parser.set_defaults(func=cmd_structure)

    dcc_parser = subparsers.add_parser("dcc", help="compute DCC-GARCH correlations on screened candidate pairs")
    dcc_parser.add_argument("--out-dir", default="analysis_out", help="prepared analysis directory")
    dcc_parser.add_argument(
        "--min-overlap-days",
        type=int,
        default=DEFAULT_DCC_MIN_OBS,
        help="minimum overlapping observations per pair and per GARCH fit",
    )
    dcc_parser.add_argument(
        "--candidate-corr-threshold",
        type=float,
        default=DEFAULT_DCC_CORR_THRESHOLD,
        help="minimum abs Pearson correlation for DCC candidate screening",
    )
    dcc_parser.add_argument("--max-pairs", type=int, default=None, help="optional maximum pair count")
    dcc_parser.set_defaults(func=cmd_dcc)

    web_export_parser = subparsers.add_parser("web-export", help="export slim JSON files for the static frontend")
    web_export_parser.add_argument(
        "--analysis-dir",
        default="analysis_out",
        help="directory containing prepared analysis CSV outputs",
    )
    web_export_parser.add_argument(
        "--out-dir",
        default="webapp/public/data",
        help="output directory for generated static JSON assets",
    )
    web_export_parser.add_argument(
        "--top-pairs",
        type=int,
        default=web_export.DEFAULT_TOP_PAIR_COUNT,
        help="number of featured pair detail files to export",
    )
    web_export_parser.add_argument(
        "--heatmap-coins",
        type=int,
        default=web_export.DEFAULT_HEATMAP_COIN_COUNT,
        help="number of top-ranked assets to include in the structure heatmap",
    )
    web_export_parser.add_argument(
        "--corr-threshold",
        type=float,
        default=DEFAULT_CORR_THRESHOLD,
        help="abs Pearson correlation threshold for centrality graph edges",
    )
    web_export_parser.set_defaults(func=cmd_web_export)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
