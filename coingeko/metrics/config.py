from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DEFAULT_MIN_HISTORY_DAYS = 90
DEFAULT_MIN_OVERLAP_DAYS = 90
DEFAULT_ROLLING_WINDOWS = (30, 60, 90)
DEFAULT_RISK_WINDOWS = (7, 30, 90)
DEFAULT_CCF_MAX_LAG = 14
DEFAULT_GRANGER_MAX_LAG = 7
DEFAULT_CORR_THRESHOLD = 0.6
DEFAULT_DCC_CORR_THRESHOLD = 0.5
DEFAULT_DCC_MIN_OBS = 180
DEFAULT_MARKET_PROXY = "cap_weighted"
DEFAULT_MIN_COVERAGE_RATIO = 0.8
DEFAULT_PCA_COMPONENTS = 10


@dataclass(frozen=True)
class AnalysisPaths:
    out_dir: Path
    prepared_prices: Path
    asset_metadata: Path
    price_wide: Path
    market_cap_wide: Path
    volume_wide: Path
    simple_returns_wide: Path
    returns_wide: Path
    market_returns: Path
    pairwise_correlation: Path
    rolling_correlation: Path
    cointegration: Path
    ccf: Path
    granger: Path
    market_exposure: Path
    risk_metrics: Path
    pca_summary: Path
    pca_loadings: Path
    clustering_linkage: Path
    centrality: Path
    dcc_garch: Path

    @classmethod
    def from_out_dir(cls, out_dir: str | Path) -> "AnalysisPaths":
        base = Path(out_dir)
        return cls(
            out_dir=base,
            prepared_prices=base / "prepared_prices.csv",
            asset_metadata=base / "asset_metadata.csv",
            price_wide=base / "price_wide.csv",
            market_cap_wide=base / "market_cap_wide.csv",
            volume_wide=base / "volume_wide.csv",
            simple_returns_wide=base / "simple_returns_wide.csv",
            returns_wide=base / "returns_wide.csv",
            market_returns=base / "market_returns.csv",
            pairwise_correlation=base / "pairwise_correlation.csv",
            rolling_correlation=base / "rolling_correlation.csv",
            cointegration=base / "cointegration.csv",
            ccf=base / "ccf.csv",
            granger=base / "granger.csv",
            market_exposure=base / "market_exposure.csv",
            risk_metrics=base / "risk_metrics.csv",
            pca_summary=base / "pca_summary.csv",
            pca_loadings=base / "pca_loadings.csv",
            clustering_linkage=base / "clustering_linkage.csv",
            centrality=base / "centrality.csv",
            dcc_garch=base / "dcc_garch.csv",
        )
