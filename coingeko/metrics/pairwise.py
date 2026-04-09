from __future__ import annotations

from dataclasses import dataclass
import warnings

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint

from .utils import iter_coin_pairs


@dataclass(frozen=True)
class CandidatePair:
    coin_id_x: str
    coin_id_y: str
    n_obs: int
    pearson_corr: float


def compute_pearson_correlation(returns_wide: pd.DataFrame, min_overlap_days: int) -> pd.DataFrame:
    if returns_wide.empty:
        return pd.DataFrame(columns=["coin_id_x", "coin_id_y", "n_obs", "pearson_corr"])

    valid = returns_wide.notna().astype(int)
    overlap = valid.T.dot(valid)
    corr = returns_wide.corr(method="pearson", min_periods=min_overlap_days)

    rows: list[dict[str, float | int | str]] = []
    columns = list(returns_wide.columns)
    for idx, coin_id_x in enumerate(columns):
        for coin_id_y in columns[idx + 1 :]:
            n_obs = int(overlap.loc[coin_id_x, coin_id_y])
            pearson_corr = corr.loc[coin_id_x, coin_id_y]
            if n_obs < min_overlap_days or pd.isna(pearson_corr):
                continue
            rows.append(
                {
                    "coin_id_x": coin_id_x,
                    "coin_id_y": coin_id_y,
                    "n_obs": n_obs,
                    "pearson_corr": float(pearson_corr),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["coin_id_x", "coin_id_y", "n_obs", "pearson_corr"])

    return pd.DataFrame(rows).sort_values(
        ["n_obs", "coin_id_x", "coin_id_y"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def select_candidate_pairs(
    returns_wide: pd.DataFrame,
    *,
    min_overlap_days: int,
    correlation_df: pd.DataFrame | None = None,
    abs_corr_threshold: float | None = None,
    max_pairs: int | None = None,
) -> list[CandidatePair]:
    correlation_df = correlation_df if correlation_df is not None else compute_pearson_correlation(
        returns_wide,
        min_overlap_days,
    )
    if correlation_df.empty:
        return []

    filtered = correlation_df.loc[correlation_df["n_obs"] >= min_overlap_days].copy()
    if abs_corr_threshold is not None:
        filtered = filtered.loc[filtered["pearson_corr"].abs() >= abs_corr_threshold]

    filtered = filtered.assign(abs_corr=filtered["pearson_corr"].abs())
    filtered = filtered.sort_values(["abs_corr", "n_obs"], ascending=[False, False])
    if max_pairs is not None:
        filtered = filtered.head(max_pairs)

    return [
        CandidatePair(
            coin_id_x=str(row.coin_id_x),
            coin_id_y=str(row.coin_id_y),
            n_obs=int(row.n_obs),
            pearson_corr=float(row.pearson_corr),
        )
        for row in filtered.itertuples(index=False)
    ]


def iter_rolling_correlations(
    returns_wide: pd.DataFrame,
    *,
    windows: tuple[int, ...],
    min_overlap_days: int,
    candidate_pairs: list[CandidatePair] | None = None,
):
    pairs = candidate_pairs or [
        CandidatePair(coin_id_x=coin_id_x, coin_id_y=coin_id_y, n_obs=0, pearson_corr=np.nan)
        for coin_id_x, coin_id_y in iter_coin_pairs(list(returns_wide.columns))
    ]

    for pair in pairs:
        aligned = returns_wide[[pair.coin_id_x, pair.coin_id_y]].dropna()
        if len(aligned) < min_overlap_days:
            continue

        series_x = aligned[pair.coin_id_x]
        series_y = aligned[pair.coin_id_y]
        for window in windows:
            if len(aligned) < window:
                continue
            rolling = series_x.rolling(window=window, min_periods=window).corr(series_y).dropna()
            for date_utc, value in rolling.items():
                yield {
                    "date_utc": pd.Timestamp(date_utc).strftime("%Y-%m-%d"),
                    "coin_id_x": pair.coin_id_x,
                    "coin_id_y": pair.coin_id_y,
                    "window": window,
                    "rolling_corr": float(value),
                }


def compute_cointegration(
    price_wide: pd.DataFrame,
    *,
    min_overlap_days: int,
    use_log_price: bool = True,
    candidate_pairs: list[CandidatePair] | None = None,
) -> pd.DataFrame:
    levels = np.log(price_wide.where(price_wide > 0)) if use_log_price else price_wide.copy()
    rows: list[dict[str, float | int | bool | str]] = []

    pairs = candidate_pairs or [
        CandidatePair(coin_id_x=coin_id_x, coin_id_y=coin_id_y, n_obs=0, pearson_corr=np.nan)
        for coin_id_x, coin_id_y in iter_coin_pairs(list(levels.columns))
    ]

    for pair in pairs:
        coin_id_x = pair.coin_id_x
        coin_id_y = pair.coin_id_y
        aligned = levels[[coin_id_x, coin_id_y]].dropna()
        n_obs = len(aligned)
        if n_obs < min_overlap_days:
            continue
        if aligned[coin_id_x].nunique() <= 1 or aligned[coin_id_y].nunique() <= 1:
            continue

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                with np.errstate(divide="ignore", invalid="ignore"):
                    test_stat, pvalue, critical_values = coint(
                        aligned[coin_id_x],
                        aligned[coin_id_y],
                        trend="c",
                        autolag="aic",
                    )
        except Exception:
            continue

        rows.append(
            {
                "coin_id_x": coin_id_x,
                "coin_id_y": coin_id_y,
                "n_obs": n_obs,
                "coint_t": float(test_stat),
                "pvalue": float(pvalue),
                "crit_1pct": float(critical_values[0]),
                "crit_5pct": float(critical_values[1]),
                "crit_10pct": float(critical_values[2]),
                "is_cointegrated_5pct": bool(pvalue < 0.05),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "coin_id_x",
                "coin_id_y",
                "n_obs",
                "coint_t",
                "pvalue",
                "crit_1pct",
                "crit_5pct",
                "crit_10pct",
                "is_cointegrated_5pct",
            ]
        )

    return pd.DataFrame(rows).sort_values(
        ["pvalue", "coin_id_x", "coin_id_y"],
        ascending=[True, True, True],
    ).reset_index(drop=True)
