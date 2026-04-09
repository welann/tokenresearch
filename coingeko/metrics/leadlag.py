from __future__ import annotations

import warnings

import pandas as pd
from statsmodels.tsa.stattools import grangercausalitytests

from .pairwise import CandidatePair


def iter_ccf_rows(
    returns_wide: pd.DataFrame,
    *,
    candidate_pairs: list[CandidatePair],
    max_lag: int,
):
    for pair in candidate_pairs:
        aligned = returns_wide[[pair.coin_id_x, pair.coin_id_y]].dropna()
        if aligned.empty:
            continue

        for lag in range(-max_lag, max_lag + 1):
            shifted = aligned[pair.coin_id_x].shift(lag)
            joined = pd.concat([shifted.rename("x"), aligned[pair.coin_id_y].rename("y")], axis=1).dropna()
            n_obs = len(joined)
            if n_obs < 2:
                continue

            yield {
                "coin_id_x": pair.coin_id_x,
                "coin_id_y": pair.coin_id_y,
                "lag": lag,
                "ccf_value": float(joined["x"].corr(joined["y"])),
                "n_obs": n_obs,
            }


def iter_granger_rows(
    returns_wide: pd.DataFrame,
    *,
    candidate_pairs: list[CandidatePair],
    max_lag: int,
    test_name: str,
):
    for pair in candidate_pairs:
        aligned = returns_wide[[pair.coin_id_x, pair.coin_id_y]].dropna()
        if len(aligned) <= max_lag + 2:
            continue

        for source_coin_id, target_coin_id in (
            (pair.coin_id_x, pair.coin_id_y),
            (pair.coin_id_y, pair.coin_id_x),
        ):
            test_frame = aligned[[target_coin_id, source_coin_id]]
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=FutureWarning)
                    results = grangercausalitytests(test_frame, maxlag=max_lag, verbose=False)
            except Exception:
                continue

            for lag, lag_output in results.items():
                test_result = lag_output[0].get(test_name)
                if test_result is None:
                    raise ValueError(f"unsupported Granger test name {test_name!r}")
                statistic, pvalue = test_result[:2]
                yield {
                    "source_coin_id": source_coin_id,
                    "target_coin_id": target_coin_id,
                    "lag": lag,
                    "test_name": test_name,
                    "statistic": float(statistic),
                    "pvalue": float(pvalue),
                    "n_obs": len(aligned),
                }
