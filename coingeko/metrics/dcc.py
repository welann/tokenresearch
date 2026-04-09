from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from arch import arch_model
from scipy.optimize import minimize

from .pairwise import CandidatePair


GARCH_SPEC = "garch(1,1)+dcc(1,1)"


@dataclass(frozen=True)
class DccEstimate:
    alpha: float
    beta: float
    correlation: pd.Series


def fit_standardized_residuals(
    returns_wide: pd.DataFrame,
    *,
    candidate_pairs: list[CandidatePair],
    min_obs: int,
) -> dict[str, pd.Series]:
    needed_coin_ids = sorted(
        {pair.coin_id_x for pair in candidate_pairs}.union({pair.coin_id_y for pair in candidate_pairs})
    )
    fitted: dict[str, pd.Series] = {}
    for coin_id in needed_coin_ids:
        series = returns_wide[coin_id].dropna()
        if len(series) < min_obs:
            continue
        try:
            model = arch_model(
                series.mul(100.0),
                mean="Constant",
                vol="GARCH",
                p=1,
                q=1,
                dist="normal",
                rescale=False,
            )
            result = model.fit(disp="off")
        except Exception:
            continue

        std_resid = pd.Series(result.std_resid, index=series.index, name=coin_id).replace([np.inf, -np.inf], np.nan)
        std_resid = std_resid.dropna()
        if len(std_resid) >= min_obs:
            fitted[coin_id] = std_resid
    return fitted


def _dcc_neg_loglik(params: np.ndarray, z_values: np.ndarray, q_bar: np.ndarray) -> float:
    alpha, beta = params
    if alpha < 0 or beta < 0 or alpha + beta >= 0.999:
        return 1e12

    q_t = q_bar.copy()
    total = 0.0
    for idx in range(len(z_values)):
        if idx > 0:
            prev = z_values[idx - 1][:, None]
            q_t = (1.0 - alpha - beta) * q_bar + alpha * (prev @ prev.T) + beta * q_t
            q_t = (q_t + q_t.T) / 2.0

        diag = np.sqrt(np.clip(np.diag(q_t), 1e-12, None))
        inv_diag = np.diag(1.0 / diag)
        r_t = inv_diag @ q_t @ inv_diag
        sign, logdet = np.linalg.slogdet(r_t)
        if sign <= 0:
            return 1e12
        try:
            quad = float(z_values[idx] @ np.linalg.solve(r_t, z_values[idx]))
        except np.linalg.LinAlgError:
            return 1e12
        total += logdet + quad
    return 0.5 * total


def estimate_dcc(aligned_residuals: pd.DataFrame) -> DccEstimate:
    z_values = aligned_residuals.to_numpy(dtype=float, copy=True)
    z_values = z_values - z_values.mean(axis=0, keepdims=True)
    q_bar = np.cov(z_values.T, ddof=0)
    q_bar = (q_bar + q_bar.T) / 2.0

    result = minimize(
        _dcc_neg_loglik,
        x0=np.array([0.02, 0.95]),
        args=(z_values, q_bar),
        method="SLSQP",
        bounds=((1e-6, 0.5), (1e-6, 0.999)),
        constraints=({"type": "ineq", "fun": lambda x: 0.999 - x[0] - x[1]},),
    )
    if result.success:
        alpha, beta = float(result.x[0]), float(result.x[1])
    else:
        alpha, beta = 0.02, 0.95

    q_t = q_bar.copy()
    correlations = []
    for idx in range(len(z_values)):
        if idx > 0:
            prev = z_values[idx - 1][:, None]
            q_t = (1.0 - alpha - beta) * q_bar + alpha * (prev @ prev.T) + beta * q_t
            q_t = (q_t + q_t.T) / 2.0

        diag = np.sqrt(np.clip(np.diag(q_t), 1e-12, None))
        inv_diag = np.diag(1.0 / diag)
        r_t = inv_diag @ q_t @ inv_diag
        correlations.append(float(np.clip(r_t[0, 1], -1.0, 1.0)))

    return DccEstimate(
        alpha=alpha,
        beta=beta,
        correlation=pd.Series(correlations, index=aligned_residuals.index),
    )


def iter_dcc_rows(
    standardized_residuals: dict[str, pd.Series],
    *,
    candidate_pairs: list[CandidatePair],
    min_obs: int,
):
    for pair in candidate_pairs:
        if pair.coin_id_x not in standardized_residuals or pair.coin_id_y not in standardized_residuals:
            continue

        aligned = pd.concat(
            [
                standardized_residuals[pair.coin_id_x].rename(pair.coin_id_x),
                standardized_residuals[pair.coin_id_y].rename(pair.coin_id_y),
            ],
            axis=1,
        ).dropna()
        if len(aligned) < min_obs:
            continue

        try:
            estimate = estimate_dcc(aligned)
        except Exception:
            continue

        for date_utc, dcc_corr in estimate.correlation.items():
            yield {
                "date_utc": pd.Timestamp(date_utc).strftime("%Y-%m-%d"),
                "coin_id_x": pair.coin_id_x,
                "coin_id_y": pair.coin_id_y,
                "dcc_corr": dcc_corr,
                "n_obs": len(aligned),
                "garch_spec": GARCH_SPEC,
                "dcc_alpha": estimate.alpha,
                "dcc_beta": estimate.beta,
            }
