from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm

from .panel import build_market_return


def compute_market_exposure(
    log_returns_wide: pd.DataFrame,
    market_cap_wide: pd.DataFrame,
    asset_metadata: pd.DataFrame,
    *,
    market_proxy: str,
    min_history_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    market_return_frame = build_market_return(
        log_returns_wide,
        market_cap_wide,
        asset_metadata,
        market_proxy=market_proxy,
    )
    market_series = market_return_frame.set_index("date_utc")["market_return"]

    rows: list[dict[str, float | int | str]] = []
    for coin_id in log_returns_wide.columns:
        aligned = pd.concat(
            [
                log_returns_wide[coin_id].rename("coin_return"),
                market_series.rename("market_return"),
            ],
            axis=1,
        ).dropna()
        if len(aligned) < min_history_days:
            continue

        if aligned["coin_return"].nunique() <= 1 or aligned["market_return"].nunique() <= 1:
            alpha = float(aligned["coin_return"].mean())
            beta = 0.0
            residuals = aligned["coin_return"] - alpha
            r_squared = np.nan
            adj_r_squared = np.nan
        else:
            exog = sm.add_constant(aligned["market_return"])
            model = sm.OLS(aligned["coin_return"], exog).fit()
            alpha = float(model.params["const"])
            beta = float(model.params["market_return"])
            residuals = model.resid
            r_squared = float(model.rsquared)
            adj_r_squared = float(model.rsquared_adj)

        residual_vol = float(residuals.std(ddof=1))
        rows.append(
            {
                "coin_id": coin_id,
                "beta": beta,
                "alpha": alpha,
                "n_obs": len(aligned),
                "market_proxy": market_proxy,
                "r_squared": r_squared,
                "adj_r_squared": adj_r_squared,
                "residual_vol": residual_vol,
                "residual_vol_annualized": residual_vol * np.sqrt(365.0),
            }
        )

    if not rows:
        output = pd.DataFrame(
            columns=[
                "coin_id",
                "beta",
                "alpha",
                "n_obs",
                "market_proxy",
                "r_squared",
                "adj_r_squared",
                "residual_vol",
                "residual_vol_annualized",
            ]
        )
        return output, market_return_frame

    output = pd.DataFrame(rows).sort_values(["coin_id"]).reset_index(drop=True)
    return output, market_return_frame
