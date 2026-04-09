from __future__ import annotations

import numpy as np
import pandas as pd


ROLLING_RISK_COLUMNS = [
    "coin_id",
    "date_utc",
    "window",
    "metric_scope",
    "realized_vol",
    "downside_semivariance",
    "downside_semivol",
    "mdd",
    "mdd_start_date",
    "mdd_trough_date",
    "latest_drawdown",
]


def _wide_metric_to_long(frame: pd.DataFrame, value_name: str, window: int) -> pd.DataFrame:
    long_frame = frame.stack().rename(value_name).reset_index()
    long_frame.columns = ["date_utc", "coin_id", value_name]
    long_frame["window"] = window
    return long_frame


def compute_risk_metrics(
    log_returns_wide: pd.DataFrame,
    price_wide: pd.DataFrame,
    *,
    windows: tuple[int, ...],
) -> pd.DataFrame:
    rolling_frames: list[pd.DataFrame] = []
    negative_returns = log_returns_wide.clip(upper=0)

    for window in windows:
        realized_vol = np.sqrt(log_returns_wide.pow(2).rolling(window=window, min_periods=window).sum())
        downside_semivariance = negative_returns.pow(2).rolling(window=window, min_periods=window).mean()
        downside_semivol = np.sqrt(downside_semivariance)

        merged = _wide_metric_to_long(realized_vol, "realized_vol", window)
        merged = merged.merge(
            _wide_metric_to_long(downside_semivariance, "downside_semivariance", window),
            on=["date_utc", "coin_id", "window"],
            how="outer",
        )
        merged = merged.merge(
            _wide_metric_to_long(downside_semivol, "downside_semivol", window),
            on=["date_utc", "coin_id", "window"],
            how="outer",
        )
        merged["metric_scope"] = "rolling"
        merged["mdd"] = np.nan
        merged["mdd_start_date"] = pd.NaT
        merged["mdd_trough_date"] = pd.NaT
        merged["latest_drawdown"] = np.nan
        rolling_frames.append(merged[ROLLING_RISK_COLUMNS])

    drawdown_rows: list[dict[str, object]] = []
    for coin_id in price_wide.columns:
        prices = price_wide[coin_id].dropna()
        if prices.empty:
            continue

        running_peak = prices.cummax()
        drawdown = prices.div(running_peak).sub(1.0)
        trough_date = drawdown.idxmin()
        peak_date = prices.loc[:trough_date].idxmax()
        drawdown_rows.append(
            {
                "coin_id": coin_id,
                "date_utc": pd.NaT,
                "window": pd.NA,
                "metric_scope": "drawdown_summary",
                "realized_vol": np.nan,
                "downside_semivariance": np.nan,
                "downside_semivol": np.nan,
                "mdd": float(drawdown.loc[trough_date]),
                "mdd_start_date": peak_date,
                "mdd_trough_date": trough_date,
                "latest_drawdown": float(drawdown.iloc[-1]),
            }
        )

    drawdown_frame = pd.DataFrame(drawdown_rows, columns=ROLLING_RISK_COLUMNS)
    return pd.concat([*rolling_frames, drawdown_frame], ignore_index=True)
