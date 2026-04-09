from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import AnalysisPaths, DEFAULT_MARKET_PROXY
from . import io


@dataclass(frozen=True)
class PanelBundle:
    prepared_long: pd.DataFrame
    asset_metadata: pd.DataFrame
    price_wide: pd.DataFrame
    market_cap_wide: pd.DataFrame
    volume_wide: pd.DataFrame
    simple_returns_wide: pd.DataFrame
    log_returns_wide: pd.DataFrame


def build_panel_bundle(prepared_long: pd.DataFrame) -> PanelBundle:
    metadata = io.build_asset_metadata(prepared_long)
    coin_order = metadata["coin_id"].tolist()

    price_wide = prepared_long.pivot(index="date_utc", columns="coin_id", values="price").sort_index()
    market_cap_wide = prepared_long.pivot(index="date_utc", columns="coin_id", values="market_cap").sort_index()
    volume_wide = prepared_long.pivot(index="date_utc", columns="coin_id", values="total_volume").sort_index()

    price_wide = price_wide.reindex(columns=coin_order)
    market_cap_wide = market_cap_wide.reindex(columns=coin_order)
    volume_wide = volume_wide.reindex(columns=coin_order)

    simple_returns_wide = price_wide.pct_change(fill_method=None)
    log_returns_wide = np.log(price_wide).diff()

    return PanelBundle(
        prepared_long=prepared_long,
        asset_metadata=metadata,
        price_wide=price_wide,
        market_cap_wide=market_cap_wide,
        volume_wide=volume_wide,
        simple_returns_wide=simple_returns_wide,
        log_returns_wide=log_returns_wide,
    )


def write_panel_bundle(paths: AnalysisPaths, bundle: PanelBundle) -> None:
    io.write_frame(paths.prepared_prices, bundle.prepared_long)
    io.write_frame(paths.asset_metadata, bundle.asset_metadata)
    io.write_wide_panel(paths.price_wide, bundle.price_wide)
    io.write_wide_panel(paths.market_cap_wide, bundle.market_cap_wide)
    io.write_wide_panel(paths.volume_wide, bundle.volume_wide)
    io.write_wide_panel(paths.simple_returns_wide, bundle.simple_returns_wide)
    io.write_wide_panel(paths.returns_wide, bundle.log_returns_wide)


def load_panel_bundle(paths: AnalysisPaths) -> PanelBundle:
    missing = [
        path
        for path in (
            paths.prepared_prices,
            paths.asset_metadata,
            paths.price_wide,
            paths.market_cap_wide,
            paths.volume_wide,
            paths.simple_returns_wide,
            paths.returns_wide,
        )
        if not path.exists()
    ]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"missing prepared analysis files, run prepare first: {joined}")

    return PanelBundle(
        prepared_long=io.read_prepared_prices(paths.prepared_prices),
        asset_metadata=io.read_asset_metadata(paths.asset_metadata),
        price_wide=io.read_wide_panel(paths.price_wide),
        market_cap_wide=io.read_wide_panel(paths.market_cap_wide),
        volume_wide=io.read_wide_panel(paths.volume_wide),
        simple_returns_wide=io.read_wide_panel(paths.simple_returns_wide),
        log_returns_wide=io.read_wide_panel(paths.returns_wide),
    )


def resolve_market_proxy_coin_id(
    proxy: str,
    asset_metadata: pd.DataFrame,
    available_coin_ids: list[str] | pd.Index,
) -> str:
    lowered = proxy.lower()
    available = {str(coin_id): str(coin_id) for coin_id in available_coin_ids}
    if proxy in available:
        return proxy

    aliases = {
        "btc": "bitcoin",
        "bitcoin": "bitcoin",
        "eth": "ethereum",
        "ethereum": "ethereum",
    }
    if lowered in aliases and aliases[lowered] in available:
        return aliases[lowered]

    symbol_lookup = {
        str(symbol).lower(): str(coin_id)
        for symbol, coin_id in zip(asset_metadata["coin_symbol"], asset_metadata["coin_id"], strict=False)
        if isinstance(symbol, str)
    }
    if lowered in symbol_lookup and symbol_lookup[lowered] in available:
        return symbol_lookup[lowered]

    raise ValueError(f"unable to resolve market proxy {proxy!r}")


def build_market_return(
    log_returns_wide: pd.DataFrame,
    market_cap_wide: pd.DataFrame,
    asset_metadata: pd.DataFrame,
    *,
    market_proxy: str = DEFAULT_MARKET_PROXY,
) -> pd.DataFrame:
    if market_proxy == "cap_weighted":
        lagged_caps = market_cap_wide.shift(1)
        weights = lagged_caps.div(lagged_caps.sum(axis=1), axis=0)
        market_return = log_returns_wide.mul(weights).sum(axis=1, min_count=1)
        effective_constituents = weights.notna().sum(axis=1)
    elif market_proxy == "equal_weighted":
        market_return = log_returns_wide.mean(axis=1, skipna=True)
        effective_constituents = log_returns_wide.notna().sum(axis=1)
    else:
        proxy_coin_id = resolve_market_proxy_coin_id(market_proxy, asset_metadata, log_returns_wide.columns)
        market_return = log_returns_wide[proxy_coin_id]
        effective_constituents = market_return.notna().astype(int)

    output = pd.DataFrame(
        {
            "date_utc": market_return.index,
            "market_proxy": market_proxy,
            "market_return": market_return.values,
            "effective_constituents": effective_constituents.values,
        }
    )
    output["date_utc"] = pd.to_datetime(output["date_utc"], utc=True, errors="coerce").dt.normalize()
    return output
