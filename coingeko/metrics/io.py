from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype


PREPARED_PRICE_COLUMNS = [
    "coin_id",
    "coin_symbol",
    "coin_name",
    "market_cap_rank",
    "run_id",
    "fetched_at_utc",
    "vs_currency",
    "days",
    "date_utc",
    "last_ts_ms",
    "last_datetime_utc",
    "price",
    "market_cap",
    "total_volume",
    "points_in_day",
]

ASSET_METADATA_COLUMNS = [
    "coin_id",
    "coin_symbol",
    "coin_name",
    "market_cap_rank",
]


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _normalize_string_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].astype("string").str.strip()
    return frame


def _normalize_numeric_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def load_universe_csv(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame = frame.rename(
        columns={
            "id": "coin_id",
            "symbol": "coin_symbol",
            "name": "coin_name",
        }
    )
    if "coin_id" not in frame.columns:
        raise ValueError(f"missing required column coin_id in {path}")

    frame = _normalize_string_columns(frame, ["coin_id", "coin_symbol", "coin_name"])
    frame = _normalize_numeric_columns(frame, ["market_cap_rank"])
    frame = frame.loc[frame["coin_id"].notna() & frame["coin_id"].ne("")]
    frame = frame[ASSET_METADATA_COLUMNS].drop_duplicates(subset=["coin_id"])
    frame = frame.sort_values(["market_cap_rank", "coin_id"], na_position="last").reset_index(drop=True)
    return frame


def load_daily_csv(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    required = {"coin_id", "date_utc", "price"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"missing columns in {path}: {sorted(missing)}")

    frame = _normalize_string_columns(
        frame,
        [
            "run_id",
            "fetched_at_utc",
            "coin_id",
            "coin_symbol",
            "coin_name",
            "vs_currency",
            "days",
        ],
    )
    frame = _normalize_numeric_columns(
        frame,
        ["market_cap_rank", "last_ts_ms", "price", "market_cap", "total_volume", "points_in_day"],
    )
    frame["date_utc"] = pd.to_datetime(frame["date_utc"], utc=True, errors="coerce").dt.normalize()
    if "last_datetime_utc" in frame.columns:
        frame["last_datetime_utc"] = pd.to_datetime(frame["last_datetime_utc"], utc=True, errors="coerce")

    frame = frame.loc[frame["coin_id"].notna() & frame["date_utc"].notna()]
    frame = frame.sort_values(["coin_id", "date_utc", "last_ts_ms"], na_position="last").reset_index(drop=True)
    return frame


def build_asset_metadata(prepared_prices: pd.DataFrame) -> pd.DataFrame:
    metadata = prepared_prices[ASSET_METADATA_COLUMNS].copy()
    metadata = metadata.drop_duplicates(subset=["coin_id"])
    metadata = metadata.sort_values(["market_cap_rank", "coin_id"], na_position="last").reset_index(drop=True)
    return metadata


def prepare_long_prices(
    universe: pd.DataFrame,
    daily_prices: pd.DataFrame,
    *,
    min_history_days: int,
) -> pd.DataFrame:
    frame = daily_prices.copy()
    frame = frame.loc[frame["coin_id"].isin(universe["coin_id"])]
    frame = frame.drop_duplicates(subset=["coin_id", "date_utc"], keep="last")
    frame = frame.loc[frame["price"].gt(0)]

    metadata = universe.set_index("coin_id")
    frame = frame.join(metadata, on="coin_id", rsuffix="_meta")

    for column in ("coin_symbol", "coin_name"):
        meta_column = f"{column}_meta"
        if meta_column in frame.columns:
            frame[column] = frame[meta_column].astype("string").replace({"": pd.NA}).combine_first(
                frame[column].astype("string").replace({"": pd.NA})
            )
    if "market_cap_rank_meta" in frame.columns:
        frame["market_cap_rank"] = frame["market_cap_rank_meta"].combine_first(frame["market_cap_rank"])

    frame = frame.drop(columns=[column for column in frame.columns if column.endswith("_meta")])

    history = frame.groupby("coin_id")["date_utc"].nunique()
    eligible = history.loc[history >= min_history_days].index
    frame = frame.loc[frame["coin_id"].isin(eligible)].copy()
    frame["market_cap_rank"] = pd.to_numeric(frame["market_cap_rank"], errors="coerce").astype("Int64")
    frame["points_in_day"] = pd.to_numeric(frame["points_in_day"], errors="coerce").astype("Int64")
    frame = frame[PREPARED_PRICE_COLUMNS]
    frame = frame.sort_values(["market_cap_rank", "coin_id", "date_utc"], na_position="last").reset_index(drop=True)
    return frame


def _prepare_dates_for_write(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in output.columns:
        if column == "date_utc":
            output[column] = pd.to_datetime(output[column], utc=True, errors="coerce").dt.strftime("%Y-%m-%d")
        elif is_datetime64_any_dtype(output[column]):
            output[column] = pd.to_datetime(output[column], utc=True, errors="coerce").dt.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
    return output


def write_frame(path: str | Path, frame: pd.DataFrame) -> None:
    path = Path(path)
    ensure_parent(path)
    output = _prepare_dates_for_write(frame)
    output.to_csv(path, index=False)


def write_wide_panel(path: str | Path, frame: pd.DataFrame) -> None:
    path = Path(path)
    ensure_parent(path)
    output = frame.copy()
    output.index = pd.to_datetime(output.index, utc=True, errors="coerce").strftime("%Y-%m-%d")
    output.index.name = "date_utc"
    output.to_csv(path)


def read_frame(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path)


def read_prepared_prices(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame = _normalize_string_columns(
        frame,
        ["run_id", "fetched_at_utc", "coin_id", "coin_symbol", "coin_name", "vs_currency", "days"],
    )
    frame = _normalize_numeric_columns(
        frame,
        ["market_cap_rank", "last_ts_ms", "price", "market_cap", "total_volume", "points_in_day"],
    )
    frame["date_utc"] = pd.to_datetime(frame["date_utc"], utc=True, errors="coerce").dt.normalize()
    if "last_datetime_utc" in frame.columns:
        frame["last_datetime_utc"] = pd.to_datetime(frame["last_datetime_utc"], utc=True, errors="coerce")
    return frame.sort_values(["market_cap_rank", "coin_id", "date_utc"], na_position="last").reset_index(drop=True)


def read_asset_metadata(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame = _normalize_string_columns(frame, ["coin_id", "coin_symbol", "coin_name"])
    frame = _normalize_numeric_columns(frame, ["market_cap_rank"])
    return frame.sort_values(["market_cap_rank", "coin_id"], na_position="last").reset_index(drop=True)


def read_wide_panel(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path, index_col=0)
    frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce").normalize()
    frame.index.name = "date_utc"
    frame.columns = [str(column) for column in frame.columns]
    return frame.sort_index()


def write_row_dicts(path: str | Path, fieldnames: Sequence[str], rows: Iterable[dict[str, Any]]) -> int:
    path = Path(path)
    ensure_parent(path)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
            count += 1
    return count
