#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CoinGecko: 拉取“当前市值 TopN”币种的 market_chart 数据，并保存到 CSV。

- universe: /coins/markets (尽量原样保存所有字段到 CSV)
- market_chart: /coins/{id}/market_chart (保存 prices / market_caps / total_volumes)
- points CSV: 原始点（API 返回多少点就存多少点）
- daily CSV: 日频版（UTC 天取当天最后一个点；points_in_day 记录当天点数）
- 控频：--min-interval
- 429/5xx：指数退避重试
- 断点续跑：--resume（依赖 done_coin_ids_*.txt + universe_*.csv）
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests

DEFAULT_BASE_URL = "https://api.coingecko.com/api/v3"
MAX_PER_PAGE = 250

POINT_FIELDS = [
    "run_id",
    "fetched_at_utc",
    "coin_id",
    "coin_symbol",
    "coin_name",
    "market_cap_rank",
    "vs_currency",
    "days",
    "ts_ms",
    "datetime_utc",
    "date_utc",
    "price",
    "market_cap",
    "total_volume",
]

DAILY_FIELDS = [
    "run_id",
    "fetched_at_utc",
    "coin_id",
    "coin_symbol",
    "coin_name",
    "market_cap_rank",
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

ERROR_FIELDS = [
    "run_id",
    "time_utc",
    "coin_id",
    "coin_symbol",
    "coin_name",
    "stage",
    "http_status",
    "error",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = max(0.0, float(min_interval))
        self._next_time = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        if now < self._next_time:
            time.sleep(self._next_time - now)
        self._next_time = time.monotonic() + self.min_interval

    def push_back(self, seconds: float) -> None:
        self._next_time = max(self._next_time, time.monotonic() + max(0.0, seconds))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def json_dumps_safe(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def to_scalar(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return v
    return json_dumps_safe(v)


def append_csv_rows(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, Any]]) -> int:
    ensure_parent(path)
    file_exists = path.exists()
    n = 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
            n += 1
    return n


def write_csv_rows(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, Any]]) -> int:
    ensure_parent(path)
    n = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
            n += 1
    return n


def request_json(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    rate_limiter: Optional[RateLimiter] = None,
    timeout: float = 30.0,
    max_retries: int = 8,
    backoff_base: float = 5.0,
    backoff_cap: float = 300.0,
) -> Any:
    last_exc: Optional[BaseException] = None

    for attempt in range(max_retries):
        if rate_limiter:
            rate_limiter.wait()

        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            last_exc = e
            wait = min(backoff_base * (2**attempt), backoff_cap)
            wait *= 0.7 + random.random() * 0.6
            if rate_limiter:
                rate_limiter.push_back(wait)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            return resp.json()

        retryable = resp.status_code in (429, 500, 502, 503, 504)
        if retryable and attempt < max_retries - 1:
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait = float(retry_after)
            else:
                wait = min(backoff_base * (2**attempt), backoff_cap)
            wait *= 0.7 + random.random() * 0.6
            if rate_limiter:
                rate_limiter.push_back(wait)
            time.sleep(wait)
            continue

        try:
            resp.raise_for_status()
        except Exception as e:
            last_exc = e
            break

    if last_exc:
        raise last_exc
    raise RuntimeError(f"request failed: {url}")


def fetch_top_n_markets(
    session: requests.Session,
    base_url: str,
    *,
    vs_currency: str,
    n: int,
    rate_limiter: RateLimiter,
    headers: Dict[str, str],
    timeout: float,
    max_retries: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page = 1
    remaining = n

    while remaining > 0:
        per_page = min(MAX_PER_PAGE, remaining)
        url = f"{base_url}/coins/markets"
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
        }
        data = request_json(
            session,
            url,
            params=params,
            headers=headers,
            rate_limiter=rate_limiter,
            timeout=timeout,
            max_retries=max_retries,
        )
        if not isinstance(data, list) or not data:
            break

        out.extend(data)
        remaining = n - len(out)
        page += 1

    return out[:n]


def fetch_market_chart(
    session: requests.Session,
    base_url: str,
    *,
    coin_id: str,
    vs_currency: str,
    days: str,
    rate_limiter: RateLimiter,
    headers: Dict[str, str],
    timeout: float,
    max_retries: int,
) -> Dict[str, Any]:
    url = f"{base_url}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days}
    data = request_json(
        session,
        url,
        params=params,
        headers=headers,
        rate_limiter=rate_limiter,
        timeout=timeout,
        max_retries=max_retries,
    )
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected response for {coin_id}: {type(data)}")
    return data


def merge_market_chart_points(mc: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    by_ts: Dict[int, Dict[str, Any]] = {}

    def upsert(series: Any, field: str) -> None:
        if not isinstance(series, list):
            return
        for item in series:
            if not (isinstance(item, (list, tuple)) and len(item) >= 2):
                continue
            ts_ms = int(item[0])
            val = item[1]
            row = by_ts.setdefault(ts_ms, {})
            row[field] = val

    upsert(mc.get("prices"), "price")
    upsert(mc.get("market_caps"), "market_cap")
    upsert(mc.get("total_volumes"), "total_volume")

    return by_ts


def build_point_rows(
    *,
    run_id: str,
    fetched_at_utc: str,
    coin: Dict[str, Any],
    vs_currency: str,
    days: str,
    by_ts: Dict[int, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    coin_id = str(coin.get("id", ""))

    for ts_ms in sorted(by_ts.keys()):
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        rows.append(
            {
                "run_id": run_id,
                "fetched_at_utc": fetched_at_utc,
                "coin_id": coin_id,
                "coin_symbol": str(coin.get("symbol", "")),
                "coin_name": str(coin.get("name", "")),
                "market_cap_rank": coin.get("market_cap_rank", ""),
                "vs_currency": vs_currency,
                "days": days,
                "ts_ms": ts_ms,
                "datetime_utc": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "date_utc": dt.date().isoformat(),
                "price": by_ts[ts_ms].get("price", ""),
                "market_cap": by_ts[ts_ms].get("market_cap", ""),
                "total_volume": by_ts[ts_ms].get("total_volume", ""),
            }
        )

    return rows


def downsample_daily(point_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # 每天取 UTC 当天“最后一个点”
    best: Dict[str, Dict[str, Any]] = {}
    counts: Dict[str, int] = {}

    for r in point_rows:
        day = str(r["date_utc"])
        counts[day] = counts.get(day, 0) + 1
        prev = best.get(day)
        if prev is None or int(r["ts_ms"]) > int(prev["ts_ms"]):
            best[day] = r

    out: List[Dict[str, Any]] = []
    for day in sorted(best.keys()):
        r = best[day]
        out.append(
            {
                "run_id": r["run_id"],
                "fetched_at_utc": r["fetched_at_utc"],
                "coin_id": r["coin_id"],
                "coin_symbol": r["coin_symbol"],
                "coin_name": r["coin_name"],
                "market_cap_rank": r["market_cap_rank"],
                "vs_currency": r["vs_currency"],
                "days": r["days"],
                "date_utc": day,
                "last_ts_ms": r["ts_ms"],
                "last_datetime_utc": r["datetime_utc"],
                "price": r["price"],
                "market_cap": r["market_cap"],
                "total_volume": r["total_volume"],
                "points_in_day": counts.get(day, 1),
            }
        )
    return out


def load_done_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    done: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                done.add(s)
    return done


def append_done_id(path: Path, coin_id: str) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as f:
        f.write(coin_id + "\n")


def read_universe_if_exists(path: Path) -> Optional[List[Dict[str, Any]]]:
    if not path.exists():
        return None
    coins: List[Dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # universe 里字段很多，这里只取后续要用的
            coin_id = (row.get("id") or "").strip()
            if not coin_id:
                continue
            coins.append(
                {
                    "id": coin_id,
                    "symbol": row.get("symbol", ""),
                    "name": row.get("name", ""),
                    "market_cap_rank": row.get("market_cap_rank", ""),
                }
            )
    return coins


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="coingecko_out", help="输出目录")
    p.add_argument("--vs-currency", default="usd", help="计价货币，如 usd/cny")
    p.add_argument("--days", default="365", help="market_chart days 参数（默认 365）")
    p.add_argument("--top-n", type=int, default=300, help="当前市值排名前 N（默认 300）")
    p.add_argument("--min-interval", type=float, default=1.5, help="两次请求最小间隔秒数（控频）")
    p.add_argument("--timeout", type=float, default=30.0, help="单次请求超时秒数")
    p.add_argument("--max-retries", type=int, default=8, help="429/5xx 重试次数")
    p.add_argument("--resume", action="store_true", help="断点续跑（会复用同一套输出文件）")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base url（默认 public api）")
    p.add_argument("--api-key", default="", help="可选：CoinGecko Pro key（会加 x-cg-pro-api-key 头）")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"top{args.top_n}_{args.vs_currency}_days{args.days}"
    universe_csv = out_dir / f"universe_{prefix}.csv"
    points_csv = out_dir / f"market_chart_points_{prefix}.csv"
    daily_csv = out_dir / f"market_chart_daily_{prefix}.csv"
    errors_csv = out_dir / f"errors_{prefix}.csv"
    done_txt = out_dir / f"done_coin_ids_{prefix}.txt"

    session = requests.Session()
    headers = {
        "Accept": "application/json",
        "User-Agent": "cg-topN-market-chart-downloader/1.0",
    }
    if args.api_key:
        headers["x-cg-pro-api-key"] = args.api_key

    rate = RateLimiter(args.min_interval)

    # run_id / fetched_at：用“本次启动时间”
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fetched_at_utc = utc_now_iso()

    # resume 优先复用 universe，避免 Top300 变化导致 universe 漂移
    coins = read_universe_if_exists(universe_csv) if args.resume else None
    if coins is None:
        coins = fetch_top_n_markets(
            session,
            args.base_url,
            vs_currency=args.vs_currency,
            n=args.top_n,
            rate_limiter=rate,
            headers=headers,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
        # 把 /coins/markets 返回的所有字段尽量原样落盘
        if coins:
            keys = sorted({k for c in coins for k in c.keys()})
            rows = []
            for c in coins:
                row = {"run_id": run_id, "fetched_at_utc": fetched_at_utc}
                for k in keys:
                    row[k] = to_scalar(c.get(k))
                rows.append(row)
            universe_fields = ["run_id", "fetched_at_utc"] + keys
            write_csv_rows(universe_csv, universe_fields, rows)

    done = load_done_ids(done_txt) if args.resume else set()

    total_points = 0
    total_daily = 0
    ok_coins = 0

    for idx, coin in enumerate(coins, start=1):
        coin_id = str(coin.get("id", "")).strip()
        if not coin_id:
            continue
        if args.resume and coin_id in done:
            continue

        try:
            mc = fetch_market_chart(
                session,
                args.base_url,
                coin_id=coin_id,
                vs_currency=args.vs_currency,
                days=args.days,
                rate_limiter=rate,
                headers=headers,
                timeout=args.timeout,
                max_retries=args.max_retries,
            )

            by_ts = merge_market_chart_points(mc)
            point_rows = build_point_rows(
                run_id=run_id,
                fetched_at_utc=fetched_at_utc,
                coin=coin,
                vs_currency=args.vs_currency,
                days=args.days,
                by_ts=by_ts,
            )
            daily_rows = downsample_daily(point_rows)

            total_points += append_csv_rows(points_csv, POINT_FIELDS, point_rows)
            total_daily += append_csv_rows(daily_csv, DAILY_FIELDS, daily_rows)

            append_done_id(done_txt, coin_id)
            done.add(coin_id)
            ok_coins += 1

            if idx % 10 == 0 or idx == len(coins):
                print(f"[{idx}/{len(coins)}] ok={ok_coins} points={total_points} daily={total_daily}")

        except Exception as e:
            http_status = ""
            resp = getattr(e, "response", None)
            if resp is not None:
                http_status = getattr(resp, "status_code", "")
            err_row = {
                "run_id": run_id,
                "time_utc": utc_now_iso(),
                "coin_id": coin_id,
                "coin_symbol": str(coin.get("symbol", "")),
                "coin_name": str(coin.get("name", "")),
                "stage": "market_chart",
                "http_status": http_status,
                "error": str(e),
            }
            append_csv_rows(errors_csv, ERROR_FIELDS, [err_row])
            print(f"[{idx}/{len(coins)}] ERROR {coin_id}: {e}")

    print("done")
    print(f"universe: {universe_csv}")
    print(f"points:   {points_csv}")
    print(f"daily:    {daily_csv}")
    print(f"errors:   {errors_csv}")
    print(f"done_ids: {done_txt}")


if __name__ == "__main__":
    main()
