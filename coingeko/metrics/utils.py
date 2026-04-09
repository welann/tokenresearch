from __future__ import annotations

from itertools import combinations
from typing import Iterable, Iterator, Sequence, TypeVar


T = TypeVar("T")


def parse_int_csv(raw: str) -> tuple[int, ...]:
    values = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not values:
        raise ValueError("expected at least one integer value")
    return tuple(dict.fromkeys(values))


def iter_coin_pairs(coin_ids: Sequence[str]) -> Iterator[tuple[str, str]]:
    return combinations(coin_ids, 2)


def batched(items: Iterable[T], batch_size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
