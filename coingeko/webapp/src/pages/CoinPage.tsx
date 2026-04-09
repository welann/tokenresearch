import { ChangeEvent, startTransition, useDeferredValue, useState } from "react";
import { Link, Navigate, useParams } from "react-router-dom";

import { EChart } from "../components/EChart";
import { MetricCard } from "../components/MetricCard";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import { CoinDetail, CoinSummary, coinDetailSchema, coinSummarySchema } from "../data/schemas";
import { buildBarOption, buildLineOption } from "../lib/chartOptions";
import { formatCurrency, formatPercent } from "../lib/format";
import { useAppShellContext } from "../app/AppShell";

export function CoinPage() {
  const { coinId } = useParams();
  const { manifest } = useAppShellContext();
  const coinIndexState = useJsonData("data/coins/index.json", coinSummarySchema.array());
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);
  const detailState = useJsonData(coinId ? `data/coins/${coinId}.json` : null, coinDetailSchema);

  if (!coinId && manifest?.featuredCoinIds[0]) {
    return <Navigate replace to={`/coin/${manifest.featuredCoinIds[0]}`} />;
  }

  if (coinIndexState.status === "error") {
    return <StatusBlock status="error" message={coinIndexState.error} />;
  }
  if (detailState.status === "error") {
    return <StatusBlock status="error" message={detailState.error} />;
  }
  if (coinIndexState.status !== "success" || detailState.status !== "success") {
    return <StatusBlock status="loading" message="Loading coin laboratory…" />;
  }

  const filteredCoins = coinIndexState.data.filter((coin: CoinSummary) => {
    const q = deferredQuery.trim().toLowerCase();
    if (!q) return true;
    return coin.name.toLowerCase().includes(q) || coin.symbol.toLowerCase().includes(q) || coin.coinId.includes(q);
  });
  const detail: CoinDetail = detailState.data;

  const priceOption = buildLineOption({
    series: [
      {
        name: "Price",
        data: detail.series.map((row) => [row.date, row.price]),
        color: "#0f766e",
        area: true,
      },
    ],
  });

  const volumeOption = buildBarOption({
    categories: detail.series.map((row) => row.date),
    values: detail.series.map((row) => row.volume),
    color: "#cc6b38",
  });

  const riskOption = buildLineOption({
    legend: true,
    series: [
      {
        name: "Log Return",
        data: detail.series.map((row) => [row.date, row.logReturn]),
        color: "#99ee4c",
      },
      {
        name: "Drawdown",
        data: detail.series.map((row) => [row.date, row.drawdown]),
        color: "#cc6b38",
      },
    ],
  });

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Coin Lab</p>
          <h1>{detail.summary.name}</h1>
          <p className="hero-copy">
            Inspect the single-asset trail: price path, liquidity rhythm, trailing returns, and
            drawdown regime. The left rail keeps the full export searchable.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">{detail.summary.symbol}</span>
          <span className="hero-badge">Rank {detail.summary.marketCapRank}</span>
          <span className="hero-badge">Latest {detail.summary.latestDate}</span>
        </div>
      </section>

      <section className="metric-grid">
        <MetricCard label="Price" value={formatCurrency(detail.summary.price, 2)} />
        <MetricCard label="Market Cap" value={formatCurrency(detail.summary.marketCap)} />
        <MetricCard label="30d Return" value={formatPercent(detail.summary.return30d)} />
        <MetricCard label="30d Vol" value={formatPercent(detail.summary.vol30d)} />
      </section>

      <div className="split-layout">
        <Panel
          eyebrow="Asset picker"
          title="Browse tracked assets"
          actions={
            <input
              className="search-input"
              onChange={(event: ChangeEvent<HTMLInputElement>) => {
                const nextValue = event.target.value;
                startTransition(() => setQuery(nextValue));
              }}
              placeholder="Search coin name or symbol"
              value={query}
            />
          }
        >
          <div className="picker-list">
            {filteredCoins.map((coin) => (
              <Link
                className={`picker-item ${coin.coinId === detail.summary.coinId ? "active" : ""}`.trim()}
                key={coin.coinId}
                to={`/coin/${coin.coinId}`}
              >
                <p className="picker-title">
                  {coin.name} <span className="small-note">{coin.symbol}</span>
                </p>
                <p className="picker-meta">
                  Rank {coin.marketCapRank} · {formatPercent(coin.return30d)} 30d
                </p>
              </Link>
            ))}
          </div>
        </Panel>

        <div className="stack">
          <Panel eyebrow="Price trail" title="Daily Price Path">
            <EChart className="chart-surface" option={priceOption} />
          </Panel>

          <div className="page-columns">
            <Panel eyebrow="Liquidity" title="Daily Volume">
              <EChart className="chart-surface" option={volumeOption} />
            </Panel>

            <Panel eyebrow="Risk trail" title="Returns and Drawdown">
              <EChart className="chart-surface" option={riskOption} />
            </Panel>
          </div>
        </div>
      </div>
    </div>
  );
}
