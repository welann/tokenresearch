import { Link } from "react-router-dom";

import { DataTable } from "../components/DataTable";
import { EChart } from "../components/EChart";
import { MetricCard } from "../components/MetricCard";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import { CoinSummary, OverviewPayload, overviewSchema } from "../data/schemas";
import { buildLineOption } from "../lib/chartOptions";
import { formatCurrency, formatNumber, formatPercent } from "../lib/format";
import { useAppShellContext } from "../app/AppShell";

export function OverviewPage() {
  const { manifest } = useAppShellContext();
  const overviewState = useJsonData("data/overview.json", overviewSchema);

  if (overviewState.status === "error") {
    return <StatusBlock status="error" message={overviewState.error} />;
  }
  if (overviewState.status !== "success") {
    return <StatusBlock status="loading" message="Loading overview board…" />;
  }

  const overview: OverviewPayload = overviewState.data;
  const marketIndexOption = buildLineOption({
    series: [
      {
        name: "Market Index",
        data: overview.marketIndexSeries.map((item) => [item.date, item.marketIndex]),
        color: "#0f766e",
        area: true,
      },
    ],
  });

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Overview</p>
          <h1>Read the market before you read a coin.</h1>
          <p className="hero-copy">
            This atlas condenses the exported CoinGecko panel into a static research interface:
            market pulse, strongest pairs, single-asset trails, and structure views.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">Generated {manifest?.analysisDate ?? "N/A"}</span>
          <span className="hero-badge">{overview.summary.assetCount} tracked assets</span>
          <span className="hero-badge">Static JSON, browser-safe footprint</span>
        </div>
      </section>

      <section className="metric-grid">
        <MetricCard
          label="Aggregate market cap"
          value={formatCurrency(overview.summary.latestMarketCap)}
          note="Latest summed market cap across exported assets."
        />
        <MetricCard
          label="Latest volume"
          value={formatCurrency(overview.summary.latestVolume)}
          note="Summed daily volume on the latest snapshot date."
        />
        <MetricCard
          label="Market return 30d"
          value={formatPercent(overview.summary.marketReturn30d)}
          note="Cap-weighted market proxy from the exported panel."
        />
        <MetricCard
          label="Breadth 30d"
          value={formatPercent(overview.summary.breadth30d)}
          note="Share of tracked assets with positive trailing 30d return."
        />
      </section>

      <Panel eyebrow="Pulse" title="Cap-Weighted Market Index">
        <EChart className="chart-surface" option={marketIndexOption} />
      </Panel>

      <div className="page-columns">
        <Panel eyebrow="Leaders" title="Largest Assets">
          <DataTable<CoinSummary>
            rows={overview.featuredCoins.leadersByMarketCap}
            columns={[
              {
                key: "coin",
                label: "Asset",
                render: (row) => <Link to={`/coin/${row.coinId}`}>{row.name}</Link>,
              },
              {
                key: "marketCapRank",
                label: "Rank",
                render: (row) => formatNumber(row.marketCapRank),
                sortValue: (row) => row.marketCapRank,
              },
              {
                key: "marketCap",
                label: "Market Cap",
                render: (row) => formatCurrency(row.marketCap),
                sortValue: (row) => row.marketCap,
              },
              {
                key: "return30d",
                label: "30d",
                render: (row) => formatPercent(row.return30d),
                sortValue: (row) => row.return30d,
              },
            ]}
          />
        </Panel>

        <Panel eyebrow="Momentum" title="Best 30d Performers">
          <DataTable<CoinSummary>
            rows={overview.featuredCoins.leadersByReturn30d}
            columns={[
              {
                key: "coin",
                label: "Asset",
                render: (row) => <Link to={`/coin/${row.coinId}`}>{row.name}</Link>,
              },
              {
                key: "return30d",
                label: "30d",
                render: (row) => formatPercent(row.return30d),
                sortValue: (row) => row.return30d,
              },
              {
                key: "price",
                label: "Price",
                render: (row) => formatCurrency(row.price, 2),
                sortValue: (row) => row.price,
              },
              {
                key: "vol30d",
                label: "Vol 30d",
                render: (row) => formatPercent(row.vol30d),
                sortValue: (row) => row.vol30d,
              },
            ]}
          />
        </Panel>
      </div>

      <div className="page-columns">
        <Panel eyebrow="Heat" title="Highest 30d Volatility">
          <DataTable<CoinSummary>
            rows={overview.featuredCoins.leadersByVol30d}
            columns={[
              {
                key: "coin",
                label: "Asset",
                render: (row) => <Link to={`/coin/${row.coinId}`}>{row.name}</Link>,
              },
              {
                key: "vol30d",
                label: "Vol 30d",
                render: (row) => formatPercent(row.vol30d),
                sortValue: (row) => row.vol30d,
              },
              {
                key: "return7d",
                label: "7d",
                render: (row) => formatPercent(row.return7d),
                sortValue: (row) => row.return7d,
              },
            ]}
          />
        </Panel>

        <Panel eyebrow="Pairs" title="Featured Relationship Grid">
          <DataTable<OverviewPayload["featuredPairs"][number]>
            rows={overview.featuredPairs}
            columns={[
              {
                key: "pair",
                label: "Pair",
                render: (row) => <Link to={`/pair/${row.pairKey}`}>{row.coinIdX} / {row.coinIdY}</Link>,
              },
              {
                key: "pearsonCorr",
                label: "Corr",
                render: (row) => formatNumber(row.pearsonCorr, 3),
                sortValue: (row) => row.pearsonCorr,
              },
              {
                key: "nObs",
                label: "Obs",
                render: (row) => formatNumber(row.nObs),
                sortValue: (row) => row.nObs,
              },
            ]}
          />
        </Panel>
      </div>
    </div>
  );
}
