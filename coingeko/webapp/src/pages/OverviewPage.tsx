import { ChangeEvent, startTransition, useDeferredValue, useState } from "react";
import { Link } from "react-router-dom";

import { useAppShellContext } from "../app/AppShell";
import { DataTable } from "../components/DataTable";
import { EChart } from "../components/EChart";
import { MetricCard } from "../components/MetricCard";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import {
  CoinSummary,
  OverviewPayload,
  PairIndexItem,
  coinSummarySchema,
  overviewSchema,
  pairIndexItemSchema,
} from "../data/schemas";
import { buildLineOption } from "../lib/chartOptions";
import { formatCurrency, formatNumber, formatPercent } from "../lib/format";

type AssetSortKey = "marketCap" | "volume" | "return30d" | "return90d" | "vol30d";

const assetSorters: Record<AssetSortKey, (row: CoinSummary) => number | null> = {
  marketCap: (row) => row.marketCap,
  volume: (row) => row.volume,
  return30d: (row) => row.return30d,
  return90d: (row) => row.return90d,
  vol30d: (row) => row.vol30d,
};

export function OverviewPage() {
  const { manifest, sourceCatalog } = useAppShellContext();
  const overviewState = useJsonData("data/overview.json", overviewSchema);
  const coinIndexState = useJsonData("data/coins/index.json", coinSummarySchema.array());
  const pairIndexState = useJsonData("data/pairs/index.json", pairIndexItemSchema.array());

  const [assetQuery, setAssetQuery] = useState("");
  const [assetRankLimit, setAssetRankLimit] = useState(60);
  const [assetRowLimit, setAssetRowLimit] = useState(18);
  const [assetSort, setAssetSort] = useState<AssetSortKey>("marketCap");

  const [pairQuery, setPairQuery] = useState("");
  const [pairMinCorr, setPairMinCorr] = useState(0.75);
  const [pairMinObs, setPairMinObs] = useState(300);
  const [pairRankScore, setPairRankScore] = useState(40);
  const [pairRowLimit, setPairRowLimit] = useState(18);

  const deferredAssetQuery = useDeferredValue(assetQuery);
  const deferredPairQuery = useDeferredValue(pairQuery);

  if (overviewState.status === "error") {
    return <StatusBlock status="error" message={overviewState.error} />;
  }
  if (coinIndexState.status === "error") {
    return <StatusBlock status="error" message={coinIndexState.error} />;
  }
  if (pairIndexState.status === "error") {
    return <StatusBlock status="error" message={pairIndexState.error} />;
  }
  if (
    overviewState.status !== "success" ||
    coinIndexState.status !== "success" ||
    pairIndexState.status !== "success"
  ) {
    return <StatusBlock status="loading" message="Loading overview board…" />;
  }

  const overview: OverviewPayload = overviewState.data;
  const coinRows = coinIndexState.data;
  const pairRows = pairIndexState.data;

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

  const normalizedAssetQuery = deferredAssetQuery.trim().toLowerCase();
  const assetCandidates = coinRows.filter((coin) => {
    if (coin.marketCapRank > assetRankLimit) {
      return false;
    }
    if (!normalizedAssetQuery) {
      return true;
    }
    return (
      coin.name.toLowerCase().includes(normalizedAssetQuery) ||
      coin.symbol.toLowerCase().includes(normalizedAssetQuery) ||
      coin.coinId.includes(normalizedAssetQuery)
    );
  });

  const visibleAssets = [...assetCandidates]
    .sort((left, right) => {
      const leftValue = assetSorters[assetSort](left);
      const rightValue = assetSorters[assetSort](right);
      if (leftValue === null || leftValue === undefined) return 1;
      if (rightValue === null || rightValue === undefined) return -1;
      return rightValue - leftValue;
    })
    .slice(0, assetRowLimit);

  const normalizedPairQuery = deferredPairQuery.trim().toLowerCase();
  const pairCandidates = pairRows.filter((pair) => {
    if (pair.absCorr < pairMinCorr || pair.nObs < pairMinObs || pair.rankScore > pairRankScore) {
      return false;
    }
    if (!normalizedPairQuery) {
      return true;
    }
    return (
      pair.pairKey.includes(normalizedPairQuery) ||
      pair.coinIdX.includes(normalizedPairQuery) ||
      pair.coinIdY.includes(normalizedPairQuery) ||
      pair.labelX.toLowerCase().includes(normalizedPairQuery) ||
      pair.labelY.toLowerCase().includes(normalizedPairQuery) ||
      pair.symbolX.toLowerCase().includes(normalizedPairQuery) ||
      pair.symbolY.toLowerCase().includes(normalizedPairQuery)
    );
  });

  const visiblePairs = [...pairCandidates]
    .sort((left, right) => {
      if (right.absCorr !== left.absCorr) {
        return right.absCorr - left.absCorr;
      }
      return left.rankScore - right.rankScore;
    })
    .slice(0, pairRowLimit);

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Overview</p>
          <h1>Screen the whole export, not a hand-picked slice.</h1>
          <p className="hero-copy">
            The overview now starts from the full static indexes: every tracked asset, every
            exported pair summary, and every discoverable source file in the build.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">Generated {manifest?.analysisDate ?? "N/A"}</span>
          <span className="hero-badge">{overview.summary.assetCount} tracked assets</span>
          <span className="hero-badge">{pairRows.length} pair summaries</span>
          <span className="hero-badge">{sourceCatalog.length} visible sources</span>
        </div>
      </section>

      <section className="metric-grid">
        <MetricCard
          label="Aggregate market cap"
          value={formatCurrency(overview.summary.latestMarketCap)}
          note="Latest summed market cap across the prepared universe."
        />
        <MetricCard
          label="Latest volume"
          value={formatCurrency(overview.summary.latestVolume)}
          note="Summed daily volume on the latest available date."
        />
        <MetricCard
          label="Market return 30d"
          value={formatPercent(overview.summary.marketReturn30d)}
          note="Cap-weighted market proxy derived from the exported panel."
        />
        <MetricCard
          label="Breadth 30d"
          value={formatPercent(overview.summary.breadth30d)}
          note="Share of tracked assets with positive trailing 30-day performance."
        />
      </section>

      <Panel eyebrow="Pulse" title="Cap-Weighted Market Index">
        <EChart className="chart-surface" option={marketIndexOption} />
      </Panel>

      <div className="page-columns">
        <Panel
          eyebrow="Universe"
          title="Asset Screener"
          actions={
            <div className="control-grid control-grid-compact">
              <input
                className="search-input"
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  startTransition(() => setAssetQuery(event.target.value));
                }}
                placeholder="Search asset name, symbol, or id"
                value={assetQuery}
              />
              <label className="control-field">
                <span>Max rank</span>
                <input
                  className="search-input"
                  min={1}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setAssetRankLimit(Number(event.target.value) || 999));
                  }}
                  type="number"
                  value={assetRankLimit}
                />
              </label>
              <label className="control-field">
                <span>Sort</span>
                <select
                  className="search-input"
                  onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                    startTransition(() => setAssetSort(event.target.value as AssetSortKey));
                  }}
                  value={assetSort}
                >
                  <option value="marketCap">Market cap</option>
                  <option value="volume">Volume</option>
                  <option value="return30d">Return 30d</option>
                  <option value="return90d">Return 90d</option>
                  <option value="vol30d">Vol 30d</option>
                </select>
              </label>
              <label className="control-field">
                <span>Rows</span>
                <input
                  className="search-input"
                  min={5}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setAssetRowLimit(Number(event.target.value) || 18));
                  }}
                  type="number"
                  value={assetRowLimit}
                />
              </label>
            </div>
          }
        >
          <p className="detail-copy">
            Showing {visibleAssets.length} of {assetCandidates.length} filtered assets.
          </p>
          <DataTable<CoinSummary>
            columns={[
              {
                key: "coin",
                label: "Asset",
                render: (row) => <Link to={`/coin/${row.coinId}`}>{row.name}</Link>,
              },
              {
                key: "symbol",
                label: "Symbol",
                render: (row) => row.symbol,
              },
              {
                key: "rank",
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
                key: "volume",
                label: "Volume",
                render: (row) => formatCurrency(row.volume),
                sortValue: (row) => row.volume,
              },
              {
                key: "return30d",
                label: "30d",
                render: (row) => formatPercent(row.return30d),
                sortValue: (row) => row.return30d,
              },
              {
                key: "vol30d",
                label: "Vol 30d",
                render: (row) => formatPercent(row.vol30d),
                sortValue: (row) => row.vol30d,
              },
            ]}
            getRowKey={(row) => row.coinId}
            initialSortKey="marketCap"
            rows={visibleAssets}
          />
        </Panel>

        <Panel
          eyebrow="Relationships"
          title="Pair Screener"
          actions={
            <div className="control-grid control-grid-compact">
              <input
                className="search-input"
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  startTransition(() => setPairQuery(event.target.value));
                }}
                placeholder="Search pair ids, labels, or symbols"
                value={pairQuery}
              />
              <label className="control-field">
                <span>Min |corr|</span>
                <input
                  className="search-input"
                  max={1}
                  min={0}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setPairMinCorr(Number(event.target.value) || 0));
                  }}
                  step="0.05"
                  type="number"
                  value={pairMinCorr}
                />
              </label>
              <label className="control-field">
                <span>Min obs</span>
                <input
                  className="search-input"
                  min={1}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setPairMinObs(Number(event.target.value) || 0));
                  }}
                  type="number"
                  value={pairMinObs}
                />
              </label>
              <label className="control-field">
                <span>Max rank score</span>
                <input
                  className="search-input"
                  min={2}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setPairRankScore(Number(event.target.value) || 999));
                  }}
                  type="number"
                  value={pairRankScore}
                />
              </label>
              <label className="control-field">
                <span>Rows</span>
                <input
                  className="search-input"
                  min={5}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setPairRowLimit(Number(event.target.value) || 18));
                  }}
                  type="number"
                  value={pairRowLimit}
                />
              </label>
            </div>
          }
        >
          <p className="detail-copy">
            Showing {visiblePairs.length} of {pairCandidates.length} filtered pair summaries.
          </p>
          <DataTable<PairIndexItem>
            columns={[
              {
                key: "pair",
                label: "Pair",
                render: (row) => (
                  <Link to={`/pair/${row.pairKey}`}>
                    {row.labelX} / {row.labelY}
                  </Link>
                ),
              },
              {
                key: "symbols",
                label: "Symbols",
                render: (row) => `${row.symbolX} · ${row.symbolY}`,
              },
              {
                key: "corr",
                label: "Corr",
                render: (row) => formatNumber(row.pearsonCorr, 3),
                sortValue: (row) => row.pearsonCorr,
              },
              {
                key: "obs",
                label: "Obs",
                render: (row) => formatNumber(row.nObs),
                sortValue: (row) => row.nObs,
              },
              {
                key: "rankScore",
                label: "Rank score",
                render: (row) => formatNumber(row.rankScore),
                sortValue: (row) => row.rankScore,
              },
            ]}
            getRowKey={(row) => row.pairKey}
            initialSortKey="corr"
            rows={visiblePairs}
          />
        </Panel>
      </div>

      <Panel eyebrow="Catalog" title="Available Source Files">
        <p className="detail-copy">
          Every entry here is an actual data source in the static build. Exported tables open in the
          Sources page; larger sources route you to the page that can explore them safely.
        </p>
        <DataTable<typeof sourceCatalog[number]>
          columns={[
            {
              key: "title",
              label: "Source",
              render: (row) => <Link to={`/sources?source=${row.id}`}>{row.title}</Link>,
            },
            {
              key: "category",
              label: "Category",
              render: (row) => row.category,
            },
            {
              key: "viewer",
              label: "Best view",
              render: (row) => row.viewer,
            },
            {
              key: "rowCount",
              label: "Rows",
              render: (row) => formatNumber(row.rowCount),
              sortValue: (row) => row.rowCount,
            },
            {
              key: "columns",
              label: "Columns",
              render: (row) => formatNumber(row.columns.length),
              sortValue: (row) => row.columns.length,
            },
            {
              key: "status",
              label: "Access",
              render: (row) => (row.exportedPath ? "Previewable" : "Route only"),
            },
          ]}
          getRowKey={(row) => row.id}
          rows={sourceCatalog}
        />
      </Panel>
    </div>
  );
}
