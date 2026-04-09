import { ChangeEvent, startTransition, useDeferredValue, useState } from "react";
import { Link, Navigate, useParams } from "react-router-dom";

import { useAppShellContext } from "../app/AppShell";
import { DataTable } from "../components/DataTable";
import { EChart } from "../components/EChart";
import { MetricCard } from "../components/MetricCard";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import {
  CoinDetail,
  CoinSummary,
  RiskRow,
  coinDetailSchema,
  coinSummarySchema,
} from "../data/schemas";
import { buildLineOption } from "../lib/chartOptions";
import { formatCurrency, formatNumber, formatPercent } from "../lib/format";

type SeriesMetricKey = "price" | "marketCap" | "volume";
type RiskMetricKey = "realizedVol" | "downsideSemivol" | "latestDrawdown";

const seriesMetricLabels: Record<SeriesMetricKey, string> = {
  price: "Price",
  marketCap: "Market Cap",
  volume: "Volume",
};

const riskMetricLabels: Record<RiskMetricKey, string> = {
  realizedVol: "Realized Vol",
  downsideSemivol: "Downside Semivol",
  latestDrawdown: "Latest Drawdown",
};

function latestRollingRiskRows(rows: RiskRow[]) {
  const result: RiskRow[] = [];

  for (const window of [7, 30, 90]) {
    const lastRow = [...rows]
      .reverse()
      .find((row) => row.metricScope === "rolling" && row.window === window);
    if (lastRow) {
      result.push(lastRow);
    }
  }

  const drawdownSummary = rows.find((row) => row.metricScope === "drawdown_summary");
  if (drawdownSummary) {
    result.push(drawdownSummary);
  }

  return result;
}

export function CoinPage() {
  const { coinId } = useParams();
  const { manifest } = useAppShellContext();
  const coinIndexState = useJsonData("data/coins/index.json", coinSummarySchema.array());

  const selectedCoinId =
    coinId ?? (coinIndexState.status === "success" ? coinIndexState.data[0]?.coinId ?? null : null);
  const detailState = useJsonData(selectedCoinId ? `data/coins/${selectedCoinId}.json` : null, coinDetailSchema);

  const [query, setQuery] = useState("");
  const [rankLimit, setRankLimit] = useState(80);
  const [visibleCount, setVisibleCount] = useState(32);
  const [seriesMetric, setSeriesMetric] = useState<SeriesMetricKey>("price");
  const [riskWindow, setRiskWindow] = useState(30);
  const [riskMetric, setRiskMetric] = useState<RiskMetricKey>("realizedVol");
  const [rawRowCount, setRawRowCount] = useState(40);

  const deferredQuery = useDeferredValue(query);

  if (coinIndexState.status === "error") {
    return <StatusBlock status="error" message={coinIndexState.error} />;
  }
  if (detailState.status === "error") {
    return <StatusBlock status="error" message={detailState.error} />;
  }
  if (coinIndexState.status !== "success" || detailState.status !== "success") {
    return <StatusBlock status="loading" message="Loading coin laboratory…" />;
  }

  if (!coinId && selectedCoinId) {
    return <Navigate replace to={`/coin/${selectedCoinId}`} />;
  }

  const detail: CoinDetail = detailState.data;
  const normalizedQuery = deferredQuery.trim().toLowerCase();
  const filteredCoins = coinIndexState.data
    .filter((coin: CoinSummary) => {
      if (coin.marketCapRank > rankLimit) {
        return false;
      }
      if (!normalizedQuery) {
        return true;
      }
      return (
        coin.name.toLowerCase().includes(normalizedQuery) ||
        coin.symbol.toLowerCase().includes(normalizedQuery) ||
        coin.coinId.includes(normalizedQuery)
      );
    })
    .slice(0, visibleCount);

  const primarySeriesOption = buildLineOption({
    series: [
      {
        name: seriesMetricLabels[seriesMetric],
        data: detail.series.map((row) => [row.date, row[seriesMetric]]),
        color: "#0f766e",
        area: seriesMetric !== "volume",
      },
    ],
  });

  const trailingSignalOption = buildLineOption({
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

  const rollingRiskRows = detail.riskRows.filter(
    (row) => row.metricScope === "rolling" && row.window === riskWindow,
  );
  const riskSeriesOption = buildLineOption({
    series: [
      {
        name: riskMetricLabels[riskMetric],
        data: rollingRiskRows.map((row) => [row.date ?? "", row[riskMetric]]),
        color: riskMetric === "latestDrawdown" ? "#cc6b38" : "#0f766e",
      },
    ],
  });

  const latestRiskRows = latestRollingRiskRows(detail.riskRows);
  const rawSeriesRows = detail.series.slice(-rawRowCount).reverse();

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Coin Lab</p>
          <h1>{detail.summary.name}</h1>
          <p className="hero-copy">
            Every single-asset metric now starts from the full coin index. Search the universe,
            filter by rank, switch source metrics, and inspect both rolling risk rows and market
            model output for any exported coin.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">{detail.summary.symbol}</span>
          <span className="hero-badge">Rank {detail.summary.marketCapRank}</span>
          <span className="hero-badge">Latest {detail.summary.latestDate}</span>
          <span className="hero-badge">{manifest?.availableSources.length ?? 0} sources in build</span>
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
          eyebrow="Universe"
          title="Browse tracked assets"
          actions={
            <div className="control-grid control-grid-compact">
              <input
                className="search-input"
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  startTransition(() => setQuery(event.target.value));
                }}
                placeholder="Search coin name, symbol, or id"
                value={query}
              />
              <label className="control-field">
                <span>Max rank</span>
                <input
                  className="search-input"
                  min={1}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setRankLimit(Number(event.target.value) || 999));
                  }}
                  type="number"
                  value={rankLimit}
                />
              </label>
              <label className="control-field">
                <span>Rows</span>
                <input
                  className="search-input"
                  min={5}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setVisibleCount(Number(event.target.value) || 32));
                  }}
                  type="number"
                  value={visibleCount}
                />
              </label>
            </div>
          }
        >
          <p className="detail-copy">
            Showing {filteredCoins.length} assets from the filtered universe.
          </p>
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
                  Rank {coin.marketCapRank} · {formatPercent(coin.return30d)} 30d ·{" "}
                  {formatPercent(coin.vol30d)} vol
                </p>
              </Link>
            ))}
          </div>
        </Panel>

        <div className="stack">
          <Panel
            eyebrow="Series explorer"
            title={seriesMetricLabels[seriesMetric]}
            actions={
              <label className="control-field">
                <span>Metric</span>
                <select
                  className="search-input"
                  onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                    startTransition(() => setSeriesMetric(event.target.value as SeriesMetricKey));
                  }}
                  value={seriesMetric}
                >
                  <option value="price">Price</option>
                  <option value="marketCap">Market cap</option>
                  <option value="volume">Volume</option>
                </select>
              </label>
            }
          >
            <EChart className="chart-surface" option={primarySeriesOption} />
          </Panel>

          <div className="page-columns">
            <Panel eyebrow="Daily tape" title="Returns and Drawdown">
              <EChart className="chart-surface" option={trailingSignalOption} />
            </Panel>

            <Panel eyebrow="Market model" title="Exposure Snapshot">
              {detail.exposure ? (
                <div className="info-grid">
                  <div className="info-pair">
                    <span>Beta</span>
                    <strong>{formatNumber(detail.exposure.beta, 3)}</strong>
                  </div>
                  <div className="info-pair">
                    <span>Alpha</span>
                    <strong>{formatNumber(detail.exposure.alpha, 4)}</strong>
                  </div>
                  <div className="info-pair">
                    <span>R²</span>
                    <strong>{formatNumber(detail.exposure.rSquared, 3)}</strong>
                  </div>
                  <div className="info-pair">
                    <span>Adj. R²</span>
                    <strong>{formatNumber(detail.exposure.adjRSquared, 3)}</strong>
                  </div>
                  <div className="info-pair">
                    <span>Residual vol</span>
                    <strong>{formatPercent(detail.exposure.residualVol, 2)}</strong>
                  </div>
                  <div className="info-pair">
                    <span>Residual vol ann.</span>
                    <strong>{formatPercent(detail.exposure.residualVolAnnualized, 2)}</strong>
                  </div>
                </div>
              ) : (
                <StatusBlock status="idle" message="No market exposure output was exported." />
              )}
            </Panel>
          </div>

          <div className="page-columns">
            <Panel
              eyebrow="Risk"
              title={riskMetricLabels[riskMetric]}
              actions={
                <div className="control-grid control-grid-compact">
                  <label className="control-field">
                    <span>Window</span>
                    <select
                      className="search-input"
                      onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                        startTransition(() => setRiskWindow(Number(event.target.value)));
                      }}
                      value={riskWindow}
                    >
                      <option value={7}>7d</option>
                      <option value={30}>30d</option>
                      <option value={90}>90d</option>
                    </select>
                  </label>
                  <label className="control-field">
                    <span>Metric</span>
                    <select
                      className="search-input"
                      onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                        startTransition(() => setRiskMetric(event.target.value as RiskMetricKey));
                      }}
                      value={riskMetric}
                    >
                      <option value="realizedVol">Realized vol</option>
                      <option value="downsideSemivol">Downside semivol</option>
                      <option value="latestDrawdown">Latest drawdown</option>
                    </select>
                  </label>
                </div>
              }
            >
              {rollingRiskRows.length > 0 ? (
                <EChart className="chart-surface" option={riskSeriesOption} />
              ) : (
                <StatusBlock status="idle" message="No rolling risk rows were exported for this window." />
              )}
            </Panel>

            <Panel eyebrow="Latest rows" title="Risk Snapshot Table">
              <DataTable<RiskRow>
                columns={[
                  {
                    key: "scope",
                    label: "Scope",
                    render: (row) =>
                      row.metricScope === "drawdown_summary" ? "Drawdown summary" : `${row.window}d rolling`,
                  },
                  {
                    key: "realizedVol",
                    label: "Realized vol",
                    render: (row) => formatPercent(row.realizedVol, 2),
                    sortValue: (row) => row.realizedVol,
                  },
                  {
                    key: "downsideSemivol",
                    label: "Downside semivol",
                    render: (row) => formatPercent(row.downsideSemivol, 2),
                    sortValue: (row) => row.downsideSemivol,
                  },
                  {
                    key: "latestDrawdown",
                    label: "Latest drawdown",
                    render: (row) => formatPercent(row.latestDrawdown, 2),
                    sortValue: (row) => row.latestDrawdown,
                  },
                  {
                    key: "mdd",
                    label: "MDD",
                    render: (row) => formatPercent(row.mdd, 2),
                    sortValue: (row) => row.mdd,
                  },
                ]}
                getRowKey={(row, index) => `${row.metricScope}-${row.window ?? "summary"}-${index}`}
                rows={latestRiskRows}
              />
            </Panel>
          </div>

          <Panel
            eyebrow="Raw tape"
            title="Recent Daily Rows"
            actions={
              <label className="control-field">
                <span>Rows</span>
                <input
                  className="search-input"
                  min={10}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setRawRowCount(Number(event.target.value) || 40));
                  }}
                  type="number"
                  value={rawRowCount}
                />
              </label>
            }
          >
            <DataTable<CoinDetail["series"][number]>
              columns={[
                {
                  key: "date",
                  label: "Date",
                  render: (row) => row.date,
                },
                {
                  key: "price",
                  label: "Price",
                  render: (row) => formatCurrency(row.price, 2),
                  sortValue: (row) => row.price,
                },
                {
                  key: "marketCap",
                  label: "Market cap",
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
                  key: "logReturn",
                  label: "Log return",
                  render: (row) => formatNumber(row.logReturn, 4),
                  sortValue: (row) => row.logReturn,
                },
                {
                  key: "drawdown",
                  label: "Drawdown",
                  render: (row) => formatPercent(row.drawdown, 2),
                  sortValue: (row) => row.drawdown,
                },
              ]}
              getRowKey={(row) => row.date}
              rows={rawSeriesRows}
            />
          </Panel>
        </div>
      </div>
    </div>
  );
}
