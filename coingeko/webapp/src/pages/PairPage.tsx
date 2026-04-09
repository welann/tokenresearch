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
  DccRow,
  PairIndexItem,
  coinDetailSchema,
  dccRowSchema,
  pairIndexItemSchema,
  tableRowsSchema,
} from "../data/schemas";
import {
  alignPairSeries,
  buildCcfSeries,
  buildNormalizedPriceSeries,
  buildRelativeStrengthSeries,
  buildRollingCorrelationSeries,
} from "../lib/analytics";
import { buildBarOption, buildLineOption } from "../lib/chartOptions";
import { formatNumber, formatTableValue } from "../lib/format";

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function matchesCointegrationRow(row: Record<string, unknown>, pair: PairIndexItem) {
  const candidatePairKey = asString(row.pairKey);
  if (candidatePairKey && candidatePairKey === pair.pairKey) {
    return true;
  }
  const left = asString(row.coinIdX);
  const right = asString(row.coinIdY);
  return left === pair.coinIdX && right === pair.coinIdY;
}

function matchesGrangerRow(row: Record<string, unknown>, pair: PairIndexItem) {
  const sourceCoinId = asString(row.sourceCoinId);
  const targetCoinId = asString(row.targetCoinId);
  if (!sourceCoinId || !targetCoinId) {
    return false;
  }
  const ids = new Set([sourceCoinId, targetCoinId]);
  return ids.has(pair.coinIdX) && ids.has(pair.coinIdY);
}

export function PairPage() {
  const { pairKey } = useParams();
  const { manifest } = useAppShellContext();
  const pairIndexState = useJsonData("data/pairs/index.json", pairIndexItemSchema.array());

  const selectedPairKey =
    pairKey ?? (pairIndexState.status === "success" ? pairIndexState.data[0]?.pairKey ?? null : null);
  const selectedPair =
    pairIndexState.status === "success"
      ? pairIndexState.data.find((item) => item.pairKey === selectedPairKey) ?? null
      : null;

  const leftCoinState = useJsonData(
    selectedPair ? `data/coins/${selectedPair.coinIdX}.json` : null,
    coinDetailSchema,
  );
  const rightCoinState = useJsonData(
    selectedPair ? `data/coins/${selectedPair.coinIdY}.json` : null,
    coinDetailSchema,
  );
  const cointegrationState = useJsonData(
    manifest?.availableSources.includes("cointegration") ? "data/pairs/cointegration.json" : null,
    tableRowsSchema,
  );
  const grangerState = useJsonData(
    manifest?.availableSources.includes("granger") ? "data/pairs/granger.json" : null,
    tableRowsSchema,
  );
  const dccState = useJsonData(
    manifest?.availableSources.includes("dcc_garch") && selectedPairKey
      ? `data/pairs/dcc/${selectedPairKey}.json`
      : null,
    dccRowSchema.array(),
  );

  const [query, setQuery] = useState("");
  const [minAbsCorr, setMinAbsCorr] = useState(0.75);
  const [minObs, setMinObs] = useState(300);
  const [maxRankScore, setMaxRankScore] = useState(40);
  const [visibleCount, setVisibleCount] = useState(24);
  const [maxLag, setMaxLag] = useState(10);

  const deferredQuery = useDeferredValue(query);

  if (pairIndexState.status === "error") {
    return <StatusBlock status="error" message={pairIndexState.error} />;
  }
  if (leftCoinState.status === "error") {
    return <StatusBlock status="error" message={leftCoinState.error} />;
  }
  if (rightCoinState.status === "error") {
    return <StatusBlock status="error" message={rightCoinState.error} />;
  }
  if (cointegrationState.status === "error") {
    return <StatusBlock status="error" message={cointegrationState.error} />;
  }
  if (grangerState.status === "error") {
    return <StatusBlock status="error" message={grangerState.error} />;
  }
  if (dccState.status === "error") {
    return <StatusBlock status="error" message={dccState.error} />;
  }
  if (pairIndexState.status === "success" && selectedPairKey && !selectedPair) {
    return <StatusBlock status="error" message={`Pair not found: ${selectedPairKey}`} />;
  }
  if (
    pairIndexState.status !== "success" ||
    leftCoinState.status !== "success" ||
    rightCoinState.status !== "success"
  ) {
    return <StatusBlock status="loading" message="Loading pair laboratory…" />;
  }

  if (!selectedPair) {
    return <StatusBlock status="error" message={`Pair not found: ${selectedPairKey ?? "unknown"}`} />;
  }

  if (!pairKey && selectedPairKey) {
    return <Navigate replace to={`/pair/${selectedPairKey}`} />;
  }

  const normalizedQuery = deferredQuery.trim().toLowerCase();
  const filteredPairs = pairIndexState.data
    .filter((pair) => {
      if (pair.absCorr < minAbsCorr || pair.nObs < minObs || pair.rankScore > maxRankScore) {
        return false;
      }
      if (!normalizedQuery) {
        return true;
      }
      return (
        pair.pairKey.includes(normalizedQuery) ||
        pair.coinIdX.includes(normalizedQuery) ||
        pair.coinIdY.includes(normalizedQuery) ||
        pair.labelX.toLowerCase().includes(normalizedQuery) ||
        pair.labelY.toLowerCase().includes(normalizedQuery) ||
        pair.symbolX.toLowerCase().includes(normalizedQuery) ||
        pair.symbolY.toLowerCase().includes(normalizedQuery)
      );
    })
    .sort((left, right) => {
      if (right.absCorr !== left.absCorr) {
        return right.absCorr - left.absCorr;
      }
      return left.rankScore - right.rankScore;
    })
    .slice(0, visibleCount);

  const leftCoin = leftCoinState.data as CoinDetail;
  const rightCoin = rightCoinState.data as CoinDetail;
  const aligned = alignPairSeries(leftCoin.series, rightCoin.series);
  const normalizedSeries = buildNormalizedPriceSeries(aligned);
  const relativeStrengthSeries = buildRelativeStrengthSeries(aligned);
  const ccfRows = buildCcfSeries(aligned, maxLag);

  const normalizedPriceOption = buildLineOption({
    legend: true,
    series: [
      {
        name: selectedPair.symbolX,
        data: normalizedSeries.left,
        color: "#0f766e",
      },
      {
        name: selectedPair.symbolY,
        data: normalizedSeries.right,
        color: "#cc6b38",
      },
    ],
  });

  const rollingCorrelationOption = buildLineOption({
    legend: true,
    series: [
      {
        name: "30d",
        data: buildRollingCorrelationSeries(aligned, 30),
        color: "#99ee4c",
      },
      {
        name: "60d",
        data: buildRollingCorrelationSeries(aligned, 60),
        color: "#0f766e",
      },
      {
        name: "90d",
        data: buildRollingCorrelationSeries(aligned, 90),
        color: "#cc6b38",
      },
    ],
  });

  const relativeStrengthOption = buildLineOption({
    series: [
      {
        name: `${selectedPair.symbolX}/${selectedPair.symbolY}`,
        data: relativeStrengthSeries,
        color: "#0f766e",
        area: true,
      },
    ],
  });

  const ccfOption = buildBarOption({
    categories: ccfRows.map((row) => `${row.lag}`),
    values: ccfRows.map((row) => row.value),
    color: "#99ee4c",
  });

  const dccRows = dccState.status === "success" ? (dccState.data as DccRow[]) : [];
  const dccOption =
    dccRows.length > 0
      ? buildLineOption({
          series: [
            {
              name: "DCC",
              data: dccRows.map((row) => [row.date, row.dccCorr]),
              color: "#cc6b38",
            },
          ],
        })
      : null;

  const cointegrationRows =
    cointegrationState.status === "success"
      ? cointegrationState.data.filter((row) => matchesCointegrationRow(row, selectedPair))
      : [];
  const grangerRows =
    grangerState.status === "success"
      ? grangerState.data.filter((row) => matchesGrangerRow(row, selectedPair))
      : [];

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Pair Lab</p>
          <h1>
            {selectedPair.labelX} × {selectedPair.labelY}
          </h1>
          <p className="hero-copy">
            Any pair in the exported correlation matrix is now explorable. The charts below derive
            rolling co-movement and lead-lag structure directly from the two coin series, then layer
            optional test outputs when those source files exist.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">{selectedPair.symbolX}</span>
          <span className="hero-badge">{selectedPair.symbolY}</span>
          <span className="hero-badge">{selectedPair.nObs} obs</span>
          <span className="hero-badge">Rank score {selectedPair.rankScore}</span>
        </div>
      </section>

      <section className="metric-grid">
        <MetricCard label="Pearson corr" value={formatNumber(selectedPair.pearsonCorr, 3)} />
        <MetricCard label="Absolute corr" value={formatNumber(selectedPair.absCorr, 3)} />
        <MetricCard label="Relative strength" value={formatNumber(relativeStrengthSeries.at(-1)?.[1], 3)} />
        <MetricCard label="Observations" value={formatNumber(selectedPair.nObs)} />
      </section>

      <div className="split-layout">
        <Panel
          eyebrow="Screen"
          title="Pair filter stack"
          actions={
            <div className="control-grid control-grid-compact">
              <input
                className="search-input"
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  startTransition(() => setQuery(event.target.value));
                }}
                placeholder="Search labels, ids, or symbols"
                value={query}
              />
              <label className="control-field">
                <span>Min |corr|</span>
                <input
                  className="search-input"
                  max={1}
                  min={0}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setMinAbsCorr(Number(event.target.value) || 0));
                  }}
                  step="0.05"
                  type="number"
                  value={minAbsCorr}
                />
              </label>
              <label className="control-field">
                <span>Min obs</span>
                <input
                  className="search-input"
                  min={1}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setMinObs(Number(event.target.value) || 0));
                  }}
                  type="number"
                  value={minObs}
                />
              </label>
              <label className="control-field">
                <span>Max rank score</span>
                <input
                  className="search-input"
                  min={2}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setMaxRankScore(Number(event.target.value) || 999));
                  }}
                  type="number"
                  value={maxRankScore}
                />
              </label>
              <label className="control-field">
                <span>Rows</span>
                <input
                  className="search-input"
                  min={5}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setVisibleCount(Number(event.target.value) || 24));
                  }}
                  type="number"
                  value={visibleCount}
                />
              </label>
            </div>
          }
        >
          <p className="detail-copy">
            Showing {filteredPairs.length} pairs from the current filtered slice.
          </p>
          <div className="picker-list">
            {filteredPairs.map((pair) => (
              <Link
                className={`picker-item ${pair.pairKey === selectedPair.pairKey ? "active" : ""}`.trim()}
                key={pair.pairKey}
                to={`/pair/${pair.pairKey}`}
              >
                <p className="picker-title">
                  {pair.labelX} / {pair.labelY}
                </p>
                <p className="picker-meta">
                  Corr {formatNumber(pair.pearsonCorr, 3)} · Obs {formatNumber(pair.nObs)} · Score{" "}
                  {formatNumber(pair.rankScore)}
                </p>
              </Link>
            ))}
          </div>
        </Panel>

        <div className="stack">
          <Panel eyebrow="Relative tape" title="Normalized Price Index">
            <EChart className="chart-surface" option={normalizedPriceOption} />
          </Panel>

          <Panel eyebrow="Co-movement" title="Rolling correlation">
            <EChart className="chart-surface" option={rollingCorrelationOption} />
          </Panel>

          <div className="page-columns">
            <Panel eyebrow="Spread" title="Relative Strength Ratio">
              <EChart className="chart-surface" option={relativeStrengthOption} />
            </Panel>

            <Panel
              eyebrow="Lead-lag"
              title="Cross-correlation by lag"
              actions={
                <label className="control-field">
                  <span>Max lag</span>
                  <input
                    className="search-input"
                    max={30}
                    min={1}
                    onChange={(event: ChangeEvent<HTMLInputElement>) => {
                      startTransition(() => setMaxLag(Number(event.target.value) || 10));
                    }}
                    type="number"
                    value={maxLag}
                  />
                </label>
              }
            >
              <p className="detail-copy">Positive lag means the left asset is leading the right asset.</p>
              <EChart className="chart-surface" option={ccfOption} />
            </Panel>
          </div>

          <div className="page-columns">
            <Panel eyebrow="Optional tests" title="DCC-GARCH">
              {dccOption ? (
                <EChart className="chart-surface" option={dccOption} />
              ) : (
                <StatusBlock status="idle" message="No DCC output was exported for this pair." />
              )}
            </Panel>

            <Panel eyebrow="Optional tests" title="Cointegration Rows">
              <DataTable<Record<string, unknown>>
                columns={
                  cointegrationRows.length > 0
                    ? Object.keys(cointegrationRows[0]).map((key) => ({
                        key,
                        label: key,
                        render: (row: Record<string, unknown>) => formatTableValue(row[key], key),
                      }))
                    : [
                        {
                          key: "empty",
                          label: "Status",
                          render: () => "No exported cointegration rows for this pair.",
                        },
                      ]
                }
                getRowKey={(_row, index) => `${selectedPair.pairKey}-cointegration-${index}`}
                rows={cointegrationRows.length > 0 ? cointegrationRows : [{}]}
              />
            </Panel>
          </div>

          <Panel eyebrow="Optional tests" title="Granger Rows">
            <DataTable<Record<string, unknown>>
              columns={
                grangerRows.length > 0
                  ? Object.keys(grangerRows[0]).map((key) => ({
                      key,
                      label: key,
                      render: (row: Record<string, unknown>) => formatTableValue(row[key], key),
                    }))
                  : [
                      {
                        key: "empty",
                        label: "Status",
                        render: () => "No exported Granger rows for this pair.",
                      },
                    ]
              }
              getRowKey={(_row, index) => `${selectedPair.pairKey}-granger-${index}`}
              rows={grangerRows.length > 0 ? grangerRows : [{}]}
            />
          </Panel>
        </div>
      </div>
    </div>
  );
}
