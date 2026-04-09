import { ChangeEvent, startTransition, useDeferredValue, useState } from "react";
import { Link, Navigate, useParams } from "react-router-dom";

import { EChart } from "../components/EChart";
import { MetricCard } from "../components/MetricCard";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import { PairDetail, PairIndexItem, pairDetailSchema, pairIndexItemSchema } from "../data/schemas";
import { buildBarOption, buildLineOption } from "../lib/chartOptions";
import { formatNumber } from "../lib/format";
import { useAppShellContext } from "../app/AppShell";

export function PairPage() {
  const { pairKey } = useParams();
  const { manifest } = useAppShellContext();
  const pairIndexState = useJsonData("data/pairs/index.json", pairIndexItemSchema.array());
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);
  const detailState = useJsonData(pairKey ? `data/pairs/${pairKey}.json` : null, pairDetailSchema);

  if (!pairKey && manifest?.featuredPairKeys[0]) {
    return <Navigate replace to={`/pair/${manifest.featuredPairKeys[0]}`} />;
  }

  if (pairIndexState.status === "error") {
    return <StatusBlock status="error" message={pairIndexState.error} />;
  }
  if (detailState.status === "error") {
    return <StatusBlock status="error" message={detailState.error} />;
  }
  if (pairIndexState.status !== "success" || detailState.status !== "success") {
    return <StatusBlock status="loading" message="Loading pair laboratory…" />;
  }

  const filteredPairs = pairIndexState.data.filter((pair: PairIndexItem) => {
    const q = deferredQuery.trim().toLowerCase();
    if (!q) return true;
    return pair.coinIdX.includes(q) || pair.coinIdY.includes(q) || pair.pairKey.includes(q);
  });
  const detail: PairDetail = detailState.data;

  const rollingSeries = [30, 60, 90].map((window) => ({
    name: `${window}d`,
    color: window === 30 ? "#99ee4c" : window === 60 ? "#0f766e" : "#cc6b38",
    data: detail.rollingCorrelation
      .filter((row) => row.window === window)
      .map((row) => [row.date, row.value] as [string, number | null]),
  }));

  const rollingOption = buildLineOption({
    legend: true,
    series: rollingSeries,
  });

  const spreadOption = buildLineOption({
    series: [
      {
        name: "Relative Strength",
        data: detail.relativeStrength.map((row) => [row.date, row.value]),
        color: "#0f766e",
        area: true,
      },
    ],
  });

  const ccfOption = buildBarOption({
    categories: detail.ccf.map((row) => `${row.lag}`),
    values: detail.ccf.map((row) => row.value),
    color: "#99ee4c",
  });

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Pair Lab</p>
          <h1>
            {detail.summary.labelX} × {detail.summary.labelY}
          </h1>
          <p className="hero-copy">
            Focus on exported high-signal pairs. This view brings together static correlation,
            rolling co-movement, relative strength drift, and a compact lead-lag scan.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">{detail.summary.symbolX}</span>
          <span className="hero-badge">{detail.summary.symbolY}</span>
          <span className="hero-badge">{detail.summary.nObs} obs</span>
        </div>
      </section>

      <section className="metric-grid">
        <MetricCard label="Pearson corr" value={formatNumber(detail.summary.pearsonCorr, 3)} />
        <MetricCard label="Absolute corr" value={formatNumber(detail.summary.absCorr, 3)} />
        <MetricCard label="Observations" value={formatNumber(detail.summary.nObs)} />
        <MetricCard label="Pair key" value={detail.summary.pairKey} />
      </section>

      <div className="split-layout">
        <Panel
          eyebrow="Curated list"
          title="Featured pairs"
          actions={
            <input
              className="search-input"
              onChange={(event: ChangeEvent<HTMLInputElement>) => {
                const nextValue = event.target.value;
                startTransition(() => setQuery(nextValue));
              }}
              placeholder="Search pair ids"
              value={query}
            />
          }
        >
          <div className="picker-list">
            {filteredPairs.map((pair) => (
              <Link
                className={`picker-item ${pair.pairKey === detail.summary.pairKey ? "active" : ""}`.trim()}
                key={pair.pairKey}
                to={`/pair/${pair.pairKey}`}
              >
                <p className="picker-title">
                  {pair.coinIdX} / {pair.coinIdY}
                </p>
                <p className="picker-meta">
                  Corr {formatNumber(pair.pearsonCorr, 3)} · Obs {formatNumber(pair.nObs)}
                </p>
              </Link>
            ))}
          </div>
        </Panel>

        <div className="stack">
          <Panel eyebrow="Co-movement" title="Rolling correlation by window">
            <EChart className="chart-surface" option={rollingOption} />
          </Panel>

          <div className="page-columns">
            <Panel eyebrow="Spread tape" title="Relative strength spread">
              <EChart className="chart-surface" option={spreadOption} />
            </Panel>

            <Panel eyebrow="Lead-lag" title="Cross-correlation by lag">
              <EChart className="chart-surface" option={ccfOption} />
            </Panel>
          </div>
        </div>
      </div>
    </div>
  );
}
