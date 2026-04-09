import { ChangeEvent, startTransition, useDeferredValue, useState } from "react";

import { DataTable } from "../components/DataTable";
import { EChart } from "../components/EChart";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import {
  CoinSummary,
  PairIndexItem,
  StructurePayload,
  coinSummarySchema,
  pairIndexItemSchema,
  structureSchema,
} from "../data/schemas";
import { buildBarOption, buildHeatmapOption, buildLineOption } from "../lib/chartOptions";
import { formatNumber, formatTableValue } from "../lib/format";

type CentralityMetric = "degreeCentrality" | "betweennessCentrality" | "eigenvectorCentrality";

const centralityMetricLabels: Record<CentralityMetric, string> = {
  degreeCentrality: "Degree",
  betweennessCentrality: "Betweenness",
  eigenvectorCentrality: "Eigenvector",
};

export function StructurePage() {
  const structureState = useJsonData("data/structure.json", structureSchema);
  const coinIndexState = useJsonData("data/coins/index.json", coinSummarySchema.array());
  const pairIndexState = useJsonData("data/pairs/index.json", pairIndexItemSchema.array());

  const [heatmapQuery, setHeatmapQuery] = useState("");
  const [heatmapCount, setHeatmapCount] = useState(18);
  const [selectedComponent, setSelectedComponent] = useState(1);
  const [centralityMetric, setCentralityMetric] = useState<CentralityMetric>("degreeCentrality");
  const [centralityQuery, setCentralityQuery] = useState("");
  const [centralityCount, setCentralityCount] = useState(18);
  const [loadingCount, setLoadingCount] = useState(16);
  const [linkageCount, setLinkageCount] = useState(40);

  const deferredHeatmapQuery = useDeferredValue(heatmapQuery);
  const deferredCentralityQuery = useDeferredValue(centralityQuery);

  if (structureState.status === "error") {
    return <StatusBlock status="error" message={structureState.error} />;
  }
  if (coinIndexState.status === "error") {
    return <StatusBlock status="error" message={coinIndexState.error} />;
  }
  if (pairIndexState.status === "error") {
    return <StatusBlock status="error" message={pairIndexState.error} />;
  }
  if (
    structureState.status !== "success" ||
    coinIndexState.status !== "success" ||
    pairIndexState.status !== "success"
  ) {
    return <StatusBlock status="loading" message="Loading structure board…" />;
  }

  const structure: StructurePayload = structureState.data;
  const coinIndex = coinIndexState.data;
  const pairIndex = pairIndexState.data;

  const normalizedHeatmapQuery = deferredHeatmapQuery.trim().toLowerCase();
  const heatmapCoins = coinIndex
    .filter((coin: CoinSummary) => {
      if (!normalizedHeatmapQuery) {
        return true;
      }
      return (
        coin.name.toLowerCase().includes(normalizedHeatmapQuery) ||
        coin.symbol.toLowerCase().includes(normalizedHeatmapQuery) ||
        coin.coinId.includes(normalizedHeatmapQuery)
      );
    })
    .sort((left, right) => left.marketCapRank - right.marketCapRank)
    .slice(0, heatmapCount);

  const heatmapCoinIds = heatmapCoins.map((coin) => coin.coinId);
  const pairLookup = new Map(pairIndex.map((pair: PairIndexItem) => [pair.pairKey, pair.pearsonCorr]));
  const heatmapRows = heatmapCoinIds.flatMap((coinIdX) =>
    heatmapCoinIds.map((coinIdY) => ({
      x: coinIdX,
      y: coinIdY,
      value:
        coinIdX === coinIdY
          ? 1
          : pairLookup.get([coinIdX, coinIdY].sort().join("__")) ?? null,
    })),
  );

  const heatmapOption = buildHeatmapOption({
    axes: heatmapCoinIds,
    rows: heatmapRows,
  });

  const pcaOption = buildLineOption({
    legend: true,
    series: [
      {
        name: "Explained",
        data: structure.pcaSummary.map((row) => [String(row.component), row.explainedVarianceRatio]),
        color: "#99ee4c",
      },
      {
        name: "Cumulative",
        data: structure.pcaSummary.map((row) => [String(row.component), row.cumulativeRatio]),
        color: "#0f766e",
      },
    ],
  });

  const selectedLoadings = structure.pcaLoadings
    .filter((row) => row.component === selectedComponent)
    .sort((left, right) => Math.abs(right.loading) - Math.abs(left.loading))
    .slice(0, loadingCount);

  const loadingsOption = buildBarOption({
    categories: selectedLoadings.map((row) => row.coinId),
    values: selectedLoadings.map((row) => row.loading),
    color: "#cc6b38",
  });

  const normalizedCentralityQuery = deferredCentralityQuery.trim().toLowerCase();
  const filteredCentrality = structure.centrality
    .filter((row) => {
      if (!normalizedCentralityQuery) {
        return true;
      }
      return row.coinId.toLowerCase().includes(normalizedCentralityQuery);
    })
    .sort((left, right) => right[centralityMetric] - left[centralityMetric]);

  const centralityChartRows = filteredCentrality.slice(0, centralityCount);
  const centralityOption = buildBarOption({
    categories: centralityChartRows.map((row) => row.coinId),
    values: centralityChartRows.map((row) => row[centralityMetric]),
    color: "#0f766e",
  });

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Structure</p>
          <h1>Inspect the market graph with filters, not presets.</h1>
          <p className="hero-copy">
            The structure page now builds the heatmap from the full pair index, then exposes PCA,
            network centrality, and clustering linkage as separate filtered work surfaces.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">{pairIndex.length} correlation rows</span>
          <span className="hero-badge">{structure.pcaSummary.length} PCA components</span>
          <span className="hero-badge">{structure.centrality.length} centrality rows</span>
          <span className="hero-badge">{structure.clusteringLinkage.length} linkage rows</span>
        </div>
      </section>

      <Panel
        eyebrow="Map"
        title="Correlation Heatmap"
        actions={
          <div className="control-grid control-grid-compact">
            <input
              className="search-input"
              onChange={(event: ChangeEvent<HTMLInputElement>) => {
                startTransition(() => setHeatmapQuery(event.target.value));
              }}
              placeholder="Filter heatmap assets"
              value={heatmapQuery}
            />
            <label className="control-field">
              <span>Asset count</span>
              <input
                className="search-input"
                max={40}
                min={4}
                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                  startTransition(() => setHeatmapCount(Number(event.target.value) || 18));
                }}
                type="number"
                value={heatmapCount}
              />
            </label>
          </div>
        }
      >
        <p className="detail-copy">
          Heatmap built from {heatmapCoinIds.length} selected assets using the full pairwise
          correlation index.
        </p>
        <EChart className="chart-surface" option={heatmapOption} />
      </Panel>

      <div className="page-columns">
        <Panel eyebrow="Principal components" title="Explained Variance Curve">
          <EChart className="chart-surface" option={pcaOption} />
        </Panel>

        <Panel
          eyebrow="Loadings"
          title={`Top exposures for PC${selectedComponent}`}
          actions={
            <div className="control-grid control-grid-compact">
              <label className="control-field">
                <span>Component</span>
                <select
                  className="search-input"
                  onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                    startTransition(() => setSelectedComponent(Number(event.target.value)));
                  }}
                  value={selectedComponent}
                >
                  {structure.pcaSummary.map((row) => (
                    <option key={row.component} value={row.component}>
                      Component {row.component}
                    </option>
                  ))}
                </select>
              </label>
              <label className="control-field">
                <span>Bars</span>
                <input
                  className="search-input"
                  max={40}
                  min={4}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setLoadingCount(Number(event.target.value) || 16));
                  }}
                  type="number"
                  value={loadingCount}
                />
              </label>
            </div>
          }
        >
          <EChart className="chart-surface" option={loadingsOption} />
        </Panel>
      </div>

      <div className="page-columns">
        <Panel
          eyebrow="Network"
          title={`${centralityMetricLabels[centralityMetric]} leaders`}
          actions={
            <div className="control-grid control-grid-compact">
              <label className="control-field">
                <span>Metric</span>
                <select
                  className="search-input"
                  onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                    startTransition(() => setCentralityMetric(event.target.value as CentralityMetric));
                  }}
                  value={centralityMetric}
                >
                  <option value="degreeCentrality">Degree</option>
                  <option value="betweennessCentrality">Betweenness</option>
                  <option value="eigenvectorCentrality">Eigenvector</option>
                </select>
              </label>
              <label className="control-field">
                <span>Bars</span>
                <input
                  className="search-input"
                  max={50}
                  min={4}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setCentralityCount(Number(event.target.value) || 18));
                  }}
                  type="number"
                  value={centralityCount}
                />
              </label>
            </div>
          }
        >
          <EChart className="chart-surface" option={centralityOption} />
        </Panel>

        <Panel
          eyebrow="Centrality"
          title="Full network table"
          actions={
            <input
              className="search-input"
              onChange={(event: ChangeEvent<HTMLInputElement>) => {
                startTransition(() => setCentralityQuery(event.target.value));
              }}
              placeholder="Filter by coin id"
              value={centralityQuery}
            />
          }
        >
          <DataTable<StructurePayload["centrality"][number]>
            columns={[
              {
                key: "coinId",
                label: "Coin",
                render: (row) => row.coinId,
              },
              {
                key: "degreeCentrality",
                label: "Degree",
                render: (row) => formatNumber(row.degreeCentrality, 3),
                sortValue: (row) => row.degreeCentrality,
              },
              {
                key: "betweennessCentrality",
                label: "Betweenness",
                render: (row) => formatNumber(row.betweennessCentrality, 3),
                sortValue: (row) => row.betweennessCentrality,
              },
              {
                key: "eigenvectorCentrality",
                label: "Eigenvector",
                render: (row) => formatNumber(row.eigenvectorCentrality, 3),
                sortValue: (row) => row.eigenvectorCentrality,
              },
            ]}
            getRowKey={(row) => row.coinId}
            rows={filteredCentrality}
          />
        </Panel>
      </div>

      <Panel
        eyebrow="Clustering"
        title="Linkage Matrix Preview"
        actions={
          <label className="control-field">
            <span>Rows</span>
            <input
              className="search-input"
              max={200}
              min={10}
              onChange={(event: ChangeEvent<HTMLInputElement>) => {
                startTransition(() => setLinkageCount(Number(event.target.value) || 40));
              }}
              type="number"
              value={linkageCount}
            />
          </label>
        }
      >
        <DataTable<Record<string, unknown>>
          columns={(structure.clusteringLinkage[0]
            ? Object.keys(structure.clusteringLinkage[0])
            : ["status"]
          ).map((key) => ({
            key,
            label: key,
            render: (row: Record<string, unknown>) => formatTableValue(row[key], key),
          }))}
          getRowKey={(_row, index) => `linkage-${index}`}
          rows={structure.clusteringLinkage.slice(0, linkageCount)}
        />
      </Panel>
    </div>
  );
}
