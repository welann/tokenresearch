import { ChangeEvent, startTransition, useState } from "react";

import { DataTable } from "../components/DataTable";
import { EChart } from "../components/EChart";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import { StructurePayload, structureSchema } from "../data/schemas";
import { buildBarOption, buildHeatmapOption, buildLineOption } from "../lib/chartOptions";
import { formatNumber } from "../lib/format";

export function StructurePage() {
  const structureState = useJsonData("data/structure.json", structureSchema);
  const [selectedComponent, setSelectedComponent] = useState(1);

  if (structureState.status === "error") {
    return <StatusBlock status="error" message={structureState.error} />;
  }
  if (structureState.status !== "success") {
    return <StatusBlock status="loading" message="Loading structure board…" />;
  }

  const structure: StructurePayload = structureState.data;
  const selectedLoadings = structure.pcaLoadings
    .filter((row) => row.component === selectedComponent)
    .sort((left, right) => Math.abs(right.loading) - Math.abs(left.loading))
    .slice(0, 12);

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

  const loadingsOption = buildBarOption({
    categories: selectedLoadings.map((row) => row.coinId),
    values: selectedLoadings.map((row) => row.loading),
    color: "#cc6b38",
  });

  const centralityOption = buildBarOption({
    categories: structure.centrality.slice(0, 12).map((row) => row.coinId),
    values: structure.centrality.slice(0, 12).map((row) => row.degreeCentrality),
    color: "#0f766e",
  });

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Structure</p>
          <h1>Find the rooms inside the market.</h1>
          <p className="hero-copy">
            This page compresses the exported relationship surface into a correlation map, central
            nodes, and a PCA view of common motion. It is the quickest way to see whether a market
            is rotating together or fragmenting apart.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">{structure.heatmap.coinIds.length} assets in heatmap</span>
          <span className="hero-badge">{structure.pcaSummary.length} PCA components</span>
          <span className="hero-badge">{structure.centrality.length} centrality rows</span>
        </div>
      </section>

      <Panel eyebrow="Map" title="Top-asset correlation heatmap">
        <EChart
          className="chart-surface"
          option={buildHeatmapOption({
            axes: structure.heatmap.coinIds,
            rows: structure.heatmap.matrix,
          })}
        />
      </Panel>

      <div className="page-columns">
        <Panel eyebrow="Principal components" title="Explained variance curve">
          <EChart className="chart-surface" option={pcaOption} />
        </Panel>

        <Panel
          eyebrow="Loadings"
          title={`Top exposures for PC${selectedComponent}`}
          actions={
            <select
              className="search-input"
              onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                const next = Number(event.target.value);
                startTransition(() => setSelectedComponent(next));
              }}
              value={selectedComponent}
            >
              {structure.pcaSummary.map((row) => (
                <option key={row.component} value={row.component}>
                  Component {row.component}
                </option>
              ))}
            </select>
          }
        >
          <EChart className="chart-surface" option={loadingsOption} />
        </Panel>
      </div>

      <div className="page-columns">
        <Panel eyebrow="Network" title="Degree centrality leaders">
          <EChart className="chart-surface" option={centralityOption} />
        </Panel>

        <Panel eyebrow="Centrality table" title="Top network nodes">
          <DataTable<StructurePayload["centrality"][number]>
            rows={structure.centrality}
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
          />
        </Panel>
      </div>
    </div>
  );
}
