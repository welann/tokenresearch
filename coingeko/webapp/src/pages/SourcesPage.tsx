import { ChangeEvent, startTransition, useDeferredValue, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { useAppShellContext } from "../app/AppShell";
import { DataTable } from "../components/DataTable";
import { Panel } from "../components/Panel";
import { StatusBlock } from "../components/StatusBlock";
import { useJsonData } from "../data/hooks";
import { tableRowsSchema } from "../data/schemas";
import { formatNumber, formatTableValue } from "../lib/format";

export function SourcesPage() {
  const { sourceCatalog } = useAppShellContext();
  const [searchParams, setSearchParams] = useSearchParams();
  const [query, setQuery] = useState("");
  const [rowLimit, setRowLimit] = useState(60);

  const deferredQuery = useDeferredValue(query);
  const categoryFilter = searchParams.get("category") ?? "all";
  const filteredCatalog =
    categoryFilter === "all"
      ? sourceCatalog
      : sourceCatalog.filter((source) => source.category === categoryFilter);

  const selectedSourceId =
    searchParams.get("source") ??
    filteredCatalog[0]?.id ??
    sourceCatalog[0]?.id ??
    null;
  const selectedSource =
    filteredCatalog.find((source) => source.id === selectedSourceId) ??
    sourceCatalog.find((source) => source.id === selectedSourceId) ??
    null;

  const tableState = useJsonData(
    selectedSource?.exportedPath ? `data/${selectedSource.exportedPath}` : null,
    tableRowsSchema,
  );

  if (tableState.status === "error") {
    return <StatusBlock status="error" message={tableState.error} />;
  }

  const tableRows = tableState.status === "success" ? tableState.data : [];
  const normalizedQuery = deferredQuery.trim().toLowerCase();
  const visibleRows = tableRows
    .filter((row) => {
      if (!normalizedQuery) {
        return true;
      }
      return Object.values(row).some((value) => String(value ?? "").toLowerCase().includes(normalizedQuery));
    })
    .slice(0, rowLimit);

  return (
    <div className="page-stack">
      <section className="page-hero">
        <div>
          <p className="hero-kicker">Sources</p>
          <h1>Browse the exported source files directly.</h1>
          <p className="hero-copy">
            This page is the raw catalog view. Small and medium tables load here on demand. Larger
            datasets stay behind the dedicated Coin, Pair, and Structure pages where they can be
            explored with safer filters.
          </p>
        </div>
        <div className="hero-badges">
          <span className="hero-badge">{sourceCatalog.length} source entries</span>
          <span className="hero-badge">{filteredCatalog.length} visible under current category</span>
          <span className="hero-badge">{selectedSource?.title ?? "No source selected"}</span>
        </div>
      </section>

      <div className="page-columns">
        <Panel
          eyebrow="Catalog"
          title="Source registry"
          actions={
            <div className="control-grid control-grid-compact">
              <label className="control-field">
                <span>Category</span>
                <select
                  className="search-input"
                  onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                    const next = new URLSearchParams(searchParams);
                    next.set("category", event.target.value);
                    if (event.target.value !== "all") {
                      const firstMatch = sourceCatalog.find((source) => source.category === event.target.value);
                      if (firstMatch) {
                        next.set("source", firstMatch.id);
                      }
                    }
                    startTransition(() => setSearchParams(next));
                  }}
                  value={categoryFilter}
                >
                  <option value="all">All</option>
                  <option value="global">Global</option>
                  <option value="coin">Coin</option>
                  <option value="pair">Pair</option>
                  <option value="structure">Structure</option>
                </select>
              </label>
            </div>
          }
        >
          <div className="source-grid">
            {filteredCatalog.map((source) => (
              <button
                className={`source-card ${source.id === selectedSource?.id ? "active" : ""}`.trim()}
                key={source.id}
                onClick={() => {
                  const next = new URLSearchParams(searchParams);
                  next.set("source", source.id);
                  startTransition(() => setSearchParams(next));
                }}
                type="button"
              >
                <p className="picker-title">{source.title}</p>
                <p className="picker-meta">
                  {source.category} · {source.viewer} · {formatNumber(source.rowCount)}
                </p>
                <p className="small-note">{source.description}</p>
              </button>
            ))}
          </div>
        </Panel>

        <Panel
          eyebrow="Preview"
          title={selectedSource?.title ?? "Select a source"}
          actions={
            selectedSource?.exportedPath ? (
              <div className="control-grid control-grid-compact">
                <input
                  className="search-input"
                  onChange={(event: ChangeEvent<HTMLInputElement>) => {
                    startTransition(() => setQuery(event.target.value));
                  }}
                  placeholder="Search visible rows"
                  value={query}
                />
                <label className="control-field">
                  <span>Rows</span>
                  <input
                    className="search-input"
                    min={10}
                    onChange={(event: ChangeEvent<HTMLInputElement>) => {
                      startTransition(() => setRowLimit(Number(event.target.value) || 60));
                    }}
                    type="number"
                    value={rowLimit}
                  />
                </label>
              </div>
            ) : null
          }
        >
          {selectedSource ? (
            <>
              <div className="info-grid">
                <div className="info-pair">
                  <span>Source file</span>
                  <strong>{selectedSource.sourceFile}</strong>
                </div>
                <div className="info-pair">
                  <span>Category</span>
                  <strong>{selectedSource.category}</strong>
                </div>
                <div className="info-pair">
                  <span>Viewer</span>
                  <strong>{selectedSource.viewer}</strong>
                </div>
                <div className="info-pair">
                  <span>Rows</span>
                  <strong>{formatNumber(selectedSource.rowCount)}</strong>
                </div>
                <div className="info-pair">
                  <span>Columns</span>
                  <strong>{formatNumber(selectedSource.columns.length)}</strong>
                </div>
                <div className="info-pair">
                  <span>Table export</span>
                  <strong>{selectedSource.exportedPath ? "Available" : "Route only"}</strong>
                </div>
              </div>

              {selectedSource.exportedPath ? (
                tableState.status === "loading" ? (
                  <StatusBlock status="loading" message="Loading source table…" />
                ) : (
                  <DataTable<Record<string, unknown>>
                    columns={(visibleRows[0] ? Object.keys(visibleRows[0]) : selectedSource.columns).map((key) => ({
                      key,
                      label: key,
                      render: (row: Record<string, unknown>) => formatTableValue(row[key], key),
                    }))}
                    getRowKey={(_row, index) => `${selectedSource.id}-${index}`}
                    rows={visibleRows}
                  />
                )
              ) : (
                <StatusBlock
                  status="idle"
                  message={`This source is available in the build but should be explored through the ${selectedSource.viewer} view rather than raw table loading.`}
                />
              )}
            </>
          ) : (
            <StatusBlock status="idle" message="No source is available for the current filter." />
          )}
        </Panel>
      </div>
    </div>
  );
}
