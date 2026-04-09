import { NavLink, Outlet, useOutletContext } from "react-router-dom";

import { useJsonData } from "../data/hooks";
import { Manifest, manifestSchema } from "../data/schemas";
import { StatusBlock } from "../components/StatusBlock";

type AppShellContext = {
  manifest: Manifest | null;
};

export function useAppShellContext() {
  return useOutletContext<AppShellContext>();
}

export function AppShell() {
  const manifestState = useJsonData("data/manifest.json", manifestSchema);

  return (
    <div className="app-shell">
      <aside className="shell-rail">
        <div className="brand-block">
          <p className="brand-kicker">CoinGecko research stack</p>
          <h1>Metrics Atlas</h1>
          <p className="brand-copy">
            A static field notebook for reading market structure, lead-lag signals, and
            pairwise relationships from the exported daily panel.
          </p>
        </div>

        <nav className="shell-nav">
          <NavLink to="/">Overview</NavLink>
          <NavLink to="/coin">Coin Lab</NavLink>
          <NavLink to="/pair">Pair Lab</NavLink>
          <NavLink to="/structure">Structure</NavLink>
        </nav>

        <section className="rail-card">
          {manifestState.status === "success" ? (
            <>
              <p className="rail-label">Snapshot date</p>
              <p className="rail-value">{manifestState.data.analysisDate}</p>
              <p className="rail-label">Tracked assets</p>
              <p className="rail-value">{manifestState.data.assetCount}</p>
            </>
          ) : manifestState.status === "error" ? (
            <StatusBlock status="error" message={manifestState.error} />
          ) : (
            <StatusBlock status="loading" message="Loading manifest…" />
          )}
        </section>
      </aside>

      <main className="shell-main">
        <Outlet
          context={{
            manifest: manifestState.status === "success" ? manifestState.data : null,
          }}
        />
      </main>
    </div>
  );
}
