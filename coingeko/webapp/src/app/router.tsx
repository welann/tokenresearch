import { lazy, Suspense, type ReactNode } from "react";

import { createHashRouter } from "react-router-dom";

import { AppShell } from "./AppShell";

const OverviewPage = lazy(async () => {
  const module = await import("../pages/OverviewPage");
  return { default: module.OverviewPage };
});

const CoinPage = lazy(async () => {
  const module = await import("../pages/CoinPage");
  return { default: module.CoinPage };
});

const PairPage = lazy(async () => {
  const module = await import("../pages/PairPage");
  return { default: module.PairPage };
});

const StructurePage = lazy(async () => {
  const module = await import("../pages/StructurePage");
  return { default: module.StructurePage };
});

const SourcesPage = lazy(async () => {
  const module = await import("../pages/SourcesPage");
  return { default: module.SourcesPage };
});

function RouteFallback({ children }: { children: ReactNode }) {
  return (
    <Suspense
      fallback={
        <div className="status-block status-loading">
          <p>Loading route…</p>
        </div>
      }
    >
      {children}
    </Suspense>
  );
}

export const router = createHashRouter([
  {
    path: "/",
    element: <AppShell />,
    children: [
      {
        index: true,
        element: (
          <RouteFallback>
            <OverviewPage />
          </RouteFallback>
        ),
      },
      {
        path: "coin",
        element: (
          <RouteFallback>
            <CoinPage />
          </RouteFallback>
        ),
      },
      {
        path: "coin/:coinId",
        element: (
          <RouteFallback>
            <CoinPage />
          </RouteFallback>
        ),
      },
      {
        path: "pair",
        element: (
          <RouteFallback>
            <PairPage />
          </RouteFallback>
        ),
      },
      {
        path: "pair/:pairKey",
        element: (
          <RouteFallback>
            <PairPage />
          </RouteFallback>
        ),
      },
      {
        path: "structure",
        element: (
          <RouteFallback>
            <StructurePage />
          </RouteFallback>
        ),
      },
      {
        path: "sources",
        element: (
          <RouteFallback>
            <SourcesPage />
          </RouteFallback>
        ),
      },
    ],
  },
]);
