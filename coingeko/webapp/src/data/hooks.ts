import { useEffect, useEffectEvent, useState } from "react";
import { ZodType } from "zod";

import { fetchJson } from "./api";

type AsyncState<T> =
  | { status: "idle" | "loading" }
  | { status: "success"; data: T }
  | { status: "error"; error: string };

export function useJsonData<T>(relativePath: string | null, schema: ZodType<T>): AsyncState<T> {
  const [state, setState] = useState<AsyncState<T>>(
    relativePath ? { status: "loading" } : { status: "idle" },
  );

  const loadData = useEffectEvent(async (signal: AbortSignal) => {
    if (!relativePath) {
      setState({ status: "idle" });
      return;
    }

    setState({ status: "loading" });
    try {
      const data = await fetchJson(relativePath, schema, signal);
      if (!signal.aborted) {
        setState({ status: "success", data });
      }
    } catch (error) {
      if (!signal.aborted) {
        setState({
          status: "error",
          error: error instanceof Error ? error.message : "Unknown data loading error",
        });
      }
    }
  });

  useEffect(() => {
    const controller = new AbortController();
    void loadData(controller.signal);
    return () => controller.abort();
  }, [relativePath]);

  return state;
}
