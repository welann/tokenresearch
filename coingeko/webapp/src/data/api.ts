import { ZodType } from "zod";

const baseUrl = import.meta.env.BASE_URL;

export function withBaseUrl(relativePath: string): string {
  const normalized = relativePath.startsWith("/") ? relativePath.slice(1) : relativePath;
  return `${baseUrl}${normalized}`;
}

export async function fetchJson<T>(
  relativePath: string,
  schema: ZodType<T>,
  signal?: AbortSignal,
): Promise<T> {
  const response = await fetch(withBaseUrl(relativePath), { signal });
  if (!response.ok) {
    throw new Error(`Request failed for ${relativePath}: ${response.status}`);
  }
  const raw = (await response.json()) as unknown;
  return schema.parse(raw);
}
