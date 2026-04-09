export function formatCurrency(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: Math.abs(value) >= 1_000_000_000 ? "compact" : "standard",
    maximumFractionDigits: digits,
  }).format(value);
}

export function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: digits,
    signDisplay: "exceptZero",
  }).format(value);
}

export function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return new Intl.NumberFormat("en-US", {
    notation: Math.abs(value) >= 1_000_000 ? "compact" : "standard",
    maximumFractionDigits: digits,
  }).format(value);
}

export function formatTableValue(value: unknown, key = ""): string {
  if (value === null || value === undefined) {
    return "N/A";
  }
  if (typeof value === "number") {
    if (Number.isNaN(value)) {
      return "N/A";
    }
    const normalizedKey = key.toLowerCase();
    if (
      normalizedKey.includes("price") ||
      normalizedKey.includes("marketcap") ||
      normalizedKey.includes("market_cap") ||
      normalizedKey.includes("volume")
    ) {
      return formatCurrency(value, 2);
    }
    if (normalizedKey.includes("return") || normalizedKey.includes("drawdown")) {
      return formatPercent(value, 2);
    }
    return formatNumber(value, 4);
  }
  if (typeof value === "boolean") {
    return value ? "True" : "False";
  }
  return String(value);
}

export function titleFromSlug(slug: string): string {
  return slug
    .split("-")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}
