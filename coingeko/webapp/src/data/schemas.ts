import { z } from "zod";

const nullableNumber = z.number().nullable();
const nullableString = z.string().nullable();

export const manifestSchema = z.object({
  generatedAt: z.string(),
  analysisDate: z.string(),
  assetCount: z.number(),
  featuredCoinIds: z.array(z.string()),
  featuredPairKeys: z.array(z.string()),
  routes: z.array(z.string()),
  availableSources: z.array(z.string()),
});

export const coinSummarySchema = z.object({
  coinId: z.string(),
  symbol: z.string(),
  name: z.string(),
  marketCapRank: z.number(),
  latestDate: z.string(),
  price: z.number(),
  marketCap: z.number(),
  volume: z.number(),
  return7d: nullableNumber,
  return30d: nullableNumber,
  return90d: nullableNumber,
  vol30d: nullableNumber,
  latestDrawdown: nullableNumber,
});

export const sourceCatalogItemSchema = z.object({
  id: z.string(),
  title: z.string(),
  description: z.string(),
  category: z.string(),
  viewer: z.string(),
  rowCount: z.number().nullable(),
  columns: z.array(z.string()),
  exportedPath: nullableString,
  sourceFile: z.string(),
});

export const overviewSchema = z.object({
  summary: z.object({
    assetCount: z.number(),
    dateStart: z.string(),
    dateEnd: z.string(),
    latestMarketCap: z.number(),
    latestVolume: z.number(),
    marketReturn30d: z.number(),
    breadth30d: z.number(),
  }),
  marketIndexSeries: z.array(
    z.object({
      date: z.string(),
      marketIndex: z.number(),
      marketReturn: nullableNumber,
    }),
  ),
});

export const coinExposureSchema = z
  .object({
    beta: nullableNumber,
    alpha: nullableNumber,
    nObs: z.number(),
    marketProxy: z.string(),
    rSquared: nullableNumber,
    adjRSquared: nullableNumber,
    residualVol: nullableNumber,
    residualVolAnnualized: nullableNumber,
  })
  .nullable();

export const riskRowSchema = z.object({
  date: nullableString,
  window: nullableNumber,
  metricScope: z.string(),
  realizedVol: nullableNumber,
  downsideSemivariance: nullableNumber,
  downsideSemivol: nullableNumber,
  mdd: nullableNumber,
  mddStartDate: nullableString,
  mddTroughDate: nullableString,
  latestDrawdown: nullableNumber,
});

export const coinDetailSchema = z.object({
  summary: coinSummarySchema,
  series: z.array(
    z.object({
      date: z.string(),
      price: z.number(),
      marketCap: z.number(),
      volume: z.number(),
      pointsInDay: nullableNumber,
      logReturn: nullableNumber,
      drawdown: nullableNumber,
    }),
  ),
  exposure: coinExposureSchema,
  riskRows: z.array(riskRowSchema),
});

export const pairIndexItemSchema = z.object({
  pairKey: z.string(),
  coinIdX: z.string(),
  coinIdY: z.string(),
  labelX: z.string(),
  labelY: z.string(),
  symbolX: z.string(),
  symbolY: z.string(),
  marketCapRankX: z.number(),
  marketCapRankY: z.number(),
  rankScore: z.number(),
  nObs: z.number(),
  pearsonCorr: z.number(),
  absCorr: z.number(),
});

export const dccRowSchema = z.object({
  date: z.string(),
  coinIdX: z.string(),
  coinIdY: z.string(),
  dccCorr: z.number(),
  n_obs: z.number().optional(),
  nObs: z.number().optional(),
  garchSpec: nullableString,
  dccAlpha: nullableNumber,
  dccBeta: nullableNumber,
});

export const tableRowsSchema = z.array(z.record(z.string(), z.unknown()));

export const structureSchema = z.object({
  heatmapUniverse: z.array(z.string()),
  heatmapRows: z.array(
    z.object({
      x: z.string(),
      y: z.string(),
      value: nullableNumber,
    }),
  ),
  pcaSummary: z.array(
    z.object({
      component: z.number(),
      explainedVariance: z.number(),
      explainedVarianceRatio: z.number(),
      cumulativeRatio: z.number(),
    }),
  ),
  pcaLoadings: z.array(
    z.object({
      component: z.number(),
      coinId: z.string(),
      loading: z.number(),
    }),
  ),
  centrality: z.array(
    z.object({
      coinId: z.string(),
      degreeCentrality: z.number(),
      betweennessCentrality: z.number(),
      eigenvectorCentrality: z.number(),
      graphType: z.string(),
    }),
  ),
  clusteringLinkage: z.array(z.record(z.string(), z.unknown())),
});

export type Manifest = z.infer<typeof manifestSchema>;
export type CoinSummary = z.infer<typeof coinSummarySchema>;
export type SourceCatalogItem = z.infer<typeof sourceCatalogItemSchema>;
export type OverviewPayload = z.infer<typeof overviewSchema>;
export type CoinDetail = z.infer<typeof coinDetailSchema>;
export type CoinExposure = z.infer<typeof coinExposureSchema>;
export type RiskRow = z.infer<typeof riskRowSchema>;
export type PairIndexItem = z.infer<typeof pairIndexItemSchema>;
export type DccRow = z.infer<typeof dccRowSchema>;
export type TableRow = z.infer<typeof tableRowsSchema>[number];
export type StructurePayload = z.infer<typeof structureSchema>;
