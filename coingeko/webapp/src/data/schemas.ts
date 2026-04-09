import { z } from "zod";

const nullableNumber = z.number().nullable();

export const manifestSchema = z.object({
  generatedAt: z.string(),
  analysisDate: z.string(),
  assetCount: z.number(),
  featuredCoinIds: z.array(z.string()),
  featuredPairKeys: z.array(z.string()),
  routes: z.array(z.string()),
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
  featuredCoins: z.object({
    leadersByMarketCap: z.array(coinSummarySchema),
    leadersByReturn30d: z.array(coinSummarySchema),
    leadersByVol30d: z.array(coinSummarySchema),
  }),
  featuredPairs: z.array(
    z.object({
      pairKey: z.string(),
      coinIdX: z.string(),
      coinIdY: z.string(),
      nObs: z.number(),
      pearsonCorr: z.number(),
      absCorr: z.number(),
    }),
  ),
});

export const coinDetailSchema = z.object({
  summary: coinSummarySchema,
  series: z.array(
    z.object({
      date: z.string(),
      price: z.number(),
      marketCap: z.number(),
      volume: z.number(),
      logReturn: nullableNumber,
      drawdown: nullableNumber,
    }),
  ),
});

export const pairIndexItemSchema = z.object({
  pairKey: z.string(),
  coinIdX: z.string(),
  coinIdY: z.string(),
  nObs: z.number(),
  pearsonCorr: z.number(),
  absCorr: z.number(),
});

export const pairDetailSchema = z.object({
  summary: pairIndexItemSchema.extend({
    labelX: z.string(),
    labelY: z.string(),
    symbolX: z.string(),
    symbolY: z.string(),
  }),
  rollingCorrelation: z.array(
    z.object({
      date: z.string(),
      window: z.number(),
      value: nullableNumber,
    }),
  ),
  relativeStrength: z.array(
    z.object({
      date: z.string(),
      value: z.number(),
    }),
  ),
  ccf: z.array(
    z.object({
      lag: z.number(),
      value: nullableNumber,
      nObs: z.number(),
    }),
  ),
});

export const structureSchema = z.object({
  heatmap: z.object({
    coinIds: z.array(z.string()),
    matrix: z.array(
      z.object({
        x: z.string(),
        y: z.string(),
        value: nullableNumber,
      }),
    ),
  }),
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
    }),
  ),
});

export type Manifest = z.infer<typeof manifestSchema>;
export type CoinSummary = z.infer<typeof coinSummarySchema>;
export type OverviewPayload = z.infer<typeof overviewSchema>;
export type CoinDetail = z.infer<typeof coinDetailSchema>;
export type PairIndexItem = z.infer<typeof pairIndexItemSchema>;
export type PairDetail = z.infer<typeof pairDetailSchema>;
export type StructurePayload = z.infer<typeof structureSchema>;
