import { CoinDetail } from "../data/schemas";

type CoinSeriesRow = CoinDetail["series"][number];

export type PairAlignedRow = {
  date: string;
  priceX: number;
  priceY: number;
  returnX: number | null;
  returnY: number | null;
};

function correlation(valuesX: number[], valuesY: number[]): number | null {
  if (valuesX.length < 2 || valuesY.length < 2 || valuesX.length !== valuesY.length) {
    return null;
  }

  const meanX = valuesX.reduce((sum, value) => sum + value, 0) / valuesX.length;
  const meanY = valuesY.reduce((sum, value) => sum + value, 0) / valuesY.length;
  let covariance = 0;
  let varianceX = 0;
  let varianceY = 0;

  for (let index = 0; index < valuesX.length; index += 1) {
    const deltaX = valuesX[index] - meanX;
    const deltaY = valuesY[index] - meanY;
    covariance += deltaX * deltaY;
    varianceX += deltaX ** 2;
    varianceY += deltaY ** 2;
  }

  if (varianceX <= 0 || varianceY <= 0) {
    return null;
  }

  return covariance / Math.sqrt(varianceX * varianceY);
}

export function alignPairSeries(left: CoinSeriesRow[], right: CoinSeriesRow[]): PairAlignedRow[] {
  const rightLookup = new Map(right.map((row) => [row.date, row]));
  const aligned: PairAlignedRow[] = [];

  for (const leftRow of left) {
    const rightRow = rightLookup.get(leftRow.date);
    if (!rightRow) {
      continue;
    }
    aligned.push({
      date: leftRow.date,
      priceX: leftRow.price,
      priceY: rightRow.price,
      returnX: leftRow.logReturn,
      returnY: rightRow.logReturn,
    });
  }

  return aligned;
}

export function buildNormalizedPriceSeries(aligned: PairAlignedRow[]) {
  if (aligned.length === 0) {
    return { left: [], right: [] };
  }

  const baseX = aligned[0].priceX;
  const baseY = aligned[0].priceY;
  return {
    left: aligned.map((row) => [row.date, (row.priceX / baseX) * 100] as [string, number]),
    right: aligned.map((row) => [row.date, (row.priceY / baseY) * 100] as [string, number]),
  };
}

export function buildRelativeStrengthSeries(aligned: PairAlignedRow[]) {
  return aligned.map((row) => [row.date, row.priceX / row.priceY] as [string, number]);
}

export function buildRollingCorrelationSeries(aligned: PairAlignedRow[], window: number) {
  return aligned.map((_, index) => {
    const slice = aligned.slice(Math.max(0, index - window + 1), index + 1);
    const windowPairs = slice.filter(
      (row) => row.returnX !== null && row.returnY !== null,
    ) as Array<PairAlignedRow & { returnX: number; returnY: number }>;

    if (windowPairs.length < window) {
      return [aligned[index].date, null] as [string, number | null];
    }

    return [
      aligned[index].date,
      correlation(
        windowPairs.map((row) => row.returnX),
        windowPairs.map((row) => row.returnY),
      ),
    ] as [string, number | null];
  });
}

export function buildCcfSeries(aligned: PairAlignedRow[], maxLag: number) {
  const rows: Array<{ lag: number; value: number | null; nObs: number }> = [];

  for (let lag = -maxLag; lag <= maxLag; lag += 1) {
    const valuesX: number[] = [];
    const valuesY: number[] = [];

    for (let index = 0; index < aligned.length; index += 1) {
      const shiftedIndex = index + lag;
      if (shiftedIndex < 0 || shiftedIndex >= aligned.length) {
        continue;
      }

      const current = aligned[index];
      const shifted = aligned[shiftedIndex];
      if (current.returnX === null || shifted.returnY === null) {
        continue;
      }

      valuesX.push(current.returnX);
      valuesY.push(shifted.returnY);
    }

    rows.push({
      lag,
      value: correlation(valuesX, valuesY),
      nObs: valuesX.length,
    });
  }

  return rows;
}
