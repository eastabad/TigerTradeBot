/**
 * Indicator Storage Module
 * Handles saving and retrieving indicator calculation results from database
 */

import { db } from "../db";
import { indicatorResults, InsertIndicatorResult, IndicatorResultRow } from "@shared/schema";
import { eq, and } from "drizzle-orm";
import { AllIndicatorResults } from "./indicators";

export async function saveIndicatorResult(
  symbol: string,
  timeframe: string,
  indicators: AllIndicatorResults,
  price: number,
  barTimestamp: Date
): Promise<void> {
  const data: InsertIndicatorResult = {
    symbol,
    timeframe,
    tsiClose: indicators.tsi.tsiClose,
    tsiSignal: indicators.tsi.tsiSignal,
    tsiDirection: indicators.tsi.directionState,
    tsiHeikinBullish: indicators.tsi.heikinBullish,
    tsiHeikinBearish: indicators.tsi.heikinBearish,
    tsiMaBullish: indicators.tsi.maBullish,
    tsiMaBearish: indicators.tsi.maBearish,
    qqeRsiSt: indicators.qqe.rsiSt,
    qqeTsSt: indicators.qqe.tsSt,
    qqeRsiLt: indicators.qqe.rsiLt,
    qqeTsLt: indicators.qqe.tsLt,
    qqeState: indicators.qqe.state,
    qqeStBull: indicators.qqe.stBull,
    qqeLtBull: indicators.qqe.ltBull,
    qqeIsOverbought: indicators.qqe.isOverbought,
    qqeIsOversold: indicators.qqe.isOversold,
    momStateMom: indicators.momentum.stateMom,
    momStateSw: indicators.momentum.stateSw,
    momStateLs: indicators.momentum.stateLs,
    momComposite: indicators.momentum.composite,
    price,
    barTimestamp,
  };

  await db
    .insert(indicatorResults)
    .values(data)
    .onConflictDoUpdate({
      target: [indicatorResults.symbol, indicatorResults.timeframe],
      set: {
        ...data,
        calculatedAt: new Date(),
      },
    });
}

export async function getIndicatorResult(
  symbol: string,
  timeframe: string
): Promise<IndicatorResultRow | null> {
  const results = await db
    .select()
    .from(indicatorResults)
    .where(and(
      eq(indicatorResults.symbol, symbol),
      eq(indicatorResults.timeframe, timeframe)
    ))
    .limit(1);

  return results.length > 0 ? results[0] : null;
}

export async function getAllIndicatorResultsForSymbol(
  symbol: string
): Promise<Record<string, IndicatorResultRow>> {
  const results = await db
    .select()
    .from(indicatorResults)
    .where(eq(indicatorResults.symbol, symbol));

  const byTimeframe: Record<string, IndicatorResultRow> = {};
  for (const r of results) {
    byTimeframe[r.timeframe] = r;
  }
  return byTimeframe;
}

export async function getAllIndicatorResults(): Promise<IndicatorResultRow[]> {
  return db.select().from(indicatorResults);
}

export interface FlatIndicatorResult {
  symbol: string;
  timeframe: string;
  tsiClose: number | null;
  tsiSignal: number | null;
  tsiDirection: number | null;
  tsiHeikinBullish: boolean | null;
  tsiHeikinBearish: boolean | null;
  tsiMaBullish: boolean | null;
  tsiMaBearish: boolean | null;
  qqeRsiSt: number | null;
  qqeTsSt: number | null;
  qqeRsiLt: number | null;
  qqeTsLt: number | null;
  qqeState: string | null; // "bullish_aligned" | "pullback" | "rebound" | "bearish_aligned"
  qqeStBull: boolean | null;
  qqeLtBull: boolean | null;
  qqeIsOverbought: boolean | null;
  qqeIsOversold: boolean | null;
  momStateMom: number | null;
  momStateSw: number | null;
  momStateLs: number | null;
  momComposite: number | null;
  price: number | null;
  barTimestamp: Date | null;
  calculatedAt: Date | null;
}

export function toFlatResult(row: IndicatorResultRow): FlatIndicatorResult {
  return {
    symbol: row.symbol,
    timeframe: row.timeframe,
    tsiClose: row.tsiClose,
    tsiSignal: row.tsiSignal,
    tsiDirection: row.tsiDirection,
    tsiHeikinBullish: row.tsiHeikinBullish,
    tsiHeikinBearish: row.tsiHeikinBearish,
    tsiMaBullish: row.tsiMaBullish,
    tsiMaBearish: row.tsiMaBearish,
    qqeRsiSt: row.qqeRsiSt,
    qqeTsSt: row.qqeTsSt,
    qqeRsiLt: row.qqeRsiLt,
    qqeTsLt: row.qqeTsLt,
    qqeState: row.qqeState,
    qqeStBull: row.qqeStBull,
    qqeLtBull: row.qqeLtBull,
    qqeIsOverbought: row.qqeIsOverbought,
    qqeIsOversold: row.qqeIsOversold,
    momStateMom: row.momStateMom,
    momStateSw: row.momStateSw,
    momStateLs: row.momStateLs,
    momComposite: row.momComposite,
    price: row.price,
    barTimestamp: row.barTimestamp,
    calculatedAt: row.calculatedAt,
  };
}
