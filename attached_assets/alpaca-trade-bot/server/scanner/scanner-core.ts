/**
 * Scanner Core Module
 * Handles: Data fetching, K-line caching, indicator calculation, strategy matching
 */

import { db } from "../db";
import { watchlist, barCache, scanResults, scannerState } from "@shared/schema";
import { eq, and, desc, sql } from "drizzle-orm";
import { BarData, calculateAllIndicators, AllIndicatorResults } from "./indicators";
import { evaluateAllStrategies, StrategyMatch, STRATEGIES } from "./strategies";
import { saveIndicatorResult } from "./indicator-storage";

const ALPACA_DATA_URL = "https://data.alpaca.markets/v2";

interface AlpacaBar {
  t: string;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
  vw?: number;
}

interface ScanResult {
  symbol: string;
  timeframe: string;
  price: number;
  strategies: StrategyMatch[];
  indicators: AllIndicatorResults;
  scannedAt: Date;
}

// ==================== Data Fetching ====================

async function fetchBarsFromAlpaca(
  symbol: string,
  timeframe: string,
  maxBars: number = 5000
): Promise<AlpacaBar[]> {
  const apiKey = process.env.strategytest_apikey;
  const secretKey = process.env.strategytest_SECRETkey;

  if (!apiKey || !secretKey) {
    throw new Error("Alpaca API keys not configured");
  }

  // Calculate start date based on timeframe - only need ~300 bars per level
  // Each trading day ≈ 6.5 hours = 390 minutes
  const daysBack: Record<string, number> = {
    "5Min": 7,       // ~500 bars (300 + buffer)
    "15Min": 20,     // ~500 bars
    "1Hour": 60,     // ~400 bars
    "4Hour": 250,    // ~400 bars
  };
  const days = daysBack[timeframe] || 20;
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);

  const allBars: AlpacaBar[] = [];
  let nextPageToken: string | null = null;
  const pageLimit = 1000; // Max per API call

  // Fetch with pagination until we have enough bars or no more data
  do {
    let url = `${ALPACA_DATA_URL}/stocks/${symbol}/bars?timeframe=${timeframe}&limit=${pageLimit}&start=${startDate.toISOString()}`;
    if (nextPageToken) {
      url += `&page_token=${nextPageToken}`;
    }

    const response = await fetch(url, {
      headers: {
        "APCA-API-KEY-ID": apiKey,
        "APCA-API-SECRET-KEY": secretKey,
      },
    });

    if (!response.ok) {
      throw new Error(`Alpaca API error: ${response.status} ${response.statusText}`);
    }

    const data = await response.json();
    const bars = data.bars || [];
    allBars.push(...bars);
    nextPageToken = data.next_page_token || null;

  } while (nextPageToken && allBars.length < maxBars);

  // Return most recent bars up to maxBars
  return allBars.slice(-maxBars);
}

// ==================== Bar Cache Management ====================

async function getLastCachedBarTime(
  symbol: string,
  timeframe: string
): Promise<Date | null> {
  const result = await db
    .select({ timestamp: barCache.timestamp })
    .from(barCache)
    .where(and(eq(barCache.symbol, symbol), eq(barCache.timeframe, timeframe)))
    .orderBy(desc(barCache.timestamp))
    .limit(1);

  return result.length > 0 ? result[0].timestamp : null;
}

async function saveBarsToCache(
  symbol: string,
  timeframe: string,
  bars: AlpacaBar[]
): Promise<void> {
  if (bars.length === 0) return;

  for (const bar of bars) {
    try {
      await db
        .insert(barCache)
        .values({
          symbol,
          timeframe,
          timestamp: new Date(bar.t),
          open: bar.o,
          high: bar.h,
          low: bar.l,
          close: bar.c,
          volume: bar.v,
          vwap: bar.vw || null,
        })
        .onConflictDoUpdate({
          target: [barCache.symbol, barCache.timeframe, barCache.timestamp],
          set: {
            open: bar.o,
            high: bar.h,
            low: bar.l,
            close: bar.c,
            volume: bar.v,
            vwap: bar.vw || null,
          },
        });
    } catch (error) {
      // Ignore duplicate key errors
    }
  }
}

async function getCachedBars(
  symbol: string,
  timeframe: string,
  limit: number = 200
): Promise<BarData[]> {
  const results = await db
    .select()
    .from(barCache)
    .where(and(eq(barCache.symbol, symbol), eq(barCache.timeframe, timeframe)))
    .orderBy(desc(barCache.timestamp))
    .limit(limit);

  // Reverse to get chronological order
  return results.reverse().map((bar) => ({
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
    volume: bar.volume,
    timestamp: bar.timestamp,
  }));
}

// ==================== Incremental Data Update ====================

async function updateBarsIncremental(
  symbol: string,
  timeframe: string,
  fullRefresh: boolean = false
): Promise<BarData[]> {
  const lastBarTime = fullRefresh ? null : await getLastCachedBarTime(symbol, timeframe);
  
  let limit = 1000; // Enough bars for indicator convergence
  
  if (lastBarTime) {
    // Calculate how many bars we need based on time difference
    const now = new Date();
    const diffMs = now.getTime() - lastBarTime.getTime();
    const diffMins = diffMs / (1000 * 60);
    
    // Estimate bars needed based on timeframe
    const timeframeMins: Record<string, number> = {
      "5Min": 5,
      "15Min": 15,
      "1Hour": 60,
      "4Hour": 240,
    };
    
    const tfMins = timeframeMins[timeframe] || 15;
    limit = Math.min(Math.ceil(diffMins / tfMins) + 5, 1000);
  }
  // For fresh fetch, don't set startDate - API will return the most recent bars

  // Fetch new bars from Alpaca
  const bars = await fetchBarsFromAlpaca(symbol, timeframe, limit);
  
  // Save to cache
  await saveBarsToCache(symbol, timeframe, bars);
  
  // Return cached bars for indicator calculation
  return getCachedBars(symbol, timeframe, 5000);
}

// ==================== Watchlist Management ====================

export async function getActiveWatchlist(): Promise<string[]> {
  const results = await db
    .select({ symbol: watchlist.symbol })
    .from(watchlist)
    .where(eq(watchlist.isActive, true));
  
  return results.map((r) => r.symbol);
}

export async function addToWatchlist(symbol: string, name?: string, sector?: string): Promise<void> {
  await db
    .insert(watchlist)
    .values({ symbol: symbol.toUpperCase(), name, sector })
    .onConflictDoUpdate({
      target: watchlist.symbol,
      set: { name, sector, isActive: true },
    });
}

export async function removeFromWatchlist(symbol: string): Promise<void> {
  await db
    .update(watchlist)
    .set({ isActive: false })
    .where(eq(watchlist.symbol, symbol.toUpperCase()));
}

export async function importWatchlist(symbols: string[]): Promise<number> {
  let count = 0;
  for (const symbol of symbols) {
    try {
      await addToWatchlist(symbol.trim().toUpperCase());
      count++;
    } catch (error) {
      console.error(`Failed to add ${symbol}:`, error);
    }
  }
  return count;
}

// ==================== Scanning Logic ====================

export async function scanSymbol(
  symbol: string,
  timeframe: string,
  fullRefresh: boolean = false
): Promise<ScanResult | null> {
  try {
    // Get bar data (incremental update)
    const bars = await updateBarsIncremental(symbol, timeframe, fullRefresh);
    
    if (bars.length < 50) {
      console.log(`Insufficient data for ${symbol} ${timeframe}: ${bars.length} bars`);
      return null;
    }

    // Calculate all indicators
    const indicators = calculateAllIndicators(bars);
    
    // Evaluate all strategies
    const strategies = evaluateAllStrategies(indicators, timeframe);
    
    const lastBar = bars[bars.length - 1];
    
    // Save indicator results to database for cross-timeframe strategy evaluation
    await saveIndicatorResult(
      symbol,
      timeframe,
      indicators,
      lastBar.close,
      lastBar.timestamp
    );
    
    return {
      symbol,
      timeframe,
      price: lastBar.close,
      strategies,
      indicators,
      scannedAt: new Date(),
    };
  } catch (error) {
    console.error(`Error scanning ${symbol} ${timeframe}:`, error);
    return null;
  }
}

export async function scanAllSymbols(
  timeframe: string,
  fullRefresh: boolean = false,
  batchSize: number = 10,
  delayMs: number = 1000
): Promise<ScanResult[]> {
  const symbols = await getActiveWatchlist();
  const results: ScanResult[] = [];
  
  console.log(`Starting scan for ${symbols.length} symbols on ${timeframe}`);
  
  // Process in batches to respect API rate limits
  for (let i = 0; i < symbols.length; i += batchSize) {
    const batch = symbols.slice(i, i + batchSize);
    
    const batchResults = await Promise.all(
      batch.map((symbol) => scanSymbol(symbol, timeframe, fullRefresh))
    );
    
    for (const result of batchResults) {
      if (result) {
        results.push(result);
      }
    }
    
    // Rate limit delay between batches
    if (i + batchSize < symbols.length) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  
  console.log(`Scan complete: ${results.length} symbols processed`);
  
  return results;
}

// ==================== Save & Query Results ====================

export async function saveScanResults(results: ScanResult[]): Promise<number> {
  let count = 0;
  
  for (const result of results) {
    for (const strategy of result.strategies) {
      try {
        await db.insert(scanResults).values({
          symbol: result.symbol,
          strategyId: strategy.strategyId,
          strategyName: strategy.strategyName,
          signalType: strategy.signalType || "NEUTRAL",
          timeframe: result.timeframe,
          indicatorValues: JSON.stringify(result.indicators),
          price: result.price,
        });
        count++;
      } catch (error) {
        console.error(`Failed to save result for ${result.symbol}:`, error);
      }
    }
  }
  
  return count;
}

export async function getLatestScanResults(
  limit: number = 50,
  timeframe?: string,
  strategyId?: string
): Promise<typeof scanResults.$inferSelect[]> {
  let query = db.select().from(scanResults);
  
  const conditions = [];
  if (timeframe) {
    conditions.push(eq(scanResults.timeframe, timeframe));
  }
  if (strategyId) {
    conditions.push(eq(scanResults.strategyId, strategyId));
  }
  
  if (conditions.length > 0) {
    query = query.where(and(...conditions)) as typeof query;
  }
  
  return query.orderBy(desc(scanResults.scannedAt)).limit(limit);
}

export async function getResultsGroupedByStrategy(
  timeframe?: string
): Promise<Record<string, { symbol: string; signalType: string; price: number; scannedAt: Date }[]>> {
  const results = await getLatestScanResults(200, timeframe);
  
  const grouped: Record<string, { symbol: string; signalType: string; price: number; scannedAt: Date }[]> = {};
  
  for (const result of results) {
    if (!grouped[result.strategyId]) {
      grouped[result.strategyId] = [];
    }
    grouped[result.strategyId].push({
      symbol: result.symbol,
      signalType: result.signalType,
      price: result.price || 0,
      scannedAt: result.scannedAt || new Date(),
    });
  }
  
  return grouped;
}

// ==================== Full Scan Orchestration ====================

export interface FullScanResult {
  timeframe: string;
  scannedCount: number;
  signalsFound: number;
  signals: {
    symbol: string;
    strategy: string;
    signalType: string;
    price: number;
  }[];
  duration: number;
}

export async function runFullScan(
  timeframes: string[] = ["5Min", "15Min", "1Hour", "4Hour"],
  fullRefresh: boolean = false
): Promise<FullScanResult[]> {
  const allResults: FullScanResult[] = [];
  
  // Track scan state
  await setScannerState("scan_running", "true");
  
  try {
    for (let i = 0; i < timeframes.length; i++) {
      const timeframe = timeframes[i];
      const startTime = Date.now();
      
      // Update progress
      await setScannerState("scan_current_timeframe", timeframe);
      await setScannerState("scan_progress", String(Math.round((i / timeframes.length) * 100)));
      
      // Run scan
      const results = await scanAllSymbols(timeframe, fullRefresh);
      
      // Filter results with signals
      const withSignals = results.filter((r) => r.strategies.length > 0);
      
      // Save to database
      await saveScanResults(withSignals);
      
      // Record last scan time for this timeframe
      await setLastScanTime(timeframe);
      
      // Format output
      const signals = withSignals.flatMap((r) =>
        r.strategies.map((s) => ({
          symbol: r.symbol,
          strategy: s.strategyName,
          signalType: s.signalType || "NEUTRAL",
          price: r.price,
        }))
      );
      
      allResults.push({
        timeframe,
        scannedCount: results.length,
        signalsFound: signals.length,
        signals,
        duration: Date.now() - startTime,
      });
    }
  } finally {
    // Clear scan state
    await setScannerState("scan_running", "false");
    await setScannerState("scan_current_timeframe", "");
    await setScannerState("scan_progress", "100");
  }
  
  return allResults;
}

// ==================== Scanner State ====================

export async function getScannerState(key: string): Promise<string | null> {
  const result = await db
    .select({ value: scannerState.value })
    .from(scannerState)
    .where(eq(scannerState.key, key))
    .limit(1);
  
  return result.length > 0 ? result[0].value : null;
}

export async function setScannerState(key: string, value: string): Promise<void> {
  await db
    .insert(scannerState)
    .values({ key, value })
    .onConflictDoUpdate({
      target: scannerState.key,
      set: { value, updatedAt: new Date() },
    });
}

export async function getLastScanTime(timeframe: string): Promise<Date | null> {
  const value = await getScannerState(`last_scan_${timeframe}`);
  return value ? new Date(value) : null;
}

export async function setLastScanTime(timeframe: string): Promise<void> {
  await setScannerState(`last_scan_${timeframe}`, new Date().toISOString());
}

export async function getScanStatus(): Promise<{
  isRunning: boolean;
  currentTimeframe?: string;
  progress?: number;
}> {
  const running = await getScannerState("scan_running");
  const timeframe = await getScannerState("scan_current_timeframe");
  const progress = await getScannerState("scan_progress");
  
  return {
    isRunning: running === "true",
    currentTimeframe: timeframe || undefined,
    progress: progress ? parseInt(progress) : undefined,
  };
}

// ==================== Cleanup ====================

export async function cleanupOldData(daysToKeep: number = 7): Promise<void> {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
  
  // Clean old bar cache
  await db.delete(barCache).where(
    sql`${barCache.timestamp} < ${cutoffDate}`
  );
  
  // Clean old scan results
  await db.delete(scanResults).where(
    sql`${scanResults.scannedAt} < ${cutoffDate}`
  );
  
  console.log(`Cleaned up data older than ${daysToKeep} days`);
}
