/**
 * Configurable Strategy Engine
 * Supports cross-timeframe strategies with user-defined conditions
 */

import { db } from "../db";
import { 
  customStrategies, 
  CustomStrategy, 
  indicatorResults, 
  IndicatorResultRow,
  strategySignalState,
  signalEntries,
  StrategySignalState,
  SignalEntry
} from "@shared/schema";
import { eq, and, isNull, desc, sql } from "drizzle-orm";

export interface CustomCondition {
  timeframe: string;
  field: string;
  operator: "==" | "!=" | ">" | "<" | ">=" | "<=";
  value: number | boolean | string;
}

export interface CustomStrategyConfig {
  signalType: "LONG" | "SHORT";
  conditions: CustomCondition[];
}

export interface CustomStrategyMatchResult {
  strategyId: string;
  strategyName: string;
  signalType: "LONG" | "SHORT";
  matchedConditions: CustomCondition[];
}

// 简化的条件字段配置
// TSI: 只保留Heikin和MA的多空状态
// QQE: 4种状态 - bullish_aligned, pullback, rebound, bearish_aligned
// Momentum Sw/Ls状态: 2=strong bull, 1=bull, -1=bear, -2=strong bear
// Composite状态: 4=完美多头, 3=强多头, 1/2=震荡偏多, 0=中性, -1/-2=震荡偏空, -3=强空头, -4=完美空头

export interface FieldConfig {
  field: string;
  label: string;
  type: "boolean" | "select" | "number";
  options?: { value: string | number | boolean; label: string }[];
}

export const FIELD_CONFIGS: FieldConfig[] = [
  // TSI - Heikin Ashi状态
  { field: "tsiHeikinBullish", label: "TSI Heikin多头", type: "boolean", options: [
    { value: true, label: "是" }, { value: false, label: "否" }
  ]},
  { field: "tsiHeikinBearish", label: "TSI Heikin空头", type: "boolean", options: [
    { value: true, label: "是" }, { value: false, label: "否" }
  ]},
  // TSI - MA状态
  { field: "tsiMaBullish", label: "TSI MA多头", type: "boolean", options: [
    { value: true, label: "是" }, { value: false, label: "否" }
  ]},
  { field: "tsiMaBearish", label: "TSI MA空头", type: "boolean", options: [
    { value: true, label: "是" }, { value: false, label: "否" }
  ]},
  // QQE - 4种状态
  { field: "qqeState", label: "QQE状态", type: "select", options: [
    { value: "bullish_aligned", label: "多头共振" },
    { value: "pullback", label: "回调" },
    { value: "rebound", label: "反弹" },
    { value: "bearish_aligned", label: "空头共振" },
  ]},
  // QQE RSI值 (0-100)
  { field: "qqeRsiSt", label: "QQE RSI短周期", type: "number" },
  // QQE超卖/超买状态
  { field: "qqeIsOversold", label: "QQE超卖(RSI<30)", type: "boolean", options: [
    { value: true, label: "是" }, { value: false, label: "否" }
  ]},
  { field: "qqeIsOverbought", label: "QQE超买(RSI>70)", type: "boolean", options: [
    { value: true, label: "是" }, { value: false, label: "否" }
  ]},
  // Momentum Sw状态 (-2~2)
  { field: "momStateSw", label: "Momentum Sw状态", type: "select", options: [
    { value: 2, label: "强多 (2)" },
    { value: 1, label: "偏多 (1)" },
    { value: 0, label: "中性 (0)" },
    { value: -1, label: "偏空 (-1)" },
    { value: -2, label: "强空 (-2)" },
  ]},
  // Momentum Ls状态 (-2~2)
  { field: "momStateLs", label: "Momentum Ls状态", type: "select", options: [
    { value: 2, label: "强多 (2)" },
    { value: 1, label: "偏多 (1)" },
    { value: 0, label: "中性 (0)" },
    { value: -1, label: "偏空 (-1)" },
    { value: -2, label: "强空 (-2)" },
  ]},
  // Composite Score (-4~4)
  { field: "momComposite", label: "Momentum综合得分", type: "select", options: [
    { value: 4, label: "完美多头 (4)" },
    { value: 3, label: "强多头 (3)" },
    { value: 2, label: "震荡偏多 (2)" },
    { value: 1, label: "震荡偏多 (1)" },
    { value: 0, label: "中性 (0)" },
    { value: -1, label: "震荡偏空 (-1)" },
    { value: -2, label: "震荡偏空 (-2)" },
    { value: -3, label: "强空头 (-3)" },
    { value: -4, label: "完美空头 (-4)" },
  ]},
];

const VALID_FIELDS = FIELD_CONFIGS.map(f => f.field);

// Parse entry timeframe from strategy name (e.g., "5Min入场-xxx" -> "5Min")
function getEntryTimeframeFromName(strategyName: string): string | null {
  const patterns = [
    { regex: /^5Min/, tf: "5Min" },
    { regex: /^15Min/, tf: "15Min" },
    { regex: /^1H/, tf: "1Hour" },
    { regex: /^4H/, tf: "4Hour" },
  ];
  
  for (const p of patterns) {
    if (p.regex.test(strategyName)) {
      return p.tf;
    }
  }
  return null;
}

// Get entry timeframe for a strategy - prefer name parsing, fallback to first condition
export function getStrategyEntryTimeframe(strategy: CustomStrategy): string {
  const nameTimeframe = getEntryTimeframeFromName(strategy.name);
  if (nameTimeframe) {
    return nameTimeframe;
  }
  
  // Fallback: use first condition's timeframe
  try {
    const config: CustomStrategyConfig = JSON.parse(strategy.conditionsJson);
    return config.conditions[0]?.timeframe || "15Min";
  } catch {
    return "15Min";
  }
}

function evaluateCondition(
  condition: CustomCondition,
  indicatorData: IndicatorResultRow
): boolean {
  const fieldValue = (indicatorData as any)[condition.field];
  if (fieldValue === null || fieldValue === undefined) {
    return false;
  }

  const targetValue = condition.value;

  switch (condition.operator) {
    case "==":
      return fieldValue === targetValue;
    case "!=":
      return fieldValue !== targetValue;
    case ">":
      return typeof fieldValue === "number" && fieldValue > (targetValue as number);
    case "<":
      return typeof fieldValue === "number" && fieldValue < (targetValue as number);
    case ">=":
      return typeof fieldValue === "number" && fieldValue >= (targetValue as number);
    case "<=":
      return typeof fieldValue === "number" && fieldValue <= (targetValue as number);
    default:
      return false;
  }
}

export async function evaluateStrategyForSymbol(
  symbol: string,
  strategy: CustomStrategy
): Promise<CustomStrategyMatchResult | null> {
  try {
    const config: CustomStrategyConfig = JSON.parse(strategy.conditionsJson);
    
    const requiredTimeframes = Array.from(new Set(config.conditions.map(c => c.timeframe)));
    
    const indicatorDataByTimeframe: Record<string, IndicatorResultRow> = {};
    for (const tf of requiredTimeframes) {
      const results = await db
        .select()
        .from(indicatorResults)
        .where(and(
          eq(indicatorResults.symbol, symbol),
          eq(indicatorResults.timeframe, tf)
        ))
        .limit(1);
      
      if (results.length === 0) {
        return null;
      }
      indicatorDataByTimeframe[tf] = results[0];
    }

    const allConditionsMet = config.conditions.every((condition) => {
      const tfData = indicatorDataByTimeframe[condition.timeframe];
      if (!tfData) return false;
      return evaluateCondition(condition, tfData);
    });

    if (allConditionsMet) {
      return {
        strategyId: strategy.id,
        strategyName: strategy.name,
        signalType: config.signalType,
        matchedConditions: config.conditions,
      };
    }

    return null;
  } catch (error) {
    console.error(`Error evaluating strategy ${strategy.name} for ${symbol}:`, error);
    return null;
  }
}

export async function runStrategyOnAllSymbols(
  strategyId: string
): Promise<Array<{ symbol: string; match: CustomStrategyMatchResult }>> {
  const strategies = await db
    .select()
    .from(customStrategies)
    .where(and(
      eq(customStrategies.id, strategyId),
      eq(customStrategies.isActive, true)
    ))
    .limit(1);

  if (strategies.length === 0) {
    return [];
  }

  const strategy = strategies[0];
  
  const allIndicators = await db.select().from(indicatorResults);
  const symbolSet = new Set<string>();
  for (const r of allIndicators) {
    symbolSet.add(r.symbol);
  }
  const symbols = Array.from(symbolSet);

  const results: Array<{ symbol: string; match: CustomStrategyMatchResult }> = [];

  for (const symbol of symbols) {
    const match = await evaluateStrategyForSymbol(symbol, strategy);
    if (match) {
      results.push({ symbol, match });
    }
  }

  return results;
}

export async function runAllActiveStrategiesOnSymbol(
  symbol: string
): Promise<CustomStrategyMatchResult[]> {
  const strategies = await db
    .select()
    .from(customStrategies)
    .where(eq(customStrategies.isActive, true));

  const matches: CustomStrategyMatchResult[] = [];

  for (const strategy of strategies) {
    const match = await evaluateStrategyForSymbol(symbol, strategy);
    if (match) {
      matches.push(match);
    }
  }

  return matches;
}

export async function runAllActiveStrategies(): Promise<
  Array<{ symbol: string; matches: CustomStrategyMatchResult[] }>
> {
  const allIndicators = await db.select().from(indicatorResults);
  const symbolSet = new Set<string>();
  for (const r of allIndicators) {
    symbolSet.add(r.symbol);
  }
  const symbols = Array.from(symbolSet);

  const results: Array<{ symbol: string; matches: CustomStrategyMatchResult[] }> = [];

  for (const symbol of symbols) {
    const matches = await runAllActiveStrategiesOnSymbol(symbol);
    if (matches.length > 0) {
      results.push({ symbol, matches });
    }
  }

  return results;
}

export async function createCustomStrategy(
  name: string,
  description: string,
  config: CustomStrategyConfig
): Promise<CustomStrategy> {
  const result = await db
    .insert(customStrategies)
    .values({
      name,
      description,
      conditionsJson: JSON.stringify(config),
      isActive: true,
    })
    .returning();

  return result[0];
}

export async function updateCustomStrategy(
  id: string,
  updates: {
    name?: string;
    description?: string;
    config?: CustomStrategyConfig;
    isActive?: boolean;
  }
): Promise<CustomStrategy | null> {
  const updateData: any = { updatedAt: new Date() };
  
  if (updates.name !== undefined) updateData.name = updates.name;
  if (updates.description !== undefined) updateData.description = updates.description;
  if (updates.config !== undefined) updateData.conditionsJson = JSON.stringify(updates.config);
  if (updates.isActive !== undefined) updateData.isActive = updates.isActive;

  const result = await db
    .update(customStrategies)
    .set(updateData)
    .where(eq(customStrategies.id, id))
    .returning();

  return result.length > 0 ? result[0] : null;
}

export async function deleteCustomStrategy(id: string): Promise<boolean> {
  const result = await db
    .delete(customStrategies)
    .where(eq(customStrategies.id, id))
    .returning();

  return result.length > 0;
}

export async function getAllCustomStrategies(): Promise<CustomStrategy[]> {
  return db.select().from(customStrategies);
}

export async function getActiveStrategies(): Promise<CustomStrategy[]> {
  return db
    .select()
    .from(customStrategies)
    .where(eq(customStrategies.isActive, true));
}

export async function getCustomStrategy(id: string): Promise<CustomStrategy | null> {
  const results = await db
    .select()
    .from(customStrategies)
    .where(eq(customStrategies.id, id))
    .limit(1);

  return results.length > 0 ? results[0] : null;
}

export function getAvailableFields(): FieldConfig[] {
  return FIELD_CONFIGS;
}

export function getAvailableTimeframes(): string[] {
  return ["5Min", "15Min", "1Hour", "4Hour"];
}

export function validateCustomStrategyConfig(config: CustomStrategyConfig): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  const validTimeframes = getAvailableTimeframes();
  const validOperators = ["==", "!=", ">", "<", ">=", "<="];

  if (!config.signalType || !["LONG", "SHORT"].includes(config.signalType)) {
    errors.push("signalType must be 'LONG' or 'SHORT'");
  }

  if (!config.conditions || !Array.isArray(config.conditions) || config.conditions.length === 0) {
    errors.push("conditions must be a non-empty array");
  } else {
    config.conditions.forEach((cond, idx) => {
      if (!validTimeframes.includes(cond.timeframe)) {
        errors.push(`Condition ${idx + 1}: invalid timeframe '${cond.timeframe}'`);
      }
      if (!VALID_FIELDS.includes(cond.field)) {
        errors.push(`Condition ${idx + 1}: invalid field '${cond.field}'`);
      }
      if (!validOperators.includes(cond.operator)) {
        errors.push(`Condition ${idx + 1}: invalid operator '${cond.operator}'`);
      }
    });
  }

  return { valid: errors.length === 0, errors };
}

// ===================== Signal State Tracking =====================

let isTrackerRunning = false;

export interface SignalStateChange {
  type: "new_entry" | "continuing" | "exit";
  strategyId: string;
  strategyName: string;
  symbol: string;
  signalType: "LONG" | "SHORT";
  price?: number;
  consecutiveMatches: number;
}

async function getSignalState(
  strategyId: string,
  symbol: string
): Promise<StrategySignalState | null> {
  const results = await db
    .select()
    .from(strategySignalState)
    .where(and(
      eq(strategySignalState.strategyId, strategyId),
      eq(strategySignalState.symbol, symbol)
    ))
    .limit(1);
  
  return results.length > 0 ? results[0] : null;
}

// Atomic upsert with row locking for concurrency safety.
// Uses CTE+FOR UPDATE to prevent race conditions where concurrent runs
// could both read stale state and emit duplicate new-entry decisions.
async function updateSignalState(
  strategyId: string,
  symbol: string,
  isMatching: boolean,
  signalType: "LONG" | "SHORT" | null
): Promise<{ isNewEntry: boolean; consecutiveMatches: number }> {
  const result = await db.execute(sql`
    WITH prev AS (
      SELECT is_matching, consecutive_matches 
      FROM strategy_signal_state 
      WHERE strategy_id = ${strategyId} AND symbol = ${symbol}
      FOR UPDATE
    ),
    new_values AS (
      SELECT 
        ${isMatching}::boolean as new_is_matching,
        COALESCE((SELECT is_matching FROM prev), false) as was_matching,
        CASE 
          WHEN ${isMatching} THEN 
            CASE WHEN COALESCE((SELECT is_matching FROM prev), false) 
              THEN COALESCE((SELECT consecutive_matches FROM prev), 0) + 1 
              ELSE 1 
            END
          ELSE 0 
        END as new_consecutive
    )
    INSERT INTO strategy_signal_state (
      id, strategy_id, symbol, is_matching, signal_type, 
      last_matched_at, first_matched_at, consecutive_matches, updated_at
    ) 
    SELECT
      gen_random_uuid(), ${strategyId}, ${symbol}, ${isMatching}, ${signalType}, 
      CASE WHEN ${isMatching} THEN NOW() ELSE NULL END,
      CASE WHEN NOT (SELECT was_matching FROM new_values) AND ${isMatching} THEN NOW() ELSE NULL END,
      (SELECT new_consecutive FROM new_values), NOW()
    ON CONFLICT (strategy_id, symbol) DO UPDATE SET
      is_matching = ${isMatching},
      signal_type = ${signalType},
      last_matched_at = CASE WHEN ${isMatching} THEN NOW() ELSE strategy_signal_state.last_matched_at END,
      first_matched_at = CASE 
        WHEN NOT strategy_signal_state.is_matching AND ${isMatching} THEN NOW()
        WHEN ${isMatching} THEN strategy_signal_state.first_matched_at
        ELSE NULL 
      END,
      consecutive_matches = CASE 
        WHEN ${isMatching} THEN 
          CASE WHEN strategy_signal_state.is_matching 
            THEN strategy_signal_state.consecutive_matches + 1 
            ELSE 1 
          END
        ELSE 0 
      END,
      updated_at = NOW()
    RETURNING 
      is_matching,
      (SELECT was_matching FROM new_values) as was_matching,
      consecutive_matches
  `);
  
  const row = (result as any).rows?.[0];
  if (!row) {
    return { isNewEntry: isMatching, consecutiveMatches: isMatching ? 1 : 0 };
  }
  
  const wasMatching = row.was_matching === true || row.was_matching === 't';
  const isNewEntry = !wasMatching && isMatching;
  
  return { isNewEntry, consecutiveMatches: row.consecutive_matches || 0 };
}

async function createSignalEntry(
  strategyId: string,
  strategyName: string,
  symbol: string,
  signalType: "LONG" | "SHORT",
  entryTimeframe: string,
  price?: number,
  indicatorSnapshot?: object
): Promise<SignalEntry | null> {
  try {
    const result = await db
      .insert(signalEntries)
      .values({
        strategyId,
        strategyName,
        symbol,
        signalType,
        entryTimeframe,
        price,
        indicatorSnapshot: indicatorSnapshot ? JSON.stringify(indicatorSnapshot) : null,
        isActive: true,
        notificationSent: false,
      })
      .returning();
    
    return result[0];
  } catch (error: any) {
    if (error?.code === '23505' || error?.message?.includes('duplicate key') || error?.message?.includes('unique constraint')) {
      console.log(`[Signal Entry] Duplicate active entry prevented for ${symbol} in ${strategyName}`);
      return null;
    }
    console.error(`[Signal Entry] DB error for ${symbol}:`, error);
    throw error;
  }
}

async function markSignalExited(
  strategyId: string,
  symbol: string,
  exitPrice?: number
): Promise<void> {
  const now = new Date();
  await db
    .update(signalEntries)
    .set({
      isActive: false,
      exitedAt: now,
      exitPrice,
    })
    .where(and(
      eq(signalEntries.strategyId, strategyId),
      eq(signalEntries.symbol, symbol),
      eq(signalEntries.isActive, true)
    ));
}

// Auxiliary match info for other timeframes (read-only, no state update)
export interface AuxiliaryMatchInfo {
  timeframe: string;
  matches: Array<{
    symbol: string;
    strategyId: string;
    strategyName: string;
    signalType: "LONG" | "SHORT";
  }>;
}

export async function runAllActiveStrategiesWithTracking(
  scanTimeframe?: string
): Promise<{
  newEntries: SignalStateChange[];
  continuing: SignalStateChange[];
  exits: SignalStateChange[];
  allMatches: Array<{ symbol: string; matches: CustomStrategyMatchResult[] }>;
  auxiliaryMatches: AuxiliaryMatchInfo[];
}> {
  if (isTrackerRunning) {
    console.log("[Signal Tracking] Already running, skipping this invocation");
    return { newEntries: [], continuing: [], exits: [], allMatches: [], auxiliaryMatches: [] };
  }
  
  isTrackerRunning = true;
  
  try {
    return await runTrackingCore(scanTimeframe);
  } finally {
    isTrackerRunning = false;
  }
}

// scanTimeframe: Only process NEW ENTRY signals for strategies whose entry timeframe matches.
// All strategies are still evaluated for match/exit status, but only matching entry-timeframe
// strategies will record new entries. This ensures signals are only generated when the
// corresponding K-line data has actually updated.
// Additionally, collect auxiliary matches from other timeframes for decision support.
async function runTrackingCore(scanTimeframe?: string): Promise<{
  newEntries: SignalStateChange[];
  continuing: SignalStateChange[];
  exits: SignalStateChange[];
  allMatches: Array<{ symbol: string; matches: CustomStrategyMatchResult[] }>;
  auxiliaryMatches: AuxiliaryMatchInfo[];
}> {
  const strategies = await db
    .select()
    .from(customStrategies)
    .where(eq(customStrategies.isActive, true));
  
  if (scanTimeframe) {
    console.log(`[Signal Tracking] Running for scanTimeframe=${scanTimeframe}`);
  }
  
  const allIndicators = await db.select().from(indicatorResults);
  const symbolSet = new Set<string>();
  for (const r of allIndicators) {
    symbolSet.add(r.symbol);
  }
  const symbols = Array.from(symbolSet);
  
  const indicatorsBySymbol: Record<string, Record<string, IndicatorResultRow>> = {};
  for (const ind of allIndicators) {
    if (!indicatorsBySymbol[ind.symbol]) {
      indicatorsBySymbol[ind.symbol] = {};
    }
    indicatorsBySymbol[ind.symbol][ind.timeframe] = ind;
  }
  
  const newEntries: SignalStateChange[] = [];
  const continuing: SignalStateChange[] = [];
  const exits: SignalStateChange[] = [];
  const allMatches: Array<{ symbol: string; matches: CustomStrategyMatchResult[] }> = [];
  
  // Collect auxiliary matches from other timeframes (read-only, for decision support)
  const auxiliaryByTimeframe: Map<string, Array<{
    symbol: string;
    strategyId: string;
    strategyName: string;
    signalType: "LONG" | "SHORT";
  }>> = new Map();
  
  const currentMatches: Map<string, CustomStrategyMatchResult> = new Map();
  
  for (const symbol of symbols) {
    const symbolMatches: CustomStrategyMatchResult[] = [];
    
    for (const strategy of strategies) {
      const key = `${strategy.id}:${symbol}`;
      const match = await evaluateStrategyForSymbol(symbol, strategy);
      
      if (match) {
        const config: CustomStrategyConfig = JSON.parse(strategy.conditionsJson);
        const entryTimeframe = getStrategyEntryTimeframe(strategy);
        const price = indicatorsBySymbol[symbol]?.[entryTimeframe]?.price || undefined;
        
        // Only process signal state for strategies whose entry timeframe matches current scan
        // or if no scanTimeframe specified (manual scan processes all)
        const shouldProcessState = !scanTimeframe || entryTimeframe === scanTimeframe;
        
        if (shouldProcessState) {
          currentMatches.set(key, match);
          symbolMatches.push(match);
          
          const { isNewEntry, consecutiveMatches } = await updateSignalState(
            strategy.id,
            symbol,
            true,
            config.signalType
          );
          
          const stateChange: SignalStateChange = {
            type: isNewEntry ? "new_entry" : "continuing",
            strategyId: strategy.id,
            strategyName: strategy.name,
            symbol,
            signalType: config.signalType,
            price,
            consecutiveMatches,
          };
          
          if (isNewEntry) {
            newEntries.push(stateChange);
            
            const indicatorSnapshot = Object.values(indicatorsBySymbol[symbol] || {}).reduce(
              (acc, ind) => {
                acc[ind.timeframe] = {
                  tsiHeikinBullish: ind.tsiHeikinBullish,
                  tsiHeikinBearish: ind.tsiHeikinBearish,
                  tsiMaBullish: ind.tsiMaBullish,
                  tsiMaBearish: ind.tsiMaBearish,
                  qqeState: ind.qqeState,
                  momComposite: ind.momComposite,
                };
                return acc;
              },
              {} as Record<string, object>
            );
            
            await createSignalEntry(
              strategy.id,
              strategy.name,
              symbol,
              config.signalType,
              entryTimeframe,
              price,
              indicatorSnapshot
            );
            
            console.log(`[Signal] NEW ENTRY: ${symbol} ${config.signalType} via ${strategy.name} (${entryTimeframe})`);
          } else {
            continuing.push(stateChange);
          }
        } else {
          // Collect as auxiliary info - strategy matches but for a different timeframe
          // This provides context for decision-making without updating state
          if (!auxiliaryByTimeframe.has(entryTimeframe)) {
            auxiliaryByTimeframe.set(entryTimeframe, []);
          }
          auxiliaryByTimeframe.get(entryTimeframe)!.push({
            symbol,
            strategyId: strategy.id,
            strategyName: strategy.name,
            signalType: config.signalType,
          });
        }
      }
    }
    
    if (symbolMatches.length > 0) {
      allMatches.push({ symbol, matches: symbolMatches });
    }
  }
  
  // Process exits - only for strategies matching current scan timeframe
  for (const strategy of strategies) {
    const config: CustomStrategyConfig = JSON.parse(strategy.conditionsJson);
    const entryTimeframe = getStrategyEntryTimeframe(strategy);
    
    // Only check exits for strategies whose entry timeframe matches current scan
    const shouldProcessExit = !scanTimeframe || entryTimeframe === scanTimeframe;
    if (!shouldProcessExit) continue;
    
    for (const symbol of symbols) {
      const key = `${strategy.id}:${symbol}`;
      
      if (!currentMatches.has(key)) {
        const existingState = await getSignalState(strategy.id, symbol);
        
        if (existingState?.isMatching) {
          const price = indicatorsBySymbol[symbol]?.[entryTimeframe]?.price || undefined;
          
          await updateSignalState(strategy.id, symbol, false, null);
          await markSignalExited(strategy.id, symbol, price);
          
          exits.push({
            type: "exit",
            strategyId: strategy.id,
            strategyName: strategy.name,
            symbol,
            signalType: config.signalType,
            price,
            consecutiveMatches: 0,
          });
          
          console.log(`[Signal] EXIT: ${symbol} no longer matches ${strategy.name}`);
        }
      }
    }
  }
  
  // Convert auxiliary matches map to array format
  const auxiliaryMatches: AuxiliaryMatchInfo[] = [];
  Array.from(auxiliaryByTimeframe.entries()).forEach(([timeframe, matches]) => {
    auxiliaryMatches.push({ timeframe, matches });
  });
  // Sort by timeframe order: 5Min, 15Min, 1Hour, 4Hour
  const tfOrder = ["5Min", "15Min", "1Hour", "4Hour"];
  auxiliaryMatches.sort((a, b) => tfOrder.indexOf(a.timeframe) - tfOrder.indexOf(b.timeframe));
  
  console.log(`[Signal Tracking] New: ${newEntries.length}, Continuing: ${continuing.length}, Exits: ${exits.length}, Auxiliary TFs: ${auxiliaryMatches.length}`);
  
  return { newEntries, continuing, exits, allMatches, auxiliaryMatches };
}

export async function getActiveSignalEntries(): Promise<SignalEntry[]> {
  return db
    .select()
    .from(signalEntries)
    .where(eq(signalEntries.isActive, true))
    .orderBy(desc(signalEntries.createdAt));
}

export async function getRecentSignalEntries(limit: number = 50): Promise<SignalEntry[]> {
  return db
    .select()
    .from(signalEntries)
    .orderBy(desc(signalEntries.createdAt))
    .limit(limit);
}

export async function getSignalEntriesBySymbol(symbol: string): Promise<SignalEntry[]> {
  return db
    .select()
    .from(signalEntries)
    .where(eq(signalEntries.symbol, symbol))
    .orderBy(desc(signalEntries.createdAt));
}
