/**
 * Strategy Module
 * Each strategy = combination of indicator conditions
 * Outputs which stocks trigger which strategies
 */

import { AllIndicatorResults } from "./indicators";

export type SignalType = "LONG" | "SHORT" | null;

export interface StrategyCondition {
  indicator: "tsi" | "qqe" | "momentum";
  field: string;
  operator: ">" | "<" | ">=" | "<=" | "==" | "!=";
  value: number | boolean | string;
}

export interface Strategy {
  id: string;
  name: string;
  description: string;
  timeframes: string[]; // Which timeframes this strategy applies to
  conditions: {
    long: StrategyCondition[];
    short: StrategyCondition[];
  };
}

export interface StrategyMatch {
  strategyId: string;
  strategyName: string;
  signalType: SignalType;
  matchedConditions: string[];
}

// ==================== Strategy Definitions ====================

// QQE状态说明:
// "bullish_aligned" - 多头共振（长短周期都看涨）
// "pullback" - 回调（长周期看涨，短周期回落）
// "rebound" - 反弹（长周期看跌，短周期反弹）
// "bearish_aligned" - 空头共振（长短周期都看跌）
// 过滤条件: isOverbought (>70), isOversold (<30)

export const STRATEGIES: Strategy[] = [
  {
    id: "tsi_qqe_momentum_bull",
    name: "三指标共振多头",
    description: "TSI看涨 + QQE多头共振 + Momentum强多",
    timeframes: ["15Min", "1Hour"],
    conditions: {
      long: [
        { indicator: "tsi", field: "isBullish", operator: "==", value: true },
        { indicator: "qqe", field: "state", operator: "==", value: "bullish_aligned" },
        { indicator: "momentum", field: "composite", operator: ">=", value: 2 },
      ],
      short: [
        { indicator: "tsi", field: "isBearish", operator: "==", value: true },
        { indicator: "qqe", field: "state", operator: "==", value: "bearish_aligned" },
        { indicator: "momentum", field: "composite", operator: "<=", value: -2 },
      ],
    },
  },
  {
    id: "momentum_trend",
    name: "动量趋势",
    description: "Momentum综合评分极端值",
    timeframes: ["1Hour", "4Hour"],
    conditions: {
      long: [
        { indicator: "momentum", field: "composite", operator: ">=", value: 3 },
        { indicator: "momentum", field: "stateLs", operator: ">=", value: 1 },
      ],
      short: [
        { indicator: "momentum", field: "composite", operator: "<=", value: -3 },
        { indicator: "momentum", field: "stateLs", operator: "<=", value: -1 },
      ],
    },
  },
  {
    id: "tsi_reversal",
    name: "TSI反转",
    description: "TSI从超卖区反转 / 从超买区回落",
    timeframes: ["15Min", "1Hour"],
    conditions: {
      long: [
        { indicator: "tsi", field: "isBullish", operator: "==", value: true },
        { indicator: "tsi", field: "tsiClose", operator: "<", value: 0 },
        { indicator: "tsi", field: "directionState", operator: "==", value: 1 },
      ],
      short: [
        { indicator: "tsi", field: "isBearish", operator: "==", value: true },
        { indicator: "tsi", field: "tsiClose", operator: ">", value: 0 },
        { indicator: "tsi", field: "directionState", operator: "==", value: -1 },
      ],
    },
  },
  {
    id: "pullback_entry",
    name: "回调入场",
    description: "长周期看涨 + 短周期回调到位 + 超跌过滤",
    timeframes: ["15Min", "1Hour"],
    conditions: {
      long: [
        { indicator: "qqe", field: "state", operator: "==", value: "pullback" },
        { indicator: "qqe", field: "isOversold", operator: "==", value: true },
        { indicator: "momentum", field: "stateLs", operator: ">=", value: 1 },
      ],
      short: [
        { indicator: "qqe", field: "state", operator: "==", value: "rebound" },
        { indicator: "qqe", field: "isOverbought", operator: "==", value: true },
        { indicator: "momentum", field: "stateLs", operator: "<=", value: -1 },
      ],
    },
  },
];

// ==================== Strategy Evaluation ====================

function getIndicatorValue(
  indicators: AllIndicatorResults,
  indicator: "tsi" | "qqe" | "momentum",
  field: string
): number | boolean | undefined {
  const indicatorData = indicators[indicator];
  return (indicatorData as unknown as Record<string, number | boolean>)[field];
}

function evaluateCondition(
  indicators: AllIndicatorResults,
  condition: StrategyCondition
): boolean {
  const value = getIndicatorValue(indicators, condition.indicator, condition.field);
  if (value === undefined) return false;

  switch (condition.operator) {
    case ">":
      return typeof value === "number" && value > (condition.value as number);
    case "<":
      return typeof value === "number" && value < (condition.value as number);
    case ">=":
      return typeof value === "number" && value >= (condition.value as number);
    case "<=":
      return typeof value === "number" && value <= (condition.value as number);
    case "==":
      return value === condition.value;
    case "!=":
      return value !== condition.value;
    default:
      return false;
  }
}

export function evaluateStrategy(
  strategy: Strategy,
  indicators: AllIndicatorResults,
  timeframe: string
): StrategyMatch | null {
  // Check if strategy applies to this timeframe
  if (!strategy.timeframes.includes(timeframe)) {
    return null;
  }

  // Check long conditions
  const longMatches = strategy.conditions.long.map((c) => evaluateCondition(indicators, c));
  const allLongMatch = longMatches.every((m) => m);

  if (allLongMatch && strategy.conditions.long.length > 0) {
    return {
      strategyId: strategy.id,
      strategyName: strategy.name,
      signalType: "LONG",
      matchedConditions: strategy.conditions.long.map(
        (c) => `${c.indicator}.${c.field} ${c.operator} ${c.value}`
      ),
    };
  }

  // Check short conditions
  const shortMatches = strategy.conditions.short.map((c) => evaluateCondition(indicators, c));
  const allShortMatch = shortMatches.every((m) => m);

  if (allShortMatch && strategy.conditions.short.length > 0) {
    return {
      strategyId: strategy.id,
      strategyName: strategy.name,
      signalType: "SHORT",
      matchedConditions: strategy.conditions.short.map(
        (c) => `${c.indicator}.${c.field} ${c.operator} ${c.value}`
      ),
    };
  }

  return null;
}

export function evaluateAllStrategies(
  indicators: AllIndicatorResults,
  timeframe: string,
  strategies: Strategy[] = STRATEGIES
): StrategyMatch[] {
  const matches: StrategyMatch[] = [];

  for (const strategy of strategies) {
    const match = evaluateStrategy(strategy, indicators, timeframe);
    if (match) {
      matches.push(match);
    }
  }

  return matches;
}

// ==================== Strategy Management ====================

export function getStrategyById(id: string): Strategy | undefined {
  return STRATEGIES.find((s) => s.id === id);
}

export function getStrategiesForTimeframe(timeframe: string): Strategy[] {
  return STRATEGIES.filter((s) => s.timeframes.includes(timeframe));
}

export function getAllStrategies(): Strategy[] {
  return STRATEGIES;
}
