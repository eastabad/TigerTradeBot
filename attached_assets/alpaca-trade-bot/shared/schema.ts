import { sql } from "drizzle-orm";
import { pgTable, text, varchar, real, boolean, timestamp, integer, unique } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Order status enum
export const OrderStatusEnum = {
  PENDING: "pending",
  NEW: "new",
  FILLED: "filled",
  PARTIALLY_FILLED: "partially_filled",
  CANCELLED: "cancelled",
  REJECTED: "rejected",
  EXPIRED: "expired",
} as const;

export type OrderStatus = typeof OrderStatusEnum[keyof typeof OrderStatusEnum];

// Order type enum
export const OrderTypeEnum = {
  MARKET: "market",
  LIMIT: "limit",
  STOP: "stop",
  STOP_LIMIT: "stop_limit",
  TRAILING_STOP: "trailing_stop",
} as const;

export type OrderType = typeof OrderTypeEnum[keyof typeof OrderTypeEnum];

// Order side enum
export const SideEnum = {
  BUY: "buy",
  SELL: "sell",
} as const;

export type Side = typeof SideEnum[keyof typeof SideEnum];

// Time in force enum
export const TimeInForceEnum = {
  DAY: "day",
  GTC: "gtc",
  IOC: "ioc",
  FOK: "fok",
  OPG: "opg",
  CLS: "cls",
} as const;

export type TimeInForce = typeof TimeInForceEnum[keyof typeof TimeInForceEnum];

// Users table (for auth if needed)
export const users = pgTable("users", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  username: text("username").notNull().unique(),
  password: text("password").notNull(),
});

export const insertUserSchema = createInsertSchema(users).pick({
  username: true,
  password: true,
});

export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;

// Trades table - stores all trade records
export const trades = pgTable("trades", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  symbol: text("symbol").notNull(),
  side: text("side").notNull(), // buy or sell
  quantity: real("quantity").notNull(),
  price: real("price"), // null for market orders
  orderType: text("order_type").notNull().default("market"),
  status: text("status").notNull().default("pending"),
  alpacaOrderId: text("alpaca_order_id"),
  signalData: text("signal_data"), // original webhook signal
  alpacaResponse: text("alpaca_response"), // Alpaca API response
  errorMessage: text("error_message"),
  filledPrice: real("filled_price"),
  filledQuantity: real("filled_quantity"),
  filledAt: timestamp("filled_at"),
  stopLossPrice: real("stop_loss_price"),
  takeProfitPrice: real("take_profit_price"),
  stopLossOrderId: text("stop_loss_order_id"),
  takeProfitOrderId: text("take_profit_order_id"),
  extendedHours: boolean("extended_hours").default(false),
  timeInForce: text("time_in_force").default("day"),
  isClosePosition: boolean("is_close_position").default(false),
  positionAvgEntryPrice: real("position_avg_entry_price"),
  positionQty: real("position_qty"),
  positionSide: text("position_side"),
  positionEntryDate: timestamp("position_entry_date"), // First entry date for the closed position
  accountId: integer("account_id"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const insertTradeSchema = createInsertSchema(trades).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});

export type InsertTrade = z.infer<typeof insertTradeSchema>;
export type Trade = typeof trades.$inferSelect;

// Trading config table
export const tradingConfigs = pgTable("trading_configs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  key: text("key").notNull().unique(),
  value: text("value").notNull(),
  description: text("description"),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const insertTradingConfigSchema = createInsertSchema(tradingConfigs).omit({
  id: true,
  updatedAt: true,
});

export type InsertTradingConfig = z.infer<typeof insertTradingConfigSchema>;
export type TradingConfig = typeof tradingConfigs.$inferSelect;

// Signal logs table
export const signalLogs = pgTable("signal_logs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  rawSignal: text("raw_signal").notNull(),
  parsedSuccessfully: boolean("parsed_successfully").default(false),
  errorMessage: text("error_message"),
  ipAddress: text("ip_address"),
  tradeId: text("trade_id"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertSignalLogSchema = createInsertSchema(signalLogs).omit({
  id: true,
  createdAt: true,
});

export type InsertSignalLog = z.infer<typeof insertSignalLogSchema>;
export type SignalLog = typeof signalLogs.$inferSelect;

// Alpaca account info type (not stored, fetched from API)
export interface AlpacaAccount {
  id: string;
  accountNumber: string;
  status: string;
  currency: string;
  buyingPower: string;
  cash: string;
  portfolioValue: string;
  equity: string;
  lastEquity: string;
  longMarketValue: string;
  shortMarketValue: string;
  daytradeCount: number;
  patternDayTrader: boolean;
  tradingBlocked: boolean;
  transfersBlocked: boolean;
  accountBlocked: boolean;
  tradeSuspendedByUser: boolean;
  multiplier: string;
  createdAt: string;
}

// Alpaca position type
export interface AlpacaPosition {
  assetId: string;
  symbol: string;
  exchange: string;
  assetClass: string;
  avgEntryPrice: string;
  qty: string;
  side: string;
  marketValue: string;
  costBasis: string;
  unrealizedPl: string;
  unrealizedPlpc: string;
  unrealizedIntradayPl: string;
  unrealizedIntradayPlpc: string;
  currentPrice: string;
  lastdayPrice: string;
  changeToday: string;
}

// Alpaca order type
export interface AlpacaOrder {
  id: string;
  clientOrderId: string;
  createdAt: string;
  updatedAt: string;
  submittedAt: string;
  filledAt: string | null;
  expiredAt: string | null;
  canceledAt: string | null;
  failedAt: string | null;
  assetId: string;
  symbol: string;
  assetClass: string;
  qty: string;
  filledQty: string;
  filledAvgPrice: string | null;
  orderClass: string;
  orderType: string;
  type: string;
  side: string;
  timeInForce: string;
  limitPrice: string | null;
  stopPrice: string | null;
  status: string;
  extendedHours: boolean;
  legs: AlpacaOrder[] | null;
}

// Webhook signal format (from TradingView)
export interface WebhookSignal {
  symbol?: string;
  ticker?: string;
  side?: string;
  action?: string;
  quantity?: number | string;
  qty?: number | string;
  size?: number | string;
  price?: number | string;
  limit_price?: number | string;
  order_type?: string;
  type?: string;
  stop_loss?: number;
  take_profit?: number;
  stopLoss?: { stopPrice?: number };
  takeProfit?: { limitPrice?: number };
  reference_price?: number;
  referencePrice?: number;
  trading_session?: string;
  session?: string;
  extended_hours?: boolean;
  time_in_force?: string;
  sentiment?: string;
  extras?: { referencePrice?: number };
}

// Parsed signal format
export interface ParsedSignal {
  symbol: string;
  side: Side;
  quantity: number | 'all';
  price?: number;
  orderType: OrderType;
  timeInForce: TimeInForce;
  stopLoss?: number;
  takeProfit?: number;
  referencePrice?: number;
  extendedHours: boolean;
  isCloseSignal: boolean;
  closeAll?: boolean;
}

// ===================== Scanner Module Tables =====================

// Watchlist - 股票池
export const watchlist = pgTable("watchlist", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  symbol: varchar("symbol", { length: 20 }).notNull().unique(),
  name: varchar("name", { length: 100 }),
  sector: varchar("sector", { length: 50 }),
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertWatchlistSchema = createInsertSchema(watchlist).omit({
  id: true,
  createdAt: true,
});
export type InsertWatchlist = z.infer<typeof insertWatchlistSchema>;
export type Watchlist = typeof watchlist.$inferSelect;

// Bar Cache - K线缓存
export const barCache = pgTable("bar_cache", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  symbol: varchar("symbol", { length: 20 }).notNull(),
  timeframe: varchar("timeframe", { length: 10 }).notNull(), // "5Min", "15Min", "1Hour", "4Hour"
  timestamp: timestamp("timestamp").notNull(),
  open: real("open").notNull(),
  high: real("high").notNull(),
  low: real("low").notNull(),
  close: real("close").notNull(),
  volume: real("volume").notNull(),
  vwap: real("vwap"),
});

export const insertBarCacheSchema = createInsertSchema(barCache).omit({
  id: true,
});
export type InsertBarCache = z.infer<typeof insertBarCacheSchema>;
export type BarCache = typeof barCache.$inferSelect;

// Scan Results - 扫描结果
export const scanResults = pgTable("scan_results", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  symbol: varchar("symbol", { length: 20 }).notNull(),
  strategyId: varchar("strategy_id", { length: 50 }).notNull(),
  strategyName: varchar("strategy_name", { length: 100 }).notNull(),
  signalType: varchar("signal_type", { length: 20 }).notNull(), // "LONG", "SHORT"
  timeframe: varchar("timeframe", { length: 10 }).notNull(),
  indicatorValues: text("indicator_values"), // JSON string of indicator snapshot
  price: real("price"),
  scannedAt: timestamp("scanned_at").defaultNow(),
});

export const insertScanResultSchema = createInsertSchema(scanResults).omit({
  id: true,
  scannedAt: true,
});
export type InsertScanResult = z.infer<typeof insertScanResultSchema>;
export type ScanResult = typeof scanResults.$inferSelect;

// Scanner State - 扫描器状态
export const scannerState = pgTable("scanner_state", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  key: varchar("key", { length: 50 }).notNull().unique(),
  value: text("value").notNull(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

// Indicator Results - 指标计算结果存储
export const indicatorResults = pgTable("indicator_results", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  symbol: varchar("symbol", { length: 20 }).notNull(),
  timeframe: varchar("timeframe", { length: 10 }).notNull(), // "5Min", "15Min", "1Hour", "4Hour"
  // TSI 指标值
  tsiClose: real("tsi_close"),
  tsiSignal: real("tsi_signal"),
  tsiDirection: integer("tsi_direction"), // 1=上升, -1=下降, 0=平
  tsiHeikinBullish: boolean("tsi_heikin_bullish"),
  tsiHeikinBearish: boolean("tsi_heikin_bearish"),
  tsiMaBullish: boolean("tsi_ma_bullish"),
  tsiMaBearish: boolean("tsi_ma_bearish"),
  // QQE 指标值
  qqeRsiSt: real("qqe_rsi_st"),
  qqeTsSt: real("qqe_ts_st"),
  qqeRsiLt: real("qqe_rsi_lt"),
  qqeTsLt: real("qqe_ts_lt"),
  qqeState: text("qqe_state"), // "bullish_aligned", "pullback", "rebound", "bearish_aligned"
  qqeStBull: boolean("qqe_st_bull"),
  qqeLtBull: boolean("qqe_lt_bull"),
  qqeIsOverbought: boolean("qqe_is_overbought"), // 过滤条件: rsiSt > 70
  qqeIsOversold: boolean("qqe_is_oversold"),     // 过滤条件: rsiSt < 30
  // Momentum 指标值
  momStateMom: integer("mom_state_mom"), // -1, 0, 1
  momStateSw: integer("mom_state_sw"), // -1, 0, 1
  momStateLs: integer("mom_state_ls"), // -1, 0, 1
  momComposite: integer("mom_composite"), // -4 to +4
  // 元数据
  price: real("price"),
  barTimestamp: timestamp("bar_timestamp"),
  calculatedAt: timestamp("calculated_at").defaultNow(),
}, (table) => ({
  symbolTimeframeUnique: unique("symbol_timeframe_unique").on(table.symbol, table.timeframe),
}));

export const insertIndicatorResultSchema = createInsertSchema(indicatorResults).omit({
  id: true,
  calculatedAt: true,
});
export type InsertIndicatorResult = z.infer<typeof insertIndicatorResultSchema>;
export type IndicatorResultRow = typeof indicatorResults.$inferSelect;

// Custom Strategies - 自定义策略（支持跨级别）
export const customStrategies = pgTable("custom_strategies", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  description: text("description"),
  isActive: boolean("is_active").default(true),
  // 策略条件 JSON: { "conditions": [...], "signalType": "LONG" | "SHORT" }
  // 每个条件: { "timeframe": "5Min", "indicator": "tsi", "field": "tsiHeikinBullish", "operator": "==", "value": true }
  conditionsJson: text("conditions_json").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const insertCustomStrategySchema = createInsertSchema(customStrategies).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});
export type InsertCustomStrategy = z.infer<typeof insertCustomStrategySchema>;
export type CustomStrategy = typeof customStrategies.$inferSelect;

// Strategy Signal State - 策略信号状态追踪（用于识别首次入场）
export const strategySignalState = pgTable("strategy_signal_state", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  strategyId: varchar("strategy_id", { length: 100 }).notNull(),
  symbol: varchar("symbol", { length: 20 }).notNull(),
  isMatching: boolean("is_matching").default(false), // 当前是否匹配
  signalType: varchar("signal_type", { length: 20 }), // "LONG" | "SHORT" | null
  lastMatchedAt: timestamp("last_matched_at"), // 上次匹配时间
  firstMatchedAt: timestamp("first_matched_at"), // 首次匹配时间（本轮）
  consecutiveMatches: integer("consecutive_matches").default(0), // 连续匹配次数
  updatedAt: timestamp("updated_at").defaultNow(),
}, (table) => ({
  strategySymbolUnique: unique("strategy_symbol_unique").on(table.strategyId, table.symbol),
}));

export const insertStrategySignalStateSchema = createInsertSchema(strategySignalState).omit({
  id: true,
  updatedAt: true,
});
export type InsertStrategySignalState = z.infer<typeof insertStrategySignalStateSchema>;
export type StrategySignalState = typeof strategySignalState.$inferSelect;

// Signal Entries - 入场信号记录（首次匹配时生成）
export const signalEntries = pgTable("signal_entries", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  strategyId: varchar("strategy_id", { length: 100 }).notNull(),
  strategyName: varchar("strategy_name", { length: 100 }).notNull(),
  symbol: varchar("symbol", { length: 20 }).notNull(),
  signalType: varchar("signal_type", { length: 20 }).notNull(), // "LONG" | "SHORT"
  entryTimeframe: varchar("entry_timeframe", { length: 10 }).notNull(),
  price: real("price"),
  indicatorSnapshot: text("indicator_snapshot"), // JSON of indicator values at entry
  isActive: boolean("is_active").default(true), // 信号是否仍然有效
  exitedAt: timestamp("exited_at"), // 信号失效时间（策略不再匹配时）
  exitPrice: real("exit_price"),
  notificationSent: boolean("notification_sent").default(false),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertSignalEntrySchema = createInsertSchema(signalEntries).omit({
  id: true,
  createdAt: true,
});
export type InsertSignalEntry = z.infer<typeof insertSignalEntrySchema>;
export type SignalEntry = typeof signalEntries.$inferSelect;

// Bar data interface (for API response)
export interface Bar {
  t: string; // timestamp
  o: number; // open
  h: number; // high
  l: number; // low
  c: number; // close
  v: number; // volume
  vw?: number; // vwap
}

// Indicator result interface
export interface IndicatorResult {
  tsi?: {
    value: number;
    signal: number;
    direction: number;
    isBullish: boolean;
    isBearish: boolean;
  };
  qqe?: {
    rsiSt: number;
    tsSt: number;
    rsiLt: number;
    tsLt: number;
    state: number; // 1=共振涨, 2=回调, 3=反弹, 4=共振跌
    buySignal: boolean;
    sellSignal: boolean;
  };
  momentum?: {
    stateMom: number;
    stateSw: number;
    stateLs: number;
    composite: number; // -4 to +4
  };
}

// Strategy signal interface
export interface StrategySignal {
  symbol: string;
  strategyId: string;
  strategyName: string;
  signalType: "LONG" | "SHORT";
  timeframe: string;
  indicators: IndicatorResult;
  price: number;
  timestamp: Date;
}
