// Using blueprint:javascript_database for PostgreSQL persistence
import type { 
  Trade, 
  InsertTrade, 
  TradingConfig, 
  InsertTradingConfig,
  SignalLog,
  InsertSignalLog 
} from "@shared/schema";
import { trades, tradingConfigs, signalLogs } from "@shared/schema";
import { db } from "./db";
import { eq, desc, and, isNotNull } from "drizzle-orm";

export interface IStorage {
  getTrades(): Promise<Trade[]>;
  getRecentTrades(limit: number): Promise<Trade[]>;
  getClosedTrades(): Promise<Trade[]>;
  getTrade(id: string): Promise<Trade | undefined>;
  getTradeByAlpacaOrderId(alpacaOrderId: string): Promise<Trade | undefined>;
  createTrade(trade: InsertTrade): Promise<Trade>;
  updateTrade(id: string, updates: Partial<Trade>): Promise<Trade | undefined>;
  updateTradeByOrderId(alpacaOrderId: string, updates: Partial<Trade>): Promise<Trade | undefined>;
  
  getConfig(key: string): Promise<string | undefined>;
  setConfig(key: string, value: string, description?: string): Promise<TradingConfig>;
  getAllConfigs(): Promise<TradingConfig[]>;
  
  createSignalLog(log: InsertSignalLog): Promise<SignalLog>;
  updateSignalLog(id: string, updates: Partial<SignalLog>): Promise<SignalLog | undefined>;
  getSignalLogs(limit: number): Promise<SignalLog[]>;
  
  initializeDefaults(): Promise<void>;
}

export class DatabaseStorage implements IStorage {
  async getTrades(): Promise<Trade[]> {
    return await db.select().from(trades).orderBy(desc(trades.createdAt));
  }

  async getRecentTrades(limit: number): Promise<Trade[]> {
    return await db.select().from(trades).orderBy(desc(trades.createdAt)).limit(limit);
  }

  async getClosedTrades(): Promise<Trade[]> {
    return await db.select().from(trades)
      .where(and(
        eq(trades.isClosePosition, true),
        isNotNull(trades.positionAvgEntryPrice)
      ))
      .orderBy(desc(trades.createdAt));
  }

  async getTrade(id: string): Promise<Trade | undefined> {
    const [trade] = await db.select().from(trades).where(eq(trades.id, id));
    return trade || undefined;
  }

  async getTradeByAlpacaOrderId(alpacaOrderId: string): Promise<Trade | undefined> {
    const [trade] = await db.select().from(trades).where(eq(trades.alpacaOrderId, alpacaOrderId));
    return trade || undefined;
  }

  async createTrade(insertTrade: InsertTrade): Promise<Trade> {
    const [trade] = await db.insert(trades).values(insertTrade).returning();
    return trade;
  }

  async updateTrade(id: string, updates: Partial<Trade>): Promise<Trade | undefined> {
    const [trade] = await db
      .update(trades)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(trades.id, id))
      .returning();
    return trade || undefined;
  }

  async updateTradeByOrderId(alpacaOrderId: string, updates: Partial<Trade>): Promise<Trade | undefined> {
    const [trade] = await db
      .update(trades)
      .set({ ...updates, updatedAt: new Date() })
      .where(eq(trades.alpacaOrderId, alpacaOrderId))
      .returning();
    return trade || undefined;
  }

  async getConfig(key: string): Promise<string | undefined> {
    const [config] = await db.select().from(tradingConfigs).where(eq(tradingConfigs.key, key));
    return config?.value;
  }

  async setConfig(key: string, value: string, description?: string): Promise<TradingConfig> {
    const existing = await db.select().from(tradingConfigs).where(eq(tradingConfigs.key, key));
    
    if (existing.length > 0) {
      const [config] = await db
        .update(tradingConfigs)
        .set({ value, description: description || existing[0].description, updatedAt: new Date() })
        .where(eq(tradingConfigs.key, key))
        .returning();
      return config;
    } else {
      const [config] = await db
        .insert(tradingConfigs)
        .values({ key, value, description: description || null })
        .returning();
      return config;
    }
  }

  async getAllConfigs(): Promise<TradingConfig[]> {
    return await db.select().from(tradingConfigs);
  }

  async createSignalLog(insertLog: InsertSignalLog): Promise<SignalLog> {
    const [log] = await db.insert(signalLogs).values(insertLog).returning();
    return log;
  }

  async updateSignalLog(id: string, updates: Partial<SignalLog>): Promise<SignalLog | undefined> {
    const [log] = await db
      .update(signalLogs)
      .set(updates)
      .where(eq(signalLogs.id, id))
      .returning();
    return log || undefined;
  }

  async getSignalLogs(limit: number): Promise<SignalLog[]> {
    return await db.select().from(signalLogs).orderBy(desc(signalLogs.createdAt)).limit(limit);
  }

  async initializeDefaults(): Promise<void> {
    const defaults = [
      { key: "TRADING_ENABLED", value: "true", description: "Enable/disable trading" },
      { key: "MAX_TRADE_AMOUNT", value: "100000", description: "Maximum trade amount in USD" },
    ];
    
    for (const config of defaults) {
      const existing = await this.getConfig(config.key);
      if (!existing) {
        await this.setConfig(config.key, config.value, config.description);
      }
    }
  }
}

export const storage = new DatabaseStorage();
