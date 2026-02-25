import Alpaca from "@alpacahq/alpaca-trade-api";
import type {
  AlpacaAccount,
  AlpacaPosition,
  AlpacaOrder,
  ParsedSignal,
} from "@shared/schema";

// Type definitions for Alpaca SDK responses
interface AlpacaRawAccount {
  id: string;
  account_number: string;
  status: string;
  currency: string;
  buying_power: string;
  cash: string;
  portfolio_value: string;
  equity: string;
  last_equity: string;
  long_market_value: string;
  short_market_value: string;
  daytrade_count: number;
  pattern_day_trader: boolean;
  trading_blocked: boolean;
  transfers_blocked: boolean;
  account_blocked: boolean;
  trade_suspended_by_user: boolean;
  multiplier: string;
  created_at: string;
}

interface AlpacaRawPosition {
  asset_id: string;
  symbol: string;
  exchange: string;
  asset_class: string;
  avg_entry_price: string;
  qty: string;
  side: string;
  market_value: string;
  cost_basis: string;
  unrealized_pl: string;
  unrealized_plpc: string;
  unrealized_intraday_pl: string;
  unrealized_intraday_plpc: string;
  current_price: string;
  lastday_price: string;
  change_today: string;
}

interface AlpacaRawOrder {
  id: string;
  client_order_id: string;
  created_at: string;
  updated_at: string;
  submitted_at: string;
  filled_at: string | null;
  expired_at: string | null;
  canceled_at: string | null;
  failed_at: string | null;
  asset_id: string;
  symbol: string;
  asset_class: string;
  qty: string;
  filled_qty: string;
  filled_avg_price: string | null;
  order_class: string;
  order_type: string;
  type: string;
  side: string;
  time_in_force: string;
  limit_price: string | null;
  stop_price: string | null;
  status: string;
  extended_hours: boolean;
  legs: AlpacaRawOrder[] | null;
}

// Account routing configuration
export interface AccountConfig {
  id: number;
  name: string;
  symbols: string[];  // Empty array means "all other symbols"
  apiKey: string;
  secretKey: string;
}

// Account routing rules - symbols assigned to each account
const ACCOUNT_1_SYMBOLS = ["SOXL", "TSLL"];
const ACCOUNT_2_SYMBOLS = ["ORCL", "OKLO", "ALAB", "HOOD", "MP", "MSTR", "COIN", "CRCL", "VST", "CRWV"];
// Account 3 handles all other symbols (empty array = catch-all)

// Check if current time is during extended hours
function isExtendedHours(): { isExtended: boolean; session: string } {
  const now = new Date();
  
  const etFormatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour: 'numeric',
    minute: 'numeric',
    hour12: false,
  });
  const parts = etFormatter.formatToParts(now);
  const hours = parseInt(parts.find(p => p.type === 'hour')?.value || '0', 10);
  const minutes = parseInt(parts.find(p => p.type === 'minute')?.value || '0', 10);
  const totalMinutes = hours * 60 + minutes;
  
  console.log(`Time check: ET=${hours}:${minutes.toString().padStart(2, '0')} (${totalMinutes} min from midnight)`);
  
  const dayOptions: Intl.DateTimeFormatOptions = { timeZone: 'America/New_York', weekday: 'short' };
  const dayOfWeek = now.toLocaleString('en-US', dayOptions);
  
  if (dayOfWeek === 'Sat' || dayOfWeek === 'Sun') {
    return { isExtended: false, session: 'closed' };
  }
  
  const PRE_MARKET_START = 4 * 60;
  const REGULAR_START = 9 * 60 + 30;
  const REGULAR_END = 16 * 60;
  const AFTER_HOURS_END = 20 * 60;
  
  if (totalMinutes >= PRE_MARKET_START && totalMinutes < REGULAR_START) {
    return { isExtended: true, session: 'pre-market' };
  } else if (totalMinutes >= REGULAR_START && totalMinutes < REGULAR_END) {
    return { isExtended: false, session: 'regular' };
  } else if (totalMinutes >= REGULAR_END && totalMinutes < AFTER_HOURS_END) {
    return { isExtended: true, session: 'after-hours' };
  } else {
    return { isExtended: false, session: 'closed' };
  }
}

class SingleAlpacaClient {
  private client: Alpaca | null = null;
  private initialized = false;
  public accountId: number;
  public accountName: string;
  public symbols: string[];

  constructor(accountId: number, accountName: string, symbols: string[], apiKey: string, secretKey: string) {
    this.accountId = accountId;
    this.accountName = accountName;
    this.symbols = symbols;
    this.initialize(apiKey, secretKey);
  }

  private initialize(apiKey: string, secretKey: string) {
    if (!apiKey || !secretKey) {
      console.log(`Account ${this.accountId} (${this.accountName}): API credentials not configured`);
      return;
    }

    try {
      this.client = new Alpaca({
        keyId: apiKey,
        secretKey: secretKey,
        paper: true,
      });
      this.initialized = true;
      console.log(`Account ${this.accountId} (${this.accountName}): Initialized for symbols: ${this.symbols.length > 0 ? this.symbols.join(', ') : 'ALL OTHER'}`);
    } catch (error) {
      console.error(`Account ${this.accountId} (${this.accountName}): Failed to initialize:`, error);
    }
  }

  isConnected(): boolean {
    return this.initialized && this.client !== null;
  }

  handlesSymbol(symbol: string): boolean {
    if (this.symbols.length === 0) return true;  // Catch-all account
    return this.symbols.includes(symbol.toUpperCase());
  }

  async getAccount(): Promise<AlpacaAccount | null> {
    if (!this.client) return null;

    try {
      const account = (await this.client.getAccount()) as unknown as AlpacaRawAccount;
      return this.transformAccount(account);
    } catch (error) {
      console.error(`Account ${this.accountId}: Error fetching account:`, error);
      return null;
    }
  }

  private transformAccount(raw: AlpacaRawAccount): AlpacaAccount {
    return {
      id: raw.id,
      accountNumber: raw.account_number,
      status: raw.status,
      currency: raw.currency,
      buyingPower: raw.buying_power,
      cash: raw.cash,
      portfolioValue: raw.portfolio_value,
      equity: raw.equity,
      lastEquity: raw.last_equity,
      longMarketValue: raw.long_market_value,
      shortMarketValue: raw.short_market_value,
      daytradeCount: raw.daytrade_count,
      patternDayTrader: raw.pattern_day_trader,
      tradingBlocked: raw.trading_blocked,
      transfersBlocked: raw.transfers_blocked,
      accountBlocked: raw.account_blocked,
      tradeSuspendedByUser: raw.trade_suspended_by_user,
      multiplier: raw.multiplier,
      createdAt: raw.created_at,
    };
  }

  async getPositions(): Promise<AlpacaPosition[]> {
    if (!this.client) return [];

    try {
      const positions = (await this.client.getPositions()) as unknown as AlpacaRawPosition[];
      return positions.map((pos) => this.transformPosition(pos));
    } catch (error) {
      console.error(`Account ${this.accountId}: Error fetching positions:`, error);
      return [];
    }
  }

  private transformPosition(raw: AlpacaRawPosition): AlpacaPosition {
    return {
      assetId: raw.asset_id,
      symbol: raw.symbol,
      exchange: raw.exchange,
      assetClass: raw.asset_class,
      avgEntryPrice: raw.avg_entry_price,
      qty: raw.qty,
      side: raw.side,
      marketValue: raw.market_value,
      costBasis: raw.cost_basis,
      unrealizedPl: raw.unrealized_pl,
      unrealizedPlpc: raw.unrealized_plpc,
      unrealizedIntradayPl: raw.unrealized_intraday_pl,
      unrealizedIntradayPlpc: raw.unrealized_intraday_plpc,
      currentPrice: raw.current_price,
      lastdayPrice: raw.lastday_price,
      changeToday: raw.change_today,
    };
  }

  async getPosition(symbol: string): Promise<AlpacaPosition | null> {
    if (!this.client) return null;

    try {
      const position = (await this.client.getPosition(symbol)) as unknown as AlpacaRawPosition;
      return this.transformPosition(position);
    } catch (error) {
      return null;
    }
  }

  async getOpenOrders(): Promise<AlpacaOrder[]> {
    if (!this.client) return [];

    try {
      const orders = (await this.client.getOrders({
        status: "open",
      })) as unknown as AlpacaRawOrder[];
      return orders.map((order) => this.transformOrder(order));
    } catch (error) {
      console.error(`Account ${this.accountId}: Error fetching open orders:`, error);
      return [];
    }
  }

  async getOrderHistory(limit = 100): Promise<AlpacaOrder[]> {
    if (!this.client) return [];

    try {
      const orders = (await this.client.getOrders({
        status: "all",
        limit,
      })) as unknown as AlpacaRawOrder[];
      return orders.map((order) => this.transformOrder(order));
    } catch (error) {
      console.error(`Account ${this.accountId}: Error fetching order history:`, error);
      return [];
    }
  }

  async getOrderById(orderId: string): Promise<AlpacaOrder | null> {
    if (!this.client) return null;

    try {
      const order = (await this.client.getOrder(orderId)) as unknown as AlpacaRawOrder;
      return this.transformOrder(order);
    } catch (error) {
      console.error(`Account ${this.accountId}: Error fetching order ${orderId}:`, error);
      return null;
    }
  }

  private transformOrder(raw: AlpacaRawOrder): AlpacaOrder {
    return {
      id: raw.id,
      clientOrderId: raw.client_order_id,
      createdAt: raw.created_at,
      updatedAt: raw.updated_at,
      submittedAt: raw.submitted_at,
      filledAt: raw.filled_at,
      expiredAt: raw.expired_at,
      canceledAt: raw.canceled_at,
      failedAt: raw.failed_at,
      assetId: raw.asset_id,
      symbol: raw.symbol,
      assetClass: raw.asset_class,
      qty: raw.qty,
      filledQty: raw.filled_qty,
      filledAvgPrice: raw.filled_avg_price,
      orderClass: raw.order_class,
      orderType: raw.order_type,
      type: raw.type,
      side: raw.side,
      timeInForce: raw.time_in_force,
      limitPrice: raw.limit_price,
      stopPrice: raw.stop_price,
      status: raw.status,
      extendedHours: raw.extended_hours,
      legs: raw.legs ? raw.legs.map((leg) => this.transformOrder(leg)) : null,
    };
  }

  async cancelOrder(orderId: string): Promise<{ success: boolean; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      await this.client.cancelOrder(orderId);
      console.log(`Account ${this.accountId}: Order cancelled: ${orderId}`);
      return { success: true };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      console.error(`Account ${this.accountId}: Error cancelling order:`, errorMessage);
      return { success: false, error: errorMessage };
    }
  }

  async closePosition(symbol: string): Promise<{ success: boolean; order?: AlpacaOrder; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      const result = (await this.client.closePosition(symbol)) as unknown as AlpacaRawOrder;
      console.log(`Account ${this.accountId}: Position closed for ${symbol}`);
      return { success: true, order: this.transformOrder(result) };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      console.error(`Account ${this.accountId}: Error closing position:`, errorMessage);
      return { success: false, error: errorMessage };
    }
  }

  async placeOrder(signal: ParsedSignal): Promise<{ success: boolean; order?: AlpacaOrder; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      const orderParams: Record<string, unknown> = {
        symbol: signal.symbol,
        side: signal.side,
        type: signal.orderType || "market",
        time_in_force: signal.timeInForce || "day",
      };

      if (signal.quantity === "all") {
        const position = await this.getPosition(signal.symbol);
        if (!position) {
          return { success: false, error: `No position found for ${signal.symbol}` };
        }
        orderParams.qty = Math.abs(parseFloat(position.qty));
      } else {
        orderParams.qty = signal.quantity;
      }

      if (signal.orderType === "limit" && signal.price) {
        orderParams.limit_price = Math.round(signal.price * 100) / 100;
      }

      if (signal.orderType === "stop" && signal.price) {
        orderParams.type = "stop";
        orderParams.stop_price = Math.round(signal.price * 100) / 100;
      }

      if (signal.orderType === "stop_limit" && signal.price) {
        orderParams.type = "stop_limit";
        if (signal.stopLoss) {
          orderParams.stop_price = Math.round(signal.stopLoss * 100) / 100;
        }
        orderParams.limit_price = Math.round(signal.price * 100) / 100;
      }

      const tradingSession = isExtendedHours();
      const isOutsideRegularHours = tradingSession.session !== 'regular';
      const shouldUseExtendedHours = signal.extendedHours || tradingSession.isExtended;
      
      if (shouldUseExtendedHours || (isOutsideRegularHours && signal.referencePrice)) {
        let extendedHoursPrice = signal.price || signal.referencePrice;
        if (!extendedHoursPrice) {
          return { 
            success: false, 
            error: `Trading outside regular hours (${tradingSession.session}) requires a price. Please provide 'price' or 'referencePrice'.` 
          };
        }
        extendedHoursPrice = Math.round(extendedHoursPrice * 100) / 100;
        orderParams.type = "limit";
        orderParams.limit_price = extendedHoursPrice;
        orderParams.time_in_force = "day";
        
        if (tradingSession.isExtended) {
          orderParams.extended_hours = true;
          console.log(`Account ${this.accountId}: Extended hours detected: ${tradingSession.session}, using limit order at ${extendedHoursPrice}`);
        } else {
          console.log(`Account ${this.accountId}: Market closed (${tradingSession.session}), using limit order at ${extendedHoursPrice} - will execute at next trading session`);
        }
        
        if (signal.stopLoss || signal.takeProfit) {
          console.log(`Account ${this.accountId}: WARNING: Outside regular hours (${tradingSession.session}) does not support OTO/bracket orders. Stop loss and take profit will be IGNORED.`);
        }
      } else {
        if (signal.stopLoss && signal.takeProfit) {
          // Use GTC for bracket orders so stop-loss/take-profit persist across trading sessions
          orderParams.time_in_force = "gtc";
          orderParams.order_class = "bracket";
          orderParams.stop_loss = { stop_price: Math.round(signal.stopLoss * 100) / 100 };
          orderParams.take_profit = { limit_price: Math.round(signal.takeProfit * 100) / 100 };
          console.log(`Account ${this.accountId}: Using GTC for bracket order with stop_loss=${signal.stopLoss}, take_profit=${signal.takeProfit}`);
        } else if (signal.stopLoss) {
          // Use GTC for OTO orders so stop-loss persists across trading sessions
          orderParams.time_in_force = "gtc";
          orderParams.order_class = "oto";
          orderParams.stop_loss = { stop_price: Math.round(signal.stopLoss * 100) / 100 };
          console.log(`Account ${this.accountId}: Using GTC for OTO order with stop_loss=${signal.stopLoss}`);
        } else if (signal.takeProfit) {
          // Use GTC for OTO orders so take-profit persists across trading sessions
          orderParams.time_in_force = "gtc";
          orderParams.order_class = "oto";
          orderParams.take_profit = { limit_price: Math.round(signal.takeProfit * 100) / 100 };
          console.log(`Account ${this.accountId}: Using GTC for OTO order with take_profit=${signal.takeProfit}`);
        }
      }

      console.log(`Account ${this.accountId}: Placing order:`, orderParams);
      const order = (await this.client.createOrder(orderParams)) as unknown as AlpacaRawOrder;
      console.log(`Account ${this.accountId}: Order placed successfully: ${order.id}`);
      
      return { success: true, order: this.transformOrder(order) };
    } catch (error: any) {
      let errorMessage = "Unknown error";
      
      // Try multiple ways to extract Alpaca API error message
      if (error?.response?.data?.message) {
        // Axios-style error with response data
        errorMessage = error.response.data.message;
        console.error(`Account ${this.accountId}: Alpaca API error:`, JSON.stringify(error.response.data, null, 2));
      } else if (error?.message) {
        // Try to parse JSON from error message (Alpaca SDK sometimes embeds JSON in message)
        try {
          const parsed = JSON.parse(error.message);
          if (parsed.message) {
            errorMessage = parsed.message;
            console.error(`Account ${this.accountId}: Alpaca API error:`, JSON.stringify(parsed, null, 2));
          } else {
            errorMessage = error.message;
          }
        } catch {
          // Not JSON, use message directly
          errorMessage = error.message;
        }
      } else if (typeof error === 'string') {
        // String error
        try {
          const parsed = JSON.parse(error);
          errorMessage = parsed.message || error;
        } catch {
          errorMessage = error;
        }
      }
      
      console.error(`Account ${this.accountId}: Error placing order:`, errorMessage);
      return { success: false, error: errorMessage };
    }
  }
}

// Multi-account manager that routes trades to appropriate accounts
class AlpacaMultiClient {
  private accounts: SingleAlpacaClient[] = [];

  constructor() {
    this.initialize();
  }

  private initialize() {
    // Strategy Test Account - single account for all symbols
    const strategyTestAccount = new SingleAlpacaClient(
      1,
      "Strategy Test",
      [],  // Empty = handles all symbols
      process.env.strategytest_apikey || "",
      process.env.strategytest_SECRETkey || ""
    );
    if (strategyTestAccount.isConnected()) {
      this.accounts.push(strategyTestAccount);
    }

    console.log(`AlpacaMultiClient: ${this.accounts.length} accounts initialized`);
  }

  // Get the client for a specific symbol
  getClientForSymbol(symbol: string): SingleAlpacaClient | null {
    const upperSymbol = symbol.toUpperCase();
    
    // First check specific symbol assignments (accounts 1 and 2)
    for (const account of this.accounts) {
      if (account.symbols.length > 0 && account.handlesSymbol(upperSymbol)) {
        return account;
      }
    }
    
    // Fall back to catch-all account (account 3)
    const catchAll = this.accounts.find(a => a.symbols.length === 0);
    return catchAll || null;
  }

  // Get all connected accounts
  getAllAccounts(): SingleAlpacaClient[] {
    return this.accounts;
  }

  // Check if any account is connected
  isConnected(): boolean {
    return this.accounts.some(a => a.isConnected());
  }

  // Get combined account info from all accounts
  async getAllAccountsInfo(): Promise<{ accountId: number; name: string; symbols: string[]; account: AlpacaAccount | null }[]> {
    const results = await Promise.all(
      this.accounts.map(async (client) => ({
        accountId: client.accountId,
        name: client.accountName,
        symbols: client.symbols,
        account: await client.getAccount(),
      }))
    );
    return results;
  }

  // Get primary account (account 1) for backward compatibility
  async getAccount(): Promise<AlpacaAccount | null> {
    const client = this.accounts[0];
    return client ? client.getAccount() : null;
  }

  // Get positions from all accounts
  async getAllPositions(): Promise<{ accountId: number; accountName: string; positions: AlpacaPosition[] }[]> {
    const results = await Promise.all(
      this.accounts.map(async (client) => ({
        accountId: client.accountId,
        accountName: client.accountName,
        positions: await client.getPositions(),
      }))
    );
    return results;
  }

  // Get positions (combined from all accounts for backward compatibility)
  async getPositions(): Promise<AlpacaPosition[]> {
    const allPositions = await this.getAllPositions();
    return allPositions.flatMap(r => r.positions);
  }

  // Get position for a specific symbol (routes to correct account)
  async getPosition(symbol: string): Promise<AlpacaPosition | null> {
    const client = this.getClientForSymbol(symbol);
    return client ? client.getPosition(symbol) : null;
  }

  // Get open orders from all accounts
  async getAllOpenOrders(): Promise<{ accountId: number; accountName: string; orders: AlpacaOrder[] }[]> {
    const results = await Promise.all(
      this.accounts.map(async (client) => ({
        accountId: client.accountId,
        accountName: client.accountName,
        orders: await client.getOpenOrders(),
      }))
    );
    return results;
  }

  // Get open orders (combined for backward compatibility)
  async getOpenOrders(): Promise<AlpacaOrder[]> {
    const allOrders = await this.getAllOpenOrders();
    return allOrders.flatMap(r => r.orders);
  }

  // Get order history for all accounts (grouped)
  async getAllOrderHistory(limit = 100): Promise<{ accountId: number; accountName: string; orders: AlpacaOrder[] }[]> {
    const results = await Promise.all(
      this.accounts.map(async (client) => ({
        accountId: client.accountId,
        accountName: client.accountName,
        orders: await client.getOrderHistory(limit),
      }))
    );
    return results;
  }

  // Get order history from all accounts (combined for backward compatibility)
  async getOrderHistory(limit = 100): Promise<AlpacaOrder[]> {
    const allOrders = await this.getAllOrderHistory(limit);
    // Combine and sort by created date
    return allOrders
      .flatMap(r => r.orders)
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .slice(0, limit);
  }

  // Get order history for a specific account
  async getOrderHistoryForAccount(accountId: number, limit = 100): Promise<AlpacaOrder[]> {
    const client = this.accounts.find(c => c.accountId === accountId);
    if (!client) {
      console.warn(`No account found with ID ${accountId}`);
      return [];
    }
    return client.getOrderHistory(limit);
  }

  // Get order by ID (searches all accounts)
  async getOrderById(orderId: string): Promise<AlpacaOrder | null> {
    for (const client of this.accounts) {
      const order = await client.getOrderById(orderId);
      if (order) {
        return order;
      }
    }
    return null;
  }

  // Cancel order (needs to find correct account)
  async cancelOrder(orderId: string): Promise<{ success: boolean; error?: string }> {
    // Try each account until we find the order
    for (const client of this.accounts) {
      const result = await client.cancelOrder(orderId);
      if (result.success) {
        return result;
      }
    }
    return { success: false, error: "Order not found in any account" };
  }

  // Close position (routes to correct account)
  async closePosition(symbol: string): Promise<{ success: boolean; order?: AlpacaOrder; error?: string }> {
    const client = this.getClientForSymbol(symbol);
    if (!client) {
      return { success: false, error: `No account configured for symbol ${symbol}` };
    }
    return client.closePosition(symbol);
  }

  // Place order (routes to correct account)
  async placeOrder(signal: ParsedSignal): Promise<{ success: boolean; order?: AlpacaOrder; error?: string }> {
    const client = this.getClientForSymbol(signal.symbol);
    if (!client) {
      return { success: false, error: `No account configured for symbol ${signal.symbol}` };
    }
    console.log(`Routing ${signal.symbol} to Account ${client.accountId} (${client.accountName})`);
    return client.placeOrder(signal);
  }

  // Get routing info for a symbol
  getRoutingInfo(symbol: string): { accountId: number; accountName: string } | null {
    const client = this.getClientForSymbol(symbol);
    if (!client) return null;
    return { accountId: client.accountId, accountName: client.accountName };
  }
}

export const alpacaMultiClient = new AlpacaMultiClient();
