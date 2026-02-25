import Alpaca from "@alpacahq/alpaca-trade-api";
import type {
  AlpacaAccount,
  AlpacaPosition,
  AlpacaOrder,
  ParsedSignal,
  Trade,
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

// Check if current time is during extended hours (pre-market or after-hours)
function isExtendedHours(): { isExtended: boolean; session: string } {
  const now = new Date();
  
  // Convert to Eastern Time using more reliable method
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
  
  // Check day of week (0 = Sunday, 6 = Saturday)
  const dayOptions: Intl.DateTimeFormatOptions = { timeZone: 'America/New_York', weekday: 'short' };
  const dayOfWeek = now.toLocaleString('en-US', dayOptions);
  
  // Market closed on weekends
  if (dayOfWeek === 'Sat' || dayOfWeek === 'Sun') {
    return { isExtended: false, session: 'closed' };
  }
  
  // Trading hours in minutes from midnight (ET):
  // Pre-market: 4:00 AM (240) - 9:30 AM (570)
  // Regular: 9:30 AM (570) - 4:00 PM (960)
  // After-hours: 4:00 PM (960) - 8:00 PM (1200)
  
  const PRE_MARKET_START = 4 * 60;        // 4:00 AM = 240
  const REGULAR_START = 9 * 60 + 30;      // 9:30 AM = 570
  const REGULAR_END = 16 * 60;            // 4:00 PM = 960
  const AFTER_HOURS_END = 20 * 60;        // 8:00 PM = 1200
  
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

class AlpacaClient {
  private client: Alpaca | null = null;
  private initialized = false;

  constructor() {
    this.initialize();
  }

  private initialize() {
    const apiKey = process.env.strategytest_apikey;
    const secretKey = process.env.strategytest_SECRETkey;

    if (!apiKey || !secretKey) {
      console.log("Alpaca API credentials not configured");
      return;
    }

    try {
      this.client = new Alpaca({
        keyId: apiKey,
        secretKey: secretKey,
        paper: true, // Always use paper trading
      });
      this.initialized = true;
      console.log("Alpaca client initialized for paper trading");
    } catch (error) {
      console.error("Failed to initialize Alpaca client:", error);
    }
  }

  isConnected(): boolean {
    return this.initialized && this.client !== null;
  }

  // Get account information
  async getAccount(): Promise<AlpacaAccount | null> {
    if (!this.client) return null;

    try {
      const account = (await this.client.getAccount()) as unknown as AlpacaRawAccount;
      return this.transformAccount(account);
    } catch (error) {
      console.error("Error fetching account:", error);
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

  // Get all positions
  async getPositions(): Promise<AlpacaPosition[]> {
    if (!this.client) return [];

    try {
      const positions = (await this.client.getPositions()) as unknown as AlpacaRawPosition[];
      return positions.map((pos) => this.transformPosition(pos));
    } catch (error) {
      console.error("Error fetching positions:", error);
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

  // Get position for a specific symbol
  async getPosition(symbol: string): Promise<AlpacaPosition | null> {
    if (!this.client) return null;

    try {
      const position = (await this.client.getPosition(
        symbol
      )) as unknown as AlpacaRawPosition;
      return this.transformPosition(position);
    } catch (error) {
      // Position not found is normal
      return null;
    }
  }

  // Get all open orders
  async getOpenOrders(): Promise<AlpacaOrder[]> {
    if (!this.client) return [];

    try {
      const orders = (await this.client.getOrders({
        status: "open",
      })) as unknown as AlpacaRawOrder[];
      return orders.map((order) => this.transformOrder(order));
    } catch (error) {
      console.error("Error fetching open orders:", error);
      return [];
    }
  }

  // Get all orders (including closed)
  async getAllOrders(limit = 100): Promise<AlpacaOrder[]> {
    if (!this.client) return [];

    try {
      const orders = (await this.client.getOrders({
        status: "all",
        limit,
      })) as unknown as AlpacaRawOrder[];
      return orders.map((order) => this.transformOrder(order));
    } catch (error) {
      console.error("Error fetching all orders:", error);
      return [];
    }
  }

  // Get specific order by ID
  async getOrder(orderId: string): Promise<AlpacaOrder | null> {
    if (!this.client) return null;

    try {
      const order = (await this.client.getOrder(
        orderId
      )) as unknown as AlpacaRawOrder;
      return this.transformOrder(order);
    } catch (error) {
      console.error("Error fetching order:", error);
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

  // Place an order
  async placeOrder(
    signal: ParsedSignal
  ): Promise<{ success: boolean; order?: AlpacaOrder; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      const orderParams: {
        symbol: string;
        qty?: number;
        side: string;
        type: string;
        time_in_force: string;
        limit_price?: number;
        stop_price?: number;
        extended_hours?: boolean;
        order_class?: string;
        take_profit?: { limit_price: number };
        stop_loss?: { stop_price: number };
      } = {
        symbol: signal.symbol,
        side: signal.side,
        type: signal.orderType,
        time_in_force: signal.timeInForce,
      };

      // Handle quantity
      if (signal.quantity !== "all") {
        orderParams.qty = signal.quantity;
      }

      // Handle limit price
      if (
        signal.orderType === "limit" ||
        signal.orderType === "stop_limit"
      ) {
        if (!signal.price) {
          return { success: false, error: "Limit orders require a price" };
        }
        orderParams.limit_price = signal.price;
      }

      // Handle stop price
      if (
        signal.orderType === "stop" ||
        signal.orderType === "stop_limit"
      ) {
        if (!signal.price) {
          return { success: false, error: "Stop orders require a price" };
        }
        orderParams.stop_price = signal.price;
      }

      // Auto-detect extended hours or use signal's extended_hours flag
      const tradingSession = isExtendedHours();
      const isOutsideRegularHours = tradingSession.session !== 'regular';
      const shouldUseExtendedHours = signal.extendedHours || tradingSession.isExtended;
      
      // Use limit order when: extended hours, market closed, or referencePrice provided with non-regular session
      if (shouldUseExtendedHours || (isOutsideRegularHours && signal.referencePrice)) {
        // Use referencePrice as limit price if no explicit price provided
        let extendedHoursPrice = signal.price || signal.referencePrice;
        if (!extendedHoursPrice) {
          return { 
            success: false, 
            error: `Trading outside regular hours (${tradingSession.session}) requires a price. Please provide 'price' or 'referencePrice'.` 
          };
        }
        // Round to 2 decimal places (Alpaca doesn't accept sub-penny pricing for stocks > $1)
        extendedHoursPrice = Math.round(extendedHoursPrice * 100) / 100;
        // Force limit order - OTO/bracket orders NOT supported outside regular hours
        orderParams.type = "limit";
        orderParams.limit_price = extendedHoursPrice;
        orderParams.time_in_force = "day";
        
        // Only set extended_hours=true during actual extended hours (4 AM - 8 PM ET)
        // If market is closed (after 8 PM or before 4 AM), order will queue for next session
        if (tradingSession.isExtended) {
          orderParams.extended_hours = true;
          console.log(`Extended hours detected: ${tradingSession.session}, using limit order at ${extendedHoursPrice}`);
        } else {
          console.log(`Market closed (${tradingSession.session}), using limit order at ${extendedHoursPrice} - will execute at next trading session`);
        }
        
        // Log warning if stop_loss or take_profit was requested but will be ignored
        if (signal.stopLoss || signal.takeProfit) {
          console.log(`WARNING: Outside regular hours (${tradingSession.session}) does not support OTO/bracket orders. Stop loss and take profit will be IGNORED.`);
        }
      } else {
        // Regular hours: Handle bracket orders with stop loss and take profit
        // Bracket orders require BOTH stop_loss AND take_profit
        // If only one is provided, we use OTO (one-triggers-other)
        if (signal.stopLoss && signal.takeProfit) {
          // Full bracket order with both legs
          orderParams.order_class = "bracket";
          orderParams.take_profit = { limit_price: signal.takeProfit };
          orderParams.stop_loss = { stop_price: signal.stopLoss };
        } else if (signal.stopLoss || signal.takeProfit) {
          // Only one exit condition - use OTO (one-triggers-other)
          orderParams.order_class = "oto";
          if (signal.takeProfit) {
            orderParams.take_profit = { limit_price: signal.takeProfit };
          }
          if (signal.stopLoss) {
            orderParams.stop_loss = { stop_price: signal.stopLoss };
          }
        }
      }

      console.log("Placing order:", orderParams);
      const rawOrder = (await this.client.createOrder(
        orderParams
      )) as unknown as AlpacaRawOrder;
      const order = this.transformOrder(rawOrder);
      console.log("Order placed successfully:", order.id);

      return { success: true, order };
    } catch (error: unknown) {
      let errorMessage = "Unknown error placing order";
      if (error instanceof Error) {
        errorMessage = error.message;
        // Log full error details for debugging
        console.error("Error placing order - Full details:", JSON.stringify(error, null, 2));
        // Check for Axios error with response data
        if ('response' in error && (error as { response?: { data?: unknown } }).response?.data) {
          console.error("Alpaca API error response:", JSON.stringify((error as { response: { data: unknown } }).response.data, null, 2));
          const responseData = (error as { response: { data: { message?: string } } }).response.data;
          if (responseData.message) {
            errorMessage = responseData.message;
          }
        }
      }
      console.error("Error placing order:", errorMessage);
      return { success: false, error: errorMessage };
    }
  }

  // Cancel an order
  async cancelOrder(
    orderId: string
  ): Promise<{ success: boolean; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      await this.client.cancelOrder(orderId);
      console.log("Order cancelled:", orderId);
      return { success: true };
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : "Unknown error cancelling order";
      console.error("Error cancelling order:", errorMessage);
      return { success: false, error: errorMessage };
    }
  }

  // Close a position
  async closePosition(
    symbol: string
  ): Promise<{ success: boolean; order?: AlpacaOrder; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      const rawOrder = (await this.client.closePosition(
        symbol
      )) as unknown as AlpacaRawOrder;
      const order = this.transformOrder(rawOrder);
      console.log("Position closed:", symbol);
      return { success: true, order };
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : "Unknown error closing position";
      console.error("Error closing position:", errorMessage);
      return { success: false, error: errorMessage };
    }
  }

  // Close all positions
  async closeAllPositions(): Promise<{ success: boolean; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      await this.client.closeAllPositions();
      console.log("All positions closed");
      return { success: true };
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : "Unknown error closing positions";
      console.error("Error closing all positions:", errorMessage);
      return { success: false, error: errorMessage };
    }
  }

  // Cancel all orders
  async cancelAllOrders(): Promise<{ success: boolean; error?: string }> {
    if (!this.client) {
      return { success: false, error: "Alpaca client not initialized" };
    }

    try {
      await this.client.cancelAllOrders();
      console.log("All orders cancelled");
      return { success: true };
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : "Unknown error cancelling orders";
      console.error("Error cancelling all orders:", errorMessage);
      return { success: false, error: errorMessage };
    }
  }
}

// Export singleton instance
export const alpacaClient = new AlpacaClient();
