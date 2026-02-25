import type { WebhookSignal, ParsedSignal, Side, OrderType, TimeInForce } from "@shared/schema";
import { SideEnum, OrderTypeEnum, TimeInForceEnum } from "@shared/schema";

export function parseSignal(signalData: WebhookSignal): ParsedSignal {
  // Extract symbol
  const symbol = (signalData.symbol || signalData.ticker || "").toUpperCase();
  if (!symbol) {
    throw new Error("Missing required field: symbol");
  }

  // Check for flat/close signal
  const sentiment = (signalData.sentiment || "").toLowerCase();
  const isCloseSignal = sentiment === "flat";

  // Extract side
  let sideStr = (signalData.side || signalData.action || "").toLowerCase();
  if (isCloseSignal) {
    sideStr = sideStr || "sell";
  }
  
  let side: Side;
  if (sideStr === "buy" || sideStr === "long") {
    side = SideEnum.BUY;
  } else if (sideStr === "sell" || sideStr === "short") {
    side = SideEnum.SELL;
  } else {
    throw new Error(`Invalid side: ${sideStr}`);
  }

  // Extract quantity
  const quantityStr = signalData.quantity ?? signalData.qty ?? signalData.size ?? "1";
  let quantity: number | "all";
  
  if (String(quantityStr).toLowerCase() === "all" || isCloseSignal) {
    quantity = "all";
  } else {
    quantity = parseFloat(String(quantityStr));
    if (isNaN(quantity) || quantity <= 0) {
      quantity = 1;
    }
  }

  // Extract price
  let price: number | undefined;
  const priceValue = signalData.price || signalData.limit_price;
  if (priceValue && String(priceValue).toLowerCase() !== "market") {
    price = parseFloat(String(priceValue));
  }

  // Extract order type
  let orderTypeStr = (signalData.order_type || signalData.type || "market").toLowerCase();
  let orderType: OrderType;
  
  switch (orderTypeStr) {
    case "market":
    case "mkt":
      orderType = OrderTypeEnum.MARKET;
      break;
    case "limit":
    case "lmt":
      orderType = OrderTypeEnum.LIMIT;
      break;
    case "stop":
      orderType = OrderTypeEnum.STOP;
      break;
    case "stop_limit":
      orderType = OrderTypeEnum.STOP_LIMIT;
      break;
    case "trailing_stop":
      orderType = OrderTypeEnum.TRAILING_STOP;
      break;
    default:
      orderType = OrderTypeEnum.MARKET;
  }

  // Extract time in force
  let tifStr = (signalData.time_in_force || "day").toLowerCase();
  let timeInForce: TimeInForce;
  
  switch (tifStr) {
    case "day":
      timeInForce = TimeInForceEnum.DAY;
      break;
    case "gtc":
      timeInForce = TimeInForceEnum.GTC;
      break;
    case "ioc":
      timeInForce = TimeInForceEnum.IOC;
      break;
    case "fok":
      timeInForce = TimeInForceEnum.FOK;
      break;
    case "opg":
      timeInForce = TimeInForceEnum.OPG;
      break;
    case "cls":
      timeInForce = TimeInForceEnum.CLS;
      break;
    default:
      timeInForce = TimeInForceEnum.DAY;
  }

  // Extract stop loss and take profit (support multiple formats)
  let stopLoss: number | undefined;
  let takeProfit: number | undefined;

  if (typeof signalData.stopLoss === 'number') {
    stopLoss = signalData.stopLoss;
  } else if (signalData.stopLoss?.stopPrice) {
    stopLoss = signalData.stopLoss.stopPrice;
  } else if (typeof signalData.stop_loss === 'number') {
    stopLoss = signalData.stop_loss;
  } else if (signalData.stop_loss?.stop_price) {
    stopLoss = signalData.stop_loss.stop_price;
  }

  if (typeof signalData.takeProfit === 'number') {
    takeProfit = signalData.takeProfit;
  } else if (signalData.takeProfit?.limitPrice) {
    takeProfit = signalData.takeProfit.limitPrice;
  } else if (typeof signalData.take_profit === 'number') {
    takeProfit = signalData.take_profit;
  } else if (signalData.take_profit?.limit_price) {
    takeProfit = signalData.take_profit.limit_price;
  }

  // Extract reference price
  let referencePrice: number | undefined;
  if (signalData.extras?.referencePrice) {
    referencePrice = signalData.extras.referencePrice;
  } else if (signalData.reference_price) {
    referencePrice = signalData.reference_price;
  } else if (signalData.referencePrice) {
    referencePrice = signalData.referencePrice;
  }

  // Check for extended hours
  const extendedHours = signalData.extended_hours || false;

  // Validate limit orders have a price
  if ((orderType === "limit" || orderType === "stop_limit") && !price) {
    throw new Error("Limit orders require a price");
  }

  return {
    symbol,
    side,
    quantity,
    price,
    orderType,
    timeInForce,
    stopLoss,
    takeProfit,
    referencePrice,
    extendedHours,
    isCloseSignal,
    closeAll: quantity === "all",
  };
}

export function createTestSignal(
  symbol = "AAPL",
  side: Side = "buy",
  quantity = 1,
  orderType: OrderType = "market",
  price?: number
): WebhookSignal {
  const signal: WebhookSignal = {
    symbol,
    side,
    quantity,
    order_type: orderType,
  };

  if (price && (orderType === "limit" || orderType === "stop_limit")) {
    signal.price = price;
  }

  return signal;
}
