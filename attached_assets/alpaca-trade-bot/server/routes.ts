import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { alpacaMultiClient } from "./alpaca-multi-client";
import { parseSignal } from "./signal-parser";
import { pollOrderAndNotify } from "./discord-notifier";
import * as scheduler from "./scanner/scheduler";
import { getActiveSignalEntries, getRecentSignalEntries, getSignalEntriesBySymbol } from "./scanner/strategy-engine";
import type { WebhookSignal, InsertTrade } from "@shared/schema";

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // Initialize default trading configs
  await storage.initializeDefaults();
  // Account status endpoint
  app.get("/api/account/status", async (_req: Request, res: Response) => {
    try {
      const connected = alpacaMultiClient.isConnected();
      if (!connected) {
        return res.json({ connected: false });
      }
      
      const account = await alpacaMultiClient.getAccount();
      res.json({ 
        connected: !!account, 
        account: account ? { status: account.status } : null 
      });
    } catch (error) {
      res.json({ connected: false });
    }
  });

  // Get all accounts info (multi-account)
  app.get("/api/accounts", async (_req: Request, res: Response) => {
    try {
      const accounts = await alpacaMultiClient.getAllAccountsInfo();
      res.json(accounts);
    } catch (error) {
      console.error("Error fetching accounts:", error);
      res.status(500).json({ error: "Failed to fetch accounts" });
    }
  });

  // Get account information (primary account for backward compatibility)
  app.get("/api/account", async (_req: Request, res: Response) => {
    try {
      const account = await alpacaMultiClient.getAccount();
      if (!account) {
        return res.status(503).json({ error: "Unable to fetch account information" });
      }
      res.json(account);
    } catch (error) {
      console.error("Error fetching account:", error);
      res.status(500).json({ error: "Failed to fetch account" });
    }
  });

  // Reset account - not supported for multi-account (too dangerous)
  app.post("/api/account/reset", async (_req: Request, res: Response) => {
    res.status(400).json({ 
      success: false, 
      error: "Account reset is disabled for multi-account setup. Please manage accounts individually via Alpaca dashboard." 
    });
  });

  // Get all positions (from all accounts)
  app.get("/api/positions", async (_req: Request, res: Response) => {
    try {
      const positions = await alpacaMultiClient.getPositions();
      res.json(positions);
    } catch (error) {
      console.error("Error fetching positions:", error);
      res.status(500).json({ error: "Failed to fetch positions" });
    }
  });

  // Get positions by account
  app.get("/api/positions/by-account", async (_req: Request, res: Response) => {
    try {
      const positionsByAccount = await alpacaMultiClient.getAllPositions();
      res.json(positionsByAccount);
    } catch (error) {
      console.error("Error fetching positions by account:", error);
      res.status(500).json({ error: "Failed to fetch positions" });
    }
  });

  // Close a specific position
  app.delete("/api/positions/:symbol", async (req: Request, res: Response) => {
    try {
      const { symbol } = req.params;
      const upperSymbol = symbol.toUpperCase();
      
      // Get position info BEFORE closing to capture avgEntryPrice
      const position = await alpacaMultiClient.getPosition(upperSymbol);
      const positionAvgEntryPrice = position ? parseFloat(position.avgEntryPrice) : undefined;
      const positionQty = position ? Math.abs(parseFloat(position.qty)) : undefined;
      const positionSide = position ? (parseFloat(position.qty) > 0 ? "long" : "short") : undefined;
      
      const routingInfo = alpacaMultiClient.getRoutingInfo(upperSymbol);
      
      const result = await alpacaMultiClient.closePosition(upperSymbol);
      
      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      // Record the trade with position metadata
      if (result.order) {
        try {
          await storage.createTrade({
            symbol: result.order.symbol,
            side: result.order.side as "buy" | "sell",
            quantity: parseFloat(result.order.qty),
            orderType: result.order.type || "market",
            status: result.order.status || "new",
            alpacaOrderId: result.order.id,
            alpacaResponse: JSON.stringify(result.order),
            signalData: JSON.stringify({ action: "close_position", symbol: symbol }),
            isClosePosition: true,
            positionAvgEntryPrice,
            positionQty,
            positionSide,
            accountId: routingInfo?.accountId,
          });
          
          // Start polling to update filledPrice
          if (routingInfo) {
            pollOrderAndNotify(
              alpacaMultiClient,
              result.order.id,
              routingInfo.accountId,
              { action: "close_position", symbol: symbol }
            );
          }
        } catch (dbError) {
          console.error("Error saving close position trade:", dbError);
        }
      }

      res.json({ success: true, order: result.order });
    } catch (error) {
      console.error("Error closing position:", error);
      res.status(500).json({ error: "Failed to close position" });
    }
  });

  // Get open orders from Alpaca (from all accounts)
  app.get("/api/orders/open", async (_req: Request, res: Response) => {
    try {
      const orders = await alpacaMultiClient.getOpenOrders();
      res.json(orders);
    } catch (error) {
      console.error("Error fetching open orders:", error);
      res.status(500).json({ error: "Failed to fetch open orders" });
    }
  });

  // Get open orders by account
  app.get("/api/orders/by-account", async (_req: Request, res: Response) => {
    try {
      const ordersByAccount = await alpacaMultiClient.getAllOpenOrders();
      res.json(ordersByAccount);
    } catch (error) {
      console.error("Error fetching orders by account:", error);
      res.status(500).json({ error: "Failed to fetch orders" });
    }
  });

  // Get order history from Alpaca (all orders including filled, cancelled, etc.)
  app.get("/api/orders/history", async (req: Request, res: Response) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const orders = await alpacaMultiClient.getOrderHistory(limit);
      res.json(orders);
    } catch (error) {
      console.error("Error fetching order history:", error);
      res.status(500).json({ error: "Failed to fetch order history" });
    }
  });

  // Get order history by account
  app.get("/api/orders/history/by-account", async (req: Request, res: Response) => {
    try {
      const limit = parseInt(req.query.limit as string) || 500;
      const ordersByAccount = await alpacaMultiClient.getAllOrderHistory(limit);
      res.json(ordersByAccount);
    } catch (error) {
      console.error("Error fetching order history by account:", error);
      res.status(500).json({ error: "Failed to fetch order history" });
    }
  });

  // Get all trades from storage
  app.get("/api/trades", async (_req: Request, res: Response) => {
    try {
      const trades = await storage.getTrades();
      res.json(trades);
    } catch (error) {
      console.error("Error fetching trades:", error);
      res.status(500).json({ error: "Failed to fetch trades" });
    }
  });

  // Get recent trades
  app.get("/api/trades/recent", async (_req: Request, res: Response) => {
    try {
      const trades = await storage.getRecentTrades(10);
      res.json(trades);
    } catch (error) {
      console.error("Error fetching recent trades:", error);
      res.status(500).json({ error: "Failed to fetch recent trades" });
    }
  });

  // Get closed trades by account (trades with isClosePosition=true and positionAvgEntryPrice set)
  app.get("/api/trades/closed/by-account", async (_req: Request, res: Response) => {
    try {
      const trades = await storage.getClosedTrades();
      // Group by accountId
      const byAccount: Record<number, typeof trades> = {};
      for (const trade of trades) {
        const acctId = trade.accountId || 0;
        if (!byAccount[acctId]) byAccount[acctId] = [];
        byAccount[acctId].push(trade);
      }
      res.json(byAccount);
    } catch (error) {
      console.error("Error fetching closed trades:", error);
      res.status(500).json({ error: "Failed to fetch closed trades" });
    }
  });

  // Backfill entry dates for historical closed trades
  app.post("/api/trades/backfill-entry-dates", async (_req: Request, res: Response) => {
    try {
      const trades = await storage.getClosedTrades();
      const tradesToUpdate = trades.filter(t => !t.positionEntryDate && t.accountId);
      
      let updated = 0;
      let failed = 0;
      
      for (const trade of tradesToUpdate) {
        try {
          const orderHistory = await alpacaMultiClient.getOrderHistoryForAccount(trade.accountId!, 500);
          
          // Filter to filled orders for this symbol
          const symbolOrders = orderHistory
            .filter(o => o.symbol === trade.symbol && o.status === "filled" && o.filledAt)
            .sort((a, b) => new Date(a.filledAt!).getTime() - new Date(b.filledAt!).getTime());
          
          // Determine entry side based on position side
          const entrySide = trade.positionSide === "long" ? "buy" : "sell";
          const firstEntryOrder = symbolOrders.find(o => o.side === entrySide);
          
          if (firstEntryOrder?.filledAt) {
            await storage.updateTrade(trade.id, {
              positionEntryDate: new Date(firstEntryOrder.filledAt),
            });
            updated++;
            console.log(`Backfilled entry date for ${trade.symbol}: ${firstEntryOrder.filledAt}`);
          } else {
            failed++;
          }
        } catch (err) {
          console.error(`Error backfilling trade ${trade.id}:`, err);
          failed++;
        }
      }
      
      res.json({ 
        success: true, 
        message: `Backfilled ${updated} trades, ${failed} failed`,
        updated,
        failed,
        total: tradesToUpdate.length
      });
    } catch (error) {
      console.error("Error backfilling entry dates:", error);
      res.status(500).json({ error: "Failed to backfill entry dates" });
    }
  });

  // Cancel an order
  app.delete("/api/orders/:orderId", async (req: Request, res: Response) => {
    try {
      const { orderId } = req.params;
      const result = await alpacaMultiClient.cancelOrder(orderId);
      
      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      // Update the trade record if it exists
      const trade = await storage.getTradeByAlpacaOrderId(orderId);
      if (trade) {
        await storage.updateTrade(trade.id, { status: "cancelled" });
      }

      res.json({ success: true });
    } catch (error) {
      console.error("Error cancelling order:", error);
      res.status(500).json({ error: "Failed to cancel order" });
    }
  });

  // Get trading configuration
  app.get("/api/config", async (_req: Request, res: Response) => {
    try {
      const tradingEnabled = await storage.getConfig("TRADING_ENABLED");
      const maxTradeAmount = await storage.getConfig("MAX_TRADE_AMOUNT");
      
      res.json({
        tradingEnabled: tradingEnabled === "true",
        maxTradeAmount: parseFloat(maxTradeAmount || "100000"),
        webhookUrl: "/api/webhook",
      });
    } catch (error) {
      console.error("Error fetching config:", error);
      res.status(500).json({ error: "Failed to fetch configuration" });
    }
  });

  // Update trading configuration
  app.patch("/api/config", async (req: Request, res: Response) => {
    try {
      const { tradingEnabled, maxTradeAmount } = req.body;
      
      if (tradingEnabled !== undefined) {
        await storage.setConfig("TRADING_ENABLED", String(tradingEnabled));
      }
      
      if (maxTradeAmount !== undefined) {
        await storage.setConfig("MAX_TRADE_AMOUNT", String(maxTradeAmount));
      }

      res.json({ success: true });
    } catch (error) {
      console.error("Error updating config:", error);
      res.status(500).json({ error: "Failed to update configuration" });
    }
  });

  // Get signal logs (for debugging webhook issues)
  app.get("/api/signal-logs", async (req: Request, res: Response) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const logs = await storage.getSignalLogs(limit);
      res.json(logs);
    } catch (error) {
      console.error("Error fetching signal logs:", error);
      res.status(500).json({ error: "Failed to fetch signal logs" });
    }
  });

  // Webhook endpoint for TradingView signals
  app.post("/api/webhook", async (req: Request, res: Response) => {
    const clientIp = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
    const rawSignal = JSON.stringify(req.body);
    
    console.log(`Received webhook from ${clientIp}:`, rawSignal);

    // Create initial signal log entry
    let signalLog;
    try {
      signalLog = await storage.createSignalLog({
        rawSignal,
        ipAddress: String(clientIp),
        parsedSuccessfully: false,
      });
    } catch (dbError) {
      console.error("Error creating signal log:", dbError);
    }

    try {
      // Check if trading is enabled
      const tradingEnabled = await storage.getConfig("TRADING_ENABLED");
      if (tradingEnabled !== "true") {
        // Update signal log with error
        if (signalLog) {
          try {
            await storage.updateSignalLog(signalLog.id, {
              parsedSuccessfully: false,
              errorMessage: "Trading is currently disabled",
            });
          } catch (logError) {
            console.error("Error updating signal log:", logError);
          }
        }
        return res.status(403).json({
          success: false,
          error: "Trading is currently disabled",
        });
      }

      // Parse the signal
      const signalData = req.body as WebhookSignal;
      const parsedSignal = parseSignal(signalData);
      
      console.log("Parsed signal:", parsedSignal);

      // Check max trade amount - use referencePrice if available, otherwise price, otherwise skip check
      const maxTradeAmount = parseFloat(await storage.getConfig("MAX_TRADE_AMOUNT") || "100000");
      const priceForEstimate = parsedSignal.referencePrice || parsedSignal.price;
      const estimatedAmount = (parsedSignal.quantity !== "all" && priceForEstimate)
        ? parsedSignal.quantity * priceForEstimate
        : 0;
      
      if (estimatedAmount > maxTradeAmount && parsedSignal.quantity !== "all") {
        const errorMsg = `Trade amount exceeds maximum allowed ($${maxTradeAmount})`;
        if (signalLog) {
          try {
            await storage.updateSignalLog(signalLog.id, {
              parsedSuccessfully: false,
              errorMessage: errorMsg,
            });
          } catch (logError) {
            console.error("Error updating signal log:", logError);
          }
        }
        return res.status(400).json({
          success: false,
          error: errorMsg,
        });
      }

      // Log routing info
      const routingInfo = alpacaMultiClient.getRoutingInfo(parsedSignal.symbol);
      if (routingInfo) {
        console.log(`Signal for ${parsedSignal.symbol} -> Account ${routingInfo.accountId} (${routingInfo.accountName})`);
      }

      // Variables to capture position info for close positions
      let closingPosition: { avgEntryPrice: number; qty: number; side: string; entryDate: Date | null } | null = null;

      // Handle close position signals
      if (parsedSignal.isCloseSignal || parsedSignal.closeAll) {
        // Get current position to know the quantity
        const position = await alpacaMultiClient.getPosition(parsedSignal.symbol);
        
        if (!position) {
          const errorMsg = `No position found for ${parsedSignal.symbol}`;
          if (signalLog) {
            try {
              await storage.updateSignalLog(signalLog.id, {
                parsedSuccessfully: false,
                errorMessage: errorMsg,
              });
            } catch (logError) {
              console.error("Error updating signal log:", logError);
            }
          }
          return res.status(400).json({
            success: false,
            error: errorMsg,
          });
        }

        // Validate that close signal direction matches position direction
        // flat + buy = ExitShort (close short position) → requires SHORT position
        // flat + sell = ExitLong (close long position) → requires LONG position
        const positionIsLong = parseFloat(position.qty) > 0;
        const signalAction = (signalData.action || signalData.side || "").toLowerCase();
        
        if (signalAction === "buy" && positionIsLong) {
          // Trying to exit short but holding long
          const errorMsg = `Cannot exit short: currently holding LONG position for ${parsedSignal.symbol}`;
          console.log(`Close signal mismatch: ${signalAction} signal but position is ${positionIsLong ? 'LONG' : 'SHORT'}`);
          if (signalLog) {
            try {
              await storage.updateSignalLog(signalLog.id, {
                parsedSuccessfully: false,
                errorMessage: errorMsg,
              });
            } catch (logError) {
              console.error("Error updating signal log:", logError);
            }
          }
          return res.status(400).json({
            success: false,
            error: errorMsg,
          });
        }
        
        if (signalAction === "sell" && !positionIsLong) {
          // Trying to exit long but holding short
          const errorMsg = `Cannot exit long: currently holding SHORT position for ${parsedSignal.symbol}`;
          console.log(`Close signal mismatch: ${signalAction} signal but position is ${positionIsLong ? 'LONG' : 'SHORT'}`);
          if (signalLog) {
            try {
              await storage.updateSignalLog(signalLog.id, {
                parsedSuccessfully: false,
                errorMessage: errorMsg,
              });
            } catch (logError) {
              console.error("Error updating signal log:", logError);
            }
          }
          return res.status(400).json({
            success: false,
            error: errorMsg,
          });
        }

        // Cancel any existing orders for this symbol to release held shares
        const openOrders = await alpacaMultiClient.getOpenOrders();
        const symbolOrders = openOrders.filter(o => o.symbol === parsedSignal.symbol);
        if (symbolOrders.length > 0) {
          console.log(`Cancelling ${symbolOrders.length} existing order(s) for ${parsedSignal.symbol} before closing position`);
          for (const order of symbolOrders) {
            await alpacaMultiClient.cancelOrder(order.id);
          }
          // Wait a moment for cancellation to process
          await new Promise(resolve => setTimeout(resolve, 500));
        }

        const positionQty = Math.abs(parseFloat(position.qty));
        const positionSide = parseFloat(position.qty) > 0 ? "long" : "short";
        
        // Look up first entry order date for this position
        let positionEntryDate: Date | null = null;
        try {
          // Get order history for this symbol to find the first entry order
          const accountId = routingInfo?.accountId;
          if (accountId) {
            const orderHistory = await alpacaMultiClient.getOrderHistoryForAccount(accountId, 500);
            // Filter to filled orders for this symbol
            const symbolOrders = orderHistory
              .filter(o => o.symbol === parsedSignal.symbol && o.status === "filled" && o.filledAt)
              .sort((a, b) => new Date(a.filledAt!).getTime() - new Date(b.filledAt!).getTime());
            
            // Find the first entry order (buy for long, sell for short)
            const entrySide = positionSide === "long" ? "buy" : "sell";
            const firstEntryOrder = symbolOrders.find(o => o.side === entrySide);
            if (firstEntryOrder?.filledAt) {
              positionEntryDate = new Date(firstEntryOrder.filledAt);
              console.log(`Found first entry date for ${parsedSignal.symbol}: ${positionEntryDate.toISOString()}`);
            }
          }
        } catch (err) {
          console.error("Error looking up entry date:", err);
        }
        
        // Capture position info for the close trade record
        closingPosition = {
          avgEntryPrice: parseFloat(position.avgEntryPrice),
          qty: positionQty,
          side: positionSide,
          entryDate: positionEntryDate,
        };
        
        // During extended hours, use limit order to close position
        // Update parsedSignal with position quantity and correct side
        parsedSignal.quantity = positionQty;
        parsedSignal.side = positionSide === "long" ? "sell" : "buy";
        
        // If we have a referencePrice and it's extended hours, placeOrder will handle it
        // Otherwise try regular closePosition
        if (!parsedSignal.referencePrice) {
          // No reference price, try regular close (will fail during extended hours)
          const result = await alpacaMultiClient.closePosition(parsedSignal.symbol);
          
          if (!result.success) {
            if (signalLog) {
              try {
                await storage.updateSignalLog(signalLog.id, {
                  parsedSuccessfully: false,
                  errorMessage: result.error || "Failed to close position",
                });
              } catch (logError) {
                console.error("Error updating signal log:", logError);
              }
            }
            return res.status(400).json({
              success: false,
              error: result.error,
            });
          }

          // Record the close trade
          if (result.order) {
            try {
              const routeInfo = alpacaMultiClient.getRoutingInfo(parsedSignal.symbol);
              const trade = await storage.createTrade({
                symbol: result.order.symbol,
                side: result.order.side as "buy" | "sell",
                quantity: parseFloat(result.order.qty),
                orderType: result.order.type || "market",
                status: result.order.status || "new",
                alpacaOrderId: result.order.id,
                signalData: rawSignal,
                alpacaResponse: JSON.stringify(result.order),
                isClosePosition: true,
                positionAvgEntryPrice: closingPosition?.avgEntryPrice,
                positionQty: closingPosition?.qty,
                positionSide: closingPosition?.side,
                positionEntryDate: closingPosition?.entryDate,
                accountId: routeInfo?.accountId,
              });

              // Update signal log with success
              if (signalLog) {
                try {
                  await storage.updateSignalLog(signalLog.id, {
                    parsedSuccessfully: true,
                    tradeId: trade.id,
                  });
                } catch (logError) {
                  console.error("Error updating signal log:", logError);
                }
              }

              // Start Discord notification polling for close position (non-blocking)
              if (routeInfo && result.order) {
                const originalSignal = typeof rawSignal === 'string' ? JSON.parse(rawSignal) : rawSignal;
                pollOrderAndNotify(
                  alpacaMultiClient,
                  result.order.id,
                  routeInfo.accountId,
                  originalSignal
                );
              }

              return res.json({
                success: true,
                trade_id: trade.id,
                order_id: result.order.id,
                message: "Position closed successfully",
              });
            } catch (dbError) {
              console.error("Error saving close trade:", dbError);
              return res.status(500).json({
                success: false,
                error: "Order placed but failed to save trade record",
              });
            }
          }
          return res.json({ success: true, message: "Position closed" });
        }
        
        // Has referencePrice - fall through to placeOrder which handles extended hours
        console.log(`Closing position via limit order: ${positionQty} shares of ${parsedSignal.symbol} at ${parsedSignal.referencePrice}`);
      }

      // Place regular order
      const result = await alpacaMultiClient.placeOrder(parsedSignal);

      if (!result.success || !result.order) {
        // Update signal log with error
        if (signalLog) {
          try {
            await storage.updateSignalLog(signalLog.id, {
              parsedSuccessfully: false,
              errorMessage: result.error || "Order placement failed",
            });
          } catch (logError) {
            console.error("Error updating signal log:", logError);
          }
        }

        // Record failed trade
        try {
          await storage.createTrade({
            symbol: parsedSignal.symbol,
            side: parsedSignal.side,
            quantity: parsedSignal.quantity === "all" ? 0 : parsedSignal.quantity,
            price: parsedSignal.price,
            orderType: parsedSignal.orderType || "market",
            status: "rejected",
            signalData: rawSignal,
            errorMessage: result.error,
            stopLossPrice: parsedSignal.stopLoss,
            takeProfitPrice: parsedSignal.takeProfit,
            extendedHours: parsedSignal.extendedHours,
            timeInForce: parsedSignal.timeInForce,
          });
        } catch (dbError) {
          console.error("Error saving failed trade:", dbError);
        }

        return res.status(400).json({
          success: false,
          error: result.error,
        });
      }

      // Record successful trade
      try {
        const trade = await storage.createTrade({
          symbol: result.order.symbol,
          side: result.order.side as "buy" | "sell",
          quantity: parseFloat(result.order.qty),
          price: result.order.limitPrice ? parseFloat(result.order.limitPrice) : undefined,
          orderType: result.order.type || "market",
          status: result.order.status || "new",
          alpacaOrderId: result.order.id,
          signalData: rawSignal,
          alpacaResponse: JSON.stringify(result.order),
          stopLossPrice: parsedSignal.stopLoss,
          takeProfitPrice: parsedSignal.takeProfit,
          extendedHours: parsedSignal.extendedHours,
          timeInForce: parsedSignal.timeInForce,
          isClosePosition: parsedSignal.isCloseSignal || parsedSignal.closeAll || false,
          positionAvgEntryPrice: closingPosition?.avgEntryPrice,
          positionQty: closingPosition?.qty,
          positionSide: closingPosition?.side,
          positionEntryDate: closingPosition?.entryDate,
          accountId: routingInfo?.accountId,
        });

        // Update signal log with success
        if (signalLog) {
          try {
            await storage.updateSignalLog(signalLog.id, {
              parsedSuccessfully: true,
              tradeId: trade.id,
            });
          } catch (logError) {
            console.error("Error updating signal log:", logError);
          }
        }

        console.log("Trade created successfully:", trade.id);

        // Start Discord notification polling (non-blocking)
        if (routingInfo && result.order) {
          const originalSignal = typeof rawSignal === 'string' ? JSON.parse(rawSignal) : rawSignal;
          pollOrderAndNotify(
            alpacaMultiClient,
            result.order.id,
            routingInfo.accountId,
            originalSignal
          );
        }

        res.json({
          success: true,
          trade_id: trade.id,
          order_id: result.order.id,
          message: "Order placed successfully",
        });
      } catch (dbError) {
        console.error("Error saving successful trade:", dbError);
        return res.status(500).json({
          success: false,
          error: "Order placed but failed to save trade record",
        });
      }

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      console.error("Error processing webhook:", errorMessage);

      // Update signal log with error
      if (signalLog) {
        try {
          await storage.updateSignalLog(signalLog.id, {
            parsedSuccessfully: false,
            errorMessage,
          });
        } catch (dbError) {
          console.error("Error updating signal log:", dbError);
        }
      }

      res.status(400).json({
        success: false,
        error: errorMessage,
      });
    }
  });

  // ==================== Scanner API Endpoints ====================
  
  const scanner = await import("./scanner");

  // Get all strategies
  app.get("/api/scanner/strategies", (_req: Request, res: Response) => {
    const strategies = scanner.getAllStrategies();
    res.json(strategies);
  });

  // Get watchlist
  app.get("/api/scanner/watchlist", async (_req: Request, res: Response) => {
    try {
      const symbols = await scanner.getActiveWatchlist();
      res.json({ symbols, count: symbols.length });
    } catch (error) {
      console.error("Error fetching watchlist:", error);
      res.status(500).json({ error: "Failed to fetch watchlist" });
    }
  });

  // Add symbol to watchlist
  app.post("/api/scanner/watchlist", async (req: Request, res: Response) => {
    try {
      const { symbol, name, sector } = req.body;
      if (!symbol) {
        return res.status(400).json({ error: "Symbol is required" });
      }
      await scanner.addToWatchlist(symbol, name, sector);
      res.json({ success: true, symbol: symbol.toUpperCase() });
    } catch (error) {
      console.error("Error adding to watchlist:", error);
      res.status(500).json({ error: "Failed to add to watchlist" });
    }
  });

  // Import multiple symbols to watchlist
  app.post("/api/scanner/watchlist/import", async (req: Request, res: Response) => {
    try {
      const { symbols } = req.body;
      if (!Array.isArray(symbols)) {
        return res.status(400).json({ error: "Symbols array is required" });
      }
      const count = await scanner.importWatchlist(symbols);
      res.json({ success: true, imported: count });
    } catch (error) {
      console.error("Error importing watchlist:", error);
      res.status(500).json({ error: "Failed to import watchlist" });
    }
  });

  // Remove symbol from watchlist
  app.delete("/api/scanner/watchlist/:symbol", async (req: Request, res: Response) => {
    try {
      const { symbol } = req.params;
      await scanner.removeFromWatchlist(symbol);
      res.json({ success: true });
    } catch (error) {
      console.error("Error removing from watchlist:", error);
      res.status(500).json({ error: "Failed to remove from watchlist" });
    }
  });

  // Scan a single symbol
  app.post("/api/scanner/scan/symbol", async (req: Request, res: Response) => {
    try {
      const { symbol, timeframe = "15Min", fullRefresh = false } = req.body;
      if (!symbol) {
        return res.status(400).json({ error: "Symbol is required" });
      }
      const result = await scanner.scanSymbol(symbol, timeframe, fullRefresh);
      if (!result) {
        return res.status(400).json({ error: "Failed to scan symbol or insufficient data" });
      }
      res.json(result);
    } catch (error) {
      console.error("Error scanning symbol:", error);
      res.status(500).json({ error: "Failed to scan symbol" });
    }
  });

  // Run full scan on all watchlist symbols
  app.post("/api/scanner/scan/full", async (req: Request, res: Response) => {
    try {
      const { timeframes = ["5Min", "15Min", "1Hour", "4Hour"], fullRefresh = false } = req.body;
      const results = await scanner.runFullScan(timeframes, fullRefresh);
      res.json(results);
    } catch (error) {
      console.error("Error running full scan:", error);
      res.status(500).json({ error: "Failed to run full scan" });
    }
  });

  // Get latest scan results
  app.get("/api/scanner/results", async (req: Request, res: Response) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const timeframe = req.query.timeframe as string | undefined;
      const strategyId = req.query.strategyId as string | undefined;
      const results = await scanner.getLatestScanResults(limit, timeframe, strategyId);
      res.json(results);
    } catch (error) {
      console.error("Error fetching scan results:", error);
      res.status(500).json({ error: "Failed to fetch scan results" });
    }
  });

  // Get results grouped by strategy
  app.get("/api/scanner/results/by-strategy", async (req: Request, res: Response) => {
    try {
      const timeframe = req.query.timeframe as string | undefined;
      const grouped = await scanner.getResultsGroupedByStrategy(timeframe);
      res.json(grouped);
    } catch (error) {
      console.error("Error fetching grouped results:", error);
      res.status(500).json({ error: "Failed to fetch grouped results" });
    }
  });

  // Cleanup old data
  app.post("/api/scanner/cleanup", async (req: Request, res: Response) => {
    try {
      const { daysToKeep = 7 } = req.body;
      await scanner.cleanupOldData(daysToKeep);
      res.json({ success: true, message: `Cleaned up data older than ${daysToKeep} days` });
    } catch (error) {
      console.error("Error cleaning up data:", error);
      res.status(500).json({ error: "Failed to cleanup data" });
    }
  });

  // ==================== Custom Strategy API Endpoints ====================

  // Get all custom strategies
  app.get("/api/scanner/custom-strategies", async (_req: Request, res: Response) => {
    try {
      const strategies = await scanner.getAllCustomStrategies();
      res.json(strategies);
    } catch (error) {
      console.error("Error fetching custom strategies:", error);
      res.status(500).json({ error: "Failed to fetch custom strategies" });
    }
  });

  // Get single custom strategy
  app.get("/api/scanner/custom-strategies/:id", async (req: Request, res: Response) => {
    try {
      const strategy = await scanner.getCustomStrategy(req.params.id);
      if (!strategy) {
        return res.status(404).json({ error: "Strategy not found" });
      }
      res.json(strategy);
    } catch (error) {
      console.error("Error fetching custom strategy:", error);
      res.status(500).json({ error: "Failed to fetch custom strategy" });
    }
  });

  // Create custom strategy
  app.post("/api/scanner/custom-strategies", async (req: Request, res: Response) => {
    try {
      const { name, description, config } = req.body;
      if (!name || !config) {
        return res.status(400).json({ error: "Name and config are required" });
      }

      const validation = scanner.validateCustomStrategyConfig(config);
      if (!validation.valid) {
        return res.status(400).json({ error: "Invalid config", details: validation.errors });
      }

      const strategy = await scanner.createCustomStrategy(name, description || "", config);
      res.json(strategy);
    } catch (error) {
      console.error("Error creating custom strategy:", error);
      res.status(500).json({ error: "Failed to create custom strategy" });
    }
  });

  // Update custom strategy
  app.patch("/api/scanner/custom-strategies/:id", async (req: Request, res: Response) => {
    try {
      const { name, description, config, isActive } = req.body;

      if (config) {
        const validation = scanner.validateCustomStrategyConfig(config);
        if (!validation.valid) {
          return res.status(400).json({ error: "Invalid config", details: validation.errors });
        }
      }

      const strategy = await scanner.updateCustomStrategy(req.params.id, {
        name,
        description,
        config,
        isActive,
      });

      if (!strategy) {
        return res.status(404).json({ error: "Strategy not found" });
      }

      res.json(strategy);
    } catch (error) {
      console.error("Error updating custom strategy:", error);
      res.status(500).json({ error: "Failed to update custom strategy" });
    }
  });

  // Delete custom strategy
  app.delete("/api/scanner/custom-strategies/:id", async (req: Request, res: Response) => {
    try {
      const success = await scanner.deleteCustomStrategy(req.params.id);
      if (!success) {
        return res.status(404).json({ error: "Strategy not found" });
      }
      res.json({ success: true });
    } catch (error) {
      console.error("Error deleting custom strategy:", error);
      res.status(500).json({ error: "Failed to delete custom strategy" });
    }
  });

  // Run custom strategies on all symbols
  app.post("/api/scanner/custom-strategies/run", async (_req: Request, res: Response) => {
    try {
      const results = await scanner.runAllActiveStrategies();
      res.json(results);
    } catch (error) {
      console.error("Error running custom strategies:", error);
      res.status(500).json({ error: "Failed to run custom strategies" });
    }
  });

  // Run specific custom strategy
  app.post("/api/scanner/custom-strategies/:id/run", async (req: Request, res: Response) => {
    try {
      const results = await scanner.runStrategyOnAllSymbols(req.params.id);
      res.json(results);
    } catch (error) {
      console.error("Error running custom strategy:", error);
      res.status(500).json({ error: "Failed to run custom strategy" });
    }
  });

  // Get available fields for strategy configuration
  app.get("/api/scanner/config/fields", (_req: Request, res: Response) => {
    res.json({
      fields: scanner.getAvailableFields(),
      timeframes: scanner.getAvailableTimeframes(),
    });
  });

  // Get indicator results for a symbol
  app.get("/api/scanner/indicators/:symbol", async (req: Request, res: Response) => {
    try {
      const results = await scanner.getAllIndicatorResultsForSymbol(req.params.symbol);
      res.json(results);
    } catch (error) {
      console.error("Error fetching indicator results:", error);
      res.status(500).json({ error: "Failed to fetch indicator results" });
    }
  });

  // Get all indicator results
  app.get("/api/scanner/indicators", async (_req: Request, res: Response) => {
    try {
      const results = await scanner.getAllIndicatorResults();
      res.json(results);
    } catch (error) {
      console.error("Error fetching indicator results:", error);
      res.status(500).json({ error: "Failed to fetch indicator results" });
    }
  });

  // ==================== Scheduler API ====================
  
  // Get scheduler status
  app.get("/api/scheduler/status", (_req: Request, res: Response) => {
    res.json(scheduler.getSchedulerStatus());
  });

  // Start scheduler
  app.post("/api/scheduler/start", (_req: Request, res: Response) => {
    try {
      scheduler.startScheduler();
      res.json({ success: true, status: scheduler.getSchedulerStatus() });
    } catch (error) {
      console.error("Error starting scheduler:", error);
      res.status(500).json({ error: "Failed to start scheduler" });
    }
  });

  // Stop scheduler
  app.post("/api/scheduler/stop", (_req: Request, res: Response) => {
    try {
      scheduler.stopScheduler();
      res.json({ success: true, status: scheduler.getSchedulerStatus() });
    } catch (error) {
      console.error("Error stopping scheduler:", error);
      res.status(500).json({ error: "Failed to stop scheduler" });
    }
  });

  // Trigger manual scan
  app.post("/api/scheduler/scan", async (req: Request, res: Response) => {
    try {
      const { timeframe } = req.body;
      const results = await scheduler.triggerManualScan(timeframe);
      res.json({ success: true, results });
    } catch (error) {
      console.error("Error running manual scan:", error);
      res.status(500).json({ error: "Failed to run scan" });
    }
  });

  // ===================== Signal Entries API =====================

  // Get active signal entries (current entry signals)
  app.get("/api/signals/active", async (_req: Request, res: Response) => {
    try {
      const signals = await getActiveSignalEntries();
      res.json(signals);
    } catch (error) {
      console.error("Error getting active signals:", error);
      res.status(500).json({ error: "Failed to get active signals" });
    }
  });

  // Get recent signal entries (includes exited)
  app.get("/api/signals/recent", async (req: Request, res: Response) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const signals = await getRecentSignalEntries(limit);
      res.json(signals);
    } catch (error) {
      console.error("Error getting recent signals:", error);
      res.status(500).json({ error: "Failed to get recent signals" });
    }
  });

  // Get signal entries by symbol
  app.get("/api/signals/symbol/:symbol", async (req: Request, res: Response) => {
    try {
      const { symbol } = req.params;
      const signals = await getSignalEntriesBySymbol(symbol.toUpperCase());
      res.json(signals);
    } catch (error) {
      console.error("Error getting signals for symbol:", error);
      res.status(500).json({ error: "Failed to get signals" });
    }
  });

  // Get latest new entries from scheduler
  app.get("/api/signals/latest-entries", (_req: Request, res: Response) => {
    try {
      const entries = scheduler.getLatestNewEntries();
      res.json(entries);
    } catch (error) {
      console.error("Error getting latest entries:", error);
      res.status(500).json({ error: "Failed to get latest entries" });
    }
  });

  // Get auxiliary matches from other timeframes (for decision support)
  app.get("/api/signals/auxiliary-matches", (_req: Request, res: Response) => {
    try {
      const auxiliaryMatches = scheduler.getLatestAuxiliaryMatches();
      res.json(auxiliaryMatches);
    } catch (error) {
      console.error("Error getting auxiliary matches:", error);
      res.status(500).json({ error: "Failed to get auxiliary matches" });
    }
  });

  // Export watchlist and strategies for sync to production
  app.get("/api/sync/export", async (_req: Request, res: Response) => {
    try {
      const watchlist = await scanner.getActiveWatchlist();
      const strategies = await scanner.getAllCustomStrategies();
      res.json({
        exportedAt: new Date().toISOString(),
        watchlist: watchlist,
        strategies: strategies.map((s: { name: string; conditionsJson: string; isActive: boolean }) => ({
          name: s.name,
          conditionsJson: s.conditionsJson,
          isActive: s.isActive,
        })),
      });
    } catch (error) {
      console.error("Error exporting data:", error);
      res.status(500).json({ error: "Failed to export data" });
    }
  });

  // Import watchlist and strategies from development
  app.post("/api/sync/import", async (req: Request, res: Response) => {
    try {
      const { watchlist, strategies } = req.body;
      
      let watchlistImported = 0;
      let strategiesImported = 0;
      
      // Import watchlist
      if (watchlist && Array.isArray(watchlist)) {
        for (const symbol of watchlist) {
          try {
            await scanner.addToWatchlist(symbol);
            watchlistImported++;
          } catch (e) {
            // Symbol may already exist, continue
          }
        }
      }
      
      // Import strategies (upsert by name)
      if (strategies && Array.isArray(strategies)) {
        const existingStrategies = await scanner.getAllCustomStrategies();
        const existingNames = new Set(existingStrategies.map((s: { name: string }) => s.name));
        
        for (const strategy of strategies) {
          if (!existingNames.has(strategy.name)) {
            const config = JSON.parse(strategy.conditionsJson);
            await scanner.createCustomStrategy(strategy.name, "", config);
            strategiesImported++;
          }
        }
      }
      
      res.json({
        success: true,
        imported: {
          watchlist: watchlistImported,
          strategies: strategiesImported,
        },
      });
    } catch (error) {
      console.error("Error importing data:", error);
      res.status(500).json({ error: "Failed to import data" });
    }
  });

  return httpServer;
}
