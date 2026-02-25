# Alpaca Paper Trading Bot

An automated trading bot that receives TradingView webhook signals and executes trades through Alpaca's paper trading API.

## Overview

This application provides a web-based interface for managing automated paper trades on Alpaca. It receives trading signals via webhook (from TradingView or other sources) and executes them in the paper trading environment.

## Features

- **Dashboard**: Overview of portfolio value, buying power, positions, and recent trades
- **Orders**: View open orders and order history, cancel orders
- **Positions**: View current positions with P&L, close positions
- **Settings**: Configure trading parameters, view webhook URL
- **Webhook API**: Receive TradingView signals and execute trades automatically
- **Stock Scanner**: Automated scanning of stocks using custom indicators (TSI, QQE, Momentum)

## Architecture

### Frontend (React + TypeScript)
- `/client/src/pages/dashboard.tsx` - Main dashboard with account summary
- `/client/src/pages/orders.tsx` - Order management
- `/client/src/pages/positions.tsx` - Position management
- `/client/src/pages/settings.tsx` - Configuration
- `/client/src/pages/scanner.tsx` - Stock scanner with watchlist and strategy management

### Backend (Express + TypeScript)
- `/server/routes.ts` - API endpoints
- `/server/alpaca-client.ts` - Alpaca API integration
- `/server/signal-parser.ts` - TradingView signal parsing
- `/server/storage.ts` - PostgreSQL database storage for trades
- `/server/db.ts` - Drizzle ORM database connection
- `/server/scanner/` - Stock scanning module
  - `indicators.ts` - Custom indicators (HeikinAshiTSI, WeightedQQE, SincMomentum)
  - `strategies.ts` - Strategy condition combinations
  - `scanner-core.ts` - Data fetching, caching, and scanning logic

### Shared Types
- `/shared/schema.ts` - TypeScript types and Zod schemas

## API Endpoints

- `GET /api/account` - Get account information
- `GET /api/account/status` - Check connection status
- `POST /api/account/reset` - Reset paper account
- `GET /api/positions` - Get all positions
- `DELETE /api/positions/:symbol` - Close a position
- `GET /api/orders/open` - Get open orders
- `DELETE /api/orders/:orderId` - Cancel an order
- `GET /api/trades` - Get trade history
- `GET /api/trades/recent` - Get recent trades
- `GET /api/config` - Get trading configuration
- `PATCH /api/config` - Update configuration
- `POST /api/webhook` - Receive trading signals

## Webhook Signal Format

```json
{
  "symbol": "AAPL",
  "side": "buy",
  "quantity": 10,
  "order_type": "market",
  "time_in_force": "day",
  "extended_hours": false,
  "stop_loss": 145.00,
  "take_profit": 165.00
}
```

### Supported Fields

| Field | Required | Values |
|-------|----------|--------|
| symbol/ticker | Yes | Stock symbol (e.g., "AAPL") |
| side/action | Yes | "buy", "sell", "long", "short" |
| quantity/qty | Yes | Number or "all" |
| order_type/type | No | "market", "limit", "stop", "stop_limit" |
| price/limit_price | For limit orders | Price value |
| time_in_force | No | "day", "gtc", "ioc", "fok" |
| extended_hours | No | true/false |
| stop_loss | No | Stop loss price |
| take_profit | No | Take profit price |
| sentiment | No | "flat" to close position |

## Environment Variables

- `ALPACA_API_KEY` - Alpaca API Key ID
- `ALPACA_SECRET_KEY` - Alpaca Secret Key

## Running the Application

The application runs on port 5000 and uses Vite for hot reloading during development.

```bash
npm run dev
```

## Database

The application uses PostgreSQL with Drizzle ORM for data persistence.

### Tables
- `trades` - Trade execution history with Alpaca order IDs
- `trading_configs` - Key-value configuration storage
- `signal_logs` - Webhook signal audit trail
- `watchlist` - Stock symbols to scan
- `bar_cache` - Cached K-line data for indicators
- `scan_results` - Stocks matching strategy conditions
- `scanner_state` - Scanner state tracking (last scan time, progress)
- `indicator_results` - Persisted indicator values per symbol per timeframe (unique on symbol+timeframe)
- `custom_strategies` - User-defined cross-timeframe strategy configurations

## Stock Scanner

The stock scanner module enables automated scanning of stocks using custom indicators.

### Scanner API Endpoints
- `GET /api/scanner/strategies` - Get available preset strategies
- `GET /api/scanner/watchlist` - Get watchlist symbols
- `POST /api/scanner/watchlist` - Add symbol to watchlist
- `POST /api/scanner/watchlist/import` - Bulk import symbols
- `DELETE /api/scanner/watchlist/:symbol` - Remove symbol
- `POST /api/scanner/scan/symbol` - Scan single symbol
- `POST /api/scanner/scan/full` - Run full scan on all watchlist symbols
- `GET /api/scanner/results` - Get scan results (supports timeframe filtering)

### Custom Strategy API Endpoints
- `GET /api/scanner/custom-strategies` - Get all custom strategies
- `POST /api/scanner/custom-strategies` - Create new custom strategy
- `PATCH /api/scanner/custom-strategies/:id` - Update custom strategy
- `DELETE /api/scanner/custom-strategies/:id` - Delete custom strategy
- `POST /api/scanner/custom-strategies/run` - Run all custom strategies
- `POST /api/scanner/custom-strategies/:id/run` - Run single custom strategy
- `GET /api/scanner/config/fields` - Get available fields for conditions
- `GET /api/scanner/indicators/:symbol` - Get indicator values for a symbol

### Scheduler API Endpoints (Auto Monitor)
- `GET /api/scheduler/status` - Get scheduler status (running, lastRun, nextRun, matches)
- `POST /api/scheduler/start` - Start automatic monitoring scheduler
- `POST /api/scheduler/stop` - Stop automatic monitoring scheduler
- `POST /api/scheduler/scan` - Trigger manual scan (optional `timeframe` in body)

### Signal Entries API Endpoints
- `GET /api/signals/active` - Get active entry signals (first-time matches)
- `GET /api/signals/recent` - Get recent signal history (includes exited)
- `GET /api/signals/symbol/:symbol` - Get signal entries by symbol
- `GET /api/signals/latest-entries` - Get latest new entries from scheduler

### Custom Indicators
1. **Heikin Ashi TSI** - True Strength Index with HA candle smoothing
2. **Weighted QQE** - Modified QQE with dual timeframe analysis
3. **Sinc Momentum** - Multi-timeframe momentum using sinc function filtering

### Predefined Strategies
- 三指标共振多头 (Triple Indicator Resonance Bull)
- QQE突破 (QQE Breakout)
- 动量趋势 (Momentum Trend)
- TSI反转 (TSI Reversal)
- 回调入场 (Pullback Entry)

### Supported Timeframes
- 5Min, 15Min, 1Hour, 4Hour

## Recent Changes

- **Timeframe-Filtered Signal Detection with Auxiliary Context (Feb 2026)**
  - Signal state updates are now tied to specific scan timeframes
  - 5Min entry strategies only generate new entry signals during 5Min scans
  - 15Min entry strategies only generate new entry signals during 15Min scans
  - etc. for 1Hour, 4Hour strategies
  - This ensures signals are only generated when the corresponding K-line data has actually updated
  - Exit detection also respects entry timeframe filtering
  - Concurrency safety: Uses atomic upsert with CTE+FOR UPDATE to prevent race conditions
  - **Auxiliary Matches**: When scanning a specific timeframe (e.g., 15Min), the system also collects current matches from OTHER timeframes (5Min, 1Hour, 4Hour) as decision-support information, without updating their states
  - New API endpoint: `GET /api/signals/auxiliary-matches` returns current matches from other timeframes
  - Frontend displays "Other Timeframes Reference" card showing auxiliary matches alongside the main entry signals

- **Added Signal Entry Tracking System (Feb 2026)**
  - Tracks signal state changes to identify first-time entry opportunities vs continuation signals
  - strategy_signal_state table stores current match status per strategy/symbol (unique constraint on strategy_id, symbol)
  - signal_entries table records first-time matches with price, timestamp, and indicator snapshot
  - Frontend "Entry Signals" tab displays active and historical entry signals
  - Distinguishes between: NEW_ENTRY (first match), CONTINUING (ongoing match), EXIT (no longer matching)
- **Aligned TSI indicator with TradingView calculations (Feb 2026)**
  - EMA uses `ewm(alpha=2/(n+1), adjust=False, min_periods=length)` matching TradingView ta.ema
  - TSI price source uses OHLC4: `(open + high + low + close) / 4`
  - Double smoothing: `f_ma(f_ma(pc, slowLength), fastLength)`
  - Swing Trade mode parameters: slowLength=35, fastLength=21, signalLength=14
  - Data fetching: 180 days for 15Min timeframe (~2700 bars for EMA convergence)
  - Note: Absolute TSI values may differ from TradingView due to historical data length differences, but trend direction and signal states remain consistent
- **Added configurable cross-timeframe strategy system**
  - indicator_results table stores calculated values per symbol per timeframe
  - custom_strategies table stores user-defined strategy configurations with conditions
  - Strategy engine evaluates cross-timeframe conditions (e.g., "5Min bullish AND 15Min bullish")
  - Frontend Custom Strategies tab with condition builder UI
- Added automated stock scanner with custom indicators (TSI, QQE, Momentum)
- Added scanner frontend page with watchlist and strategy management
- Added scanner state tracking for scan progress
- Converted from Tiger Securities API to Alpaca Paper Trading API
- Added React frontend with shadcn/ui components
- Implemented webhook endpoint for TradingView signals
- Added account, positions, orders, and settings pages
- Migrated from in-memory storage to PostgreSQL database
- Added comprehensive error handling with try/catch blocks
- Signal logging now updates records instead of creating duplicates
- All trade records include required field defaults
