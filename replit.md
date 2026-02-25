# Automated Trading System

## Overview
This project is an automated trading system that links TradingView webhook signals with Tiger Securities for algorithmic trade execution. It processes trade signals, executes orders via the Tiger Securities API, and offers a web-based dashboard for monitoring and configuration. The system supports real and paper trading, featuring an intelligent multi-phase trailing stop system for profit optimization and risk management. It also includes an independent Alpaca paper trading module for parallel testing and diversified strategies. The core vision is to deliver a robust, real-time, and adaptive trading automation solution.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture
The system is a Flask web application using SQLAlchemy and PostgreSQL. It features a custom signal parser for TradingView webhooks, integrates with the Tiger Securities OpenAPI for order execution, and uses APScheduler for real-time trailing stop monitoring. Configuration is managed via environment variables with database fallback.

### Frontend Architecture
The frontend utilizes Jinja2 templates with a Bootstrap 5 dark theme. It includes a dashboard for real-time trading statistics, trade history, system status, API credential management, and trading parameters. A dedicated UI allows for position monitoring, manual controls, and event logging related to trailing stops.

### Technical Implementations
-   **Intelligent Trailing Stop System**: Implements a multi-phase trailing stop with an 8-tier ladder and dynamic trailing based on profit thresholds and trend strength.
-   **Trend Strength Evaluation**: A composite score (0-100) from ATR Convergence, Momentum Score, and Consecutive Highs/Lows informs dynamic trailing stop decisions.
-   **K-line Data Caching**: A `BarCache` stores up to 50 bars per symbol per timeframe. `kline_service.py` handles fetching, updates, and cleanup. `atr_cache_service.py` computes ATR from cached bars.
-   **Real-time Data**: Uses Tiger WebSocket push for order status, position changes, and quotes, with API polling as fallback. Includes a self-healing mechanism for re-subscription.
-   **Safety Features**: Position checks before closing, duplicate close order prevention, and Always-On Soft Stop protection.
-   **Always-On Soft Stop**: Unified stop breach detection using `effective_stop`. Verifies OCA stop order liveness, with software takeover if OCA is dead/missing. Includes OCA stop_order_id backfill: when OCA group exists but stop_order_id is null, queries broker open orders to find and backfill the actual STP order ID before declaring "no protection."
-   **Unified Order Monitoring (OrderTracker)**: Tracks all orders from creation to fill, linking them to closed positions for P&L calculation.
-   **Position Architecture**: Manages the full lifecycle of a trading position, including entry, adds, closure, and P&L calculation, integrated with real-time holdings.
-   **Exit Order Retry Logic**: Reactivates trailing stop for retries (max 5) on exit order failure, checking for already-filled orders. Handles partial fills by adjusting TS quantity.
-   **Active Pending Exit Architecture**: When trailing stop triggers and places exit order, TS stays `is_active=True` with `trigger_reason='pending_exit:{order_id}'`. Each cycle: if stop still breached, cancels stale exit order (price drift >0.3%) and re-places at current price. If price recovers above stop, cancels exit order and resumes normal trailing. TS only deactivated when EXIT_TRAILING fill confirmed. Handles partial fills on recovery. Applies to both Alpaca and Tiger.
-   **Post-Fill Position Closure Verification**: After EXIT_TRAILING order fills, delayed (30s-60min) broker position check confirms actual closure. If position still exists, reactivates TS with remaining broker quantity. Runs in both Tiger and Alpaca scheduler loops. Window extended from 10min to 60min to catch stuck partial exits from scaling scenarios.
-   **Partial Exit TS Protection**: When SL/TP/EXIT fills but position is only partially closed (e.g., bracket SL covers original entry qty but position was scaled), TS stays active with quantity updated to remaining shares. Only deactivates TS when position is fully closed. Applies to both Tiger and Alpaca.
-   **Symmetric Dual-Confirmation Architecture**: Entry and exit sides both require order + position dual confirmation. Entry: TS starts only when order record exists AND broker confirms holding. Exit: TS ends only when exit order fills AND broker confirms position gone. Reconciliation-driven TS creation uses `from_reconciliation=True` to bypass cooldowns since dual confirmation already validates state.
-   **Holdings-vs-Position Cross-Check**: Scheduler slow loop compares broker holdings against DB positions. Detects: unprotected holdings (missing TS), direction mismatches, and missing positions. Uses 5-minute grace period before creating external positions. Runs for both Tiger and Alpaca. **Does NOT auto-create trailing stops** — only logs findings. TS creation is reserved for order/fill-based paths (signals, reconciliation, filled-trade checks).
-   **Manual Deactivation Protection**: `was_manually_deactivated()` (Tiger) and `was_manually_deactivated_alpaca()` (Alpaca) check if a symbol's most recent TS was manually stopped ("手动停用"). All auto-creation paths (`holdings_cross_check`, `tiger_reconciliation`, `scheduler_orphan`, `_check_filled_without_protection`, `_check_open_positions_without_protection`) respect this flag. Only a new webhook signal can override a manual stop.
-   **Position Status Consistency**: `_close_matching_position()` ensures Position records are marked CLOSED upon `ClosedPosition` creation.
-   **Tiger Position Qty/Direction Mismatch Detection**: `detect_closed_positions_fallback` checks for discrepancies and triggers reconciliation.
-   **Startup Orphan Cleanup**: `cleanup_orphaned_open_positions()` fixes stuck OPEN positions on scheduler start.
-   **Tiger Reconciliation Architecture**: A two-layer design: `detect_closed_positions_fallback` for position-level ghost detection and `reconcile_tiger_orders` for order-level fill matching and updates.
-   **Security and Reliability**: Comprehensive error handling, logging, and database connection pooling.
-   **Dual Account Support**: Supports real and paper trading accounts with distinct webhook endpoints.
-   **TBUS Module**: Isolated module for TBUS (US Standard account) support, with specific handling for API limitations and market data via EODHD WebSocket. Routes positions to `tbus_client.py` and `tbus_protection_service.py`.
-   **Tiger Bracket Order Architecture (Paper)**: Paper account uses bracket-only, no OCA. All entries (including market orders converted to aggressive limit at 0.5% offset) use `limit_order_with_legs` for immediate sub-order IDs (SL=STP/DAY, TP=LMT/DAY). Sub-orders expire at EOD, soft stop takes over for cross-day protection. No daily OCA rebuild—eliminates cross-day order ID mismatch anomalies entirely. Real account continues with OCA-after-fill approach. Bracket leg creation validates SL/TP direction (SL must be below entry for long, above for short) and skips invalid legs instead of sending to broker.
-   **SL/TP Price Direction Validation**: `_validate_stop_loss_price()` and `_validate_take_profit_price()` in trailing_stop_engine verify SL/TP prices are on the correct side of entry price before TS creation. Invalid prices (e.g., SL above entry for long) are discarded with warnings, preventing immediate false stop triggers. Applied in Tiger (Replit+VPS), Alpaca, and bracket order creation.
-   **EXPIRED Order Handling**: `modify_stop_with_retry()` and `modify_stop_loss_price()` now handle EXPIRED order status when attempting cancel+recreate. Previously only checked FILLED/CANCELLED, causing death loops when broker expired invalid stop orders. `place_stop_limit_order()` pre-validates stop price vs latest market price before sending to broker.
-   **Tiger API Limitations and Workarounds**: Addresses extended hours trading and attached order limitations using bracket orders (RTH) + soft stop fallback (extended hours/cross-day).
-   **Shared Market Data Watchlist**: A unified `WatchlistSymbol` DB table manages Tiger WebSocket subscriptions, supporting manual symbols, auto-add from signals, and inactivity cleanup.
-   **EODHD Price Service** (`eodhd_price_service.py`): Shared module providing EODHD API as a secondary price source for both Tiger and Alpaca trailing stop engines. Supports real-time OHLCV endpoint and us-quote-delayed extended hours endpoint with ethPrice. Session-aware smart routing (extended endpoint for pre/post market). 30-second in-memory cache. Integrated as fallback in `_fetch_current_price()` (after Tiger API, before Alpaca), `batch_refresh_stale_prices()`, and `_batch_fetch_prices()`.

### Alpaca Paper Trading Module
An independent Alpaca paper trading system runs in parallel, using separate business logic, routes, templates, and database tables. It includes OCO protection, a multi-phase trailing stop, reconciliation via Alpaca Activities API, Discord notifications, and analytics.
-   **Alpaca Trade Stream (WebSocket)**: Real-time order update notifications with auto-reconnect and thread-safe event processing.
-   **Smart Order Type by Market Hours**: Uses market orders during regular hours and limit orders with auto price fetching during extended hours.
-   **Bracket Order with GTC**: Entry orders with SL/TP use bracket orders with GTC time_in_force.
-   **Bracket Leg Modification/Cancellation**: Trailing stop engine modifies or cancels bracket SL/TP legs via Alpaca API.
-   **Duplicate Protection Prevention**: Skips OCO creation for bracket orders with existing SL/TP legs.
-   **Position Scaling (加仓) Support**: Recalculates weighted average entry price and quantity, updating trailing stops and modifying existing bracket legs via `replace_order`.
-   **Price Rounding**: All prices are rounded to two decimal places.
-   **Periodic Activities Reconciliation**: Fetches Alpaca Activities API fill data and reconciles unmatched fills into DB position records.
-   **Ghost Reconciliation**: Closes DB OPEN positions that no longer exist at Alpaca, with a grace period for unreconciled fills.
-   **Anti-Phantom Protection**: Two-layer defense to prevent ghost→phantom→ghost loops by checking OrderTracker roles and recently closed positions.
-   **Exit Fill Side Validation**: `_handle_exit_fill` validates position side before applying exit legs. Exit sell → expects long position, exit buy → expects short position. Prevents cross-position matching when old exit fills arrive after direction reversal (COIN-style bug prevention).
-   **Quick Fill Fetch**: Lightweight fill fetch to ensure up-to-date fill data for ghost reconciliation.
-   **TS Ghost-Close Cooldown**: A 30-minute cooldown prevents immediate re-creation of trailing stops after ghost reconciliation.
-   **Reconciliation Architecture (Alpaca)**: Two-layer design with Activities Reconciliation for order-level fill matching and Ghost Reconciliation for position-level alignment.

### AI Trade Analysis Module
An AI-powered trade analysis system (`trade_analysis/`) that matches trading records across multiple data sources and provides intelligent insights.
-   **Three-Layer Matching**: Exact order ID matching → Fuzzy symbol+time+qty matching → Claude AI analysis for remaining unmatched records.
-   **Data Collection** (`collector.py`): Unified collection from SignalLog, Trade, OrderTracker, ClosedPosition tables + Tiger/Alpaca broker APIs.
-   **Rule-Based Matcher** (`matcher.py`): Groups trades by closed positions, matches entry/exit trackers, detects anomalies (price mismatches, missing records, quantity discrepancies).
-   **AI Analyzer** (`ai_analyzer.py`): Uses Claude (via Replit AI Integrations) for unmatched record analysis, signal quality assessment, and anomaly diagnosis. Includes retry with exponential backoff.
-   **Reporter** (`reporter.py`): Generates comprehensive reports, saves to DB (`AnalysisReport` model), sends Discord notifications with health scores and P&L summaries.
-   **Orchestrator** (`orchestrator.py`): Coordinates the full analysis pipeline: collect → match → AI analyze → report → save → notify.
-   **Daily Scheduler**: Runs automatically at 5:00 PM ET on trading days. Manual trigger available via `/trade-analysis` web UI or `/api/trade-analysis/run` API.
-   **Health Score**: 0-100 score based on anomaly count and unmatched records, color-coded in dashboard.

### VPS Dual-Codebase Architecture
The Replit and VPS environments use isolated codebases. Replit runs the standard version without TBUS/EODHD changes, while VPS uses dedicated files with a `_vps` suffix and the `tbus/` folder. Modifications for VPS must only target `*_vps.py` files and the `tbus/` directory, and require repackaging.

## External Dependencies

### Trading Platform Integration
-   **Tiger Securities OpenAPI**
-   **TradingView Webhooks**
-   **TradersPost Webhook**
-   **Alpaca API**
-   **EODHD API**

### Development Framework
-   **Flask**
-   **SQLAlchemy**
-   **APScheduler**
-   **Bootstrap 5**
-   **Font Awesome**

### Database Support
-   **PostgreSQL**

### JavaScript Libraries
-   **Bootstrap JavaScript**
-   **Chart.js**

### Other Integrations
-   **Discord**