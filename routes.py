import os
import json
import logging
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, flash, redirect, url_for
from app import app, db
from models import Trade, TradingConfig, SignalLog, OrderStatus, OrderType, Side, CompletedTrade, ExitMethod, EntrySignalRecord, ClosedPosition
from tiger_client import TigerClient, TigerPaperClient
from signal_parser import SignalParser
from config import get_config, set_config
from discord_notifier import discord_notifier
from position_cost_manager import update_position_cost_on_fill, get_avg_cost_for_symbol, record_entry_cost_on_trade

logger = logging.getLogger(__name__)


@app.route('/')
def index():
    """Main dashboard page"""
    from models import TigerHolding
    from holdings_sync import get_sync_status
    
    recent_trades = Trade.query.order_by(Trade.created_at.desc()).limit(10).all()
    total_trades = Trade.query.count()
    successful_trades = Trade.query.filter_by(status=OrderStatus.FILLED).count()
    pending_trades = Trade.query.filter_by(status=OrderStatus.PENDING).count()
    
    account_type = request.args.get('account_type', 'real')
    sort_by = request.args.get('sort', 'symbol')
    holdings = TigerHolding.query.filter_by(
        account_type=account_type
    ).all()
    
    if sort_by == 'pnl_pct':
        holdings.sort(key=lambda h: h.unrealized_pnl_pct or 0, reverse=True)
    else:
        holdings.sort(key=lambda h: h.symbol)
    
    total_market_value = sum(h.market_value or 0 for h in holdings)
    total_unrealized_pnl = sum(h.unrealized_pnl or 0 for h in holdings)
    total_cost = sum((h.average_cost or 0) * abs(h.quantity or 0) for h in holdings)
    total_pnl_pct = (total_unrealized_pnl / total_cost * 100) if total_cost > 0 else 0
    sync_status = get_sync_status()
    
    return render_template('index.html', 
                         recent_trades=recent_trades,
                         total_trades=total_trades,
                         successful_trades=successful_trades,
                         pending_trades=pending_trades,
                         holdings=holdings,
                         account_type=account_type,
                         total_market_value=total_market_value,
                         total_unrealized_pnl=total_unrealized_pnl,
                         total_pnl_pct=total_pnl_pct,
                         holding_count=len(holdings),
                         sync_status=sync_status.get(account_type, {}),
                         sort_by=sort_by)

@app.route('/webhook', methods=['POST'])
def webhook():
    """Receive TradingView webhook signals"""
    try:
        # Get client IP
        client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
        
        # Get raw data
        raw_data = request.get_data(as_text=True)
        logger.info(f"Received webhook from {client_ip}: {raw_data}")
        
        # Log the signal
        signal_log = SignalLog()
        signal_log.raw_signal = raw_data
        signal_log.ip_address = client_ip
        signal_log.endpoint = '/webhook'
        signal_log.account_type = 'real'
        
        try:
            # Parse JSON
            signal_data = request.get_json()
            if not signal_data:
                raise ValueError("No JSON data received")
            
            # Parse the signal
            parser = SignalParser()
            parsed_signal = parser.parse(signal_data)
            
            # Validate required fields
            if not all(key in parsed_signal for key in ['symbol', 'side', 'quantity']):
                raise ValueError("Missing required fields: symbol, side, quantity")
            
            try:
                from watchlist_service import on_signal_received
                on_signal_received(parsed_signal['symbol'], source_broker='tiger')
            except Exception as wl_err:
                logger.debug(f"Watchlist update failed: {wl_err}")
            
            # Initialize trade variable
            trade = None
            
            # Check if this is a close/flat signal
            if parsed_signal.get('is_close_signal', False):
                logger.info(f"Processing close signal for {parsed_signal['symbol']}, side={parsed_signal.get('side')}")
                
                # Execute close position
                tiger_client = TigerClient()
                
                # Get current position average cost BEFORE closing
                pre_close_avg_cost = None
                try:
                    pos_result = tiger_client.get_positions(symbol=parsed_signal['symbol'])
                    if pos_result.get('success') and pos_result.get('positions'):
                        pre_close_avg_cost = pos_result['positions'][0].get('average_cost', 0)
                        logger.info(f"Pre-close avg cost for {parsed_signal['symbol']}: ${pre_close_avg_cost:.2f}")
                except Exception as e:
                    logger.error(f"Failed to get pre-close avg cost: {str(e)}")
                
                trading_session = parsed_signal.get('trading_session', 'regular')
                reference_price = parsed_signal.get('reference_price')  # Get reference price for extended hours
                signal_side = parsed_signal.get('side')
                signal_qty = parsed_signal.get('quantity') if not parsed_signal.get('close_all') else None
                result = tiger_client.close_position_with_sandbox_fallback(
                    parsed_signal['symbol'], 
                    trading_session,
                    reference_price=reference_price,
                    signal_side=signal_side,
                    signal_quantity=signal_qty
                )
                
                # Handle no_action case (signal direction doesn't match position direction)
                if result.get('no_action'):
                    logger.info(f"{result.get('message')}")
                    signal_log.parsed_successfully = True
                    signal_log.error_message = result.get('message')
                    db.session.add(signal_log)
                    db.session.commit()
                    return jsonify({
                        'success': True,
                        'no_action': True,
                        'message': result.get('message')
                    })
                
                # Create trade record for close position
                if result['success']:
                    trade = Trade()
                    trade.symbol = parsed_signal['symbol']
                    trade.side = Side(result['action'])  # Use the determined action from close_position
                    trade.quantity = result['quantity']
                    
                    # Set order type and price based on actual order placed
                    order_type_str = result.get('order_type', 'market')
                    if order_type_str == 'limit':
                        trade.order_type = OrderType.LIMIT
                        trade.price = result.get('order_price')  # Set limit price if available
                        logger.info(f"Close position using LIMIT order at ${trade.price:.2f}")
                    else:
                        trade.order_type = OrderType.MARKET
                        trade.price = None  # Market order price is determined at execution
                        logger.info("Close position using MARKET order")
                    
                    trade.signal_data = raw_data
                    trade.tiger_order_id = result['order_id']
                    trade.status = OrderStatus.PENDING
                    trade.trading_session = result.get('trading_session', trading_session)
                    trade.outside_rth = result.get('outside_rth', parsed_signal.get('outside_rth', trading_session != 'regular'))
                    trade.is_close_position = True  # Mark as close position
                    trade.account_type = 'real'  # Mark as real account trade
                    
                    orig_side_for_lookup = 'long' if result.get('action') == 'sell' else 'short'
                    try:
                        from position_service import find_open_position
                        open_pos = find_open_position(parsed_signal['symbol'], 'real', orig_side_for_lookup)
                        if open_pos and open_pos.entry_legs:
                            trade.parent_entry_order_id = open_pos.entry_legs[0].tiger_order_id
                            logger.info(f"🔗 Close order linked to Position #{open_pos.id}")
                    except Exception as link_err:
                        logger.debug(f"Entry order lookup failed: {link_err}")
                    
                    # Store pre-close average cost
                    if pre_close_avg_cost:
                        trade.entry_avg_cost = pre_close_avg_cost
                        logger.info(f"Stored pre-close avg cost ${pre_close_avg_cost:.2f} for {trade.symbol}")
                    
                    # Store Tiger API response for close position
                    trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                    
                    db.session.add(trade)
                    db.session.flush()
                    
                    signal_log.parsed_successfully = True
                    signal_log.trade_id = trade.id
                    logger.info(f"Close position order placed: {result['order_id']}")
                    
                    # Get real-time order status from Tiger API for close position
                    try:
                        import time
                        time.sleep(1)  # Brief wait for order processing
                        
                        status_result = tiger_client.get_order_status(trade.tiger_order_id)
                        if status_result['success']:
                            # Update trade with real Tiger API data - map Tiger status to our enum
                            tiger_status = status_result['status']
                            if tiger_status == 'filled':
                                trade.status = OrderStatus.FILLED
                                # entry_avg_cost already set from pre_close_avg_cost
                                    
                            elif tiger_status == 'pending':
                                trade.status = OrderStatus.PENDING
                            elif tiger_status == 'cancelled':
                                trade.status = OrderStatus.CANCELLED
                            elif tiger_status == 'rejected':
                                trade.status = OrderStatus.REJECTED
                            else:
                                trade.status = OrderStatus.PENDING  # Default fallback
                            if status_result['filled_price'] > 0:
                                trade.price = status_result['filled_price']
                                trade.filled_price = status_result['filled_price']
                            trade.filled_quantity = status_result.get('filled_quantity', 0)
                            
                            # Send Discord notification with real Tiger data for close position
                            discord_status = status_result['status']
                            discord_notifier.send_order_notification(trade, discord_status, is_close=True)
                            
                            logger.info(f"Close position Discord notification sent with real Tiger data: {discord_status}, price: {status_result.get('filled_price', 0)}")
                            
                            if tiger_status == 'filled':
                                try:
                                    from order_tracker_service import handle_fill_event
                                    fill_result, fill_status = handle_fill_event(
                                        tiger_order_id=str(trade.tiger_order_id),
                                        filled_quantity=status_result.get('filled_quantity', trade.quantity),
                                        avg_fill_price=status_result.get('filled_price', 0),
                                        realized_pnl=status_result.get('realized_pnl'),
                                        commission=status_result.get('commission'),
                                        fill_time=datetime.utcnow(),
                                        source='polling_close',
                                    )
                                    logger.info(f"📊 Close order fill processed via OrderTracker: {fill_status}")
                                except Exception as cp_err:
                                    logger.error(f"❌ Failed to process close fill: {str(cp_err)}")
                                
                                try:
                                    from trailing_stop_engine import deactivate_trailing_stop_for_symbol
                                    deactivate_trailing_stop_for_symbol(parsed_signal['symbol'], 'real', 'Exit signal filled')
                                except Exception as ts_err:
                                    logger.error(f"Failed to deactivate trailing stop on close: {ts_err}")
                        else:
                            # Fallback to pending status if can't get real-time data
                            discord_notifier.send_order_notification(trade, 'pending', is_close=True)
                    except Exception as e:
                        logger.error(f"Failed to get real-time status or send Discord notification for close position: {str(e)}")
                        # Fallback notification
                        try:
                            discord_notifier.send_order_notification(trade, 'pending', is_close=True)
                        except Exception as e2:
                            logger.error(f"Fallback Discord notification for close position also failed: {str(e2)}")
                else:
                    logger.error(f"Failed to close position: {result['error']}")
                    signal_log.error_message = result['error']
                    result['success'] = False
                    
                    # Send Discord notification for failed close position
                    if 'trade' in locals():
                        try:
                            discord_notifier.send_order_notification(trade, 'rejected', is_close=True)
                        except Exception as e:
                            logger.error(f"Failed to send Discord notification for failed close position: {str(e)}")
                
            else:
                # Regular trade signal processing
                
                # Check risk limits
                max_trade_amount = float(get_config('MAX_TRADE_AMOUNT', '1000000'))
                trade_amount = parsed_signal['quantity'] * parsed_signal.get('price', 100)  # Rough estimate for market orders
                
                if trade_amount > max_trade_amount:
                    raise ValueError(f"Trade amount ${trade_amount:.2f} exceeds maximum allowed ${max_trade_amount:.2f}")
                
                # Create trade record
                trade = Trade()
                trade.symbol = parsed_signal['symbol']
                trade.side = Side(parsed_signal['side'].lower())
                trade.quantity = parsed_signal['quantity']
                trade.price = parsed_signal.get('price')
                trade.order_type = OrderType(parsed_signal.get('order_type', 'market').lower())
                trade.signal_data = raw_data
                
                # Add stop loss and take profit
                trade.stop_loss_price = parsed_signal.get('stop_loss')
                trade.take_profit_price = parsed_signal.get('take_profit')
                
                # Add trading session settings
                trade.trading_session = parsed_signal.get('trading_session', 'regular')
                trade.outside_rth = parsed_signal.get('outside_rth', False)
                
                # Add reference price for market order conversion
                trade.reference_price = parsed_signal.get('reference_price')
                trade.account_type = 'real'  # Mark as real account trade
                
                db.session.add(trade)
                db.session.flush()  # Get the ID
                
                signal_log.parsed_successfully = True
                signal_log.trade_id = trade.id
                
                # Execute the trade
                tiger_client = TigerClient()
                result = tiger_client.place_order(trade)
            
            if result['success'] and trade is not None:
                # For regular trades, trade is already created
                # For close signals, trade is created only if close was successful
                if not hasattr(trade, 'tiger_order_id') or trade.tiger_order_id is None:
                    trade.tiger_order_id = result['order_id']
                if not hasattr(trade, 'status') or trade.status != OrderStatus.PENDING:
                    trade.status = OrderStatus.PENDING
                
                # Store Tiger API response
                trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                
                # Save attached order IDs if present
                if 'stop_loss_order_id' in result:
                    trade.stop_loss_order_id = result['stop_loss_order_id']
                    logger.info(f"Stop loss order created: {result['stop_loss_order_id']}")
                
                if 'take_profit_order_id' in result:
                    trade.take_profit_order_id = result['take_profit_order_id']
                    logger.info(f"Take profit order created: {result['take_profit_order_id']}")
                
                # Register orders to OrderTracker for unified monitoring
                from order_tracker_service import register_order
                is_close_signal = parsed_signal.get('is_close_signal', False)
                order_role = 'exit_signal' if is_close_signal else 'entry'
                register_order(
                    tiger_order_id=result['order_id'],
                    symbol=trade.symbol,
                    account_type='real',
                    role=order_role,
                    side=trade.side.value.upper(),
                    quantity=trade.quantity,
                    trade_id=trade.id
                )
                if 'stop_loss_order_id' in result:
                    register_order(
                        tiger_order_id=result['stop_loss_order_id'],
                        symbol=trade.symbol,
                        account_type='real',
                        role='stop_loss',
                        parent_order_id=result['order_id'],
                        stop_price=trade.stop_loss_price,
                        trade_id=trade.id
                    )
                if 'take_profit_order_id' in result:
                    register_order(
                        tiger_order_id=result['take_profit_order_id'],
                        symbol=trade.symbol,
                        account_type='real',
                        role='take_profit',
                        parent_order_id=result['order_id'],
                        limit_price=trade.take_profit_price,
                        trade_id=trade.id
                    )
                
                # Check if this order needs auto-protection (position increase case)
                needs_auto_protection = result.get('needs_auto_protection', False)
                protection_info = result.get('protection_info', {})
                auto_protection_symbol = result.get('symbol')
                
                if needs_auto_protection:
                    logger.info(f"Order requires auto-protection after execution: {auto_protection_symbol}")
                    # Store protection info for later processing
                    trade.needs_auto_protection = True
                    if protection_info:
                        trade.protection_info = json.dumps(protection_info)
                
                logger.info(f"Order placed successfully: {result['order_id']}")
                
                # Get real-time order status from Tiger API before sending Discord notification
                try:
                    import time
                    time.sleep(1)  # Brief wait for order processing
                    
                    status_result = tiger_client.get_order_status(trade.tiger_order_id)
                    if status_result['success']:
                        # Update trade with real Tiger API data - map Tiger status to our enum
                        tiger_status = status_result['status']
                        if tiger_status == 'filled':
                            trade.status = OrderStatus.FILLED
                            # Commit trade immediately so foreign keys work
                            db.session.commit()
                            
                            auto_protection_sl_order_id = None
                            auto_protection_tp_order_id = None
                            is_bracket_order = result.get('bracket_order', False)
                            
                            if is_bracket_order:
                                auto_protection_sl_order_id = result.get('stop_loss_order_id')
                                auto_protection_tp_order_id = result.get('take_profit_order_id')
                                if auto_protection_sl_order_id:
                                    trade.stop_loss_order_id = auto_protection_sl_order_id
                                if auto_protection_tp_order_id:
                                    trade.take_profit_order_id = auto_protection_tp_order_id
                                logger.info(f"📎 Bracket sub-order IDs: SL={auto_protection_sl_order_id}, TP={auto_protection_tp_order_id}")
                            
                            # Handle auto-protection for position increases
                            if hasattr(trade, 'needs_auto_protection') and trade.needs_auto_protection:
                                logger.info(f"Order {trade.tiger_order_id} filled, applying auto-protection for position increase")
                                try:
                                    protection_info_str = trade.protection_info if hasattr(trade, 'protection_info') else None
                                    trade.needs_auto_protection = False
                                    trade.protection_info = None
                                    db.session.commit()
                                    protection_info = json.loads(protection_info_str) if protection_info_str else {}
                                    if protection_info.get('stop_loss_price') or protection_info.get('take_profit_price'):
                                        # Check if trailing stop has switched to dynamic mode
                                        from models import TrailingStopPosition
                                        existing_ts = TrailingStopPosition.query.filter_by(
                                            symbol=trade.symbol, account_type='real', is_active=True
                                        ).first()
                                        has_switched = existing_ts.has_switched_to_trailing if existing_ts else False
                                        
                                        # If switched to dynamic trailing, don't create take profit order
                                        take_profit_for_oca = None if has_switched else protection_info.get('take_profit_price')
                                        if has_switched:
                                            logger.info(f"🔄 {trade.symbol} 已切换到动态trailing，加仓时只创建止损订单")
                                        
                                        logger.info(f"Applying OCA protection for {trade.symbol} via create_oca_protection (dedup-safe)")
                                        position_result = tiger_client.get_positions(symbol=trade.symbol)
                                        if position_result['success'] and position_result['positions']:
                                            current_quantity = position_result['positions'][0]['quantity']
                                            protection_quantity = abs(current_quantity)
                                            
                                            ts_side = 'long' if trade.side.value == 'buy' else 'short'
                                            trailing_stop_id = existing_ts.id if existing_ts else None
                                            from oca_service import create_oca_protection as create_oca_realtime
                                            oca_result, oca_status = create_oca_realtime(
                                                trailing_stop_id=trailing_stop_id,
                                                symbol=trade.symbol,
                                                side=ts_side,
                                                quantity=protection_quantity,
                                                stop_price=protection_info.get('stop_loss_price'),
                                                take_profit_price=take_profit_for_oca,
                                                account_type='real',
                                                trade_id=trade.id,
                                                entry_price=position_result['positions'][0].get('average_cost', 0),
                                                force_replace=True,
                                                creation_source='webhook_immediate'
                                            )
                                            protection_result = {
                                                'success': oca_result is not None,
                                                'stop_loss_order_id': getattr(oca_result, 'stop_order_id', None) if oca_result else None,
                                                'take_profit_order_id': getattr(oca_result, 'take_profit_order_id', None) if oca_result else None,
                                                'error': oca_status if not oca_result else None
                                            }
                                        else:
                                            logger.error(f"Could not get position for {trade.symbol} to apply OCA protection")
                                            protection_result = {'success': False, 'error': 'No position found'}
                                        if protection_result['success']:
                                            logger.info(f"Auto-protection applied successfully for {trade.symbol}: {oca_status}")
                                            auto_protection_sl_order_id = protection_result.get('stop_loss_order_id')
                                            auto_protection_tp_order_id = protection_result.get('take_profit_order_id')
                                            if auto_protection_sl_order_id:
                                                trade.stop_loss_order_id = auto_protection_sl_order_id
                                            if auto_protection_tp_order_id:
                                                trade.take_profit_order_id = auto_protection_tp_order_id
                                            
                                            # Update trailing stop position for position increase
                                            try:
                                                from trailing_stop_engine import update_trailing_stop_on_position_increase
                                                
                                                # Get current position info from Tiger
                                                avg_cost = position_result['positions'][0].get('average_cost', 0)
                                                
                                                ts_update = update_trailing_stop_on_position_increase(
                                                    symbol=trade.symbol,
                                                    account_type='real',
                                                    new_quantity=protection_quantity,
                                                    new_entry_price=avg_cost,
                                                    new_stop_loss_price=protection_info.get('stop_loss_price'),
                                                    new_take_profit_price=protection_info.get('take_profit_price'),
                                                    new_stop_loss_order_id=protection_result.get('stop_loss_order_id'),
                                                    new_take_profit_order_id=protection_result.get('take_profit_order_id')
                                                )
                                                if ts_update['success']:
                                                    logger.info(f"✅ 加仓后更新TrailingStop成功: {ts_update['message']}")
                                                    
                                                    # Immediately optimize stop loss based on current price and tier
                                                    try:
                                                        from trailing_stop_engine import calculate_optimal_stop_after_scaling
                                                        from tiger_client import get_tiger_quote_client
                                                        
                                                        # Get current price using smart price (支持盘前盘后)
                                                        quote_client = get_tiger_quote_client()
                                                        if quote_client is None:
                                                            logger.warning("Quote client not available for scaling optimization")
                                                            raise Exception("Quote client unavailable")
                                                        quote_result = quote_client.get_smart_price(trade.symbol)
                                                        if quote_result and quote_result.get('price'):
                                                            current_price = quote_result['price']
                                                            
                                                            opt_result = calculate_optimal_stop_after_scaling(
                                                                symbol=trade.symbol,
                                                                account_type='real',
                                                                current_price=current_price,
                                                                tiger_client=tiger_client
                                                            )
                                                            
                                                            if opt_result.get('stop_updated'):
                                                                logger.info(f"🎯 加仓止损优化: {opt_result['message']}")
                                                            else:
                                                                logger.info(f"📊 加仓止损检查: {opt_result.get('message', 'OK')}")
                                                    except Exception as opt_err:
                                                        logger.error(f"加仓止损优化异常: {str(opt_err)}")
                                                else:
                                                    logger.warning(f"⚠️ 加仓后更新TrailingStop: {ts_update['message']}")
                                            except Exception as ts_err:
                                                logger.error(f"❌ 加仓后更新TrailingStop异常: {str(ts_err)}")
                                        else:
                                            logger.error(f"Failed to apply auto-protection: {protection_result.get('error')}")
                                    
                                except Exception as e:
                                    logger.error(f"Error applying auto-protection: {str(e)}")
                            
                            # Update position cost tracking for regular orders
                            try:
                                filled_price = status_result.get('filled_price', 0)
                                filled_qty = status_result.get('filled_quantity', trade.quantity)
                                avg_cost = update_position_cost_on_fill(
                                    symbol=trade.symbol,
                                    side=trade.side.value,
                                    quantity=filled_qty,
                                    fill_price=filled_price,
                                    account_type='real'
                                )
                                # For sells, record the avg_cost at time of sale
                                if avg_cost and trade.side.value == 'sell':
                                    trade.entry_avg_cost = avg_cost
                                    logger.info(f"Recorded entry_avg_cost=${avg_cost:.2f} for {trade.symbol}")
                            except Exception as cost_err:
                                logger.error(f"Error updating position cost: {str(cost_err)}")
                            
                            if not getattr(trade, 'is_close_position', False):
                                try:
                                    from order_tracker_service import handle_fill_event
                                    fill_result, fill_status = handle_fill_event(
                                        tiger_order_id=str(trade.tiger_order_id),
                                        filled_quantity=status_result.get('filled_quantity', trade.quantity),
                                        avg_fill_price=filled_price,
                                        realized_pnl=status_result.get('realized_pnl'),
                                        commission=status_result.get('commission'),
                                        fill_time=datetime.utcnow(),
                                        source='polling_entry',
                                    )
                                    logger.info(f"📊 Entry fill processed via OrderTracker: {fill_status}")
                                except Exception as entry_err:
                                    logger.error(f"❌ Failed to process entry fill: {str(entry_err)}")
                                
                                try:
                                    from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
                                    from models import TrailingStopPosition
                                    ts_config = get_trailing_stop_config()
                                    if ts_config.is_enabled:
                                        ts_side = 'long' if trade.side.value == 'buy' else 'short'
                                        ts_entry = filled_price or trade.reference_price or trade.price
                                        ts_qty = status_result.get('filled_quantity', trade.quantity)
                                        ts_timeframe = parsed_signal.get('extras', {}).get('timeframe') if isinstance(parsed_signal.get('extras'), dict) else parsed_signal.get('timeframe')
                                        trailing_pos = create_trailing_stop_for_trade(
                                            trade_id=trade.id,
                                            symbol=trade.symbol,
                                            side=ts_side,
                                            entry_price=ts_entry,
                                            quantity=ts_qty,
                                            account_type='real',
                                            fixed_stop_loss=trade.stop_loss_price,
                                            fixed_take_profit=trade.take_profit_price,
                                            stop_loss_order_id=auto_protection_sl_order_id or trade.stop_loss_order_id,
                                            take_profit_order_id=auto_protection_tp_order_id or trade.take_profit_order_id,
                                            timeframe=ts_timeframe,
                                            creation_source='webhook_immediate'
                                        )
                                        if trailing_pos:
                                            logger.info(f"✅ Created/updated trailing stop #{trailing_pos.id} for {trade.symbol} on immediate fill")
                                            if trailing_pos.stop_loss_order_id:
                                                trade.stop_loss_order_id = trailing_pos.stop_loss_order_id
                                            if trailing_pos.take_profit_order_id:
                                                trade.take_profit_order_id = trailing_pos.take_profit_order_id
                                except Exception as ts_create_err:
                                    logger.error(f"❌ Failed to create trailing stop on immediate fill: {ts_create_err}")
                                    
                        elif tiger_status == 'pending':
                            trade.status = OrderStatus.PENDING
                        elif tiger_status == 'cancelled':
                            trade.status = OrderStatus.CANCELLED
                        elif tiger_status == 'rejected':
                            trade.status = OrderStatus.REJECTED
                        else:
                            trade.status = OrderStatus.PENDING  # Default fallback
                        if status_result['filled_price'] > 0:
                            trade.price = status_result['filled_price']
                            trade.filled_price = status_result['filled_price']
                        trade.filled_quantity = status_result.get('filled_quantity', 0)
                        
                        # Send Discord notification with real Tiger data
                        discord_status = status_result['status']
                        discord_notifier.send_order_notification(trade, discord_status, is_close=getattr(trade, 'is_close_position', False))
                        
                        logger.info(f"Discord notification sent with real Tiger data: {discord_status}, price: {status_result.get('filled_price', 0)}")
                    else:
                        # Fallback to pending status if can't get real-time data
                        discord_notifier.send_order_notification(trade, 'pending', is_close=getattr(trade, 'is_close_position', False))
                except Exception as e:
                    logger.error(f"Failed to get real-time status or send Discord notification: {str(e)}")
                    # Fallback notification
                    try:
                        discord_notifier.send_order_notification(trade, 'pending', is_close=getattr(trade, 'is_close_position', False))
                    except Exception as e2:
                        logger.error(f"Fallback Discord notification also failed: {str(e2)}")
                
            elif trade is not None:
                trade.status = OrderStatus.REJECTED
                trade.error_message = result.get('error', 'Unknown error')
                
                # Store Tiger API response even for failed trades
                trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                
                logger.error(f"Order rejected: {result.get('error', 'Unknown error')}")
                
                # Send Discord notification for rejected order
                try:
                    discord_notifier.send_order_notification(trade, 'rejected', is_close=getattr(trade, 'is_close_position', False))
                except Exception as e:
                    logger.error(f"Failed to send Discord notification: {str(e)}")
            
            # Update signal log with Tiger API response
            if result.get('success'):
                signal_log.tiger_status = 'success'
                signal_log.tiger_order_id = str(result.get('order_id', ''))
            else:
                signal_log.tiger_status = 'error'
                signal_log.error_message = result.get('error', 'Unknown error')
            signal_log.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
            
            db.session.add(signal_log)
            db.session.commit()
            
            return jsonify({
                'success': result['success'],
                'trade_id': trade.id if trade else None,
                'order_id': result.get('order_id'),
                'message': result.get('error', 'Order placed successfully')
            })
            
        except Exception as e:
            logger.error(f"Error processing signal: {str(e)}")
            signal_log.error_message = str(e)
            db.session.add(signal_log)
            db.session.commit()
            
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
            
    except Exception as e:
        logger.error(f"Critical error in webhook: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500


@app.route('/webhook_paper', methods=['POST'])
def webhook_paper():
    """
    Receive TradingView webhook signals for Paper Trading (模拟账户)
    Uses the same tiger_id but connects to paper trading account
    """
    try:
        client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
        raw_data = request.get_data(as_text=True)
        logger.info(f"📝 [PAPER] Received webhook from {client_ip}: {raw_data}")
        
        signal_log = SignalLog()
        signal_log.raw_signal = raw_data  # Clean signal data, account_type distinguishes paper
        signal_log.ip_address = client_ip
        signal_log.endpoint = '/webhook_paper'
        signal_log.account_type = 'paper'
        
        try:
            signal_data = request.get_json()
            if not signal_data:
                raise ValueError("No JSON data received")
            
            parser = SignalParser()
            parsed_signal = parser.parse(signal_data)
            
            if not all(key in parsed_signal for key in ['symbol', 'side', 'quantity']):
                raise ValueError("Missing required fields: symbol, side, quantity")
            
            try:
                from watchlist_service import on_signal_received
                on_signal_received(parsed_signal['symbol'], source_broker='tiger_paper')
            except Exception as wl_err:
                logger.debug(f"Watchlist update failed: {wl_err}")
            
            trade = None
            tiger_paper_client = TigerPaperClient()
            
            if parsed_signal.get('is_close_signal', False):
                logger.info(f"📝 [PAPER] Processing close signal for {parsed_signal['symbol']}, side={parsed_signal.get('side')}")
                
                # Cancel any active OCA orders first to release locked shares
                try:
                    from oca_service import cancel_oca_for_manual_close
                    # OCAGroup uses clean symbol (e.g., "GOOG") not prefixed symbol
                    oca_symbol = parsed_signal['symbol']
                    cancelled_count, cancel_msg = cancel_oca_for_manual_close(oca_symbol, 'paper')
                    if cancelled_count > 0:
                        logger.info(f"📝 [PAPER] Cancelled {cancelled_count} OCA group(s) for {oca_symbol} before close: {cancel_msg}")
                except Exception as oca_err:
                    logger.warning(f"📝 [PAPER] Failed to cancel OCA before close: {str(oca_err)}")
                
                # Get current position average cost BEFORE closing
                pre_close_avg_cost = None
                try:
                    pos_result = tiger_paper_client.get_positions(symbol=parsed_signal['symbol'])
                    if pos_result.get('success') and pos_result.get('positions'):
                        pre_close_avg_cost = pos_result['positions'][0].get('average_cost', 0)
                        logger.info(f"📝 [PAPER] Pre-close avg cost for {parsed_signal['symbol']}: ${pre_close_avg_cost:.2f}")
                except Exception as e:
                    logger.error(f"📝 [PAPER] Failed to get pre-close avg cost: {str(e)}")
                
                trading_session = parsed_signal.get('trading_session', 'regular')
                reference_price = parsed_signal.get('reference_price')
                signal_side = parsed_signal.get('side')
                signal_qty = parsed_signal.get('quantity') if not parsed_signal.get('close_all') else None
                result = tiger_paper_client.close_position_with_sandbox_fallback(
                    parsed_signal['symbol'], 
                    trading_session,
                    reference_price=reference_price,
                    signal_side=signal_side,
                    signal_quantity=signal_qty
                )
                
                # Handle no_action case (signal direction doesn't match position direction)
                if result.get('no_action'):
                    logger.info(f"📝 [PAPER] {result.get('message')}")
                    signal_log.parsed_successfully = True
                    signal_log.error_message = result.get('message')
                    db.session.add(signal_log)
                    db.session.commit()
                    return jsonify({
                        'success': True,
                        'no_action': True,
                        'message': result.get('message'),
                        'account_type': 'paper'
                    })
                
                if result['success']:
                    trade = Trade()
                    trade.symbol = parsed_signal['symbol']  # Clean symbol, use account_type for distinction
                    trade.side = Side(result['action'])
                    trade.quantity = result['quantity']
                    
                    order_type_str = result.get('order_type', 'market')
                    if order_type_str == 'limit':
                        trade.order_type = OrderType.LIMIT
                        trade.price = result.get('order_price')
                    else:
                        trade.order_type = OrderType.MARKET
                        trade.price = None
                    
                    trade.signal_data = raw_data  # Clean signal data
                    trade.tiger_order_id = result['order_id']
                    trade.status = OrderStatus.PENDING
                    trade.trading_session = result.get('trading_session', trading_session)
                    trade.outside_rth = result.get('outside_rth', parsed_signal.get('outside_rth', trading_session != 'regular'))
                    trade.is_close_position = True
                    trade.account_type = 'paper'  # Mark as paper account trade
                    
                    orig_side_paper = 'long' if result.get('action') == 'sell' else 'short'
                    try:
                        from position_service import find_open_position
                        open_pos = find_open_position(parsed_signal['symbol'], 'paper', orig_side_paper)
                        if open_pos and open_pos.entry_legs:
                            trade.parent_entry_order_id = open_pos.entry_legs[0].tiger_order_id
                            logger.info(f"🔗 [PAPER] Close order linked to Position #{open_pos.id}")
                    except Exception as link_err:
                        logger.debug(f"[PAPER] Entry order lookup failed: {link_err}")
                    
                    # Store pre-close average cost
                    if pre_close_avg_cost:
                        trade.entry_avg_cost = pre_close_avg_cost
                        logger.info(f"📝 [PAPER] Stored pre-close avg cost ${pre_close_avg_cost:.2f} for {trade.symbol}")
                    
                    trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                    
                    db.session.add(trade)
                    db.session.flush()
                    
                    signal_log.parsed_successfully = True
                    signal_log.trade_id = trade.id
                    logger.info(f"📝 [PAPER] Close position order placed: {result['order_id']}")
                    
                    try:
                        import time
                        time.sleep(1)
                        status_result = tiger_paper_client.get_order_status(trade.tiger_order_id)
                        if status_result.get('success') and status_result.get('status') == 'filled':
                            trade.status = OrderStatus.FILLED
                            if status_result.get('filled_price') and status_result['filled_price'] > 0:
                                trade.price = status_result['filled_price']
                                trade.filled_price = status_result['filled_price']
                            trade.filled_quantity = status_result.get('filled_quantity', 0)
                            db.session.commit()

                            try:
                                from order_tracker_service import handle_fill_event
                                fill_result, fill_status = handle_fill_event(
                                    tiger_order_id=str(trade.tiger_order_id),
                                    filled_quantity=status_result.get('filled_quantity', trade.quantity),
                                    avg_fill_price=status_result.get('filled_price', 0),
                                    realized_pnl=status_result.get('realized_pnl'),
                                    commission=status_result.get('commission'),
                                    fill_time=datetime.utcnow(),
                                    source='polling_paper_close',
                                )
                                logger.info(f"📊 [PAPER] Close fill processed via OrderTracker: {fill_status}")
                            except Exception as cp_err:
                                logger.error(f"❌ [PAPER] Failed to process close fill: {str(cp_err)}")
                            
                            try:
                                from trailing_stop_engine import deactivate_trailing_stop_for_symbol
                                deactivate_trailing_stop_for_symbol(parsed_signal['symbol'], 'paper', 'Exit signal filled')
                            except Exception as ts_err:
                                logger.error(f"[PAPER] Failed to deactivate trailing stop on close: {ts_err}")
                    except Exception as e:
                        logger.error(f"❌ [PAPER] Failed to get order status: {str(e)}")
                else:
                    logger.error(f"📝 [PAPER] Failed to close position: {result['error']}")
                    signal_log.error_message = result['error']
                    result['success'] = False
            else:
                max_trade_amount = float(get_config('MAX_TRADE_AMOUNT', '1000000'))
                trade_amount = parsed_signal['quantity'] * parsed_signal.get('price', 100)
                
                if trade_amount > max_trade_amount:
                    raise ValueError(f"Trade amount ${trade_amount:.2f} exceeds maximum allowed ${max_trade_amount:.2f}")
                
                trade = Trade()
                trade.symbol = parsed_signal['symbol']
                trade.side = Side(parsed_signal['side'].lower())
                trade.quantity = parsed_signal['quantity']
                trade.price = parsed_signal.get('price')
                trade.order_type = OrderType(parsed_signal.get('order_type', 'market').lower())
                trade.signal_data = raw_data  # Clean signal data
                trade.stop_loss_price = parsed_signal.get('stop_loss')
                trade.take_profit_price = parsed_signal.get('take_profit')
                trade.trading_session = parsed_signal.get('trading_session', 'regular')
                trade.outside_rth = parsed_signal.get('outside_rth', False)
                trade.reference_price = parsed_signal.get('reference_price')
                trade.account_type = 'paper'  # Mark as paper account trade
                
                db.session.add(trade)
                db.session.flush()
                
                signal_log.parsed_successfully = True
                signal_log.trade_id = trade.id
                
                result = tiger_paper_client.place_order(trade)
                
                # Keep clean symbol, account_type distinguishes paper vs real
            
            if result['success'] and trade is not None:
                if not hasattr(trade, 'tiger_order_id') or trade.tiger_order_id is None:
                    trade.tiger_order_id = result['order_id']
                if not hasattr(trade, 'status') or trade.status != OrderStatus.PENDING:
                    trade.status = OrderStatus.PENDING
                
                trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                
                if 'stop_loss_order_id' in result:
                    trade.stop_loss_order_id = result['stop_loss_order_id']
                if 'take_profit_order_id' in result:
                    trade.take_profit_order_id = result['take_profit_order_id']
                
                # Register orders to OrderTracker for unified monitoring (Paper)
                from order_tracker_service import register_order
                clean_symbol = parsed_signal['symbol']  # Use clean symbol without [PAPER] prefix
                is_close_signal = parsed_signal.get('is_close_signal', False)
                order_role = 'exit_signal' if is_close_signal else 'entry'
                register_order(
                    tiger_order_id=result['order_id'],
                    symbol=clean_symbol,
                    account_type='paper',
                    role=order_role,
                    side=trade.side.value.upper(),
                    quantity=trade.quantity,
                    trade_id=trade.id
                )
                if 'stop_loss_order_id' in result:
                    register_order(
                        tiger_order_id=result['stop_loss_order_id'],
                        symbol=clean_symbol,
                        account_type='paper',
                        role='stop_loss',
                        parent_order_id=result['order_id'],
                        stop_price=trade.stop_loss_price,
                        trade_id=trade.id
                    )
                if 'take_profit_order_id' in result:
                    register_order(
                        tiger_order_id=result['take_profit_order_id'],
                        symbol=clean_symbol,
                        account_type='paper',
                        role='take_profit',
                        parent_order_id=result['order_id'],
                        limit_price=trade.take_profit_price,
                        trade_id=trade.id
                    )
                
                needs_auto_protection = result.get('needs_auto_protection', False)
                protection_info = result.get('protection_info', {})
                if needs_auto_protection:
                    trade.needs_auto_protection = True
                    trade.protection_info = json.dumps(protection_info)
                
                try:
                    import time
                    time.sleep(1)
                    
                    status_result = tiger_paper_client.get_order_status(trade.tiger_order_id)
                    if status_result['success']:
                        tiger_status = status_result['status']
                        if tiger_status == 'filled':
                            trade.status = OrderStatus.FILLED
                            # Commit trade immediately so foreign keys work
                            db.session.commit()
                            
                            auto_protection_sl_order_id = None
                            auto_protection_tp_order_id = None
                            
                            if hasattr(trade, 'needs_auto_protection') and trade.needs_auto_protection:
                                logger.info(f"📝 [PAPER] Order {trade.tiger_order_id} filled, updating TS for scaling (bracket-only, no OCA)")
                                try:
                                    protection_info_str = trade.protection_info if hasattr(trade, 'protection_info') else None
                                    trade.needs_auto_protection = False
                                    trade.protection_info = None
                                    db.session.commit()
                                    protection_info = json.loads(protection_info_str) if protection_info_str else {}
                                    if protection_info.get('stop_loss_price') or protection_info.get('take_profit_price'):
                                        from models import TrailingStopPosition
                                        existing_ts = TrailingStopPosition.query.filter_by(
                                            symbol=parsed_signal['symbol'], account_type='paper', is_active=True
                                        ).first()
                                        
                                        position_result = tiger_paper_client.get_positions(symbol=parsed_signal['symbol'])
                                        if position_result['success'] and position_result['positions']:
                                            current_quantity = abs(position_result['positions'][0]['quantity'])
                                            avg_cost = position_result['positions'][0].get('average_cost', 0)
                                            
                                            auto_protection_sl_order_id = trade.stop_loss_order_id
                                            auto_protection_tp_order_id = trade.take_profit_order_id
                                            
                                            if existing_ts:
                                                try:
                                                    from trailing_stop_engine import update_trailing_stop_on_position_increase
                                                    
                                                    ts_update = update_trailing_stop_on_position_increase(
                                                        symbol=parsed_signal['symbol'],
                                                        account_type='paper',
                                                        new_quantity=current_quantity,
                                                        new_entry_price=avg_cost,
                                                        new_stop_loss_price=protection_info.get('stop_loss_price'),
                                                        new_take_profit_price=protection_info.get('take_profit_price'),
                                                        new_stop_loss_order_id=trade.stop_loss_order_id,
                                                        new_take_profit_order_id=trade.take_profit_order_id
                                                    )
                                                    if ts_update['success']:
                                                        logger.info(f"📝 [PAPER] 加仓后更新TrailingStop成功: {ts_update['message']}")
                                                        
                                                        try:
                                                            from trailing_stop_engine import calculate_optimal_stop_after_scaling
                                                            from tiger_client import get_tiger_quote_client
                                                            
                                                            quote_client = get_tiger_quote_client()
                                                            if quote_client is None:
                                                                logger.warning("[PAPER] Quote client not available for scaling optimization")
                                                                raise Exception("Quote client unavailable")
                                                            quote_result = quote_client.get_smart_price(parsed_signal['symbol'])
                                                            if quote_result and quote_result.get('price'):
                                                                current_price = quote_result['price']
                                                                
                                                                opt_result = calculate_optimal_stop_after_scaling(
                                                                    symbol=parsed_signal['symbol'],
                                                                    account_type='paper',
                                                                    current_price=current_price,
                                                                    tiger_client=tiger_paper_client
                                                                )
                                                                
                                                                if opt_result.get('stop_updated'):
                                                                    logger.info(f"📝 [PAPER] 🎯 加仓止损优化: {opt_result['message']}")
                                                                else:
                                                                    logger.info(f"📝 [PAPER] 📊 加仓止损检查: {opt_result.get('message', 'OK')}")
                                                        except Exception as opt_err:
                                                            logger.error(f"📝 [PAPER] 加仓止损优化异常: {str(opt_err)}")
                                                    else:
                                                        logger.warning(f"📝 [PAPER] 加仓后更新TrailingStop: {ts_update['message']}")
                                                except Exception as ts_err:
                                                    logger.error(f"📝 [PAPER] 加仓后更新TrailingStop异常: {str(ts_err)}")
                                            else:
                                                logger.info(f"📝 [PAPER] No active TS for {parsed_signal['symbol']}, "
                                                           f"bracket sub-orders provide protection (SL={trade.stop_loss_order_id}, TP={trade.take_profit_order_id})")
                                except Exception as e:
                                    logger.error(f"📝 [PAPER] Failed to update TS for scaling: {str(e)}")
                            
                            # Update position cost tracking for paper account
                            try:
                                filled_price = status_result.get('filled_price', 0)
                                filled_qty = status_result.get('filled_quantity', trade.quantity)
                                avg_cost = update_position_cost_on_fill(
                                    symbol=parsed_signal['symbol'],
                                    side=trade.side.value,
                                    quantity=filled_qty,
                                    fill_price=filled_price,
                                    account_type='paper'
                                )
                                if avg_cost and trade.side.value == 'sell':
                                    trade.entry_avg_cost = avg_cost
                                    logger.info(f"📝 [PAPER] Recorded entry_avg_cost=${avg_cost:.2f} for {trade.symbol}")
                            except Exception as cost_err:
                                logger.error(f"📝 [PAPER] Error updating position cost: {str(cost_err)}")
                            
                            if not getattr(trade, 'is_close_position', False):
                                try:
                                    from order_tracker_service import handle_fill_event
                                    fill_result, fill_status = handle_fill_event(
                                        tiger_order_id=str(trade.tiger_order_id),
                                        filled_quantity=status_result.get('filled_quantity', trade.quantity),
                                        avg_fill_price=filled_price,
                                        realized_pnl=status_result.get('realized_pnl'),
                                        commission=status_result.get('commission'),
                                        fill_time=datetime.utcnow(),
                                        source='polling_paper_entry',
                                    )
                                    logger.info(f"📊 [PAPER] Entry fill processed via OrderTracker: {fill_status}")
                                except Exception as entry_err:
                                    logger.error(f"❌ [PAPER] Failed to process entry fill: {str(entry_err)}")
                                
                                try:
                                    from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
                                    from models import TrailingStopPosition
                                    ts_config = get_trailing_stop_config()
                                    if ts_config.is_enabled:
                                        ts_side = 'long' if trade.side.value == 'buy' else 'short'
                                        ts_entry = filled_price or trade.reference_price or trade.price
                                        ts_qty = status_result.get('filled_quantity', trade.quantity)
                                        ts_timeframe = parsed_signal.get('extras', {}).get('timeframe') if isinstance(parsed_signal.get('extras'), dict) else parsed_signal.get('timeframe')
                                        trailing_pos = create_trailing_stop_for_trade(
                                            trade_id=trade.id,
                                            symbol=parsed_signal['symbol'],
                                            side=ts_side,
                                            entry_price=ts_entry,
                                            quantity=ts_qty,
                                            account_type='paper',
                                            fixed_stop_loss=trade.stop_loss_price,
                                            fixed_take_profit=trade.take_profit_price,
                                            stop_loss_order_id=auto_protection_sl_order_id or trade.stop_loss_order_id,
                                            take_profit_order_id=auto_protection_tp_order_id or trade.take_profit_order_id,
                                            timeframe=ts_timeframe,
                                            creation_source='webhook_immediate'
                                        )
                                        if trailing_pos:
                                            logger.info(f"📝 [PAPER] Created/updated trailing stop #{trailing_pos.id} for {parsed_signal['symbol']} on immediate fill")
                                            if trailing_pos.stop_loss_order_id:
                                                trade.stop_loss_order_id = trailing_pos.stop_loss_order_id
                                            if trailing_pos.take_profit_order_id:
                                                trade.take_profit_order_id = trailing_pos.take_profit_order_id
                                except Exception as ts_create_err:
                                    logger.error(f"📝 [PAPER] Failed to create trailing stop on immediate fill: {ts_create_err}")
                                    
                        elif tiger_status == 'pending':
                            trade.status = OrderStatus.PENDING
                        elif tiger_status == 'cancelled':
                            trade.status = OrderStatus.CANCELLED
                        elif tiger_status == 'rejected':
                            trade.status = OrderStatus.REJECTED
                        else:
                            trade.status = OrderStatus.PENDING
                        if status_result['filled_price'] > 0:
                            trade.price = status_result['filled_price']
                            trade.filled_price = status_result['filled_price']
                        trade.filled_quantity = status_result.get('filled_quantity', 0)
                        
                        discord_status = status_result['status']
                        discord_notifier.send_order_notification(trade, f"[PAPER] {discord_status}", is_close=getattr(trade, 'is_close_position', False))
                        logger.info(f"📝 [PAPER] Discord notification sent: {discord_status}")
                    else:
                        discord_notifier.send_order_notification(trade, '[PAPER] pending', is_close=getattr(trade, 'is_close_position', False))
                except Exception as e:
                    logger.error(f"📝 [PAPER] Failed to get real-time status: {str(e)}")
                    try:
                        discord_notifier.send_order_notification(trade, '[PAPER] pending', is_close=getattr(trade, 'is_close_position', False))
                    except Exception as e2:
                        logger.error(f"📝 [PAPER] Fallback Discord notification failed: {str(e2)}")
                
            elif trade is not None:
                trade.status = OrderStatus.REJECTED
                trade.error_message = result.get('error', 'Unknown error')
                
                trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                logger.error(f"📝 [PAPER] Order rejected: {result.get('error', 'Unknown error')}")
                
                try:
                    discord_notifier.send_order_notification(trade, '[PAPER] rejected', is_close=getattr(trade, 'is_close_position', False))
                except Exception as e:
                    logger.error(f"📝 [PAPER] Failed to send Discord notification: {str(e)}")
            
            # Update signal log with Tiger API response
            if result.get('success'):
                signal_log.tiger_status = 'success'
                signal_log.tiger_order_id = str(result.get('order_id', ''))
            else:
                signal_log.tiger_status = 'error'
            signal_log.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
            
            db.session.add(signal_log)
            db.session.commit()
            
            return jsonify({
                'success': result['success'],
                'trade_id': trade.id if trade else None,
                'order_id': result.get('order_id'),
                'account_type': 'paper',
                'message': result.get('error', '[PAPER] Order placed successfully')
            })
            
        except Exception as e:
            logger.error(f"📝 [PAPER] Error processing signal: {str(e)}")
            signal_log.error_message = str(e)
            db.session.add(signal_log)
            db.session.commit()
            
            return jsonify({
                'success': False,
                'account_type': 'paper',
                'error': str(e)
            }), 400
            
    except Exception as e:
        logger.error(f"📝 [PAPER] Critical error in webhook: {str(e)}")
        return jsonify({
            'success': False,
            'account_type': 'paper',
            'error': 'Internal server error'
        }), 500


def process_signal_for_account(raw_data: str, signal_data: dict, client_ip: str, account_type: str) -> dict:
    """
    Internal function to process a trading signal for a specific account type.
    Returns a result dict with success status and details.
    """
    try:
        parser = SignalParser()
        parsed_signal = parser.parse(signal_data)
        
        if not all(key in parsed_signal for key in ['symbol', 'side', 'quantity']):
            raise ValueError("Missing required fields: symbol, side, quantity")
        
        # Select the appropriate Tiger client
        if account_type == 'paper':
            tiger_client = TigerPaperClient()
        else:
            tiger_client = TigerClient()
        
        trade = None
        
        if parsed_signal.get('is_close_signal', False):
            logger.info(f"🔄 [{account_type.upper()}] Processing close signal for {parsed_signal['symbol']}")
            
            # Get current position average cost BEFORE closing
            pre_close_avg_cost = None
            try:
                pos_result = tiger_client.get_positions(symbol=parsed_signal['symbol'])
                if pos_result.get('success') and pos_result.get('positions'):
                    pre_close_avg_cost = pos_result['positions'][0].get('average_cost', 0)
                    logger.info(f"🔄 [{account_type.upper()}] Pre-close avg cost for {parsed_signal['symbol']}: ${pre_close_avg_cost:.2f}")
            except Exception as e:
                logger.error(f"🔄 [{account_type.upper()}] Failed to get pre-close avg cost: {str(e)}")
            
            trading_session = parsed_signal.get('trading_session', 'regular')
            reference_price = parsed_signal.get('reference_price')
            signal_side = parsed_signal.get('side')
            signal_qty = parsed_signal.get('quantity') if not parsed_signal.get('close_all') else None
            result = tiger_client.close_position_with_sandbox_fallback(
                parsed_signal['symbol'], 
                trading_session,
                reference_price=reference_price,
                signal_side=signal_side,
                signal_quantity=signal_qty
            )
            
            if result.get('no_action'):
                return {'success': True, 'no_action': True, 'message': result.get('message'), 'account_type': account_type}
            
            if result['success']:
                trade = Trade()
                trade.symbol = parsed_signal['symbol']  # Clean symbol, use account_type for distinction
                trade.side = Side(result['action'])
                trade.quantity = result['quantity']
                
                order_type_str = result.get('order_type', 'market')
                if order_type_str == 'limit':
                    trade.order_type = OrderType.LIMIT
                    trade.price = result.get('order_price')
                else:
                    trade.order_type = OrderType.MARKET
                    trade.price = None
                
                trade.signal_data = raw_data  # Clean signal data
                trade.tiger_order_id = result['order_id']
                trade.status = OrderStatus.PENDING
                trade.trading_session = result.get('trading_session', trading_session)
                trade.outside_rth = result.get('outside_rth', False)
                trade.is_close_position = True
                trade.account_type = account_type
                
                orig_side_both = 'long' if result.get('action') == 'sell' else 'short'
                try:
                    from position_service import find_open_position
                    open_pos = find_open_position(parsed_signal['symbol'], account_type, orig_side_both)
                    if open_pos and open_pos.entry_legs:
                        trade.parent_entry_order_id = open_pos.entry_legs[0].tiger_order_id
                        logger.info(f"🔗 [{account_type.upper()}] Close order linked to Position #{open_pos.id}")
                except Exception as link_err:
                    logger.debug(f"[{account_type.upper()}] Entry order lookup failed: {link_err}")
                
                # Store pre-close average cost
                if pre_close_avg_cost:
                    trade.entry_avg_cost = pre_close_avg_cost
                    logger.info(f"🔄 [{account_type.upper()}] Stored pre-close avg cost ${pre_close_avg_cost:.2f} for {trade.symbol}")
                
                trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                
                db.session.add(trade)
                db.session.commit()
                
                logger.info(f"📊 [{account_type.upper()}] Exit order placed - Position closure tracked via Position+PositionLeg")
        else:
            max_trade_amount = float(get_config('MAX_TRADE_AMOUNT', '1000000'))
            trade_amount = parsed_signal['quantity'] * parsed_signal.get('price', 100)
            
            if trade_amount > max_trade_amount:
                raise ValueError(f"Trade amount ${trade_amount:.2f} exceeds maximum allowed ${max_trade_amount:.2f}")
            
            trade = Trade()
            # Use original symbol for Tiger API call
            trade.symbol = parsed_signal['symbol']
            trade.side = Side(parsed_signal['side'].lower())
            trade.quantity = parsed_signal['quantity']
            trade.price = parsed_signal.get('price')
            trade.order_type = OrderType(parsed_signal.get('order_type', 'market').lower())
            trade.signal_data = raw_data  # Clean signal data
            trade.stop_loss_price = parsed_signal.get('stop_loss')
            trade.take_profit_price = parsed_signal.get('take_profit')
            trade.trading_session = parsed_signal.get('trading_session', 'regular')
            trade.outside_rth = parsed_signal.get('outside_rth', False)
            trade.reference_price = parsed_signal.get('reference_price')
            trade.account_type = account_type
            
            db.session.add(trade)
            db.session.flush()
            
            result = tiger_client.place_order(trade)
            
            if result['success']:
                trade.tiger_order_id = result['order_id']
                trade.status = OrderStatus.PENDING
                # Keep clean symbol, account_type distinguishes paper vs real
                
                trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                
                if 'stop_loss_order_id' in result:
                    trade.stop_loss_order_id = result['stop_loss_order_id']
                if 'take_profit_order_id' in result:
                    trade.take_profit_order_id = result['take_profit_order_id']
                
                db.session.commit()
                
                # Prepare common data for trailing stop and analytics
                signal_side = parsed_signal['side'].lower()
                ts_side = 'long' if signal_side == 'buy' else 'short'
                filled_price = result.get('avg_fill_price') or result.get('filled_price')
                entry_price = filled_price or parsed_signal.get('reference_price') or parsed_signal.get('price') or 0
                timeframe = '15'  # default
                if 'extras' in signal_data and signal_data['extras']:
                    timeframe = signal_data['extras'].get('timeframe', '15')
                ts_symbol = trade.symbol
                
                trailing_position = None
                
                # Auto-create trailing stop for new positions
                try:
                    from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
                    from models import TrailingStopMode
                    
                    ts_config = get_trailing_stop_config()
                    if ts_config.is_enabled:
                        trailing_position = create_trailing_stop_for_trade(
                            trade_id=trade.id,
                            symbol=ts_symbol,
                            side=ts_side,
                            entry_price=entry_price,
                            quantity=parsed_signal['quantity'],
                            account_type=account_type,
                            fixed_stop_loss=parsed_signal.get('stop_loss'),
                            fixed_take_profit=parsed_signal.get('take_profit'),
                            stop_loss_order_id=result.get('stop_loss_order_id'),
                            take_profit_order_id=result.get('take_profit_order_id'),
                            mode=TrailingStopMode.BALANCED,
                            timeframe=str(timeframe),
                            creation_source='webhook_immediate'
                        )
                        logger.info(f"🎯 [{account_type.upper()}] Auto-created trailing stop for {ts_symbol}, side={ts_side}, entry=${entry_price:.2f}")
                except Exception as e:
                    logger.error(f"🎯 [{account_type.upper()}] Failed to create trailing stop: {str(e)}")
                
                logger.info(f"📊 [{account_type.upper()}] Entry order tracked via Position+PositionLeg (unified architecture)")
        
        return {
            'success': result.get('success', False) if 'result' in dir() else False,
            'trade_id': trade.id if trade else None,
            'order_id': result.get('order_id') if 'result' in dir() else None,
            'account_type': account_type,
            'message': f'{account_type.upper()} order placed successfully'
        }
        
    except Exception as e:
        logger.error(f"🔄 [{account_type.upper()}] Error processing signal: {str(e)}")
        db.session.rollback()
        return {'success': False, 'error': str(e), 'account_type': account_type}


@app.route('/webhook_both', methods=['POST'])
def webhook_both():
    """
    Receive TradingView webhook signals and execute on BOTH real and paper accounts
    This allows testing with paper account while also executing real trades
    """
    try:
        client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
        raw_data = request.get_data(as_text=True)
        logger.info(f"🔄 [BOTH] Received webhook from {client_ip}: {raw_data}")
        
        # Log the signal for webhook_both
        signal_log = SignalLog()
        signal_log.raw_signal = raw_data
        signal_log.ip_address = client_ip
        signal_log.endpoint = '/webhook_both'
        signal_log.account_type = 'both'
        
        signal_data = request.get_json()
        if not signal_data:
            signal_log.tiger_status = 'error'
            signal_log.error_message = 'No JSON data received'
            db.session.add(signal_log)
            db.session.commit()
            return jsonify({'success': False, 'error': 'No JSON data received'}), 400
        
        results = {}
        
        try:
            parser = SignalParser()
            parsed_both = parser.parse(signal_data)
            if parsed_both.get('symbol'):
                from watchlist_service import on_signal_received
                on_signal_received(parsed_both['symbol'], source_broker='tiger')
        except Exception as wl_err:
            logger.debug(f"Watchlist update failed: {wl_err}")
        
        # Process for real account
        logger.info("🔄 [BOTH] Processing for REAL account...")
        results['real'] = process_signal_for_account(raw_data, signal_data, client_ip, 'real')
        
        # Process for paper account
        logger.info("🔄 [BOTH] Processing for PAPER account...")
        results['paper'] = process_signal_for_account(raw_data, signal_data, client_ip, 'paper')
        
        # Determine overall success
        overall_success = results['real'].get('success', False) or results['paper'].get('success', False)
        
        # Update signal log with results
        signal_log.parsed_successfully = True
        if results['real'].get('success') and results['paper'].get('success'):
            signal_log.tiger_status = 'success'
        elif results['real'].get('success') or results['paper'].get('success'):
            signal_log.tiger_status = 'partial'
        else:
            signal_log.tiger_status = 'error'
        
        # Collect order IDs
        order_ids = []
        if results['real'].get('order_id'):
            order_ids.append(f"Real:{results['real']['order_id']}")
        if results['paper'].get('order_id'):
            order_ids.append(f"Paper:{results['paper']['order_id']}")
        signal_log.tiger_order_id = ', '.join(order_ids) if order_ids else None
        signal_log.tiger_response = json.dumps(results, ensure_ascii=False, indent=2)
        
        db.session.add(signal_log)
        db.session.commit()
        
        # Send Discord notification for both
        try:
            discord_notifier.send_both_accounts_notification(results)
        except Exception as e:
            logger.error(f"🔄 [BOTH] Failed to send Discord notification: {str(e)}")
        
        return jsonify({
            'success': overall_success,
            'message': 'Signal processed for both accounts',
            'results': results
        })
        
    except Exception as e:
        logger.error(f"🔄 [BOTH] Critical error in webhook_both: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Internal server error'
        }), 500


@app.route('/trades')
def trades():
    """View all trades with real-time status from Tiger API"""
    page = request.args.get('page', 1, type=int)
    per_page = 20
    account_type = request.args.get('account_type', 'all')  # all, real, paper
    
    trades_query = Trade.query
    
    # Filter by account type
    if account_type == 'real':
        trades_query = trades_query.filter(
            db.or_(Trade.account_type == 'real', Trade.account_type == None)
        )
    elif account_type == 'paper':
        trades_query = trades_query.filter(Trade.account_type == 'paper')
    
    trades_query = trades_query.order_by(Trade.created_at.desc())
    trades_pagination = trades_query.paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    # Get real-time status for each trade with Tiger order ID
    # Use appropriate client based on trade's account_type
    tiger_real_client = None
    tiger_paper_client = None
    
    for trade in trades_pagination.items:
        if trade.tiger_order_id:
            try:
                # Select appropriate client based on trade's account type
                if trade.account_type == 'paper':
                    if tiger_paper_client is None:
                        tiger_paper_client = TigerPaperClient()
                    tiger_client = tiger_paper_client
                else:
                    if tiger_real_client is None:
                        tiger_real_client = TigerClient()
                    tiger_client = tiger_real_client
                
                status_update = tiger_client.get_order_status(trade.tiger_order_id)
                if status_update['success']:
                    # Update real-time status and price data
                    trade.real_time_status = status_update['status']
                    trade.real_time_filled_price = status_update.get('filled_price', 0)
                    trade.real_time_filled_quantity = status_update.get('filled_quantity', 0)
                    trade.tiger_status_info = status_update.get('tiger_status', '')
                else:
                    trade.real_time_status = trade.status.value
                    trade.real_time_filled_price = trade.filled_price
                    trade.real_time_filled_quantity = trade.filled_quantity
                    trade.tiger_status_info = 'Error fetching'
            except Exception as e:
                logger.error(f"Error getting real-time status for trade {trade.id}: {str(e)}")
                trade.real_time_status = trade.status.value
                trade.real_time_filled_price = trade.filled_price
                trade.real_time_filled_quantity = trade.filled_quantity
                trade.tiger_status_info = 'Error'
        else:
            # No Tiger order ID, use database values
            trade.real_time_status = trade.status.value
            trade.real_time_filled_price = trade.filled_price
            trade.real_time_filled_quantity = trade.filled_quantity
            trade.tiger_status_info = 'No Tiger Order ID'
    
    return render_template('trades.html', 
                         trades=trades_pagination.items,
                         pagination=trades_pagination,
                         account_type=account_type)

@app.route('/manual_signal')
def manual_signal():
    return render_template('manual_signal.html')

@app.route('/api/current_price/<symbol>')
def api_current_price(symbol):
    symbol = symbol.upper().strip()

    try:
        from tiger_push_client import get_cached_price
        cached = get_cached_price(symbol)
        if cached is not None:
            return jsonify({'success': True, 'price': round(cached, 4), 'source': 'Tiger WebSocket', 'session': 'regular'})
    except Exception:
        pass

    try:
        from tbus.tbus_quote_ws import get_eodhd_quote_manager
        eodhd = get_eodhd_quote_manager()
        if eodhd.is_running:
            eodhd_price = eodhd.get_price(symbol)
            if eodhd_price is not None:
                return jsonify({'success': True, 'price': round(eodhd_price, 4), 'source': 'EODHD WS', 'session': 'regular'})
    except ImportError:
        pass
    except Exception:
        pass

    try:
        eodhd_api_key = os.environ.get('EODHD_API_KEY', '')
        if eodhd_api_key:
            import requests as req
            resp = req.get(
                'https://eodhd.com/api/us-quote-delayed',
                params={'s': f'{symbol}.US', 'api_token': eodhd_api_key, 'fmt': 'json'},
                timeout=5
            )
            if resp.status_code == 200:
                raw = resp.json()
                qdata = raw.get('data', {}).get(f'{symbol}.US', {})
                if qdata:
                    eth_price = float(qdata.get('ethPrice', 0))
                    last_price = float(qdata.get('lastTradePrice', 0))
                    price = eth_price if eth_price > 0 else last_price
                    session = 'regular'
                    if eth_price > 0 and qdata.get('ethTime'):
                        import pytz
                        et = pytz.timezone('America/New_York')
                        now_et = datetime.now(et)
                        h, m = now_et.hour, now_et.minute
                        mins = h * 60 + m
                        if 240 <= mins < 570:
                            session = 'pre_market'
                        elif 960 <= mins < 1200:
                            session = 'post_market'
                    if price > 0:
                        return jsonify({'success': True, 'price': round(price, 4), 'source': 'EODHD API', 'session': session})
    except Exception as e:
        logger.warning(f"EODHD API price failed for {symbol}: {str(e)}")

    try:
        from tiger_client import get_tiger_quote_client
        quote_client = get_tiger_quote_client()
        result = quote_client.get_smart_price(symbol)
        if result and result.get('price'):
            return jsonify({
                'success': True,
                'price': round(float(result['price']), 4),
                'source': result.get('source', 'Tiger API'),
                'session': result.get('session', 'unknown')
            })
    except Exception as e:
        logger.warning(f"Tiger smart price failed for {symbol}: {str(e)}")

    return jsonify({'success': False, 'error': f'Unable to get price for {symbol}'})

@app.route('/watchlist', methods=['GET'])
def watchlist():
    """Watchlist management page"""
    from watchlist_service import get_watchlist_status
    status = get_watchlist_status()
    return render_template('watchlist.html', status=status)


@app.route('/watchlist/add', methods=['POST'])
def watchlist_add():
    """Add symbol(s) to watchlist"""
    from watchlist_service import upsert_watchlist_symbol
    symbols_raw = request.form.get('symbols', '')
    symbols = [s.strip().upper() for s in symbols_raw.replace(',', ' ').split() if s.strip()]
    
    if not symbols:
        flash('Please enter at least one symbol.', 'error')
        return redirect(url_for('watchlist'))
    
    added = []
    for symbol in symbols:
        try:
            upsert_watchlist_symbol(symbol, source='manual')
            added.append(symbol)
        except Exception as e:
            flash(f'Error adding {symbol}: {str(e)}', 'error')
    
    if added:
        try:
            from tiger_push_client import get_push_manager
            manager = get_push_manager()
            if manager.is_connected:
                manager.subscribe_quotes(added)
        except Exception:
            pass
        flash(f'Added {", ".join(added)} to watchlist and subscribed.', 'success')
    
    return redirect(url_for('watchlist'))


@app.route('/watchlist/remove/<symbol>', methods=['POST'])
def watchlist_remove(symbol):
    """Remove symbol from watchlist"""
    from watchlist_service import deactivate_watchlist_symbol
    symbol = symbol.upper().strip()
    
    deactivated = deactivate_watchlist_symbol(symbol)
    if deactivated:
        try:
            from tiger_push_client import get_push_manager
            manager = get_push_manager()
            if manager.is_connected:
                manager.unsubscribe_quotes([symbol])
        except Exception:
            pass
        flash(f'Removed {symbol} from watchlist.', 'success')
    else:
        flash(f'{symbol} not found in watchlist.', 'error')
    
    return redirect(url_for('watchlist'))


@app.route('/watchlist/reactivate/<symbol>', methods=['POST'])
def watchlist_reactivate(symbol):
    """Reactivate a deactivated watchlist symbol"""
    from watchlist_service import upsert_watchlist_symbol
    symbol = symbol.upper().strip()
    
    upsert_watchlist_symbol(symbol, source='manual')
    try:
        from tiger_push_client import get_push_manager
        manager = get_push_manager()
        if manager.is_connected:
            manager.subscribe_quotes([symbol])
    except Exception:
        pass
    
    flash(f'Reactivated {symbol} in watchlist.', 'success')
    return redirect(url_for('watchlist'))


@app.route('/config', methods=['GET', 'POST'])
def config():
    """Configuration management"""
    if request.method == 'POST':
        try:
            # Update configurations
            for key in ['MAX_TRADE_AMOUNT', 'TRADING_ENABLED', 'DISCORD_WEBHOOK_URL', 
                       'DISCORD_TTS_WEBHOOK_URL']:
                value = request.form.get(key, '')
                if value:
                    set_config(key, value)
            
            flash('Configuration updated successfully!', 'success')
            return redirect(url_for('config'))
            
        except Exception as e:
            flash(f'Error updating configuration: {str(e)}', 'error')
    
    # Get current configurations
    configs = {
        'MAX_TRADE_AMOUNT': get_config('MAX_TRADE_AMOUNT', '1000000'),
        'TRADING_ENABLED': get_config('TRADING_ENABLED', 'true'),
        'DISCORD_WEBHOOK_URL': get_config('DISCORD_WEBHOOK_URL', ''),
        'DISCORD_TTS_WEBHOOK_URL': get_config('DISCORD_TTS_WEBHOOK_URL', '')
    }
    
    return render_template('config.html', configs=configs)

@app.route('/trade/<int:trade_id>')
def trade_detail(trade_id):
    """View detailed information for a specific trade"""
    trade = Trade.query.get_or_404(trade_id)
    
    # Parse JSON data for display
    signal_json = None
    tiger_response_json = None
    
    try:
        if trade.signal_data:
            signal_json = json.loads(trade.signal_data)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Error parsing signal data for trade {trade_id}: {e}")
        signal_json = {"error": "Invalid JSON data"}
    
    try:
        if trade.tiger_response:
            tiger_response_json = json.loads(trade.tiger_response)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Error parsing tiger response for trade {trade_id}: {e}")
        tiger_response_json = {"error": "Invalid JSON data"}
    
    return render_template('trade_detail.html', 
                         trade=trade,
                         signal_json=signal_json,
                         tiger_response_json=tiger_response_json)

@app.route('/set_position_protection', methods=['POST'])
def set_position_protection():
    """Set stop loss and take profit for existing position"""
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400
        
        symbol = data.get('symbol', '').upper().strip()
        quantity = data.get('quantity', 0)
        stop_loss = data.get('stop_loss')
        take_profit = data.get('take_profit')
        account_type = data.get('account_type', 'real')
        
        # Validation
        if not symbol:
            return jsonify({
                'success': False,
                'error': 'Symbol is required'
            }), 400
        
        if quantity <= 0:
            return jsonify({
                'success': False,
                'error': 'Quantity must be greater than 0'
            }), 400
            
        if not stop_loss and not take_profit:
            return jsonify({
                'success': False,
                'error': 'At least one of stop_loss or take_profit must be provided'
            }), 400
        
        # Convert to float if provided
        try:
            stop_loss_price = float(stop_loss) if stop_loss else None
            take_profit_price = float(take_profit) if take_profit else None
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'Invalid price format'
            }), 400
        
        logger.info(f"Setting position protection for {symbol}: {quantity} shares, "
                   f"stop_loss: {stop_loss_price}, take_profit: {take_profit_price}")
        
        if account_type == 'paper':
            from tiger_client import TigerPaperClient
            tiger_client = TigerPaperClient()
        else:
            tiger_client = TigerClient()
        result = tiger_client.create_oca_orders_for_position(
            symbol=symbol,
            quantity=quantity,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price
        )
        
        if result['success']:
            # Create a new trade record for tracking
            trade = Trade()
            trade.symbol = symbol
            trade.side = Side.SELL  # These are protective orders
            trade.quantity = quantity
            trade.order_type = OrderType.LIMIT
            trade.price = 0.0  # No main order price for OCA
            trade.stop_loss_price = stop_loss_price
            trade.take_profit_price = take_profit_price
            trade.status = OrderStatus.PENDING
            trade.tiger_order_id = result.get('order_id')
            trade.stop_loss_order_id = result.get('stop_loss_order_id')
            trade.take_profit_order_id = result.get('take_profit_order_id')
            trade.trading_session = 'regular'
            trade.outside_rth = True
            trade.signal_data = json.dumps({
                'action': 'protect_position',
                'symbol': symbol,
                'quantity': quantity,
                'stop_loss': stop_loss_price,
                'take_profit': take_profit_price,
                'strategy': 'Position Protection OCA Orders'
            })
            trade.tiger_response = json.dumps(result)
            
            db.session.add(trade)
            db.session.commit()
            
            # Send Discord notification
            try:
                discord_notifier.send_order_notification(trade, 'pending')
            except Exception as e:
                logger.error(f"Failed to send Discord notification: {str(e)}")
            
            logger.info(f"Position protection set successfully for {symbol}")
            
            return jsonify({
                'success': True,
                'message': result.get('message'),
                'order_id': result.get('order_id'),
                'stop_loss_order_id': result.get('stop_loss_order_id'),
                'take_profit_order_id': result.get('take_profit_order_id'),
                'trade_id': trade.id
            })
        else:
            logger.error(f"Failed to set position protection: {result.get('error')}")
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Error in set_position_protection: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/trade/<int:trade_id>/status')
def trade_status(trade_id):
    """Get trade status update"""
    trade = Trade.query.get_or_404(trade_id)
    
    # Update status from Tiger if order ID exists
    if trade.tiger_order_id:
        tiger_client = TigerClient()
        status_update = tiger_client.get_order_status(trade.tiger_order_id)
        
        if status_update['success']:
            old_status = trade.status.value
            new_status = status_update['status']
            
            logger.info(f"Trade {trade_id} status comparison: old='{old_status}', new='{new_status}', tiger_status='{status_update.get('tiger_status')}'")
            logger.info(f"Status update data: {status_update}")
            
            # Map status if it changed
            if old_status != new_status:
                try:
                    trade.status = OrderStatus(new_status)
                    trade.filled_price = status_update.get('filled_price')
                    trade.filled_quantity = status_update.get('filled_quantity') 
                    trade.updated_at = datetime.utcnow()
                    
                    db.session.commit()
                    logger.info(f"Trade {trade_id} status updated: {old_status} -> {new_status}")
                    
                    # Send Discord notification for significant status changes
                    if new_status in ['filled', 'partially_filled']:
                        # Use the is_close_position flag to determine if this is a close trade
                        is_close_trade = getattr(trade, 'is_close_position', False)
                        
                        discord_notifier.send_order_notification(trade, new_status, is_close=is_close_trade)
                    
                except ValueError as e:
                    logger.error(f"Unknown status from Tiger: {new_status}, error: {e}")
            else:
                # Still update prices even if status same
                trade.filled_price = status_update.get('filled_price')
                trade.filled_quantity = status_update.get('filled_quantity')
                if status_update.get('filled_price'):
                    db.session.commit()
                    logger.info(f"Trade {trade_id} price updated: {status_update.get('filled_price')}")
    
    # Return real-time status from Tiger API if available
    real_time_status = trade.status.value
    real_time_filled_price = trade.filled_price
    real_time_filled_quantity = trade.filled_quantity
    tiger_status_info = 'Database'
    
    if trade.tiger_order_id:
        try:
            tiger_client_fresh = TigerClient()
            status_update_fresh = tiger_client_fresh.get_order_status(trade.tiger_order_id)
            if status_update_fresh['success']:
                real_time_status = status_update_fresh['status']
                real_time_filled_price = status_update_fresh.get('filled_price', 0) or trade.filled_price
                real_time_filled_quantity = status_update_fresh.get('filled_quantity', 0) or trade.filled_quantity
                tiger_status_info = f"Live: {status_update_fresh.get('tiger_status', 'Unknown')}"
        except Exception as e:
            logger.error(f"Error getting fresh status for trade {trade_id}: {str(e)}")
    
    return jsonify({
        'id': trade.id,
        'status': real_time_status,
        'filled_price': real_time_filled_price,
        'filled_quantity': real_time_filled_quantity,
        'error_message': trade.error_message,
        'updated_at': trade.updated_at.isoformat(),
        'tiger_status_info': tiger_status_info,
        'is_live_data': trade.tiger_order_id is not None
    })


@app.route('/closed-trades')
def closed_trades():
    """Display closed trades with realized P&L and forensic tracking.
    
    Uses Position(status=CLOSED) + PositionLeg as the single source of truth.
    """
    try:
        from pytz import timezone
        from models import Position, PositionStatus, PositionLeg, LegType
        eastern = timezone('US/Eastern')
        
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        symbol = request.args.get('symbol')
        account_type = request.args.get('account_type', 'real')
        
        query = Position.query.filter_by(account_type=account_type, status=PositionStatus.CLOSED)
        
        if symbol:
            query = query.filter(Position.symbol.ilike(f'%{symbol}%'))
        
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                query = query.filter(Position.closed_at >= start_dt)
            except ValueError:
                pass
        
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                query = query.filter(Position.closed_at <= end_dt)
            except ValueError:
                pass
        
        closed_positions = query.order_by(Position.closed_at.desc()).limit(100).all()
        
        trades_data = []
        for pos in closed_positions:
            exit_time_str = ''
            if pos.closed_at:
                exit_time_et = pos.closed_at.replace(tzinfo=timezone('UTC')).astimezone(eastern)
                exit_time_str = exit_time_et.strftime('%Y-%m-%d %H:%M:%S ET')
            
            entry_legs = pos.entry_legs
            exit_legs_list = pos.exit_legs
            
            entry_signals = []
            for i, leg in enumerate(entry_legs):
                entry_time_str = ''
                if leg.filled_at:
                    entry_et = leg.filled_at.replace(tzinfo=timezone('UTC')).astimezone(eastern)
                    entry_time_str = entry_et.strftime('%Y-%m-%d %H:%M:%S ET')
                
                contribution_pnl = None
                contribution_pct = None
                if pos.avg_exit_price and leg.price and leg.quantity:
                    if pos.side == 'long':
                        contribution_pnl = (pos.avg_exit_price - leg.price) * leg.quantity
                    else:
                        contribution_pnl = (leg.price - pos.avg_exit_price) * leg.quantity
                    if leg.price > 0:
                        contribution_pct = (contribution_pnl / (leg.price * leg.quantity)) * 100
                
                entry_signals.append({
                    'id': leg.id,
                    'entry_time': entry_time_str,
                    'entry_type': 'initial' if leg.leg_type == LegType.ENTRY else 'scaling',
                    'quantity': leg.quantity,
                    'entry_price': leg.price,
                    'entry_order_id': leg.tiger_order_id,
                    'contribution_pnl': contribution_pnl,
                    'contribution_pct': contribution_pct,
                    'indicator_trigger': leg.signal_indicator,
                    'original_signal': leg.signal_content
                })
            
            exit_method_str = 'unknown'
            exit_indicator = None
            exit_signal_content = None
            if exit_legs_list:
                last_exit = exit_legs_list[-1]
                if last_exit.exit_method:
                    exit_method_str = last_exit.exit_method.value
                exit_indicator = last_exit.signal_indicator
                exit_signal_content = last_exit.signal_content
            
            total_pnl_val = pos.realized_pnl or 0
            total_pnl_pct = pos.pnl_percent or 0
            
            trades_data.append({
                'id': pos.id,
                'symbol': pos.symbol,
                'side': pos.side,
                'exit_time': exit_time_str,
                'exit_price': pos.avg_exit_price,
                'exit_quantity': pos.total_exit_quantity,
                'exit_method': exit_method_str,
                'exit_indicator': exit_indicator,
                'total_pnl': total_pnl_val,
                'total_pnl_pct': total_pnl_pct,
                'avg_entry_price': pos.avg_entry_price,
                'entry_signals': entry_signals,
                'entry_count': len(entry_signals),
                'exit_signal_content': exit_signal_content
            })
        
        total_pnl = sum(t['total_pnl'] for t in trades_data)
        winning_trades = [t for t in trades_data if t['total_pnl'] > 0]
        losing_trades = [t for t in trades_data if t['total_pnl'] < 0]
        win_rate = (len(winning_trades) / len(trades_data) * 100) if trades_data else 0
        
        total_wins = sum(t['total_pnl'] for t in winning_trades)
        total_losses = abs(sum(t['total_pnl'] for t in losing_trades))
        profit_factor = (total_wins / total_losses) if total_losses > 0 else (float('inf') if total_wins > 0 else 0)
        
        return render_template('closed_trades.html',
                             trades=trades_data,
                             total_pnl=total_pnl,
                             total_trades=len(trades_data),
                             winning_count=len(winning_trades),
                             losing_count=len(losing_trades),
                             win_rate=win_rate,
                             profit_factor=profit_factor,
                             start_date=start_date,
                             end_date=end_date,
                             symbol_filter=symbol,
                             account_type=account_type)
    
    except Exception as e:
        logger.error(f"Error in closed_trades route: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f"Error: {str(e)}", 'error')
        return render_template('closed_trades.html',
                             trades=[],
                             total_pnl=0,
                             total_trades=0,
                             winning_count=0,
                             losing_count=0,
                             win_rate=0,
                             profit_factor=0,
                             start_date=None,
                             end_date=None,
                             symbol_filter=None,
                             account_type='real')

@app.route('/signal-logs')
def signal_logs():
    """Display all webhook signals received and Tiger API response status"""
    try:
        import json as json_lib
        endpoint_filter = request.args.get('ep')
        symbol_filter = request.args.get('symbol', '').strip().upper()
        status_filter = request.args.get('status', '').strip().lower()
        
        query = SignalLog.query.order_by(SignalLog.created_at.desc())
        
        if endpoint_filter:
            query = query.filter(SignalLog.endpoint == f'/{endpoint_filter}')
        
        if symbol_filter:
            query = query.filter(SignalLog.raw_signal.ilike(f'%"{symbol_filter}"%'))
        
        if status_filter:
            query = query.filter(SignalLog.tiger_status == status_filter)
        
        signals = query.limit(200).all()
        
        def extract_symbol(raw_signal):
            """Extract symbol from raw signal with multiple format support"""
            if not raw_signal:
                return None
            try:
                data = json_lib.loads(raw_signal)
                sym = data.get('ticker') or data.get('symbol')
                if sym:
                    return sym.upper().strip()
            except:
                pass
            # Try to extract from text format (e.g., "AAPL buy 100")
            import re
            match = re.search(r'"(?:ticker|symbol)"\s*:\s*"([A-Z]+)"', raw_signal, re.IGNORECASE)
            if match:
                return match.group(1).upper()
            return None
        
        for signal in signals:
            signal.symbol = extract_symbol(signal.raw_signal)
        
        all_symbols = set()
        for signal in signals:
            if signal.symbol:
                all_symbols.add(signal.symbol)
        
        return render_template('signal_logs.html',
                             signals=signals,
                             endpoint_filter=endpoint_filter,
                             symbol_filter=symbol_filter,
                             status_filter=status_filter,
                             all_symbols=sorted(all_symbols))
    
    except Exception as e:
        logger.error(f"Error in signal_logs route: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return render_template('signal_logs.html',
                             signals=[],
                             endpoint_filter=None,
                             symbol_filter='',
                             status_filter='',
                             all_symbols=[])

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'trading_enabled': get_config('TRADING_ENABLED', 'true') == 'true'
    })

@app.route('/api/debug/orders/<symbol>')
def debug_orders(symbol):
    """Debug route to check open orders for a symbol"""
    try:
        tiger_client = TigerClient()
        if not tiger_client:
            return jsonify({'error': 'Tiger client not available'}), 500
        
        result = tiger_client.get_open_orders_for_symbol(symbol)
        if not result['success']:
            return jsonify({'error': result['error']}), 500
            
        orders_info = []
        for order in result['orders']:
            order_info = {
                'id': getattr(order, 'id', 'unknown'),
                'action': getattr(order, 'action', 'unknown'), 
                'quantity': getattr(order, 'quantity', 'unknown'),
                'status': getattr(order, 'status', 'unknown'),
                'can_cancel': getattr(order, 'can_cancel', 'unknown'),
                'order_type': getattr(order, 'order_type', 'unknown'),
                'limit_price': getattr(order, 'limit_price', None),
                'aux_price': getattr(order, 'aux_price', None),
            }
            orders_info.append(order_info)
        
        return jsonify({
            'success': True,
            'symbol': symbol,
            'order_count': len(result['orders']),
            'orders': orders_info
        })
    
    except Exception as e:
        logger.error(f"Error debugging orders for {symbol}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cancel-orders/<symbol>', methods=['POST'])
def cancel_orders_for_symbol(symbol):
    """Cancel all open orders for a symbol"""
    try:
        tiger_client = TigerClient()
        if not tiger_client:
            return jsonify({'error': 'Tiger client not available'}), 500
        
        logger.info(f"Attempting to cancel all orders for {symbol}")
        result = tiger_client.force_cancel_all_orders_for_symbol(symbol)
        
        if result['success']:
            canceled_count = result.get('canceled_count', 0)
            total_orders = result.get('total_orders', 0)
            errors = result.get('errors', [])
            
            logger.info(f"Successfully canceled {canceled_count} out of {total_orders} orders for {symbol}")
            
            return jsonify({
                'success': True,
                'symbol': symbol,
                'canceled_count': canceled_count,
                'total_orders': total_orders,
                'errors': errors,
                'message': f"Canceled {canceled_count} out of {total_orders} orders for {symbol}"
            })
        else:
            logger.error(f"Failed to cancel orders for {symbol}: {result['error']}")
            return jsonify({'error': result['error']}), 500
    
    except Exception as e:
        logger.error(f"Error canceling orders for {symbol}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/trailing-stop')
def trailing_stop():
    """Trailing stop management page"""
    try:
        from models import TrailingStopPosition, TrailingStopConfig
        from trailing_stop_engine import get_trailing_stop_config
        from datetime import datetime, timedelta
        
        config = get_trailing_stop_config()
        
        ts_sort = request.args.get('sort', 'default')
        
        positions = TrailingStopPosition.query.filter_by(is_active=True).all()
        active_count = len(positions)
        switched_count = sum(1 for p in positions if p.has_switched_to_trailing)
        
        if positions:
            from trailing_stop_engine import get_realtime_price_with_websocket_fallback
            
            for pos in positions:
                clean_symbol = pos.symbol.replace('[PAPER]', '').strip()
                
                current_price = None
                trade_data = get_realtime_price_with_websocket_fallback(clean_symbol)
                if trade_data and trade_data.get('price'):
                    current_price = trade_data['price']
                
                pos._display_current_price = current_price
                
                if current_price and pos.entry_price:
                    if pos.side == 'long':
                        pos._display_profit_pct = (current_price - pos.entry_price) / pos.entry_price * 100
                    else:
                        pos._display_profit_pct = (pos.entry_price - current_price) / pos.entry_price * 100
                else:
                    pos._display_profit_pct = None
            
            if ts_sort == 'pnl_pct':
                positions.sort(key=lambda p: getattr(p, '_display_profit_pct', None) or 0, reverse=True)
        
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        triggered_today = TrailingStopPosition.query.filter(
            TrailingStopPosition.is_triggered == True,
            TrailingStopPosition.triggered_at >= today_start
        ).count()
        
        recent_triggers = TrailingStopPosition.query.filter_by(
            is_triggered=True
        ).order_by(TrailingStopPosition.triggered_at.desc()).limit(10).all()
        
        return render_template('trailing_stop.html',
                             config=config,
                             positions=positions,
                             active_count=active_count,
                             switched_count=switched_count,
                             triggered_today=triggered_today,
                             recent_triggers=recent_triggers,
                             sort_by=ts_sort)
    
    except Exception as e:
        logger.error(f"Error in trailing_stop route: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        from trailing_stop_engine import get_trailing_stop_config
        return render_template('trailing_stop.html',
                             config=get_trailing_stop_config(),
                             positions=[],
                             active_count=0,
                             switched_count=0,
                             triggered_today=0,
                             recent_triggers=[])


@app.route('/trailing-stop/config', methods=['GET', 'POST'])
def trailing_stop_config():
    """Trailing stop configuration page"""
    try:
        from trailing_stop_engine import get_trailing_stop_config
        
        config = get_trailing_stop_config()
        
        if request.method == 'POST':
            config.atr_period = int(request.form.get('atr_period', 14))
            config.tier_0_threshold = float(request.form.get('tier_0_threshold', 0.01))
            config.tier_1_threshold = float(request.form.get('tier_1_threshold', 0.03))
            config.tier_0_multiplier = float(request.form.get('tier_0_multiplier', 2.5))
            config.tier_1_multiplier = float(request.form.get('tier_1_multiplier', 2.0))
            config.tier_2_multiplier = float(request.form.get('tier_2_multiplier', 1.5))
            config.low_volatility_threshold = float(request.form.get('low_volatility_threshold', 0.008))
            config.high_volatility_threshold = float(request.form.get('high_volatility_threshold', 0.015))
            config.low_volatility_factor = float(request.form.get('low_volatility_factor', 0.8))
            config.mid_volatility_factor = float(request.form.get('mid_volatility_factor', 1.0))
            config.high_volatility_factor = float(request.form.get('high_volatility_factor', 1.2))
            config.dynamic_pct_tier1_upper = float(request.form.get('dynamic_pct_tier1_upper', 0.02))
            config.dynamic_pct_tier2_upper = float(request.form.get('dynamic_pct_tier2_upper', 0.05))
            config.dynamic_pct_tier1_percent = float(request.form.get('dynamic_pct_tier1_percent', 0.002))
            config.dynamic_pct_tier2_percent = float(request.form.get('dynamic_pct_tier2_percent', 0.005))
            config.switch_profit_ratio = float(request.form.get('switch_profit_ratio', 0.90))
            config.switch_profit_ratio_strong = float(request.form.get('switch_profit_ratio_strong', 0.95))
            config.post_switch_multiplier = float(request.form.get('post_switch_multiplier', 1.2))
            config.post_switch_trail_pct = float(request.form.get('post_switch_trail_pct', 0.05))
            config.check_interval_seconds = int(request.form.get('check_interval_seconds', 30))
            config.max_percent_stop = float(request.form.get('max_percent_stop', 0.008))
            config.trend_strength_threshold = float(request.form.get('trend_strength_threshold', 60.0))
            config.momentum_lookback = int(request.form.get('momentum_lookback', 5))
            config.atr_convergence_weight = float(request.form.get('atr_convergence_weight', 0.3))
            config.momentum_weight = float(request.form.get('momentum_weight', 0.4))
            config.consecutive_weight = float(request.form.get('consecutive_weight', 0.3))
            config.progressive_stop_enabled = 'progressive_stop_enabled' in request.form
            config.prog_tier1_profit = float(request.form.get('prog_tier1_profit', 0.01))
            config.prog_tier1_stop_at = float(request.form.get('prog_tier1_stop_at', 0.0))
            config.prog_tier2_profit = float(request.form.get('prog_tier2_profit', 0.03))
            config.prog_tier2_stop_at = float(request.form.get('prog_tier2_stop_at', 0.01))
            config.prog_tier3_profit = float(request.form.get('prog_tier3_profit', 0.05))
            config.prog_tier3_stop_at = float(request.form.get('prog_tier3_stop_at', 0.03))
            config.prog_tier4_profit = float(request.form.get('prog_tier4_profit', 0.08))
            config.prog_tier4_stop_at = float(request.form.get('prog_tier4_stop_at', 0.05))
            config.is_enabled = 'is_enabled' in request.form
            
            db.session.commit()
            flash('配置已保存', 'success')
            return redirect(url_for('trailing_stop'))
        
        return render_template('trailing_stop_config.html', config=config)
    
    except Exception as e:
        logger.error(f"Error in trailing_stop_config route: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('trailing_stop'))


@app.route('/api/trailing-stop/positions-data')
def api_trailing_stop_positions_data():
    """Return trailing stop positions data as JSON for live updates"""
    try:
        from models import TrailingStopPosition, TrailingStopConfig
        from trailing_stop_engine import get_trailing_stop_config, get_realtime_price_with_websocket_fallback
        from datetime import datetime, timedelta
        
        config = get_trailing_stop_config()
        positions = TrailingStopPosition.query.filter_by(is_active=True).all()
        active_count = len(positions)
        switched_count = sum(1 for p in positions if p.has_switched_to_trailing)
        
        positions_data = []
        if positions:
            for pos in positions:
                clean_symbol = pos.symbol.replace('[PAPER]', '').strip()
                
                current_price = None
                price_source = None
                
                trade_data = get_realtime_price_with_websocket_fallback(clean_symbol)
                if trade_data and trade_data.get('price'):
                    current_price = round(trade_data['price'], 2)
                    price_source = trade_data.get('source', 'api')
                
                if current_price is None and pos.current_profit_pct is not None and pos.entry_price:
                    if pos.side == 'long':
                        current_price = round(pos.entry_price * (1 + pos.current_profit_pct), 2)
                    else:
                        current_price = round(pos.entry_price * (1 - pos.current_profit_pct), 2)
                    price_source = 'db_cache'
                
                display_profit_pct = None
                if current_price and pos.entry_price:
                    if pos.side == 'long':
                        display_profit_pct = round((current_price - pos.entry_price) / pos.entry_price * 100, 2)
                    else:
                        display_profit_pct = round((pos.entry_price - current_price) / pos.entry_price * 100, 2)
                
                positions_data.append({
                    'id': pos.id,
                    'account_type': pos.account_type,
                    'symbol': clean_symbol,
                    'side': pos.side,
                    'entry_price': round(pos.entry_price, 2) if pos.entry_price else None,
                    'current_price': current_price,
                    'price_source': price_source,
                    'highest_price': round(pos.highest_price, 2) if pos.highest_price else (round(pos.entry_price, 2) if pos.entry_price else None),
                    'signal_stop_loss': round(pos.signal_stop_loss, 2) if pos.signal_stop_loss else None,
                    'fixed_stop_loss': round(pos.fixed_stop_loss, 2) if pos.fixed_stop_loss else None,
                    'fixed_take_profit': round(pos.fixed_take_profit, 2) if pos.fixed_take_profit else None,
                    'current_trailing_stop': round(pos.current_trailing_stop, 2) if pos.current_trailing_stop else None,
                    'display_profit_pct': display_profit_pct,
                    'current_profit_pct': round(pos.current_profit_pct * 100, 2) if pos.current_profit_pct is not None else None,
                    'has_switched_to_trailing': pos.has_switched_to_trailing,
                    'trend_strength': round(pos.trend_strength, 0) if pos.trend_strength is not None else None,
                    'momentum_score': round(pos.momentum_score, 1) if pos.momentum_score else 0,
                    'consecutive_highs': pos.consecutive_highs or 0,
                })
        
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        triggered_today = TrailingStopPosition.query.filter(
            TrailingStopPosition.is_triggered == True,
            TrailingStopPosition.triggered_at >= today_start
        ).count()
        
        return jsonify({
            'success': True,
            'active_count': active_count,
            'switched_count': switched_count,
            'triggered_today': triggered_today,
            'positions': positions_data
        })
    
    except Exception as e:
        logger.error(f"Error in trailing stop positions data: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/trailing-stop/check', methods=['POST'])
def api_trailing_stop_check():
    """Manually trigger trailing stop check for all positions"""
    try:
        from trailing_stop_engine import process_all_active_positions, get_cached_tiger_positions
        
        get_cached_tiger_positions(force_refresh=True)
        
        results = process_all_active_positions()
        
        deactivated_count = sum(1 for r in results if r.get('action') == 'deactivate')
        triggered_count = sum(1 for r in results if r.get('action') == 'trigger')
        switched_count = sum(1 for r in results if r.get('action') == 'switch')
        
        return jsonify({
            'success': True,
            'checked': len(results),
            'deactivated': deactivated_count,
            'triggered': triggered_count,
            'switched': switched_count,
            'message': f'检查完成: {len(results)}个持仓, {deactivated_count}个已停用, {triggered_count}个触发, {switched_count}个切换'
        })
    
    except Exception as e:
        logger.error(f"Error in trailing stop check: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/trailing-stop/deactivate/<int:position_id>', methods=['POST'])
def api_trailing_stop_deactivate(position_id):
    """Deactivate a trailing stop position"""
    try:
        from models import TrailingStopPosition
        
        position = TrailingStopPosition.query.get(position_id)
        if not position:
            return jsonify({'success': False, 'message': '未找到该持仓'}), 404
        
        position.is_active = False
        position.trigger_reason = "手动停用"
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'{position.symbol} 追踪已停用'
        })
    
    except Exception as e:
        logger.error(f"Error deactivating trailing stop: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/trailing-stop/cleanup-duplicates', methods=['POST'])
def api_trailing_stop_cleanup_duplicates():
    try:
        from trailing_stop_engine import cleanup_duplicate_trailing_stops
        data = request.get_json(silent=True) or {}
        dry_run = data.get('dry_run', True)
        result = cleanup_duplicate_trailing_stops(dry_run=dry_run)
        return jsonify({'success': True, **result})
    except Exception as e:
        logger.error(f"Error cleaning up duplicate trailing stops: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/trailing-stop/create', methods=['POST'])
def api_trailing_stop_create():
    """Create a new trailing stop position manually"""
    try:
        from trailing_stop_engine import create_trailing_stop_for_trade
        from models import TrailingStopMode
        
        data = request.get_json()
        
        symbol = data.get('symbol')
        side = data.get('side', 'long')
        entry_price = float(data.get('entry_price', 0))
        quantity = float(data.get('quantity', 0))
        account_type = data.get('account_type', 'real')
        fixed_stop_loss = float(data.get('fixed_stop_loss')) if data.get('fixed_stop_loss') else None
        fixed_take_profit = float(data.get('fixed_take_profit')) if data.get('fixed_take_profit') else None
        
        if not symbol or not entry_price or not quantity:
            return jsonify({'success': False, 'message': '缺少必要参数'}), 400
        
        position = create_trailing_stop_for_trade(
            trade_id=None,
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            quantity=quantity,
            account_type=account_type,
            fixed_stop_loss=fixed_stop_loss,
            fixed_take_profit=fixed_take_profit,
            creation_source='manual'
        )
        
        return jsonify({
            'success': True,
            'position_id': position.id,
            'message': f'{symbol} 追踪止损已创建'
        })
    
    except Exception as e:
        logger.error(f"Error creating trailing stop: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/trailing-stop/scheduler-status')
def api_trailing_stop_scheduler_status():
    """Get the status of the trailing stop scheduler"""
    try:
        from trailing_stop_scheduler import get_scheduler_status
        status = get_scheduler_status()
        return jsonify({
            'success': True,
            **status
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/tiger-open-orders')
def tiger_open_orders():
    """Page to display Tiger open orders (SL/TP sub-orders) with symbol search"""
    try:
        from tiger_client import TigerClient, TigerPaperClient

        account_type = request.args.get('account_type', 'paper')
        symbol_filter = request.args.get('symbol', '').strip().upper()

        if account_type == 'paper':
            tiger = TigerPaperClient()
        else:
            tiger = TigerClient()

        all_orders = []
        error_msg = None
        try:
            open_orders = tiger.client.get_open_orders(account=tiger.client_config.account)
            for order in open_orders:
                order_symbol = order.contract.symbol if hasattr(order, 'contract') and order.contract else 'N/A'
                if symbol_filter and symbol_filter not in order_symbol:
                    continue

                order_type_str = str(getattr(order, 'order_type', ''))
                if 'STP' in order_type_str:
                    role = 'Stop Loss'
                    role_class = 'danger'
                    display_price = getattr(order, 'aux_price', None) or getattr(order, 'limit_price', None)
                elif order_type_str == 'LMT':
                    role = 'Take Profit'
                    role_class = 'success'
                    display_price = getattr(order, 'limit_price', None)
                else:
                    role = order_type_str
                    role_class = 'secondary'
                    display_price = getattr(order, 'limit_price', None) or getattr(order, 'aux_price', None)

                limit_price = getattr(order, 'limit_price', None)
                aux_price = getattr(order, 'aux_price', None)

                all_orders.append({
                    'id': str(order.id) if hasattr(order, 'id') else None,
                    'symbol': order_symbol,
                    'action': getattr(order, 'action', None),
                    'order_type': order_type_str,
                    'role': role,
                    'role_class': role_class,
                    'quantity': getattr(order, 'quantity', None),
                    'display_price': display_price,
                    'limit_price': limit_price,
                    'aux_price': aux_price,
                    'status': str(getattr(order, 'status', '')),
                    'parent_id': str(order.parent_id) if hasattr(order, 'parent_id') and order.parent_id else None,
                    'time_in_force': str(getattr(order, 'time_in_force', '')),
                    'outside_rth': getattr(order, 'outside_rth', None),
                })
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error fetching Tiger open orders: {e}")

        symbols = sorted(set(o['symbol'] for o in all_orders))

        return render_template('tiger_open_orders.html',
                             orders=all_orders,
                             account_type=account_type,
                             symbol_filter=symbol_filter,
                             symbols=symbols,
                             order_count=len(all_orders),
                             error_msg=error_msg)
    except Exception as e:
        logger.error(f"Error in tiger_open_orders page: {e}")
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('index'))


@app.route('/api/tiger/open-orders')
def api_tiger_open_orders():
    """Query Tiger API for open orders (including stop loss / take profit)"""
    try:
        from tiger_client import TigerClient, TigerPaperClient
        
        account_type = request.args.get('account', 'paper')
        symbol_filter = request.args.get('symbol', None)
        
        if account_type == 'paper':
            tiger = TigerPaperClient()
        else:
            tiger = TigerClient()
        
        open_orders = tiger.client.get_open_orders(account=tiger.client_config.account)
        
        result = []
        for order in open_orders:
            order_symbol = order.contract.symbol if hasattr(order, 'contract') else 'N/A'
            
            if symbol_filter and order_symbol != symbol_filter:
                continue
            
            result.append({
                'id': str(order.id) if hasattr(order, 'id') else None,
                'symbol': order_symbol,
                'action': order.action if hasattr(order, 'action') else None,
                'order_type': str(order.order_type) if hasattr(order, 'order_type') else None,
                'quantity': order.quantity if hasattr(order, 'quantity') else None,
                'limit_price': order.limit_price if hasattr(order, 'limit_price') else None,
                'aux_price': order.aux_price if hasattr(order, 'aux_price') else None,
                'status': str(order.status) if hasattr(order, 'status') else None,
                'parent_id': str(order.parent_id) if hasattr(order, 'parent_id') and order.parent_id else None,
                'time_in_force': str(order.time_in_force) if hasattr(order, 'time_in_force') else None,
                'outside_rth': order.outside_rth if hasattr(order, 'outside_rth') else None,
            })
        
        return jsonify({
            'success': True,
            'account': account_type,
            'count': len(result),
            'orders': result
        })
    except Exception as e:
        logger.error(f"Error fetching open orders: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/trailing-stop/sync', methods=['POST'])
def api_trailing_stop_sync():
    """Synchronize all active trailing stop positions with Tiger API order status"""
    try:
        from trailing_stop_engine import sync_all_active_positions
        from tiger_client import TigerClient
        
        # Get tiger client
        tiger = TigerClient()
        if not tiger:
            return jsonify({
                'success': False,
                'message': 'Tiger client not available'
            }), 500
        
        # Run sync
        results = sync_all_active_positions(tiger)
        
        # Count issues and fixes
        total_positions = len(results)
        total_issues = sum(len(r.get('issues_found', [])) for r in results)
        total_fixes = sum(len(r.get('fixes_applied', [])) for r in results)
        
        return jsonify({
            'success': True,
            'positions_checked': total_positions,
            'issues_found': total_issues,
            'fixes_applied': total_fixes,
            'details': results,
            'message': f'同步完成: 检查{total_positions}个仓位, 发现{total_issues}个问题, 修复{total_fixes}个'
        })
    
    except Exception as e:
        logger.error(f"Error syncing trailing stops: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/sync-completed-trades', methods=['POST'])
def api_sync_completed_trades():
    """Sync CompletedTrade records with ClosedPosition data to fix is_open status"""
    try:
        from models import CompletedTrade, ClosedPosition
        
        synced_count = 0
        
        # Find all open CompletedTrades
        open_trades = CompletedTrade.query.filter_by(is_open=True).all()
        
        for trade in open_trades:
            # Try to find matching ClosedPosition
            closed_pos = ClosedPosition.query.filter_by(
                symbol=trade.symbol,
                account_type=trade.account_type
            ).order_by(ClosedPosition.exit_time.desc()).first()
            
            if closed_pos and closed_pos.exit_time:
                # Check if closed_pos exit_time is after trade entry_time
                if trade.entry_time and closed_pos.exit_time > trade.entry_time:
                    trade.is_open = False
                    trade.exit_time = closed_pos.exit_time
                    trade.exit_price = closed_pos.exit_price
                    trade.exit_quantity = closed_pos.exit_quantity
                    trade.exit_method = closed_pos.exit_method
                    trade.pnl_amount = closed_pos.total_pnl
                    trade.pnl_percent = closed_pos.total_pnl_pct
                    trade.remaining_quantity = 0
                    
                    if trade.entry_time:
                        hold_seconds = int((closed_pos.exit_time - trade.entry_time).total_seconds())
                        trade.hold_duration_seconds = hold_seconds
                    
                    synced_count += 1
                    logger.info(f"Synced CompletedTrade #{trade.id} ({trade.symbol}) with ClosedPosition #{closed_pos.id}")
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'synced_count': synced_count,
            'message': f'同步完成: 更新了{synced_count}条CompletedTrade记录'
        })
        
    except Exception as e:
        logger.error(f"Error syncing completed trades: {str(e)}")
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/fix-historical-data', methods=['POST'])
def api_fix_historical_data():
    """
    One-time fix: Pull historical filled orders from Tiger API and update CompletedTrade status.
    This fixes is_open=True records that should be closed.
    
    POST body (optional):
    - days: Number of days to look back (default 7)
    - account_type: 'paper', 'real', or 'both' (default 'both')
    - dry_run: If true, only report what would be fixed (default false)
    """
    try:
        from models import CompletedTrade, ClosedPosition, Trade, ExitMethod
        from datetime import datetime, timedelta
        
        data = request.get_json() or {}
        days = data.get('days', 7)
        account_type_filter = data.get('account_type', 'both')
        dry_run = data.get('dry_run', False)
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        results = {
            'fixed_count': 0,
            'skipped_count': 0,
            'error_count': 0,
            'details': [],
            'dry_run': dry_run
        }
        
        account_types = []
        if account_type_filter in ['paper', 'both']:
            account_types.append(('paper', TigerPaperClient))
        if account_type_filter in ['real', 'both']:
            account_types.append(('real', TigerClient))
        
        for account_type, client_class in account_types:
            try:
                client = client_class()
                filled_result = client.get_filled_orders(start_date=start_date, end_date=end_date)
                
                if not filled_result.get('success'):
                    results['details'].append(f"{account_type}: Failed to get filled orders")
                    continue
                
                orders = filled_result.get('orders', [])
                results['details'].append(f"{account_type}: Found {len(orders)} filled orders")
                
                # Group by symbol to find exit orders
                for order in orders:
                    try:
                        order_id = str(order.get('order_id', ''))
                        symbol = order.get('symbol', '')
                        action = order.get('action', '').upper()
                        avg_fill_price = order.get('avg_fill_price', 0)
                        filled_qty = order.get('filled', 0)
                        fill_time = order.get('trade_time')
                        
                        # Check if this is a SELL order (exit for long position)
                        if action != 'SELL':
                            continue
                        
                        open_trade = CompletedTrade.query.filter(
                            CompletedTrade.symbol == symbol,
                            CompletedTrade.account_type == account_type,
                            CompletedTrade.is_open == True
                        ).order_by(CompletedTrade.entry_time.asc()).first()
                        
                        if not open_trade:
                            continue
                        
                        # Parse fill time
                        exit_time = datetime.utcnow()
                        if fill_time:
                            try:
                                if isinstance(fill_time, (int, float)):
                                    exit_time = datetime.fromtimestamp(fill_time / 1000)
                                elif isinstance(fill_time, str):
                                    exit_time = datetime.fromisoformat(fill_time.replace('Z', '+00:00'))
                            except:
                                pass
                        
                        # Calculate P&L
                        entry_price = open_trade.entry_price or 0
                        pnl = None
                        pnl_pct = None
                        if entry_price and avg_fill_price and open_trade.entry_quantity:
                            pnl = (avg_fill_price - entry_price) * open_trade.entry_quantity
                            pnl_pct = (avg_fill_price - entry_price) / entry_price * 100
                        
                        exit_method = ExitMethod.WEBHOOK_SIGNAL
                        try:
                            from models import OrderTracker, OrderRole
                            exit_tracker = OrderTracker.query.filter(
                                OrderTracker.symbol == clean_symbol,
                                OrderTracker.account_type == open_trade.account_type,
                                OrderTracker.status == 'FILLED',
                                OrderTracker.role.in_([OrderRole.EXIT_TRAILING, OrderRole.EXIT_SIGNAL, OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT])
                            ).order_by(OrderTracker.updated_at.desc()).first()
                            if exit_tracker:
                                tracker_exit_map = {
                                    OrderRole.EXIT_TRAILING: ExitMethod.TRAILING_STOP,
                                    OrderRole.EXIT_SIGNAL: ExitMethod.WEBHOOK_SIGNAL,
                                    OrderRole.STOP_LOSS: ExitMethod.STOP_LOSS,
                                    OrderRole.TAKE_PROFIT: ExitMethod.TAKE_PROFIT,
                                }
                                exit_method = tracker_exit_map.get(exit_tracker.role, ExitMethod.WEBHOOK_SIGNAL)
                                logger.info(f"Exit method from OrderTracker: {exit_method.value} (role={exit_tracker.role.value})")
                        except Exception as tracker_err:
                            logger.debug(f"Could not check OrderTracker for exit method: {tracker_err}")
                        if exit_method == ExitMethod.WEBHOOK_SIGNAL:
                            if open_trade.original_stop_loss and avg_fill_price <= open_trade.original_stop_loss * 1.01:
                                exit_method = ExitMethod.STOP_LOSS
                            elif open_trade.original_take_profit and avg_fill_price >= open_trade.original_take_profit * 0.99:
                                exit_method = ExitMethod.TAKE_PROFIT
                        
                        if dry_run:
                            pnl_str = f"{pnl:.2f}" if pnl else "0"
                            pnl_pct_str = f"{pnl_pct:.2f}" if pnl_pct else "0"
                            results['details'].append(
                                f"Would fix: {db_symbol} entry@${entry_price:.2f} -> exit@${avg_fill_price:.2f} "
                                f"P&L=${pnl_str} ({pnl_pct_str}%)"
                            )
                            results['fixed_count'] += 1
                        else:
                            # Update CompletedTrade
                            open_trade.is_open = False
                            open_trade.exit_time = exit_time
                            open_trade.exit_price = avg_fill_price
                            open_trade.exit_quantity = filled_qty
                            open_trade.exit_method = exit_method
                            open_trade.pnl_amount = pnl
                            open_trade.pnl_percent = pnl_pct
                            open_trade.remaining_quantity = 0
                            
                            if open_trade.entry_time:
                                hold_seconds = int((exit_time - open_trade.entry_time).total_seconds())
                                open_trade.hold_duration_seconds = hold_seconds
                            
                            db.session.commit()
                            
                            pnl_str = f"{pnl:.2f}" if pnl else "0"
                            pnl_pct_str = f"{pnl_pct:.2f}" if pnl_pct else "0"
                            results['details'].append(
                                f"Fixed: {db_symbol} #{open_trade.id} exit@${avg_fill_price:.2f} "
                                f"P&L=${pnl_str} ({pnl_pct_str}%)"
                            )
                            results['fixed_count'] += 1
                        
                    except Exception as order_err:
                        results['error_count'] += 1
                        results['details'].append(f"Error processing order: {str(order_err)}")
                        continue
                        
            except Exception as client_err:
                results['details'].append(f"{account_type}: Client error - {str(client_err)}")
                continue
        
        return jsonify({
            'success': True,
            'message': f'修复完成: 更新了{results["fixed_count"]}条记录' + (' (预览模式)' if dry_run else ''),
            **results
        })
        
    except Exception as e:
        logger.error(f"Error fixing historical data: {str(e)}")
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/close-orphan-trades', methods=['POST'])
def api_close_orphan_trades():
    """
    Close CompletedTrade records that are marked as open but position no longer exists in Tiger.
    Checks actual Tiger positions and marks non-existent ones as closed.
    Now also fetches historical filled orders to find actual exit price for P&L calculation.
    
    POST body (optional):
    - account_type: 'paper', 'real', or 'both' (default 'both')
    - dry_run: If true, only report what would be closed (default false)
    - days: Number of days to look back for historical orders (default 30)
    """
    try:
        from models import CompletedTrade, ExitMethod
        from datetime import datetime, timedelta
        
        data = request.get_json() or {}
        account_type_filter = data.get('account_type', 'both')
        dry_run = data.get('dry_run', False)
        days = data.get('days', 30)
        
        results = {
            'closed_count': 0,
            'still_open_count': 0,
            'error_count': 0,
            'details': [],
            'dry_run': dry_run
        }
        
        # Get current positions from Tiger
        tiger_positions = {'paper': set(), 'real': set()}
        
        # Get historical filled orders to find exit prices
        historical_exits = {'paper': {}, 'real': {}}  # symbol -> list of exit orders
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        if account_type_filter in ['paper', 'both']:
            try:
                paper_client = TigerPaperClient()
                paper_result = paper_client.get_positions()
                if paper_result.get('success'):
                    for pos in paper_result.get('positions', []):
                        tiger_positions['paper'].add(pos.get('symbol', ''))
                    results['details'].append(f"Paper持仓: {list(tiger_positions['paper'])}")
                
                # Get historical filled orders for exit price lookup
                filled_result = paper_client.get_filled_orders(start_date=start_date, end_date=end_date)
                if filled_result.get('success'):
                    for order in filled_result.get('orders', []):
                        if order.get('action', '').upper() == 'SELL':
                            symbol = order.get('symbol', '')
                            if symbol not in historical_exits['paper']:
                                historical_exits['paper'][symbol] = []
                            historical_exits['paper'][symbol].append({
                                'price': order.get('avg_fill_price', 0),
                                'quantity': order.get('filled', 0),
                                'time': order.get('trade_time')
                            })
                    results['details'].append(f"Paper历史卖出订单: {len(filled_result.get('orders', []))}条")
            except Exception as e:
                results['details'].append(f"Paper数据获取失败: {str(e)}")
        
        if account_type_filter in ['real', 'both']:
            try:
                real_client = TigerClient()
                real_result = real_client.get_positions()
                if real_result.get('success'):
                    for pos in real_result.get('positions', []):
                        tiger_positions['real'].add(pos.get('symbol', ''))
                    results['details'].append(f"Real持仓: {list(tiger_positions['real'])}")
                
                # Get historical filled orders for exit price lookup
                filled_result = real_client.get_filled_orders(start_date=start_date, end_date=end_date)
                if filled_result.get('success'):
                    for order in filled_result.get('orders', []):
                        if order.get('action', '').upper() == 'SELL':
                            symbol = order.get('symbol', '')
                            if symbol not in historical_exits['real']:
                                historical_exits['real'][symbol] = []
                            historical_exits['real'][symbol].append({
                                'price': order.get('avg_fill_price', 0),
                                'quantity': order.get('filled', 0),
                                'time': order.get('trade_time')
                            })
                    results['details'].append(f"Real历史卖出订单: {len(filled_result.get('orders', []))}条")
            except Exception as e:
                results['details'].append(f"Real数据获取失败: {str(e)}")
        
        # Find all open CompletedTrades
        open_trades = CompletedTrade.query.filter_by(is_open=True).all()
        results['details'].append(f"Open CompletedTrade总数: {len(open_trades)}")
        
        for trade in open_trades:
            try:
                # Skip if account type doesn't match filter
                if account_type_filter != 'both' and trade.account_type != account_type_filter:
                    continue
                
                current_positions = tiger_positions.get(trade.account_type, set())
                
                if trade.symbol in current_positions:
                    # Position still exists, skip
                    results['still_open_count'] += 1
                    continue
                
                # Position doesn't exist in Tiger - mark as closed
                # Try to find exit price from historical orders
                exit_orders = historical_exits.get(trade.account_type, {}).get(clean_symbol, [])
                
                exit_price = None
                exit_time = datetime.utcnow()
                exit_method = ExitMethod.MANUAL
                
                # Find the most recent SELL order for this symbol after entry time
                if exit_orders and trade.entry_time:
                    for exit_order in sorted(exit_orders, key=lambda x: x.get('time') or 0, reverse=True):
                        order_time = exit_order.get('time')
                        if order_time:
                            try:
                                if isinstance(order_time, (int, float)):
                                    order_datetime = datetime.fromtimestamp(order_time / 1000)
                                else:
                                    order_datetime = datetime.fromisoformat(str(order_time).replace('Z', '+00:00'))
                                
                                if order_datetime > trade.entry_time:
                                    exit_price = exit_order.get('price')
                                    exit_time = order_datetime
                                    exit_method = ExitMethod.WEBHOOK_SIGNAL
                                    break
                            except:
                                pass
                
                # Fallback: use most recent exit order if no time match
                if not exit_price and exit_orders:
                    exit_price = exit_orders[0].get('price')
                    exit_method = ExitMethod.WEBHOOK_SIGNAL
                
                # Calculate P&L
                pnl = None
                pnl_pct = None
                entry_price = trade.entry_price or 0
                
                if exit_price and entry_price and trade.entry_quantity:
                    pnl = (exit_price - entry_price) * trade.entry_quantity
                    pnl_pct = (exit_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                
                if dry_run:
                    entry_str = f"{entry_price:.2f}" if entry_price else "0"
                    exit_str = f"{exit_price:.2f}" if exit_price else "未知"
                    pnl_str = f"{pnl:.2f}" if pnl else "0"
                    pnl_pct_str = f"{pnl_pct:.2f}" if pnl_pct else "0"
                    results['details'].append(
                        f"Would close: {trade.symbol} #{trade.id} entry@${entry_str} -> exit@${exit_str} P&L=${pnl_str} ({pnl_pct_str}%)"
                    )
                    results['closed_count'] += 1
                else:
                    trade.is_open = False
                    trade.exit_time = exit_time
                    trade.exit_method = exit_method
                    trade.remaining_quantity = 0
                    
                    if exit_price:
                        trade.exit_price = exit_price
                        trade.pnl_amount = pnl
                        trade.pnl_percent = pnl_pct
                    else:
                        # No exit price found, use entry price as placeholder
                        trade.exit_price = entry_price
                        trade.pnl_amount = 0
                        trade.pnl_percent = 0
                    
                    if trade.entry_time:
                        hold_seconds = int((exit_time - trade.entry_time).total_seconds())
                        trade.hold_duration_seconds = hold_seconds
                    
                    db.session.commit()
                    
                    exit_str = f"{exit_price:.2f}" if exit_price else "未知"
                    pnl_str = f"{pnl:.2f}" if pnl else "0"
                    results['details'].append(
                        f"Closed: {trade.symbol} #{trade.id} exit@${exit_str} P&L=${pnl_str}"
                    )
                    results['closed_count'] += 1
                    
            except Exception as trade_err:
                results['error_count'] += 1
                results['details'].append(f"Error: {trade.symbol} - {str(trade_err)}")
                db.session.rollback()
                continue
        
        return jsonify({
            'success': True,
            'message': f'完成: 关闭了{results["closed_count"]}条记录, {results["still_open_count"]}条仍持仓' + 
                      (' (预览模式)' if dry_run else ''),
            **results
        })
        
    except Exception as e:
        logger.error(f"Error closing orphan trades: {str(e)}")
        db.session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/test_close_order', methods=['POST'])
def test_close_order():
    """Test Tiger API close order functionality (dry run - does not actually place order)"""
    try:
        data = request.get_json() or {}
        account_type = data.get('account_type', 'paper')
        symbol = data.get('symbol', 'AAPL')
        
        if account_type == 'paper':
            tiger = TigerPaperClient()
            account_name = 'Paper'
        else:
            tiger = TigerClient()
            account_name = 'Real'
        
        result = {
            'account_type': account_type,
            'account_name': account_name,
            'symbol': symbol,
            'tiger_initialized': tiger is not None,
            'tiger_client_exists': getattr(tiger, 'client', None) is not None if tiger else False,
            'client_config_exists': getattr(tiger, 'client_config', None) is not None if tiger else False,
        }
        
        if tiger and tiger.client_config:
            result['account_id'] = tiger.client_config.account
            result['tiger_id'] = tiger.client_config.tiger_id
        
        if tiger and tiger.client:
            result['ready_to_place_order'] = True
            result['message'] = f'{account_name}账户 Tiger客户端初始化成功，可以执行平仓'
        else:
            result['ready_to_place_order'] = False
            result['message'] = f'{account_name}账户 Tiger客户端初始化失败，无法执行平仓'
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error testing close order: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/trade-analytics')
def trade_analytics():
    """Trade analytics page - entry-based flow analysis using Position + PositionLeg.
    
    Each row = one entry PositionLeg (ENTRY or ADD leg).
    Exit info comes from the parent Position.
    """
    from models import Position, PositionStatus, PositionLeg, LegType, TigerHolding, ExitMethod
    from datetime import datetime, timedelta
    import pytz
    import json
    
    account_type = request.args.get('account_type', 'paper')
    signal_grade_filter = request.args.get('signal_grade', '')
    exit_method_filter = request.args.get('exit_method', '')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    symbol_search = request.args.get('symbol', '').strip().upper()
    status_filter = request.args.get('status', '')
    page = request.args.get('page', 1, type=int)
    per_page = 30
    
    query = PositionLeg.query.join(
        Position, PositionLeg.position_id == Position.id
    ).filter(
        Position.account_type == account_type,
        PositionLeg.leg_type.in_([LegType.ENTRY, LegType.ADD])
    )
    
    if status_filter == 'open':
        query = query.filter(Position.status == PositionStatus.OPEN)
    elif status_filter == 'closed':
        query = query.filter(Position.status == PositionStatus.CLOSED)
    
    if symbol_search:
        query = query.filter(Position.symbol.ilike(f'%{symbol_search}%'))
    
    if signal_grade_filter:
        query = query.filter(PositionLeg.signal_grade == signal_grade_filter)
    
    if exit_method_filter:
        try:
            em = ExitMethod(exit_method_filter)
            exit_pos_ids = db.session.query(PositionLeg.position_id).filter(
                PositionLeg.leg_type == LegType.EXIT,
                PositionLeg.exit_method == em
            ).distinct()
            query = query.filter(Position.id.in_(exit_pos_ids))
        except ValueError:
            pass
    
    if start_date:
        try:
            sd = datetime.strptime(start_date, '%Y-%m-%d')
            query = query.filter(PositionLeg.filled_at >= sd)
        except ValueError:
            pass
    
    if end_date:
        try:
            ed = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            query = query.filter(PositionLeg.filled_at < ed)
        except ValueError:
            pass
    
    query = query.order_by(PositionLeg.filled_at.desc().nullslast(), PositionLeg.created_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    entry_legs = pagination.items
    
    eastern = pytz.timezone('US/Eastern')
    holdings_map = {}
    holdings = TigerHolding.query.filter_by(account_type=account_type).all()
    for h in holdings:
        holdings_map[h.symbol] = h
    
    pos_ids = list(set(leg.position_id for leg in entry_legs))
    pos_map = {}
    if pos_ids:
        for pos in Position.query.filter(Position.id.in_(pos_ids)).all():
            pos_map[pos.id] = pos
    
    display_entries = []
    for leg in entry_legs:
        pos = pos_map.get(leg.position_id)
        if not pos:
            continue
        is_open = pos.status == PositionStatus.OPEN
        
        clean_symbol = pos.symbol.replace('[PAPER]', '').strip() if pos.symbol else pos.symbol
        
        entry_time_et = None
        if leg.filled_at:
            try:
                entry_time_et = leg.filled_at.replace(tzinfo=pytz.UTC).astimezone(eastern)
            except:
                entry_time_et = leg.filled_at
        
        exit_price = None
        exit_time_et = None
        exit_method_val = None
        hold_duration_seconds = None
        pnl_amount = None
        pnl_percent = None
        
        if pos.status == PositionStatus.CLOSED:
            exit_price = pos.avg_exit_price
            exit_legs_list = pos.exit_legs
            if exit_legs_list:
                last_exit = exit_legs_list[-1]
                exit_method_val = last_exit.exit_method
                if not exit_price and last_exit.price:
                    exit_price = last_exit.price
            if pos.closed_at:
                try:
                    exit_time_et = pos.closed_at.replace(tzinfo=pytz.UTC).astimezone(eastern)
                except:
                    exit_time_et = pos.closed_at
            
            if exit_price and leg.price and leg.quantity:
                if pos.side == 'long':
                    pnl_amount = (exit_price - leg.price) * leg.quantity
                else:
                    pnl_amount = (leg.price - exit_price) * leg.quantity
                if leg.price > 0:
                    pnl_percent = (pnl_amount / (leg.price * leg.quantity)) * 100
            elif pos.realized_pnl is not None and not exit_price:
                pnl_amount = pos.realized_pnl
                if pos.avg_entry_price and pos.total_entry_quantity and pos.avg_entry_price > 0:
                    pnl_percent = (pos.realized_pnl / (pos.avg_entry_price * pos.total_entry_quantity)) * 100
            
            if leg.filled_at and pos.closed_at:
                hold_duration_seconds = (pos.closed_at - leg.filled_at).total_seconds()
        
        current_price = None
        unrealized_pnl = None
        unrealized_pnl_pct = None
        if is_open:
            holding = holdings_map.get(clean_symbol)
            if holding:
                current_price = holding.latest_price
                if current_price and leg.price and leg.quantity:
                    if pos.side == 'long':
                        unrealized_pnl = (current_price - leg.price) * leg.quantity
                    else:
                        unrealized_pnl = (leg.price - current_price) * leg.quantity
                    if leg.price > 0:
                        unrealized_pnl_pct = (unrealized_pnl / (leg.price * leg.quantity)) * 100
        
        display_signal_type = _extract_signal_type(leg.signal_content, leg.signal_indicator)
        
        display_entry = {
            'id': leg.id,
            'position_id': pos.id,
            'position_key': pos.position_key,
            'symbol': clean_symbol,
            'side': pos.side,
            'quantity': leg.quantity,
            'is_scaling': leg.leg_type == LegType.ADD,
            'is_open': is_open,
            'entry_price': leg.price,
            'entry_time_et': entry_time_et,
            'entry_order_id': leg.tiger_order_id,
            'signal_grade': leg.signal_grade,
            'signal_score': leg.signal_score,
            'signal_timeframe': leg.signal_timeframe,
            'display_signal_type': display_signal_type,
            'signal_indicator': leg.signal_indicator,
            'raw_json': leg.signal_content,
            'stop_price': leg.stop_price,
            'take_profit_price': leg.take_profit_price,
            'current_price': current_price,
            'unrealized_pnl': unrealized_pnl,
            'unrealized_pnl_pct': unrealized_pnl_pct,
            'exit_price': exit_price,
            'exit_time_et': exit_time_et,
            'exit_method': exit_method_val,
            'pnl_amount': pnl_amount,
            'pnl_percent': pnl_percent,
            'hold_duration_seconds': hold_duration_seconds,
        }
        display_entries.append(display_entry)
    
    stats_query = Position.query.filter_by(
        account_type=account_type, status=PositionStatus.CLOSED
    ).filter(Position.realized_pnl != None)

    if symbol_search:
        stats_query = stats_query.filter(Position.symbol.ilike(f'%{symbol_search}%'))

    if start_date:
        try:
            sd = datetime.strptime(start_date, '%Y-%m-%d')
            stats_query = stats_query.filter(Position.closed_at >= sd)
        except ValueError:
            pass

    if end_date:
        try:
            ed = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            stats_query = stats_query.filter(Position.closed_at < ed)
        except ValueError:
            pass

    if signal_grade_filter:
        stats_query = stats_query.filter(
            Position.id.in_(
                db.session.query(PositionLeg.position_id).filter(
                    PositionLeg.leg_type == LegType.ENTRY,
                    PositionLeg.signal_grade == signal_grade_filter
                )
            )
        )

    if exit_method_filter:
        try:
            em = ExitMethod(exit_method_filter)
            stats_query = stats_query.filter(
                Position.id.in_(
                    db.session.query(PositionLeg.position_id).filter(
                        PositionLeg.leg_type == LegType.EXIT,
                        PositionLeg.exit_method == em
                    )
                )
            )
        except ValueError:
            pass

    closed_positions = stats_query.all()
    
    total_pnl = 0
    winning_pnls = []
    losing_pnls = []
    grade_data = {}
    exit_method_data = {}
    
    for pos in closed_positions:
        pnl = pos.realized_pnl
        if pnl is None:
            continue
        total_pnl += pnl
        if pnl >= 0:
            winning_pnls.append(pnl)
        else:
            losing_pnls.append(pnl)
        
        first_entry_legs = pos.entry_legs
        grade_key = first_entry_legs[0].signal_grade if first_entry_legs and first_entry_legs[0].signal_grade else 'Unknown'
        if grade_key not in grade_data:
            grade_data[grade_key] = {'count': 0, 'pnl': 0, 'wins': 0}
        grade_data[grade_key]['count'] += 1
        grade_data[grade_key]['pnl'] += pnl
        if pnl > 0:
            grade_data[grade_key]['wins'] += 1
        
        exit_legs_list = pos.exit_legs
        method_key = exit_legs_list[-1].exit_method.value if exit_legs_list and exit_legs_list[-1].exit_method else 'unknown'
        if method_key not in exit_method_data:
            exit_method_data[method_key] = {'count': 0, 'pnl': 0, 'wins': 0}
        exit_method_data[method_key]['count'] += 1
        exit_method_data[method_key]['pnl'] += pnl
        if pnl > 0:
            exit_method_data[method_key]['wins'] += 1
    
    total_closed = len(closed_positions)
    win_rate = (len(winning_pnls) / total_closed * 100) if total_closed else 0
    avg_win = sum(winning_pnls) / len(winning_pnls) if winning_pnls else 0
    avg_loss = abs(sum(losing_pnls) / len(losing_pnls)) if losing_pnls else 0
    gross_win = sum(winning_pnls) if winning_pnls else 0
    gross_loss = abs(sum(losing_pnls)) if losing_pnls else 0
    profit_factor = gross_win / gross_loss if gross_loss > 0 else 99999.0 if gross_win > 0 else 0.0
    
    grade_stats = {}
    for g in ['A', 'B', 'C', 'Unknown']:
        if g in grade_data:
            d = grade_data[g]
            grade_stats[g] = {
                'count': d['count'],
                'pnl': d['pnl'],
                'win_rate': d['wins'] / d['count'] * 100 if d['count'] else 0
            }
    
    exit_stats = {}
    for mk, md in exit_method_data.items():
        exit_stats[mk] = {
            'count': md['count'],
            'pnl': md['pnl'],
            'win_rate': md['wins'] / md['count'] * 100 if md['count'] else 0
        }
    
    return render_template('trade_analytics.html',
        trades=display_entries,
        pagination=pagination,
        account_type=account_type,
        signal_grade=signal_grade_filter,
        exit_method_filter=exit_method_filter,
        start_date=start_date,
        end_date=end_date,
        symbol_search=symbol_search,
        status_filter=status_filter,
        total_pnl=total_pnl,
        total_trades=total_closed,
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        profit_factor=profit_factor,
        grade_stats=grade_stats,
        exit_stats=exit_stats,
        ExitMethod=ExitMethod
    )


@app.route('/backfill-signal-data', methods=['POST'])
def backfill_signal_data():
    from signal_utils import parse_signal_fields
    from sqlalchemy import or_
    from models import PositionLeg, LegType, OrderTracker, EntrySignalRecord

    updated = 0
    skipped = 0
    no_data = 0
    grade_fixed = 0
    try:
        target_legs = PositionLeg.query.filter(
            PositionLeg.leg_type.in_([LegType.ENTRY, LegType.ADD]),
            or_(
                PositionLeg.signal_content == None,
                PositionLeg.signal_grade == None,
            ),
        ).all()

        for leg in target_legs:
            if leg.signal_content and not leg.signal_grade:
                parsed = parse_signal_fields(leg.signal_content)
                if parsed['signal_grade']:
                    leg.signal_grade = parsed['signal_grade']
                    if parsed['signal_score'] is not None:
                        leg.signal_score = parsed['signal_score']
                    if parsed['signal_indicator'] and not leg.signal_indicator:
                        leg.signal_indicator = parsed['signal_indicator']
                    if parsed['signal_timeframe'] and not leg.signal_timeframe:
                        leg.signal_timeframe = parsed['signal_timeframe']
                    grade_fixed += 1
                continue

            if not leg.tiger_order_id:
                no_data += 1
                continue

            tracker = OrderTracker.query.filter_by(tiger_order_id=leg.tiger_order_id).first()
            if not tracker or not tracker.trade_id:
                trade = Trade.query.filter_by(tiger_order_id=leg.tiger_order_id).first()
                if not trade or not trade.signal_data:
                    entry_signal = EntrySignalRecord.query.filter_by(
                        entry_order_id=leg.tiger_order_id
                    ).first()
                    if entry_signal and entry_signal.raw_json:
                        parsed = parse_signal_fields(entry_signal.raw_json)
                        leg.signal_content = parsed['signal_content']
                        leg.signal_grade = parsed['signal_grade'] or entry_signal.signal_grade
                        leg.signal_score = parsed['signal_score'] if parsed['signal_score'] is not None else entry_signal.signal_score
                        leg.signal_indicator = parsed['signal_indicator'] or entry_signal.indicator_trigger
                        leg.signal_timeframe = parsed['signal_timeframe'] or entry_signal.timeframe
                        updated += 1
                    else:
                        no_data += 1
                    continue
                signal_data = trade.signal_data
            else:
                trade = Trade.query.get(tracker.trade_id)
                if not trade or not trade.signal_data:
                    no_data += 1
                    continue
                signal_data = trade.signal_data

            try:
                parsed = parse_signal_fields(signal_data)
                leg.signal_content = parsed['signal_content']
                leg.signal_grade = parsed['signal_grade']
                leg.signal_score = parsed['signal_score']
                leg.signal_indicator = parsed['signal_indicator']
                leg.signal_timeframe = parsed['signal_timeframe']

                if not leg.stop_price and hasattr(trade, 'stop_loss_price') and trade.stop_loss_price:
                    leg.stop_price = trade.stop_loss_price
                if not leg.take_profit_price and hasattr(trade, 'take_profit_price') and trade.take_profit_price:
                    leg.take_profit_price = trade.take_profit_price

                updated += 1
            except Exception as e:
                logger.warning(f"Tiger backfill parse error for leg {leg.id}: {e}")
                skipped += 1

        db.session.commit()
        flash(f'Tiger signal backfill: {updated} content filled, {grade_fixed} grades fixed, {no_data} no source, {skipped} errors', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Backfill error: {str(e)}', 'danger')
        logger.error(f"Tiger signal backfill failed: {e}", exc_info=True)

    return redirect(url_for('trade_analytics'))


@app.route('/sync-entry-records', methods=['POST'])
def sync_entry_records():
    """Sync entry_signal_record table with all display fields from PositionLeg + Position + TrailingStop"""
    from models import (Position, PositionStatus, PositionLeg, LegType, EntrySignalRecord,
                        TrailingStopPosition)
    
    account_type = request.form.get('account_type', 'paper')
    created = 0
    updated = 0
    errors = 0
    
    try:
        entry_legs = PositionLeg.query.join(Position).filter(
            PositionLeg.leg_type.in_([LegType.ENTRY, LegType.ADD]),
            Position.account_type == account_type
        ).all()
        
        pos_ids = list(set(leg.position_id for leg in entry_legs))
        positions_map = {}
        for pos in Position.query.filter(Position.id.in_(pos_ids)).all():
            positions_map[pos.id] = pos
        
        ts_map = {}
        ts_ids = [p.trailing_stop_id for p in positions_map.values() if p.trailing_stop_id]
        if ts_ids:
            for ts in TrailingStopPosition.query.filter(TrailingStopPosition.id.in_(ts_ids)).all():
                ts_map[ts.id] = ts
        
        exit_legs_map = {}
        if pos_ids:
            all_exit_legs = PositionLeg.query.filter(
                PositionLeg.position_id.in_(pos_ids),
                PositionLeg.leg_type == LegType.EXIT
            ).order_by(PositionLeg.filled_at.asc()).all()
            for el in all_exit_legs:
                exit_legs_map.setdefault(el.position_id, []).append(el)
        
        for leg in entry_legs:
            try:
                pos = positions_map.get(leg.position_id)
                if not pos:
                    continue
                
                existing = EntrySignalRecord.query.filter_by(
                    entry_order_id=leg.tiger_order_id,
                    account_type=account_type
                ).first() if leg.tiger_order_id else None
                
                if not existing:
                    existing = EntrySignalRecord.query.filter_by(
                        position_id=pos.id,
                        account_type=account_type
                    ).first()
                
                if not existing:
                    existing = EntrySignalRecord.query.filter_by(
                        symbol=pos.symbol.replace('[PAPER]', '').strip(),
                        entry_price=leg.price,
                        quantity=leg.quantity,
                        account_type=account_type
                    ).first()
                
                is_open = pos.status == PositionStatus.OPEN
                clean_symbol = pos.symbol.replace('[PAPER]', '').strip()
                
                exit_price = None
                exit_time = None
                exit_method_str = None
                hold_duration = None
                pnl_amount = None
                pnl_percent = None
                
                if not is_open:
                    pos_exit_legs = exit_legs_map.get(pos.id, [])
                    if pos_exit_legs:
                        total_exit_qty = sum(el.quantity or 0 for el in pos_exit_legs)
                        if total_exit_qty > 0:
                            exit_price = sum((el.price or 0) * (el.quantity or 0) for el in pos_exit_legs) / total_exit_qty
                        last_exit = pos_exit_legs[-1]
                        exit_time = last_exit.filled_at
                        for el in pos_exit_legs:
                            if el.exit_method:
                                exit_method_str = el.exit_method.value if hasattr(el.exit_method, 'value') else str(el.exit_method)
                                break
                    
                    if exit_price and leg.price and leg.quantity:
                        if pos.side == 'long':
                            pnl_amount = (exit_price - leg.price) * leg.quantity
                        else:
                            pnl_amount = (leg.price - exit_price) * leg.quantity
                        if leg.price > 0:
                            pnl_percent = (pnl_amount / (leg.price * leg.quantity)) * 100
                    
                    if leg.filled_at and pos.closed_at:
                        hold_duration = (pos.closed_at - leg.filled_at).total_seconds()
                
                ts = ts_map.get(pos.trailing_stop_id) if pos.trailing_stop_id else None
                stop_price = leg.stop_price or (ts.stop_loss_price if ts else None)
                tp_price = leg.take_profit_price or (ts.take_profit_price if ts else None)
                
                if existing:
                    existing.position_id = pos.id
                    existing.position_key = pos.position_key
                    existing.stop_price = stop_price
                    existing.take_profit_price = tp_price
                    existing.exit_price = exit_price
                    existing.exit_time = exit_time
                    existing.exit_method = exit_method_str
                    existing.hold_duration_seconds = hold_duration
                    if pnl_amount is not None:
                        existing.contribution_pnl = pnl_amount
                    if pnl_percent is not None:
                        existing.contribution_pct = pnl_percent
                    if not existing.entry_order_id and leg.tiger_order_id:
                        existing.entry_order_id = leg.tiger_order_id
                    if not existing.raw_json and leg.signal_content:
                        existing.raw_json = leg.signal_content
                    if not existing.indicator_trigger and leg.signal_indicator:
                        existing.indicator_trigger = leg.signal_indicator
                    if not existing.signal_grade and leg.signal_grade:
                        existing.signal_grade = leg.signal_grade
                    if not existing.signal_score and leg.signal_score:
                        existing.signal_score = leg.signal_score
                    if not existing.timeframe and leg.signal_timeframe:
                        existing.timeframe = leg.signal_timeframe
                    updated += 1
                else:
                    new_record = EntrySignalRecord(
                        position_id=pos.id,
                        position_key=pos.position_key,
                        closed_position_id=None,
                        symbol=clean_symbol,
                        account_type=account_type,
                        entry_time=leg.filled_at,
                        entry_price=leg.price,
                        quantity=leg.quantity,
                        side=pos.side,
                        is_scaling=(leg.leg_type == LegType.ADD),
                        entry_order_id=leg.tiger_order_id,
                        raw_json=leg.signal_content,
                        indicator_trigger=leg.signal_indicator,
                        signal_grade=leg.signal_grade,
                        signal_score=leg.signal_score,
                        timeframe=leg.signal_timeframe,
                        signal_stop_loss=leg.stop_price,
                        signal_take_profit=leg.take_profit_price,
                        stop_price=stop_price,
                        take_profit_price=tp_price,
                        exit_price=exit_price,
                        exit_time=exit_time,
                        exit_method=exit_method_str,
                        hold_duration_seconds=hold_duration,
                        contribution_pnl=pnl_amount,
                        contribution_pct=pnl_percent,
                    )
                    db.session.add(new_record)
                    created += 1
            except Exception as e:
                logger.warning(f"Sync entry record error for leg {leg.id}: {e}")
                errors += 1
        
        db.session.commit()
        flash(f'Entry records sync ({account_type}): {created} created, {updated} updated, {errors} errors', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Sync error: {str(e)}', 'danger')
        logger.error(f"Entry records sync failed: {e}", exc_info=True)
    
    return redirect(url_for('trade_analytics'))


def _extract_signal_type(entry_signal_content, fallback_type):
    """Extract display-friendly signal type from entry signal content (extras.indicator)"""
    import json
    
    if not entry_signal_content:
        return fallback_type or 'Unknown'
    
    content = entry_signal_content.strip()
    
    try:
        data = json.loads(content)
        if isinstance(data, dict):
            extras = data.get('extras', {})
            indicator = None
            if isinstance(extras, dict) and extras.get('indicator'):
                indicator = extras['indicator']
            elif data.get('indicator'):
                indicator = data['indicator']
            
            if indicator:
                parts = indicator.split()
                if len(parts) > 2:
                    return ' '.join(parts[:2])
                return indicator
            
            if data.get('signal'):
                return data['signal']
        return fallback_type or 'Unknown'
    except:
        pass
    
    if len(content) < 30:
        return content
    
    return content[:30] + '...'


@app.route('/fix-completed-trade-pnl', methods=['POST'])
def fix_completed_trade_pnl():
    """修复CompletedTrade中缺失的exit_price和P&L数据"""
    from models import CompletedTrade
    from tiger_client import get_tiger_quote_client
    
    try:
        # 找到所有exit_price为空的已关闭交易
        trades_to_fix = CompletedTrade.query.filter(
            CompletedTrade.is_open == False,
            CompletedTrade.exit_price == None
        ).all()
        
        fixed_count = 0
        errors = []
        
        quote_client = get_tiger_quote_client()
        
        for trade in trades_to_fix:
            try:
                if quote_client:
                    quote_result = quote_client.get_smart_price(trade.symbol)
                    if quote_result and quote_result.get('price'):
                        exit_price = quote_result['price']
                        trade.exit_price = exit_price
                        
                        # 计算P&L
                        if trade.entry_price and exit_price:
                            if trade.side == 'long':
                                trade.pnl_amount = (exit_price - trade.entry_price) * (trade.entry_quantity or 0)
                                trade.pnl_percent = ((exit_price - trade.entry_price) / trade.entry_price) * 100
                            else:
                                trade.pnl_amount = (trade.entry_price - exit_price) * (trade.entry_quantity or 0)
                                trade.pnl_percent = ((trade.entry_price - exit_price) / trade.entry_price) * 100
                            
                            fixed_count += 1
                            logger.info(f"Fixed P&L for {trade.symbol}: exit_price={exit_price}, pnl={trade.pnl_amount:.2f}")
            except Exception as e:
                errors.append(f"{trade.symbol}: {str(e)}")
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'fixed_count': fixed_count,
            'total_needing_fix': len(trades_to_fix),
            'errors': errors if errors else None,
            'message': f"Fixed {fixed_count} of {len(trades_to_fix)} trades"
        })
        
    except Exception as e:
        logger.error(f"Error fixing P&L: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/test-quote/<symbol>')
def test_quote(symbol):
    """Test quote API for debugging - returns all available price sources"""
    import numpy as np
    
    def convert_numpy(obj):
        """Convert numpy types to Python native types"""
        if isinstance(obj, dict):
            return {k: convert_numpy(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [convert_numpy(v) for v in obj]
        elif isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return obj
    
    try:
        from tiger_client import get_tiger_quote_client
        
        client = get_tiger_quote_client()
        if not client:
            return jsonify({'error': 'Quote client not available'}), 500
        
        results = {
            'symbol': symbol,
            'market_session': client.get_market_session(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Test smart_price
        try:
            smart = client.get_smart_price(symbol)
            results['smart_price'] = convert_numpy(smart)
        except Exception as e:
            results['smart_price_error'] = str(e)
        
        # Test extended hours
        try:
            extended = client.get_extended_hours_price(symbol)
            results['extended_hours'] = convert_numpy(extended)
        except Exception as e:
            results['extended_hours_error'] = str(e)
        
        # Test latest trade (briefs)
        try:
            latest = client.get_latest_trade(symbol)
            results['latest_trade'] = convert_numpy(latest)
        except Exception as e:
            results['latest_trade_error'] = str(e)
        
        # Test overnight ticks
        try:
            overnight = client.get_overnight_price(symbol)
            results['overnight'] = convert_numpy(overnight)
        except Exception as e:
            results['overnight_error'] = str(e)
        
        # Test session ticks (get last 5 overnight ticks)
        try:
            ticks = client.get_session_ticks(symbol, session='overnight', limit=5)
            results['overnight_ticks'] = convert_numpy(ticks)
        except Exception as e:
            results['overnight_ticks_error'] = str(e)
        
        # Test WebSocket cached price
        try:
            from tiger_push_client import get_push_manager
            push_manager = get_push_manager()
            if push_manager:
                cached = push_manager.get_cached_quote(symbol)
                results['websocket_cache'] = convert_numpy(cached)
            else:
                results['websocket_cache'] = None
        except Exception as e:
            results['websocket_cache_error'] = str(e)
        
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/websocket-status')
def websocket_status():
    """Show WebSocket subscription status and all cached prices"""
    import numpy as np
    
    def convert_numpy(obj):
        if isinstance(obj, dict):
            return {k: convert_numpy(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [convert_numpy(v) for v in obj]
        elif isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return obj
    
    try:
        from tiger_push_client import get_push_manager
        push_manager = get_push_manager()
        
        if not push_manager:
            return jsonify({'error': 'Push manager not initialized'}), 500
        
        subscribed = push_manager.subscribed_symbols
        
        cached_quotes = {}
        for symbol in subscribed:
            quote = push_manager.get_cached_quote(symbol)
            if quote:
                cached_quotes[symbol] = convert_numpy(quote)
        
        return jsonify({
            'connected': push_manager.is_connected,
            'subscribed_symbols': subscribed,
            'subscribed_count': len(subscribed),
            'cached_quotes': cached_quotes,
            'cached_count': len(cached_quotes)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/tiger-closed-history')
def tiger_closed_history():
    """Display closed trades history from Tiger API directly (Tiger API 历史成交记录)"""
    try:
        from tiger_client import TigerClient, TigerPaperClient
        from pytz import timezone
        eastern = timezone('US/Eastern')
        
        account_type = request.args.get('account_type', 'real')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        symbol_filter = request.args.get('symbol', '').strip().upper() or None
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        
        if account_type == 'paper':
            tiger_client = TigerPaperClient()
        else:
            tiger_client = TigerClient()
        
        result = tiger_client.get_filled_orders(
            start_date=start_date,
            end_date=end_date,
            symbol=symbol_filter,
            limit=500
        )
        
        orders = []
        total_pnl = 0
        total_commission = 0
        buy_count = 0
        sell_count = 0
        
        if result.get('success'):
            for order in result.get('orders', []):
                trade_time = order.get('trade_time')
                if trade_time:
                    from datetime import datetime
                    if isinstance(trade_time, (int, float)):
                        trade_dt = datetime.fromtimestamp(trade_time / 1000, tz=eastern)
                    else:
                        trade_dt = trade_time
                        if hasattr(trade_dt, 'astimezone'):
                            trade_dt = trade_dt.astimezone(eastern)
                    order['trade_time_str'] = trade_dt.strftime('%Y-%m-%d %H:%M:%S ET')
                else:
                    order['trade_time_str'] = 'N/A'
                
                order['avg_cost'] = order.get('avg_fill_price', 0)
                
                realized_pnl = order.get('realized_pnl', 0) or 0
                commission = order.get('commission', 0) or 0
                total_pnl += realized_pnl
                total_commission += commission
                
                action = order.get('action', '')
                if 'BUY' in action.upper():
                    buy_count += 1
                else:
                    sell_count += 1
                
                orders.append(order)
        
        total_orders = len(orders)
        total_pages = (total_orders + per_page - 1) // per_page if total_orders > 0 else 1
        page = max(1, min(page, total_pages))
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_orders = orders[start_idx:end_idx]
        
        return render_template('tiger_closed_history.html',
                             orders=paginated_orders,
                             account_type=account_type,
                             start_date=start_date,
                             end_date=end_date,
                             symbol_filter=symbol_filter,
                             total_pnl=total_pnl,
                             total_commission=total_commission,
                             buy_count=buy_count,
                             sell_count=sell_count,
                             page=page,
                             per_page=per_page,
                             total_pages=total_pages,
                             total_orders=total_orders,
                             error=result.get('error') if not result.get('success') else None)
    
    except Exception as e:
        logger.error(f"Error in tiger_closed_history: {str(e)}")
        import traceback
        traceback.print_exc()
        return render_template('tiger_closed_history.html',
                             orders=[],
                             account_type='real',
                             start_date=None,
                             end_date=None,
                             symbol_filter=None,
                             total_pnl=0,
                             total_commission=0,
                             buy_count=0,
                             sell_count=0,
                             page=1,
                             per_page=50,
                             total_pages=1,
                             total_orders=0,
                             error=str(e))


@app.route('/api/reconcile-orders', methods=['POST'])
def api_reconcile_orders():
    """API endpoint to reconcile Tiger orders with ClosedPosition records"""
    try:
        from flask import current_app
        from trailing_stop_scheduler import reconcile_tiger_orders
        
        data = request.get_json() or {}
        account_type = data.get('account_type', 'paper')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if account_type not in ['paper', 'real']:
            return jsonify({'success': False, 'error': 'Invalid account_type. Must be "paper" or "real"'}), 400
        
        if start_date:
            try:
                from datetime import datetime
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                return jsonify({'success': False, 'error': 'Invalid start_date format. Use YYYY-MM-DD'}), 400
        
        if end_date:
            try:
                from datetime import datetime
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                return jsonify({'success': False, 'error': 'Invalid end_date format. Use YYYY-MM-DD'}), 400
        
        result = reconcile_tiger_orders(
            app=current_app._get_current_object(),
            account_type=account_type,
            start_date=start_date,
            end_date=end_date
        )
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error in reconcile orders API: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/admin/cleanup-data', methods=['GET', 'POST'])
def admin_cleanup_data():
    """Admin endpoint to cleanup historical trading data.
    GET: Show confirmation page
    POST with confirm=YES: Execute cleanup
    """
    from models import (
        ClosedPosition, EntrySignalRecord, OrderTracker,
        TrailingStopPosition, TrailingStopLog, CompletedTrade,
        PositionCost, SignalLog, Trade
    )
    
    if request.method == 'GET':
        counts = {
            'closed_position': db.session.query(ClosedPosition).count(),
            'entry_signal_record': db.session.query(EntrySignalRecord).count(),
            'order_tracker': db.session.query(OrderTracker).count(),
            'trailing_stop_position': db.session.query(TrailingStopPosition).count(),
            'trailing_stop_log': db.session.query(TrailingStopLog).count(),
            'completed_trade': db.session.query(CompletedTrade).count(),
            'position_cost': db.session.query(PositionCost).count(),
            'signal_log': db.session.query(SignalLog).count(),
            'trade': db.session.query(Trade).count(),
        }
        total = sum(counts.values())
        
        html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Admin - Cleanup Data</title>
            <link href="https://cdn.replit.com/agent/bootstrap-agent-dark-theme.min.css" rel="stylesheet">
        </head>
        <body class="bg-dark text-light p-5">
            <div class="container">
                <h1 class="mb-4">🗑️ Production Data Cleanup</h1>
                <div class="alert alert-warning">
                    <strong>⚠️ Warning:</strong> This will permanently delete all trading history data!
                </div>
                <table class="table table-dark table-striped">
                    <thead><tr><th>Table</th><th>Records</th></tr></thead>
                    <tbody>
                        <tr><td>ClosedPosition (已平仓记录)</td><td>{counts['closed_position']}</td></tr>
                        <tr><td>EntrySignalRecord (入场信号记录)</td><td>{counts['entry_signal_record']}</td></tr>
                        <tr><td>OrderTracker (订单跟踪)</td><td>{counts['order_tracker']}</td></tr>
                        <tr><td>TrailingStopPosition (跟踪止损仓位)</td><td>{counts['trailing_stop_position']}</td></tr>
                        <tr><td>TrailingStopLog (跟踪止损日志)</td><td>{counts['trailing_stop_log']}</td></tr>
                        <tr><td>CompletedTrade (已完成交易)</td><td>{counts['completed_trade']}</td></tr>
                        <tr><td>PositionCost (仓位成本)</td><td>{counts['position_cost']}</td></tr>
                        <tr><td>SignalLog (信号日志)</td><td>{counts['signal_log']}</td></tr>
                        <tr><td>Trade (交易记录)</td><td>{counts['trade']}</td></tr>
                        <tr class="table-info"><td><strong>Total</strong></td><td><strong>{total}</strong></td></tr>
                    </tbody>
                </table>
                <form method="POST" class="mt-4">
                    <div class="mb-3">
                        <label class="form-label">Type <code>YES</code> to confirm deletion:</label>
                        <input type="text" name="confirm" class="form-control" style="max-width:200px" autocomplete="off">
                    </div>
                    <button type="submit" class="btn btn-danger btn-lg">🗑️ Delete All Data</button>
                    <a href="/" class="btn btn-secondary btn-lg ms-2">Cancel</a>
                </form>
            </div>
        </body>
        </html>
        '''
        return html
    
    confirm = request.form.get('confirm', '')
    if confirm != 'YES':
        flash('Cleanup cancelled - confirmation not provided', 'warning')
        return redirect(url_for('admin_cleanup_data'))
    
    try:
        from sqlalchemy import text
        
        tables_to_truncate = [
            'trailing_stop_log',
            'trailing_stop_position', 
            'closed_position',
            'order_tracker',
            'entry_signal_record',
            'completed_trade',
            'position_cost',
            'signal_log',
            'trade'
        ]
        
        deleted_counts = {}
        for table in tables_to_truncate:
            count_result = db.session.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = count_result.scalar()
            deleted_counts[table] = count
            db.session.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
        
        db.session.commit()
        
        total = sum(deleted_counts.values())
        logger.info(f"Admin cleanup: truncated {total} records from {len(tables_to_truncate)} tables")
        
        flash(f'Successfully deleted {total} records!', 'success')
        return redirect('/')
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Cleanup error: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error during cleanup: {str(e)}', 'danger')
        return redirect(url_for('admin_cleanup_data'))


@app.route('/admin/fix-entry-matching', methods=['GET', 'POST'])
def admin_fix_entry_matching():
    """Admin endpoint to fix unlinked EntrySignalRecord by re-running FIFO matching."""
    from models import ClosedPosition, EntrySignalRecord
    from sqlalchemy import or_
    
    if request.method == 'GET':
        unlinked_entries = EntrySignalRecord.query.filter(
            EntrySignalRecord.closed_position_id == None
        ).count()
        
        closed_positions = ClosedPosition.query.all()
        
        html = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Admin - Fix Entry Matching</title>
            <link href="https://cdn.replit.com/agent/bootstrap-agent-dark-theme.min.css" rel="stylesheet">
        </head>
        <body class="bg-dark text-light p-5">
            <div class="container">
                <h1 class="mb-4">🔧 Fix Entry Signal Matching</h1>
                <div class="alert alert-info">
                    <strong>ℹ️ Info:</strong> This will re-run FIFO matching for all ClosedPosition records
                    to link unmatched EntrySignalRecord entries.
                </div>
                <p>Unlinked Entry Records: <strong>{unlinked_entries}</strong></p>
                <p>Total ClosedPosition Records: <strong>{len(closed_positions)}</strong></p>
                <form method="POST">
                    <button type="submit" class="btn btn-primary">Run Fix</button>
                    <a href="/" class="btn btn-secondary">Cancel</a>
                </form>
            </div>
        </body>
        </html>
        '''
        return html
    
    try:
        fixed_count = 0
        closed_positions = ClosedPosition.query.all()
        
        for cp in closed_positions:
            linked_entries = EntrySignalRecord.query.filter(
                EntrySignalRecord.closed_position_id == cp.id
            ).count()
            
            if linked_entries > 0:
                continue
            
            unlinked_entries = EntrySignalRecord.query.filter(
                EntrySignalRecord.symbol == cp.symbol,
                EntrySignalRecord.account_type == cp.account_type,
                EntrySignalRecord.side == cp.side,
                EntrySignalRecord.closed_position_id == None
            ).order_by(EntrySignalRecord.entry_time.asc()).all()
            
            if not unlinked_entries:
                continue
            
            exit_qty = abs(cp.exit_quantity) if cp.exit_quantity else 0
            remaining_qty = exit_qty
            
            for entry in unlinked_entries:
                if remaining_qty <= 0:
                    break
                
                entry_qty = abs(entry.quantity) if entry.quantity else 0
                matched_qty = min(entry_qty, remaining_qty)
                
                if matched_qty > 0:
                    entry.closed_position_id = cp.id
                    if cp.exit_price and entry.entry_price:
                        if entry.side == 'long':
                            entry.contribution_pnl = (cp.exit_price - entry.entry_price) * entry_qty
                        else:
                            entry.contribution_pnl = (entry.entry_price - cp.exit_price) * entry_qty
                        if entry.entry_price > 0:
                            entry.contribution_pct = entry.contribution_pnl / (entry.entry_price * entry_qty) * 100
                    
                    remaining_qty -= matched_qty
                    fixed_count += 1
                    logger.info(f"🔧 Fixed: Entry #{entry.id} -> ClosedPosition #{cp.id}")
            
            if cp.exit_quantity and cp.exit_quantity < 0:
                cp.exit_quantity = abs(cp.exit_quantity)
                if cp.side == 'short' and cp.exit_price and cp.avg_entry_price:
                    cp.total_pnl = (cp.avg_entry_price - cp.exit_price) * cp.exit_quantity
                    if cp.avg_entry_price > 0:
                        cp.total_pnl_pct = cp.total_pnl / (cp.avg_entry_price * cp.exit_quantity) * 100
        
        db.session.commit()
        
        flash(f'Successfully fixed {fixed_count} entry-exit linkages', 'success')
        return redirect(url_for('admin_fix_entry_matching'))
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Fix matching error: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error: {str(e)}', 'danger')
        return redirect(url_for('admin_fix_entry_matching'))


@app.route('/admin/reconciliation', methods=['GET', 'POST'])
def admin_reconciliation():
    """Reconciliation dashboard - view Tiger raw orders and trigger reconciliation."""
    from models import TigerFilledOrder, ReconciliationRun
    from reconciliation_service import fetch_and_store_filled_orders, reconcile_today, reconcile_all_history
    from datetime import date, timedelta
    
    message = None
    message_type = 'info'
    
    if request.method == 'POST':
        action = request.form.get('action', '')
        account_type = request.form.get('account_type', 'real')
        target_date_str = request.form.get('target_date', '')
        
        target_date = None
        if target_date_str:
            try:
                target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
            except ValueError:
                target_date = date.today()
        
        if action == 'fetch':
            start_date = target_date_str or date.today().strftime('%Y-%m-%d')
            end_date = start_date
            total, new = fetch_and_store_filled_orders(
                account_type=account_type,
                start_date=start_date,
                end_date=end_date
            )
            message = f'Fetched {total} orders for {account_type}, {new} new stored'
            message_type = 'success' if total > 0 else 'warning'
            
        elif action == 'reconcile':
            run = reconcile_today(
                account_type=account_type,
                run_type='manual',
                target_date=target_date
            )
            if run.status == 'completed':
                message = (f'Reconciliation completed: {run.positions_matched} matched, '
                          f'{run.records_corrected} corrected, {run.records_created} created')
                message_type = 'success'
            else:
                message = f'Reconciliation failed: {run.error_message}'
                message_type = 'danger'
        
        elif action == 'fetch_range':
            days_back = int(request.form.get('days_back', 7))
            start = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            end = date.today().strftime('%Y-%m-%d')
            total, new = fetch_and_store_filled_orders(
                account_type=account_type,
                start_date=start,
                end_date=end
            )
            message = f'Fetched {total} orders ({days_back} days) for {account_type}, {new} new stored'
            message_type = 'success'
        
        elif action == 'full_reconcile':
            days_back = int(request.form.get('days_back', 90))
            run = reconcile_all_history(
                account_type=account_type,
                days_back=days_back
            )
            if run.status == 'completed':
                message = (f'Full reconciliation completed: {run.positions_matched} matched, '
                          f'{run.records_corrected} corrected, {run.records_created} created')
                message_type = 'success'
            else:
                message = f'Full reconciliation failed: {run.error_message}'
                message_type = 'danger'
    
    recent_runs = ReconciliationRun.query.order_by(
        ReconciliationRun.started_at.desc()
    ).limit(20).all()
    
    today_str = date.today().strftime('%Y-%m-%d')
    
    real_orders_today = TigerFilledOrder.query.filter_by(account_type='real').count()
    paper_orders_today = TigerFilledOrder.query.filter_by(account_type='paper').count()
    
    real_unreconciled = TigerFilledOrder.query.filter_by(
        account_type='real', is_open=False, reconciled=False
    ).count()
    paper_unreconciled = TigerFilledOrder.query.filter_by(
        account_type='paper', is_open=False, reconciled=False
    ).count()
    
    recent_orders = TigerFilledOrder.query.order_by(
        TigerFilledOrder.trade_time.desc()
    ).limit(50).all()
    
    html = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Reconciliation Dashboard</title>
        <link href="https://cdn.replit.com/agent/bootstrap-agent-dark-theme.min.css" rel="stylesheet">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    </head>
    <body class="bg-dark text-light">
        <div class="container-fluid p-4">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h1><i class="fas fa-balance-scale"></i> 智能对账系统</h1>
                <a href="/" class="btn btn-outline-secondary"><i class="fas fa-home"></i> Dashboard</a>
            </div>
            
            {"<div class='alert alert-" + message_type + " alert-dismissible'>" + message + "</div>" if message else ""}
            
            <div class="row mb-4">
                <div class="col-md-3">
                    <div class="card bg-secondary">
                        <div class="card-body text-center">
                            <h5>Real 总订单</h5>
                            <h2>{real_orders_today}</h2>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card bg-secondary">
                        <div class="card-body text-center">
                            <h5>Paper 总订单</h5>
                            <h2>{paper_orders_today}</h2>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card bg-warning text-dark">
                        <div class="card-body text-center">
                            <h5>Real 待对账</h5>
                            <h2>{real_unreconciled}</h2>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card bg-warning text-dark">
                        <div class="card-body text-center">
                            <h5>Paper 待对账</h5>
                            <h2>{paper_unreconciled}</h2>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="row mb-4">
                <div class="col-md-6">
                    <div class="card bg-dark border-info">
                        <div class="card-header bg-info text-dark"><strong><i class="fas fa-download"></i> 拉取Tiger成交数据</strong></div>
                        <div class="card-body">
                            <form method="POST" class="mb-3">
                                <input type="hidden" name="action" value="fetch">
                                <div class="row g-2 align-items-end">
                                    <div class="col-md-4">
                                        <label class="form-label">账户</label>
                                        <select name="account_type" class="form-select form-select-sm">
                                            <option value="real">Real</option>
                                            <option value="paper">Paper</option>
                                        </select>
                                    </div>
                                    <div class="col-md-4">
                                        <label class="form-label">日期</label>
                                        <input type="date" name="target_date" class="form-control form-control-sm" value="{today_str}">
                                    </div>
                                    <div class="col-md-4">
                                        <button type="submit" class="btn btn-info btn-sm w-100">拉取当日</button>
                                    </div>
                                </div>
                            </form>
                            <form method="POST">
                                <input type="hidden" name="action" value="fetch_range">
                                <div class="row g-2 align-items-end">
                                    <div class="col-md-4">
                                        <select name="account_type" class="form-select form-select-sm">
                                            <option value="real">Real</option>
                                            <option value="paper">Paper</option>
                                        </select>
                                    </div>
                                    <div class="col-md-4">
                                        <select name="days_back" class="form-select form-select-sm">
                                            <option value="7">最近7天</option>
                                            <option value="14">最近14天</option>
                                            <option value="30">最近30天</option>
                                        </select>
                                    </div>
                                    <div class="col-md-4">
                                        <button type="submit" class="btn btn-outline-info btn-sm w-100">批量拉取</button>
                                    </div>
                                </div>
                            </form>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card bg-dark border-success">
                        <div class="card-header bg-success text-dark"><strong><i class="fas fa-check-double"></i> 执行对账</strong></div>
                        <div class="card-body">
                            <form method="POST">
                                <input type="hidden" name="action" value="reconcile">
                                <div class="row g-2 align-items-end">
                                    <div class="col-md-4">
                                        <label class="form-label">账户</label>
                                        <select name="account_type" class="form-select form-select-sm">
                                            <option value="real">Real</option>
                                            <option value="paper">Paper</option>
                                        </select>
                                    </div>
                                    <div class="col-md-4">
                                        <label class="form-label">日期</label>
                                        <input type="date" name="target_date" class="form-control form-control-sm" value="{today_str}">
                                    </div>
                                    <div class="col-md-4">
                                        <button type="submit" class="btn btn-success btn-sm w-100">立即对账</button>
                                    </div>
                                </div>
                            </form>
                            <hr class="my-3">
                            <form method="POST">
                                <input type="hidden" name="action" value="full_reconcile">
                                <div class="row g-2 align-items-end">
                                    <div class="col-md-4">
                                        <label class="form-label">账户</label>
                                        <select name="account_type" class="form-select form-select-sm">
                                            <option value="paper">Paper</option>
                                            <option value="real">Real</option>
                                        </select>
                                    </div>
                                    <div class="col-md-4">
                                        <label class="form-label">回溯天数</label>
                                        <select name="days_back" class="form-select form-select-sm">
                                            <option value="30">30天</option>
                                            <option value="60">60天</option>
                                            <option value="90" selected>90天</option>
                                        </select>
                                    </div>
                                    <div class="col-md-4">
                                        <button type="submit" class="btn btn-warning btn-sm w-100" onclick="return confirm('将重置所有对账标记并全量重新对账，确定？')">
                                            <i class="fas fa-sync"></i> 全量重新对账
                                        </button>
                                    </div>
                                </div>
                            </form>
                            <div class="mt-2">
                                <small class="text-muted">
                                    <i class="fas fa-info-circle"></i> 
                                    全量对账：重置对账标记 → 拉取历史数据 → 按日期逐日匹配开仓/平仓 → 修正所有ClosedPosition记录
                                </small>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="card bg-dark border-secondary mb-4">
                <div class="card-header"><strong><i class="fas fa-history"></i> 对账历史</strong></div>
                <div class="card-body p-0">
                    <table class="table table-dark table-striped table-sm mb-0">
                        <thead>
                            <tr>
                                <th>时间</th>
                                <th>日期</th>
                                <th>账户</th>
                                <th>类型</th>
                                <th>状态</th>
                                <th>拉取</th>
                                <th>新增</th>
                                <th>匹配</th>
                                <th>更正</th>
                                <th>新建</th>
                            </tr>
                        </thead>
                        <tbody>'''
    
    for run in recent_runs:
        status_badge = 'success' if run.status == 'completed' else ('danger' if run.status == 'failed' else 'warning')
        html += f'''
                            <tr>
                                <td>{run.started_at.strftime('%m-%d %H:%M') if run.started_at else '-'}</td>
                                <td>{run.run_date}</td>
                                <td>{run.account_type}</td>
                                <td>{run.run_type}</td>
                                <td><span class="badge bg-{status_badge}">{run.status}</span></td>
                                <td>{run.total_orders_fetched or 0}</td>
                                <td>{run.new_orders_stored or 0}</td>
                                <td>{run.positions_matched or 0}</td>
                                <td>{run.records_corrected or 0}</td>
                                <td>{run.records_created or 0}</td>
                            </tr>'''
    
    if not recent_runs:
        html += '<tr><td colspan="10" class="text-center text-muted">暂无对账记录</td></tr>'
    
    html += '''
                        </tbody>
                    </table>
                </div>
            </div>
            
            <div class="card bg-dark border-secondary">
                <div class="card-header"><strong><i class="fas fa-list"></i> Tiger原始成交记录 (最近50条)</strong></div>
                <div class="card-body p-0">
                    <table class="table table-dark table-striped table-sm mb-0">
                        <thead>
                            <tr>
                                <th>Order ID</th>
                                <th>账户</th>
                                <th>股票</th>
                                <th>方向</th>
                                <th>开/平</th>
                                <th>数量</th>
                                <th>成交价</th>
                                <th>盈亏</th>
                                <th>手续费</th>
                                <th>成交时间</th>
                                <th>已对账</th>
                            </tr>
                        </thead>
                        <tbody>'''
    
    for order in recent_orders:
        is_open_badge = '<span class="badge bg-primary">开仓</span>' if order.is_open else '<span class="badge bg-danger">平仓</span>'
        recon_badge = '<span class="badge bg-success">✓</span>' if order.reconciled else '<span class="badge bg-secondary">—</span>'
        pnl_color = 'text-success' if (order.realized_pnl or 0) > 0 else ('text-danger' if (order.realized_pnl or 0) < 0 else '')
        html += f'''
                            <tr>
                                <td><small>{order.order_id}</small></td>
                                <td>{order.account_type}</td>
                                <td><strong>{order.symbol}</strong></td>
                                <td>{order.action}</td>
                                <td>{is_open_badge}</td>
                                <td>{order.filled or order.quantity or 0}</td>
                                <td>${order.avg_fill_price or 0:.2f}</td>
                                <td class="{pnl_color}">${order.realized_pnl or 0:.2f}</td>
                                <td>${order.commission or 0:.2f}</td>
                                <td><small>{order.trade_time_str or '-'}</small></td>
                                <td>{recon_badge}</td>
                            </tr>'''
    
    if not recent_orders:
        html += '<tr><td colspan="11" class="text-center text-muted">暂无数据，请先拉取</td></tr>'
    
    html += '''
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </body>
    </html>'''
    
    return html


# ==================== HOLDINGS ROUTES ====================

@app.route('/holdings')
def holdings():
    """View current Tiger holdings from local database (synced periodically)."""
    from models import TigerHolding
    from holdings_sync import get_sync_status
    
    account_type = request.args.get('account_type', 'real')
    
    holdings_data = TigerHolding.query.filter_by(
        account_type=account_type
    ).order_by(TigerHolding.symbol.asc()).all()
    
    total_market_value = sum(h.market_value or 0 for h in holdings_data)
    total_unrealized_pnl = sum(h.unrealized_pnl or 0 for h in holdings_data)
    total_cost = sum((h.average_cost or 0) * abs(h.quantity or 0) for h in holdings_data)
    total_pnl_pct = (total_unrealized_pnl / total_cost * 100) if total_cost > 0 else 0
    
    sync_status = get_sync_status()
    
    return render_template('holdings.html',
        holdings=holdings_data,
        account_type=account_type,
        total_market_value=total_market_value,
        total_unrealized_pnl=total_unrealized_pnl,
        total_pnl_pct=total_pnl_pct,
        sync_status=sync_status.get(account_type, {}),
        holding_count=len(holdings_data),
    )


@app.route('/holdings/sync', methods=['POST'])
def sync_holdings_manual():
    """Manually trigger holdings sync."""
    from holdings_sync import sync_holdings
    
    account_type = request.form.get('account_type', 'real')
    result = sync_holdings(account_type)
    
    if result.get('success'):
        flash(f"Holdings synced: {result['total']} positions ({result['created']} new, {result['updated']} updated, {result['removed']} removed)", 'success')
    else:
        flash(f"Sync failed: {result.get('error', 'Unknown error')}", 'error')
    
    return redirect(url_for('holdings', account_type=account_type))


# ==================== POSITION ROUTES ====================

@app.route('/positions')
def position_list():
    """Position list page - shows all positions with filtering."""
    from models import Position, PositionStatus, TigerHolding
    
    account_type = request.args.get('account_type', 'real')
    status_filter = request.args.get('status', '')
    start_date = request.args.get('start_date', '')
    symbol_filter = request.args.get('symbol', '').upper().strip()
    
    query = Position.query.filter_by(account_type=account_type)
    
    if status_filter == 'closed':
        query = query.filter_by(status=PositionStatus.CLOSED)
    elif status_filter == 'open':
        query = query.filter_by(status=PositionStatus.OPEN)
    
    if start_date:
        try:
            from datetime import datetime as dt_cls
            sd = dt_cls.strptime(start_date, '%Y-%m-%d').date()
            query = query.filter(Position.trade_date >= sd)
        except ValueError:
            pass
    
    if symbol_filter:
        query = query.filter(Position.symbol.ilike(f'%{symbol_filter}%'))
    
    positions = query.order_by(Position.opened_at.desc()).all()
    
    holdings_map = {}
    holdings = TigerHolding.query.filter_by(account_type=account_type).all()
    for h in holdings:
        holdings_map[h.symbol] = h
    
    closed_positions = [p for p in positions if p.status == PositionStatus.CLOSED and p.realized_pnl is not None]
    summary = {}
    if closed_positions:
        wins = [p for p in closed_positions if p.realized_pnl >= 0]
        losses = [p for p in closed_positions if p.realized_pnl < 0]
        total_pnl = sum(p.realized_pnl for p in closed_positions)
        gross_profit = sum(p.realized_pnl for p in wins)
        gross_loss = abs(sum(p.realized_pnl for p in losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 99999.0 if gross_profit > 0 else 0.0
        avg_win = gross_profit / len(wins) if wins else 0
        avg_loss = gross_loss / len(losses) if losses else 0
        summary = {
            'total': len(closed_positions),
            'wins': len(wins),
            'losses': len(losses),
            'win_rate': len(wins) / len(closed_positions) * 100 if closed_positions else 0,
            'total_pnl': total_pnl,
            'avg_pnl': total_pnl / len(closed_positions) if closed_positions else 0,
            'profit_factor': profit_factor,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'gross_profit': gross_profit,
            'gross_loss': gross_loss,
        }
    
    return render_template('position_list.html',
        positions=positions,
        holdings_map=holdings_map,
        account_type=account_type,
        status_filter=status_filter,
        start_date=start_date,
        symbol_filter=symbol_filter,
        summary=summary,
    )


@app.route('/positions/<int:position_id>')
def position_detail(position_id):
    """Position detail page - shows complete order chain."""
    from models import Position, PositionLeg, LegType, TigerHolding, PositionStatus
    
    position = Position.query.get_or_404(position_id)
    entry_legs = position.entry_legs
    exit_legs = position.exit_legs
    
    all_legs = PositionLeg.query.filter_by(position_id=position.id).order_by(
        PositionLeg.filled_at.asc()
    ).all()
    
    holding = None
    if position.status == PositionStatus.OPEN:
        holding = TigerHolding.query.filter_by(
            account_type=position.account_type,
            symbol=position.symbol
        ).first()
    
    return render_template('position_detail.html',
        position=position,
        entry_legs=entry_legs,
        exit_legs=exit_legs,
        all_legs=all_legs,
        holding=holding,
    )


@app.route('/positions/backfill', methods=['POST'])
def backfill_positions_route():
    """Trigger position backfill from historical data."""
    from position_backfill import backfill_positions
    import threading
    
    account_type = request.form.get('account_type', 'real')
    
    def _run_backfill(acct):
        try:
            with app.app_context():
                result = backfill_positions(account_type=acct)
                logging.info(f"✅ Backfill complete for {acct}: "
                           f"{result['positions_created']} positions, "
                           f"{result['entry_legs_created']} entries, "
                           f"{result['exit_legs_created']} exits, "
                           f"{len(result['errors'])} errors")
        except Exception as e:
            logging.error(f"❌ Backfill failed for {acct}: {str(e)}")
    
    t = threading.Thread(target=_run_backfill, args=(account_type,), daemon=True)
    t.start()
    
    flash(f"Position rebuild started for {account_type} account. Refresh the page in a few seconds to see results.", 'info')
    return redirect(url_for('position_list', account_type=account_type))


@app.route('/admin/logs')
def admin_logs():
    from models import SystemLog

    page = request.args.get('page', 1, type=int)
    per_page = 100
    level = request.args.get('level', '')
    category = request.args.get('category', '')
    symbol = request.args.get('symbol', '').strip().upper()
    source = request.args.get('source', '')
    search = request.args.get('search', '').strip()

    query = SystemLog.query

    if level:
        query = query.filter(SystemLog.level == level)
    if category:
        query = query.filter(SystemLog.category == category)
    if symbol:
        query = query.filter(SystemLog.symbol == symbol)
    if source:
        query = query.filter(SystemLog.source == source)
    if search:
        query = query.filter(SystemLog.message.ilike(f'%{search}%'))

    total_count = query.count()
    total_pages = max(1, (total_count + per_page - 1) // per_page)
    logs = query.order_by(SystemLog.timestamp.desc()).offset((page - 1) * per_page).limit(per_page).all()

    sources_result = db.session.query(SystemLog.source).distinct().filter(SystemLog.source.isnot(None)).all()
    sources = sorted([s[0] for s in sources_result])

    filter_args = {}
    if level:
        filter_args['level'] = level
    if category:
        filter_args['category'] = category
    if symbol:
        filter_args['symbol'] = symbol
    if source:
        filter_args['source'] = source
    if search:
        filter_args['search'] = search

    return render_template('admin_logs.html',
                           logs=logs,
                           page=page,
                           total_pages=total_pages,
                           total_count=total_count,
                           sources=sources,
                           filter_args=filter_args)


@app.route('/admin/logs/poll')
def admin_logs_poll():
    from models import SystemLog
    after_id = request.args.get('after_id', 0, type=int)
    level = request.args.get('level', '')
    category = request.args.get('category', '')
    symbol = request.args.get('symbol', '').strip().upper()
    source = request.args.get('source', '')
    search = request.args.get('search', '').strip()

    query = SystemLog.query.filter(SystemLog.id > after_id)
    if level:
        query = query.filter(SystemLog.level == level)
    if category:
        query = query.filter(SystemLog.category == category)
    if symbol:
        query = query.filter(SystemLog.symbol == symbol)
    if source:
        query = query.filter(SystemLog.source == source)
    if search:
        query = query.filter(SystemLog.message.ilike(f'%{search}%'))

    logs = query.order_by(SystemLog.id.asc()).limit(200).all()

    total_query = SystemLog.query
    if level:
        total_query = total_query.filter(SystemLog.level == level)
    if category:
        total_query = total_query.filter(SystemLog.category == category)
    if symbol:
        total_query = total_query.filter(SystemLog.symbol == symbol)
    if source:
        total_query = total_query.filter(SystemLog.source == source)
    if search:
        total_query = total_query.filter(SystemLog.message.ilike(f'%{search}%'))
    total_count = total_query.count()

    return jsonify({
        'logs': [{
            'id': log.id,
            'timestamp': log.timestamp.strftime('%m-%d %H:%M:%S'),
            'level': log.level,
            'category': log.category or '-',
            'symbol': log.symbol or '',
            'source': log.source or '',
            'message': log.message,
        } for log in logs],
        'total_count': total_count,
    })


@app.route('/admin/error-patterns')
def admin_error_patterns():
    from error_analyzer import analyze_errors

    hours = request.args.get('hours', 24, type=int)
    min_count = request.args.get('min_count', 2, type=int)
    system = request.args.get('system', 'both')
    source = request.args.get('source', '')
    levels = request.args.getlist('level') or ['ERROR', 'WARNING', 'CRITICAL']

    analysis = analyze_errors(
        hours=hours,
        min_count=min_count,
        levels=levels,
        source_filter=source or None,
        system=system,
    )

    return render_template('error_patterns.html',
                           analysis=analysis,
                           hours=hours,
                           min_count=min_count,
                           system=system,
                           source=source,
                           levels=levels)


@app.route('/test_new_account')
def test_new_account():
    """Test connectivity with a new Tiger account config file (temporary, read-only)"""
    import traceback
    from tigeropen.common.consts import Language
    from tigeropen.tiger_open_config import TigerOpenClientConfig
    from tigeropen.trade.trade_client import TradeClient
    from tigeropen.quote.quote_client import QuoteClient

    config_path = './attached_assets/tiger_openapi_config (3).properties'
    results = {
        'config_loaded': False,
        'trade_client_connected': False,
        'quote_client_connected': False,
        'account_info': None,
        'positions': None,
        'assets': None,
        'market_quote': None,
        'errors': []
    }

    try:
        config_data = {}
        with open(config_path, 'r') as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    config_data[key] = value

        results['config_loaded'] = True
        results['tiger_id'] = config_data.get('tiger_id')
        results['account'] = config_data.get('account')
        results['license'] = config_data.get('license')

        client_config = TigerOpenClientConfig(sandbox_debug=False)
        client_config.tiger_id = config_data.get('tiger_id')
        client_config.account = config_data.get('account')
        client_config.private_key = config_data.get('private_key_pk8')
        client_config.language = Language.zh_CN

        # 1. Test TradeClient
        try:
            trade_client = TradeClient(client_config)
            results['trade_client_connected'] = True

            try:
                assets = trade_client.get_assets()
                if assets:
                    asset_list = assets if isinstance(assets, list) else [assets]
                    results['assets'] = []
                    for a in asset_list:
                        results['assets'].append({
                            'account': getattr(a, 'account', str(a)),
                            'net_liquidation': getattr(a, 'summary', {}).get('net_liquidation', None) if isinstance(getattr(a, 'summary', None), dict) else getattr(a, 'net_liquidation', None),
                            'buying_power': getattr(a, 'summary', {}).get('buying_power', None) if isinstance(getattr(a, 'summary', None), dict) else getattr(a, 'buying_power', None),
                        })
            except Exception as e:
                results['errors'].append(f"get_assets error: {str(e)}")

            try:
                positions = trade_client.get_positions()
                if positions:
                    results['positions'] = []
                    for p in positions:
                        results['positions'].append({
                            'symbol': getattr(p, 'contract', {}).get('symbol', None) if isinstance(getattr(p, 'contract', None), dict) else getattr(p, 'symbol', str(p)),
                            'quantity': getattr(p, 'quantity', None),
                            'average_cost': getattr(p, 'average_cost', None),
                            'market_value': getattr(p, 'market_value', None),
                        })
                else:
                    results['positions'] = []
            except Exception as e:
                results['errors'].append(f"get_positions error: {str(e)}")

            try:
                orders = trade_client.get_orders(sec_type=None, limit=5)
                if orders:
                    results['recent_orders'] = []
                    for o in orders[:5]:
                        results['recent_orders'].append({
                            'order_id': getattr(o, 'id', None),
                            'symbol': getattr(o, 'contract', {}).get('symbol', None) if isinstance(getattr(o, 'contract', None), dict) else getattr(o, 'symbol', None),
                            'action': str(getattr(o, 'action', '')),
                            'status': str(getattr(o, 'status', '')),
                        })
                else:
                    results['recent_orders'] = []
            except Exception as e:
                results['errors'].append(f"get_orders error: {str(e)}")

        except Exception as e:
            results['errors'].append(f"TradeClient init error: {str(e)}")
            results['trade_client_traceback'] = traceback.format_exc()

        # 2. Test QuoteClient
        try:
            quote_client = QuoteClient(client_config)
            results['quote_client_connected'] = True

            try:
                briefs = quote_client.get_stock_briefs(['AAPL', 'MSFT', 'GOOGL'])
                if briefs is not None:
                    results['market_quote'] = []
                    for b in briefs:
                        results['market_quote'].append({
                            'symbol': getattr(b, 'symbol', None),
                            'latest_price': getattr(b, 'latest_price', None),
                            'pre_close': getattr(b, 'pre_close', None),
                            'volume': getattr(b, 'volume', None),
                        })
            except Exception as e:
                results['errors'].append(f"get_stock_briefs error: {str(e)}")

        except Exception as e:
            results['errors'].append(f"QuoteClient init error: {str(e)}")

    except Exception as e:
        results['errors'].append(f"Config load error: {str(e)}")
        results['traceback'] = traceback.format_exc()

    return jsonify(results)


@app.route('/monitor')
def tiger_monitor():
    from tiger_monitor_service import build_lifecycle, get_global_health, get_recent_closed_lifecycles

    symbol = request.args.get('symbol', '').strip().upper()
    order_id = request.args.get('order_id', '').strip()
    position_id = request.args.get('position_id', '', type=str).strip()
    account_type = request.args.get('account_type', '').strip()

    lifecycles = None
    search_performed = False

    if symbol or order_id or position_id:
        search_performed = True
        pid = int(position_id) if position_id and position_id.isdigit() else None
        result = build_lifecycle(
            symbol=symbol if symbol else None,
            order_id=order_id if order_id else None,
            position_id=pid,
            account_type=account_type if account_type else None,
        )
        if result is None:
            lifecycles = []
        elif isinstance(result, list):
            lifecycles = result
        else:
            lifecycles = [result]

    health = get_global_health(account_type=account_type if account_type else None)

    recent_closed = []
    if not search_performed:
        recent_closed = get_recent_closed_lifecycles(
            account_type=account_type if account_type else None,
            limit=20,
        )

    return render_template('monitor.html',
                           symbol=symbol,
                           order_id=order_id,
                           position_id=position_id,
                           account_type=account_type,
                           lifecycles=lifecycles,
                           search_performed=search_performed,
                           health=health,
                           recent_closed=recent_closed)


@app.route('/admin/close-position/<int:pos_id>', methods=['POST'])
def admin_close_position(pos_id):
    import os
    token = request.args.get('token', '')
    expected = os.environ.get('SESSION_SECRET', '')
    if not expected or token != expected:
        return jsonify({'error': 'unauthorized'}), 403

    from models import Position, PositionStatus, TrailingStopPosition
    from datetime import datetime

    pos = Position.query.get(pos_id)
    if not pos:
        return jsonify({'error': f'Position #{pos_id} not found'}), 404
    if pos.status == PositionStatus.CLOSED:
        return jsonify({'message': f'Position #{pos_id} {pos.symbol} already CLOSED'})

    results = []
    pos.status = PositionStatus.CLOSED
    pos.closed_at = datetime.utcnow()
    results.append(f'Position #{pos.id} {pos.symbol} {pos.side}: CLOSED')

    if pos.trailing_stop_id:
        ts = TrailingStopPosition.query.get(pos.trailing_stop_id)
        if ts and ts.is_active:
            ts.is_active = False
            ts.is_triggered = True
            ts.trigger_reason = ts.trigger_reason or 'Manual admin close'
            results.append(f'TS #{ts.id}: deactivated')

    db.session.commit()
    logger.info(f'🔧 Admin close: {results}')
    return jsonify({'results': results})


@app.route('/admin/fix-lifecycle-positions', methods=['GET', 'POST'])
def fix_lifecycle_positions():
    import os
    token = request.args.get('token', '')
    expected = os.environ.get('SESSION_SECRET', '')
    if not expected or token != expected:
        return jsonify({'error': 'unauthorized'}), 403

    from models import Position, PositionStatus, TrailingStopPosition, ClosedPosition
    
    pos_ids = [71, 80, 81, 83, 94, 98]
    ts_ids = [1718, 1725, 1740, 1744, 1733, 1735]
    cp_ids = [686, 687, 697, 698, 699, 703]
    
    if request.method == 'GET':
        positions = Position.query.filter(Position.id.in_(pos_ids)).all()
        tss = TrailingStopPosition.query.filter(TrailingStopPosition.id.in_(ts_ids)).all()
        cps = ClosedPosition.query.filter(ClosedPosition.id.in_(cp_ids)).all()
        return jsonify({
            'positions': [{
                'id': p.id, 'symbol': p.symbol, 'status': p.status.value if p.status else None,
                'closed_at': str(p.closed_at) if p.closed_at else None
            } for p in positions],
            'trailing_stops': [{
                'id': ts.id, 'symbol': ts.symbol, 'is_active': ts.is_active,
                'is_triggered': ts.is_triggered, 'trigger_reason': ts.trigger_reason
            } for ts in tss],
            'closed_positions': [{'id': cp.id, 'symbol': cp.symbol} for cp in cps],
            'action': 'POST to execute fix'
        })
    
    already_open = Position.query.filter(
        Position.id.in_(pos_ids), Position.status == PositionStatus.OPEN
    ).count()
    if already_open == len(pos_ids):
        return jsonify({'message': 'All positions already OPEN, fix was already applied'})

    results = []
    
    positions = Position.query.filter(Position.id.in_(pos_ids)).all()
    for p in positions:
        old_status = p.status.value if p.status else None
        p.status = PositionStatus.OPEN
        p.closed_at = None
        results.append(f'Position #{p.id} {p.symbol}: {old_status} -> OPEN')
    
    tss = TrailingStopPosition.query.filter(TrailingStopPosition.id.in_(ts_ids)).all()
    for ts in tss:
        ts.is_active = True
        ts.is_triggered = False
        ts.triggered_at = None
        ts.trigger_reason = None
        results.append(f'TS #{ts.id} {ts.symbol}: reactivated')
    
    cps = ClosedPosition.query.filter(ClosedPosition.id.in_(cp_ids)).all()
    for cp in cps:
        results.append(f'ClosedPosition #{cp.id} {cp.symbol}: deleted')
        db.session.delete(cp)
    
    db.session.commit()
    logger.info(f'🔧 Lifecycle fix applied: {results}')
    results.append('All fixes committed successfully')
    
    return jsonify({'results': results})
