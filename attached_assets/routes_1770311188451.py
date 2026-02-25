import json
import logging
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, flash, redirect, url_for
from app import app, db
from models import Trade, TradingConfig, SignalLog, OrderStatus, OrderType, Side, CompletedTrade, ExitMethod
from tiger_client import TigerClient, TigerPaperClient
from signal_parser import SignalParser
from signal_analyzer import parse_signal_grades
from config import get_config, set_config
from discord_notifier import discord_notifier
from position_cost_manager import update_position_cost_on_fill, get_avg_cost_for_symbol, record_entry_cost_on_trade

logger = logging.getLogger(__name__)

@app.route('/')
def index():
    """Main dashboard page"""
    recent_trades = Trade.query.order_by(Trade.created_at.desc()).limit(10).all()
    total_trades = Trade.query.count()
    successful_trades = Trade.query.filter_by(status=OrderStatus.FILLED).count()
    pending_trades = Trade.query.filter_by(status=OrderStatus.PENDING).count()
    
    return render_template('index.html', 
                         recent_trades=recent_trades,
                         total_trades=total_trades,
                         successful_trades=successful_trades,
                         pending_trades=pending_trades)

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
                result = tiger_client.close_position_with_sandbox_fallback(
                    parsed_signal['symbol'], 
                    trading_session,
                    reference_price=reference_price,
                    signal_side=signal_side
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
                            
                            # Handle auto-protection for position increases
                            if hasattr(trade, 'needs_auto_protection') and trade.needs_auto_protection:
                                logger.info(f"Order {trade.tiger_order_id} filled, applying auto-protection for position increase")
                                try:
                                    protection_info = json.loads(trade.protection_info) if hasattr(trade, 'protection_info') else {}
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
                                        
                                        logger.info(f"Applying OCA protection for {trade.symbol}")
                                        position_result = tiger_client.get_positions(symbol=trade.symbol)
                                        if position_result['success'] and position_result['positions']:
                                            current_quantity = position_result['positions'][0]['quantity']
                                            protection_quantity = abs(current_quantity)
                                            
                                            protection_result = tiger_client.create_oca_orders_for_position(
                                                symbol=trade.symbol,
                                                quantity=protection_quantity,
                                                stop_loss_price=protection_info.get('stop_loss_price'),
                                                take_profit_price=take_profit_for_oca
                                            )
                                        else:
                                            logger.error(f"Could not get position for {trade.symbol} to apply OCA protection")
                                            protection_result = {'success': False, 'error': 'No position found'}
                                        if protection_result['success']:
                                            logger.info(f"Auto-protection applied successfully for {trade.symbol}")
                                            if 'stop_loss_order_id' in protection_result:
                                                trade.stop_loss_order_id = protection_result['stop_loss_order_id']
                                            if 'take_profit_order_id' in protection_result:
                                                trade.take_profit_order_id = protection_result['take_profit_order_id']
                                            
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
                                    
                                    # Clear protection flags
                                    trade.needs_auto_protection = False
                                    trade.protection_info = None
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
                            
                            # Auto-create trailing stop for non-close orders
                            if not getattr(trade, 'is_close_position', False):
                                try:
                                    from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
                                    from models import TrailingStopMode
                                    
                                    ts_config = get_trailing_stop_config()
                                    if ts_config.is_enabled:
                                        ts_side = 'long' if trade.side.value == 'buy' else 'short'
                                        entry_price = filled_price or parsed_signal.get('reference_price') or parsed_signal.get('price')
                                        timeframe = signal_data.get('extras', {}).get('timeframe', '15') if isinstance(signal_data, dict) else '15'
                                        
                                        trailing_position = create_trailing_stop_for_trade(
                                            trade_id=trade.id,
                                            symbol=trade.symbol,
                                            side=ts_side,
                                            entry_price=entry_price,
                                            quantity=trade.quantity,
                                            account_type='real',
                                            fixed_stop_loss=trade.stop_loss_price,
                                            fixed_take_profit=trade.take_profit_price,
                                            stop_loss_order_id=trade.stop_loss_order_id,
                                            take_profit_order_id=trade.take_profit_order_id,
                                            mode=TrailingStopMode.BALANCED,
                                            timeframe=str(timeframe)
                                        )
                                        logger.info(f"🎯 Auto-created trailing stop for {trade.symbol}, side={ts_side}, entry=${entry_price:.2f}")
                                except Exception as ts_err:
                                    logger.error(f"🎯 Failed to create trailing stop: {str(ts_err)}")
                                    
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
        signal_log.raw_signal = f"[PAPER] {raw_data}"
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
            
            trade = None
            tiger_paper_client = TigerPaperClient()
            
            if parsed_signal.get('is_close_signal', False):
                logger.info(f"📝 [PAPER] Processing close signal for {parsed_signal['symbol']}, side={parsed_signal.get('side')}")
                
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
                result = tiger_paper_client.close_position_with_sandbox_fallback(
                    parsed_signal['symbol'], 
                    trading_session,
                    reference_price=reference_price,
                    signal_side=signal_side
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
                    trade.symbol = f"[PAPER]{parsed_signal['symbol']}"
                    trade.side = Side(result['action'])
                    trade.quantity = result['quantity']
                    
                    order_type_str = result.get('order_type', 'market')
                    if order_type_str == 'limit':
                        trade.order_type = OrderType.LIMIT
                        trade.price = result.get('order_price')
                    else:
                        trade.order_type = OrderType.MARKET
                        trade.price = None
                    
                    trade.signal_data = f"[PAPER] {raw_data}"
                    trade.tiger_order_id = result['order_id']
                    trade.status = OrderStatus.PENDING
                    trade.trading_session = result.get('trading_session', trading_session)
                    trade.outside_rth = result.get('outside_rth', parsed_signal.get('outside_rth', trading_session != 'regular'))
                    trade.is_close_position = True
                    trade.account_type = 'paper'  # Mark as paper account trade
                    
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
                    
                    trade.symbol = f"[PAPER]{parsed_signal['symbol']}"
                    
                    # Update CompletedTrade record for paper webhook exit
                    try:
                        symbol_to_find = f"[PAPER]{parsed_signal['symbol']}"
                        completed_trade = CompletedTrade.query.filter_by(
                            symbol=symbol_to_find,
                            account_type='paper',
                            is_open=True
                        ).order_by(CompletedTrade.created_at.desc()).first()
                        
                        if completed_trade:
                            exit_price = result.get('order_price') or reference_price or parsed_signal.get('reference_price')
                            # Fallback: Get current market price from Tiger API if exit_price is not available
                            # 使用智能价格获取 - 盘前盘后时段使用timeline API
                            if not exit_price:
                                try:
                                    from tiger_client import get_tiger_quote_client
                                    quote_client = get_tiger_quote_client()
                                    if quote_client:
                                        trade_data = quote_client.get_smart_price(parsed_signal['symbol'])
                                        if trade_data and trade_data.get('price'):
                                            exit_price = trade_data['price']
                                            price_session = trade_data.get('session', 'regular')
                                            logger.info(f"📊 [PAPER] Using {price_session} price {exit_price} as exit price for {parsed_signal['symbol']}")
                                except Exception as price_err:
                                    logger.warning(f"📊 [PAPER] Failed to get market price for exit: {str(price_err)}")
                            
                            # 如果订单已成交，等待后从Tiger API获取实际成交价
                            if not exit_price and result.get('success') and result.get('order_id'):
                                try:
                                    import time
                                    time.sleep(1)
                                    order_status = tiger_paper_client.get_order_status(result['order_id'])
                                    if order_status.get('success') and order_status.get('filled_price'):
                                        exit_price = order_status['filled_price']
                                        logger.info(f"📊 [PAPER] Using Tiger filled price {exit_price} as exit price")
                                except Exception as status_err:
                                    logger.warning(f"📊 [PAPER] Failed to get order status for exit price: {str(status_err)}")
                            
                            completed_trade.exit_method = ExitMethod.WEBHOOK_SIGNAL
                            completed_trade.exit_signal_content = raw_data
                            completed_trade.exit_time = datetime.utcnow()
                            completed_trade.exit_price = exit_price
                            completed_trade.exit_quantity = result['quantity']
                            completed_trade.is_open = False
                            
                            if completed_trade.entry_price and exit_price:
                                if completed_trade.side == 'long':
                                    completed_trade.pnl_percent = ((exit_price - completed_trade.entry_price) / completed_trade.entry_price) * 100
                                    completed_trade.pnl_amount = (exit_price - completed_trade.entry_price) * completed_trade.entry_quantity
                                else:
                                    completed_trade.pnl_percent = ((completed_trade.entry_price - exit_price) / completed_trade.entry_price) * 100
                                    completed_trade.pnl_amount = (completed_trade.entry_price - exit_price) * completed_trade.entry_quantity
                            
                            db.session.commit()
                            pnl_str = f"{completed_trade.pnl_percent:.2f}%" if completed_trade.pnl_percent else "N/A"
                            logger.info(f"📊 [PAPER] Updated CompletedTrade #{completed_trade.id} with webhook exit, exit_price: {exit_price}, P&L: {pnl_str}")
                        else:
                            logger.warning(f"📊 [PAPER] No open CompletedTrade found for {symbol_to_find}")
                    except Exception as ct_error:
                        logger.error(f"📊 [PAPER] Failed to update CompletedTrade on exit: {str(ct_error)}")
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
                trade.signal_data = f"[PAPER] {raw_data}"
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
                
                trade.symbol = f"[PAPER]{parsed_signal['symbol']}"
            
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
                            
                            if hasattr(trade, 'needs_auto_protection') and trade.needs_auto_protection:
                                logger.info(f"📝 [PAPER] Order {trade.tiger_order_id} filled, applying auto-protection")
                                try:
                                    protection_info = json.loads(trade.protection_info) if hasattr(trade, 'protection_info') else {}
                                    if protection_info.get('stop_loss_price') or protection_info.get('take_profit_price'):
                                        # Check if trailing stop has switched to dynamic mode
                                        from models import TrailingStopPosition
                                        existing_ts = TrailingStopPosition.query.filter_by(
                                            symbol=parsed_signal['symbol'], account_type='paper', is_active=True
                                        ).first()
                                        has_switched = existing_ts.has_switched_to_trailing if existing_ts else False
                                        
                                        # If switched to dynamic trailing, don't create take profit order
                                        take_profit_for_oca = None if has_switched else protection_info.get('take_profit_price')
                                        if has_switched:
                                            logger.info(f"📝 [PAPER] 🔄 {parsed_signal['symbol']} 已切换到动态trailing，加仓时只创建止损订单")
                                        
                                        position_result = tiger_paper_client.get_positions(symbol=parsed_signal['symbol'])
                                        if position_result['success'] and position_result['positions']:
                                            current_quantity = position_result['positions'][0]['quantity']
                                            protection_quantity = abs(current_quantity)
                                            
                                            protection_result = tiger_paper_client.create_oca_orders_for_position(
                                                symbol=parsed_signal['symbol'],
                                                quantity=protection_quantity,
                                                stop_loss_price=protection_info.get('stop_loss_price'),
                                                take_profit_price=take_profit_for_oca
                                            )
                                            
                                            if protection_result['success']:
                                                logger.info(f"📝 [PAPER] Auto-protection applied successfully for {parsed_signal['symbol']}")
                                                trade.needs_auto_protection = False
                                                
                                                # Update trailing stop position for position increase (paper)
                                                try:
                                                    from trailing_stop_engine import update_trailing_stop_on_position_increase
                                                    
                                                    avg_cost = position_result['positions'][0].get('average_cost', 0)
                                                    
                                                    ts_update = update_trailing_stop_on_position_increase(
                                                        symbol=parsed_signal['symbol'],
                                                        account_type='paper',
                                                        new_quantity=protection_quantity,
                                                        new_entry_price=avg_cost,
                                                        new_stop_loss_price=protection_info.get('stop_loss_price'),
                                                        new_take_profit_price=protection_info.get('take_profit_price'),
                                                        new_stop_loss_order_id=protection_result.get('stop_loss_order_id'),
                                                        new_take_profit_order_id=protection_result.get('take_profit_order_id')
                                                    )
                                                    if ts_update['success']:
                                                        logger.info(f"📝 [PAPER] 加仓后更新TrailingStop成功: {ts_update['message']}")
                                                        
                                                        # Immediately optimize stop loss based on current price and tier
                                                        try:
                                                            from trailing_stop_engine import calculate_optimal_stop_after_scaling
                                                            from tiger_client import get_tiger_quote_client
                                                            
                                                            # Get current price using smart price (支持盘前盘后)
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
                                except Exception as e:
                                    logger.error(f"📝 [PAPER] Failed to apply auto-protection: {str(e)}")
                            
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
                            
                            # Auto-create trailing stop for non-close orders (paper account)
                            if not getattr(trade, 'is_close_position', False):
                                try:
                                    from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
                                    from models import TrailingStopMode
                                    
                                    ts_config = get_trailing_stop_config()
                                    if ts_config.is_enabled:
                                        ts_side = 'long' if trade.side.value == 'buy' else 'short'
                                        entry_price = filled_price or parsed_signal.get('reference_price') or parsed_signal.get('price')
                                        timeframe = signal_data.get('extras', {}).get('timeframe', '15') if isinstance(signal_data, dict) else '15'
                                        
                                        trailing_position = create_trailing_stop_for_trade(
                                            trade_id=trade.id,
                                            symbol=trade.symbol,
                                            side=ts_side,
                                            entry_price=entry_price,
                                            quantity=trade.quantity,
                                            account_type='paper',
                                            fixed_stop_loss=trade.stop_loss_price,
                                            fixed_take_profit=trade.take_profit_price,
                                            stop_loss_order_id=trade.stop_loss_order_id,
                                            take_profit_order_id=trade.take_profit_order_id,
                                            mode=TrailingStopMode.BALANCED,
                                            timeframe=str(timeframe)
                                        )
                                        logger.info(f"🎯 [PAPER] Auto-created trailing stop for {trade.symbol}, side={ts_side}, entry=${entry_price:.2f}")
                                except Exception as ts_err:
                                    logger.error(f"🎯 [PAPER] Failed to create trailing stop: {str(ts_err)}")
                                
                                # Create CompletedTrade record for analytics (paper account)
                                try:
                                    signal_grades = parse_signal_grades(signal_data)
                                    completed_trade = CompletedTrade(
                                        symbol=trade.symbol,
                                        account_type='paper',
                                        entry_signal_content=raw_data,
                                        entry_time=datetime.utcnow(),
                                        entry_price=entry_price,
                                        entry_quantity=trade.quantity,
                                        side=ts_side,
                                        original_stop_loss=trade.stop_loss_price,
                                        original_take_profit=trade.take_profit_price,
                                        trade_id=trade.id,
                                        trailing_stop_id=trailing_position.id if trailing_position else None,
                                        signal_indicator=signal_grades.get('signal_indicator'),
                                        signal_type=signal_grades.get('signal_type'),
                                        signal_grade=signal_grades.get('signal_grade'),
                                        signal_score=signal_grades.get('signal_score'),
                                        htf_grade=signal_grades.get('htf_grade'),
                                        htf_score=signal_grades.get('htf_score'),
                                        htf_pass_status=signal_grades.get('htf_pass_status'),
                                        trend_strength=signal_grades.get('trend_strength'),
                                        signal_timeframe=signal_grades.get('signal_timeframe'),
                                        is_open=True
                                    )
                                    db.session.add(completed_trade)
                                    db.session.commit()
                                    logger.info(f"📊 [PAPER] Created CompletedTrade record #{completed_trade.id} for {trade.symbol}")
                                except Exception as ct_error:
                                    db.session.rollback()
                                    logger.error(f"📊 [PAPER] Failed to create CompletedTrade: {str(ct_error)}")
                                    
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
            prefix = "[PAPER]"
        else:
            tiger_client = TigerClient()
            prefix = ""
        
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
            result = tiger_client.close_position_with_sandbox_fallback(
                parsed_signal['symbol'], 
                trading_session,
                reference_price=reference_price,
                signal_side=signal_side
            )
            
            if result.get('no_action'):
                return {'success': True, 'no_action': True, 'message': result.get('message'), 'account_type': account_type}
            
            if result['success']:
                trade = Trade()
                trade.symbol = f"{prefix}{parsed_signal['symbol']}" if prefix else parsed_signal['symbol']
                trade.side = Side(result['action'])
                trade.quantity = result['quantity']
                
                order_type_str = result.get('order_type', 'market')
                if order_type_str == 'limit':
                    trade.order_type = OrderType.LIMIT
                    trade.price = result.get('order_price')
                else:
                    trade.order_type = OrderType.MARKET
                    trade.price = None
                
                trade.signal_data = f"{prefix} {raw_data}" if prefix else raw_data
                trade.tiger_order_id = result['order_id']
                trade.status = OrderStatus.PENDING
                trade.trading_session = result.get('trading_session', trading_session)
                trade.outside_rth = result.get('outside_rth', False)
                trade.is_close_position = True
                trade.account_type = account_type
                
                # Store pre-close average cost
                if pre_close_avg_cost:
                    trade.entry_avg_cost = pre_close_avg_cost
                    logger.info(f"🔄 [{account_type.upper()}] Stored pre-close avg cost ${pre_close_avg_cost:.2f} for {trade.symbol}")
                
                trade.tiger_response = json.dumps(result, ensure_ascii=False, indent=2)
                
                db.session.add(trade)
                db.session.commit()
                
                # Update CompletedTrade record for webhook exit
                try:
                    symbol_to_find = f"{prefix}{parsed_signal['symbol']}" if prefix else parsed_signal['symbol']
                    completed_trade = CompletedTrade.query.filter_by(
                        symbol=symbol_to_find,
                        account_type=account_type,
                        is_open=True
                    ).order_by(CompletedTrade.created_at.desc()).first()
                    
                    if completed_trade:
                        exit_price = result.get('order_price') or reference_price or parsed_signal.get('reference_price')
                        # Fallback: Get current market price from Tiger API if exit_price is not available
                        # 使用智能价格获取 - 盘前盘后时段使用timeline API
                        if not exit_price:
                            try:
                                from tiger_client import get_tiger_quote_client
                                quote_client = get_tiger_quote_client()
                                if quote_client:
                                    trade_data = quote_client.get_smart_price(parsed_signal['symbol'])
                                    if trade_data and trade_data.get('price'):
                                        exit_price = trade_data['price']
                                        price_session = trade_data.get('session', 'regular')
                                        logger.info(f"📊 Using {price_session} price {exit_price} as exit price for {parsed_signal['symbol']}")
                            except Exception as price_err:
                                logger.warning(f"📊 Failed to get market price for exit: {str(price_err)}")
                        
                        # 如果订单已成交，等待后从Tiger API获取实际成交价
                        if not exit_price and result.get('success') and result.get('order_id'):
                            try:
                                import time
                                time.sleep(1)  # 等待订单处理
                                order_status = tiger_client.get_order_status(result['order_id'])
                                if order_status.get('success') and order_status.get('filled_price'):
                                    exit_price = order_status['filled_price']
                                    logger.info(f"📊 Using Tiger filled price {exit_price} as exit price")
                            except Exception as status_err:
                                logger.warning(f"📊 Failed to get order status for exit price: {str(status_err)}")
                        
                        completed_trade.exit_method = ExitMethod.WEBHOOK_SIGNAL
                        completed_trade.exit_signal_content = raw_data
                        completed_trade.exit_time = datetime.utcnow()
                        completed_trade.exit_price = exit_price
                        completed_trade.exit_quantity = result['quantity']
                        completed_trade.is_open = False
                        
                        # Calculate P&L
                        if completed_trade.entry_price and exit_price:
                            if completed_trade.side == 'long':
                                completed_trade.pnl_amount = (exit_price - completed_trade.entry_price) * completed_trade.entry_quantity
                                completed_trade.pnl_percent = ((exit_price - completed_trade.entry_price) / completed_trade.entry_price) * 100
                            else:  # short
                                completed_trade.pnl_amount = (completed_trade.entry_price - exit_price) * completed_trade.entry_quantity
                                completed_trade.pnl_percent = ((completed_trade.entry_price - exit_price) / completed_trade.entry_price) * 100
                        
                        # Calculate hold duration
                        if completed_trade.entry_time:
                            hold_duration = datetime.utcnow() - completed_trade.entry_time
                            completed_trade.hold_duration_seconds = int(hold_duration.total_seconds())
                        
                        db.session.commit()
                        logger.info(f"📊 [{account_type.upper()}] Updated CompletedTrade #{completed_trade.id} with webhook exit, P&L: {completed_trade.pnl_percent:.2f}%")
                    else:
                        logger.warning(f"📊 [{account_type.upper()}] No open CompletedTrade found for {symbol_to_find}")
                except Exception as ct_error:
                    logger.error(f"📊 [{account_type.upper()}] Failed to update CompletedTrade on exit: {str(ct_error)}")
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
            trade.signal_data = f"{prefix} {raw_data}" if prefix else raw_data
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
                # Add prefix to symbol for database record after API call
                trade.symbol = f"{prefix}{parsed_signal['symbol']}" if prefix else parsed_signal['symbol']
                
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
                            timeframe=str(timeframe)
                        )
                        logger.info(f"🎯 [{account_type.upper()}] Auto-created trailing stop for {ts_symbol}, side={ts_side}, entry=${entry_price:.2f}")
                except Exception as e:
                    logger.error(f"🎯 [{account_type.upper()}] Failed to create trailing stop: {str(e)}")
                
                # Create CompletedTrade record for analytics (independent of trailing stop)
                try:
                    signal_grades = parse_signal_grades(signal_data)
                    completed_trade = CompletedTrade(
                        symbol=ts_symbol,
                        account_type=account_type,
                        entry_signal_content=raw_data,
                        entry_time=datetime.utcnow(),
                        entry_price=entry_price,
                        entry_quantity=parsed_signal['quantity'],
                        side=ts_side,
                        original_stop_loss=parsed_signal.get('stop_loss'),
                        original_take_profit=parsed_signal.get('take_profit'),
                        trade_id=trade.id,
                        trailing_stop_id=trailing_position.id if trailing_position else None,
                        signal_indicator=signal_grades.get('signal_indicator'),
                        signal_type=signal_grades.get('signal_type'),
                        signal_grade=signal_grades.get('signal_grade'),
                        signal_score=signal_grades.get('signal_score'),
                        htf_grade=signal_grades.get('htf_grade'),
                        htf_score=signal_grades.get('htf_score'),
                        htf_pass_status=signal_grades.get('htf_pass_status'),
                        trend_strength=signal_grades.get('trend_strength'),
                        signal_timeframe=signal_grades.get('signal_timeframe'),
                        is_open=True
                    )
                    db.session.add(completed_trade)
                    db.session.commit()
                    logger.info(f"📊 [{account_type.upper()}] Created CompletedTrade record #{completed_trade.id} for {ts_symbol}")
                except Exception as ct_error:
                    db.session.rollback()
                    logger.error(f"📊 [{account_type.upper()}] Failed to create CompletedTrade: {str(ct_error)}")
        
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

@app.route('/config', methods=['GET', 'POST'])
def config():
    """Configuration management"""
    if request.method == 'POST':
        try:
            # Update configurations
            for key in ['TIGER_API_KEY', 'TIGER_SECRET_KEY', 'TIGER_ACCOUNT', 
                       'MAX_TRADE_AMOUNT', 'TRADING_ENABLED', 'DISCORD_WEBHOOK_URL', 
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
        'TIGER_API_KEY': get_config('TIGER_API_KEY', ''),
        'TIGER_SECRET_KEY': get_config('TIGER_SECRET_KEY', ''),
        'TIGER_ACCOUNT': get_config('TIGER_ACCOUNT', ''),
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
        
        # Create OCA orders through Tiger client
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

@app.route('/positions')
def positions():
    """View current positions"""
    try:
        tiger_client = TigerClient()
        result = tiger_client.get_positions()
        
        if result['success']:
            positions_data = result['positions']
            total_market_value = sum(pos['market_value'] for pos in positions_data if pos['market_value'])
            total_pnl = sum(pos['unrealized_pnl'] for pos in positions_data if pos['unrealized_pnl'])
        else:
            positions_data = []
            total_market_value = 0
            total_pnl = 0
            flash(f"Error fetching positions: {result['error']}", 'error')
        
        return render_template('positions.html', 
                             positions=positions_data,
                             total_market_value=total_market_value,
                             total_pnl=total_pnl)
    
    except Exception as e:
        logger.error(f"Error in positions route: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return render_template('positions.html', 
                             positions=[],
                             total_market_value=0,
                             total_pnl=0)

@app.route('/closed-trades')
def closed_trades():
    """Display closed trades with realized P&L (已平仓交易记录)"""
    try:
        # Get date range from query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        symbol = request.args.get('symbol')
        account_type = request.args.get('account_type', 'real')  # real or paper
        
        # Select appropriate client
        if account_type == 'paper':
            tiger_client = TigerPaperClient()
        else:
            tiger_client = TigerClient()
        
        # Get filled orders from Tiger API
        result = tiger_client.get_filled_orders(
            start_date=start_date,
            end_date=end_date,
            symbol=symbol,
            limit=100
        )
        
        # Get current positions to get average cost for each symbol
        positions_result = tiger_client.get_positions()
        position_costs = {}
        if positions_result.get('success'):
            for pos in positions_result.get('positions', []):
                position_costs[pos['symbol']] = pos.get('average_cost', 0)
        
        if result['success']:
            orders = result['orders']
            
            # Add average cost from current positions
            for order in orders:
                order['avg_cost'] = position_costs.get(order.get('symbol'), 0)
            
            # Calculate totals
            total_pnl = sum(order.get('realized_pnl', 0) or 0 for order in orders)
            total_commission = sum(order.get('commission', 0) or 0 for order in orders)
            
            # Separate buy and sell orders for summary
            sell_orders = [o for o in orders if 'SELL' in str(o.get('action', '')).upper()]
            buy_orders = [o for o in orders if 'BUY' in str(o.get('action', '')).upper()]
        else:
            orders = []
            total_pnl = 0
            total_commission = 0
            sell_orders = []
            buy_orders = []
            flash(f"Error fetching orders: {result.get('error', 'Unknown error')}", 'error')
        
        return render_template('closed_trades.html',
                             orders=orders,
                             total_pnl=total_pnl,
                             total_commission=total_commission,
                             sell_count=len(sell_orders),
                             buy_count=len(buy_orders),
                             start_date=start_date,
                             end_date=end_date,
                             symbol_filter=symbol,
                             account_type=account_type)
    
    except Exception as e:
        logger.error(f"Error in closed_trades route: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return render_template('closed_trades.html',
                             orders=[],
                             total_pnl=0,
                             total_commission=0,
                             sell_count=0,
                             buy_count=0,
                             start_date=None,
                             end_date=None,
                             symbol_filter=None,
                             account_type='real')

@app.route('/signal-logs')
def signal_logs():
    """Display all webhook signals received and Tiger API response status"""
    try:
        endpoint_filter = request.args.get('ep')
        
        query = SignalLog.query.order_by(SignalLog.created_at.desc())
        
        if endpoint_filter:
            query = query.filter(SignalLog.endpoint == f'/{endpoint_filter}')
        
        signals = query.limit(200).all()
        
        return render_template('signal_logs.html',
                             signals=signals,
                             endpoint_filter=endpoint_filter)
    
    except Exception as e:
        logger.error(f"Error in signal_logs route: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return render_template('signal_logs.html',
                             signals=[],
                             endpoint_filter=None)

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
        from tiger_client import TigerClient
        from datetime import datetime, timedelta
        
        config = get_trailing_stop_config()
        
        positions = TrailingStopPosition.query.filter_by(is_active=True).all()
        active_count = len(positions)
        switched_count = sum(1 for p in positions if p.has_switched_to_trailing)
        
        if positions:
            from trailing_stop_engine import get_cached_tiger_positions
            tiger_positions = get_cached_tiger_positions()
            
            for pos in positions:
                clean_symbol = pos.symbol.replace('[PAPER]', '').strip()
                account_positions = tiger_positions.get(pos.account_type, {})
                tiger_pos = account_positions.get(clean_symbol)
                
                if tiger_pos:
                    avg_cost = tiger_pos.get('average_cost', 0)
                    quantity = abs(tiger_pos.get('quantity', 0))
                    unrealized_pnl = tiger_pos.get('unrealized_pnl', 0)
                    
                    if avg_cost and quantity:
                        pos.display_profit_pct = (unrealized_pnl / (avg_cost * quantity)) * 100
                    else:
                        pos.display_profit_pct = None
                    
                    market_value = tiger_pos.get('market_value', 0)
                    if market_value and quantity:
                        pos.current_price = abs(market_value / quantity)
                    else:
                        pos.current_price = None
                else:
                    pos.current_price = None
                    pos.display_profit_pct = None
        
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
                             recent_triggers=recent_triggers)
    
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


@app.route('/api/trailing-stop/check', methods=['POST'])
def api_trailing_stop_check():
    """Manually trigger trailing stop check for all positions"""
    try:
        from trailing_stop_engine import process_all_active_positions
        
        results = process_all_active_positions()
        
        triggered_count = sum(1 for r in results if r.get('action') == 'trigger')
        switched_count = sum(1 for r in results if r.get('action') == 'switch')
        
        return jsonify({
            'success': True,
            'checked': len(results),
            'triggered': triggered_count,
            'switched': switched_count,
            'message': f'检查完成: {len(results)}个持仓, {triggered_count}个触发, {switched_count}个切换'
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
            fixed_take_profit=fixed_take_profit
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
    """Trade analytics page showing signal quality and P&L metrics"""
    from models import CompletedTrade, ExitMethod
    from datetime import datetime, timedelta
    import pytz
    
    # Get filter parameters
    account_type = request.args.get('account_type', 'paper')
    signal_grade = request.args.get('signal_grade', '')
    exit_method = request.args.get('exit_method', '')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    
    # Build query
    query = CompletedTrade.query.filter_by(account_type=account_type, is_open=False)
    
    if signal_grade:
        query = query.filter(CompletedTrade.signal_grade == signal_grade)
    
    if exit_method:
        try:
            exit_method_enum = ExitMethod(exit_method)
            query = query.filter(CompletedTrade.exit_method == exit_method_enum)
        except ValueError:
            pass
    
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            query = query.filter(CompletedTrade.exit_time >= start_dt)
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            query = query.filter(CompletedTrade.exit_time < end_dt)
        except ValueError:
            pass
    
    # Order by exit_time desc
    trades = query.order_by(CompletedTrade.exit_time.desc()).all()
    
    # Calculate summary statistics
    total_pnl = sum(t.pnl_amount or 0 for t in trades)
    winning_trades = [t for t in trades if (t.pnl_amount or 0) > 0]
    losing_trades = [t for t in trades if (t.pnl_amount or 0) < 0]
    
    win_rate = (len(winning_trades) / len(trades) * 100) if trades else 0
    avg_win = sum(t.pnl_amount for t in winning_trades) / len(winning_trades) if winning_trades else 0
    avg_loss = abs(sum(t.pnl_amount for t in losing_trades) / len(losing_trades)) if losing_trades else 0
    profit_factor = (sum(t.pnl_amount for t in winning_trades) / abs(sum(t.pnl_amount for t in losing_trades))) if losing_trades and sum(t.pnl_amount for t in winning_trades) else 0
    
    # Group by signal grade
    grade_stats = {}
    for grade in ['A', 'B', 'C', None]:
        grade_trades = [t for t in trades if t.signal_grade == grade]
        if grade_trades:
            grade_key = grade or 'Unknown'
            grade_stats[grade_key] = {
                'count': len(grade_trades),
                'pnl': sum(t.pnl_amount or 0 for t in grade_trades),
                'win_rate': len([t for t in grade_trades if (t.pnl_amount or 0) > 0]) / len(grade_trades) * 100
            }
    
    # Group by exit method
    exit_stats = {}
    for method in ExitMethod:
        method_trades = [t for t in trades if t.exit_method == method]
        if method_trades:
            exit_stats[method.value] = {
                'count': len(method_trades),
                'pnl': sum(t.pnl_amount or 0 for t in method_trades),
                'win_rate': len([t for t in method_trades if (t.pnl_amount or 0) > 0]) / len(method_trades) * 100
            }
    
    # Convert to Eastern Time for display
    eastern = pytz.timezone('US/Eastern')
    for trade in trades:
        if trade.entry_time:
            trade.entry_time_et = trade.entry_time.replace(tzinfo=pytz.UTC).astimezone(eastern)
        else:
            trade.entry_time_et = None
        if trade.exit_time:
            trade.exit_time_et = trade.exit_time.replace(tzinfo=pytz.UTC).astimezone(eastern)
        else:
            trade.exit_time_et = None
    
    return render_template('trade_analytics.html',
        trades=trades,
        account_type=account_type,
        signal_grade=signal_grade,
        exit_method_filter=exit_method,
        start_date=start_date,
        end_date=end_date,
        total_pnl=total_pnl,
        total_trades=len(trades),
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        profit_factor=profit_factor,
        grade_stats=grade_stats,
        exit_stats=exit_stats,
        ExitMethod=ExitMethod
    )


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
                # 从symbol中提取纯symbol（去掉[PAPER]前缀）
                clean_symbol = trade.symbol.replace('[PAPER]', '').strip()
                
                # 尝试获取当前价格作为exit_price的近似值
                if quote_client:
                    quote_result = quote_client.get_smart_price(clean_symbol)
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


