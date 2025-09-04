import json
import logging
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, flash, redirect, url_for
from app import app, db
from models import Trade, TradingConfig, SignalLog, OrderStatus, OrderType, Side
from tiger_client import TigerClient
from signal_parser import SignalParser
from config import get_config, set_config
from discord_notifier import discord_notifier

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
                logger.info(f"Processing close signal for {parsed_signal['symbol']}")
                
                # Execute close position
                tiger_client = TigerClient()
                trading_session = parsed_signal.get('trading_session', 'regular')
                result = tiger_client.close_position_with_sandbox_fallback(parsed_signal['symbol'], trading_session)
                
                # Create trade record for close position
                if result['success']:
                    trade = Trade()
                    trade.symbol = parsed_signal['symbol']
                    trade.side = Side(result['action'])  # Use the determined action from close_position
                    trade.quantity = result['quantity']
                    trade.price = None  # Market order for close
                    trade.order_type = OrderType.MARKET
                    trade.signal_data = raw_data
                    trade.tiger_order_id = result['order_id']
                    trade.status = OrderStatus.PENDING
                    trade.trading_session = trading_session
                    trade.outside_rth = parsed_signal.get('outside_rth', trading_session != 'regular')
                    trade.is_close_position = True  # Mark as close position
                    
                    db.session.add(trade)
                    db.session.flush()
                    
                    signal_log.parsed_successfully = True
                    signal_log.trade_id = trade.id
                    logger.info(f"Close position order placed: {result['order_id']}")
                else:
                    logger.error(f"Failed to close position: {result['error']}")
                    signal_log.error_message = result['error']
                    result['success'] = False
                
            else:
                # Regular trade signal processing
                
                # Check risk limits
                max_trade_amount = float(get_config('MAX_TRADE_AMOUNT', '10000'))
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
                
                # Save attached order IDs if present
                if 'stop_loss_order_id' in result:
                    trade.stop_loss_order_id = result['stop_loss_order_id']
                    logger.info(f"Stop loss order created: {result['stop_loss_order_id']}")
                
                if 'take_profit_order_id' in result:
                    trade.take_profit_order_id = result['take_profit_order_id']
                    logger.info(f"Take profit order created: {result['take_profit_order_id']}")
                
                logger.info(f"Order placed successfully: {result['order_id']}")
                
                # Send Discord notification for successful order placement
                try:
                    discord_notifier.send_order_notification(trade, 'pending', is_close=getattr(trade, 'is_close_position', False))
                except Exception as e:
                    logger.error(f"Failed to send Discord notification: {str(e)}")
                
            elif trade is not None:
                trade.status = OrderStatus.REJECTED
                trade.error_message = result.get('error', 'Unknown error')
                logger.error(f"Order rejected: {result.get('error', 'Unknown error')}")
                
                # Send Discord notification for rejected order
                try:
                    discord_notifier.send_order_notification(trade, 'rejected', is_close=getattr(trade, 'is_close_position', False))
                except Exception as e:
                    logger.error(f"Failed to send Discord notification: {str(e)}")
            
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

@app.route('/trades')
def trades():
    """View all trades with real-time status from Tiger API"""
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    trades_query = Trade.query.order_by(Trade.created_at.desc())
    trades_pagination = trades_query.paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    # Get real-time status for each trade with Tiger order ID
    tiger_client = TigerClient()
    for trade in trades_pagination.items:
        if trade.tiger_order_id:
            try:
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
                         pagination=trades_pagination)

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
        'MAX_TRADE_AMOUNT': get_config('MAX_TRADE_AMOUNT', '10000'),
        'TRADING_ENABLED': get_config('TRADING_ENABLED', 'true'),
        'DISCORD_WEBHOOK_URL': get_config('DISCORD_WEBHOOK_URL', ''),
        'DISCORD_TTS_WEBHOOK_URL': get_config('DISCORD_TTS_WEBHOOK_URL', '')
    }
    
    return render_template('config.html', configs=configs)

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

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'trading_enabled': get_config('TRADING_ENABLED', 'true') == 'true'
    })
