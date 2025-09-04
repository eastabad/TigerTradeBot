import json
import logging
from datetime import datetime, timedelta
from flask import render_template, request, jsonify, flash, redirect, url_for
from app import app, db
from models import Trade, TradingConfig, SignalLog, OrderStatus, OrderType, Side
from tiger_client import TigerClient
from signal_parser import SignalParser
from config import get_config, set_config

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
            
            db.session.add(trade)
            db.session.flush()  # Get the ID
            
            signal_log.parsed_successfully = True
            signal_log.trade_id = trade.id
            
            # Execute the trade
            tiger_client = TigerClient()
            result = tiger_client.place_order(trade)
            
            if result['success']:
                trade.tiger_order_id = result['order_id']
                trade.status = OrderStatus.PENDING
                logger.info(f"Order placed successfully: {result['order_id']}")
            else:
                trade.status = OrderStatus.REJECTED
                trade.error_message = result['error']
                logger.error(f"Order rejected: {result['error']}")
            
            db.session.add(signal_log)
            db.session.commit()
            
            return jsonify({
                'success': result['success'],
                'trade_id': trade.id,
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
    """View all trades"""
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    trades_query = Trade.query.order_by(Trade.created_at.desc())
    trades_pagination = trades_query.paginate(
        page=page, per_page=per_page, error_out=False
    )
    
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
                       'MAX_TRADE_AMOUNT', 'TRADING_ENABLED']:
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
        'TRADING_ENABLED': get_config('TRADING_ENABLED', 'true')
    }
    
    return render_template('config.html', configs=configs)

@app.route('/api/trade/<int:trade_id>/status')
def trade_status(trade_id):
    """Get trade status update"""
    trade = Trade.query.get_or_404(trade_id)
    
    # Update status from Tiger if order ID exists
    if trade.tiger_order_id and trade.status == OrderStatus.PENDING:
        tiger_client = TigerClient()
        status_update = tiger_client.get_order_status(trade.tiger_order_id)
        
        if status_update['success']:
            old_status = trade.status
            trade.status = OrderStatus(status_update['status'])
            trade.filled_price = status_update.get('filled_price')
            trade.filled_quantity = status_update.get('filled_quantity')
            trade.updated_at = datetime.utcnow()
            
            if old_status != trade.status:
                db.session.commit()
                logger.info(f"Trade {trade_id} status updated: {old_status} -> {trade.status}")
    
    return jsonify({
        'id': trade.id,
        'status': trade.status.value,
        'filled_price': trade.filled_price,
        'filled_quantity': trade.filled_quantity,
        'error_message': trade.error_message,
        'updated_at': trade.updated_at.isoformat()
    })

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'trading_enabled': get_config('TRADING_ENABLED', 'true') == 'true'
    })
