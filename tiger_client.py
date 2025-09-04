import os
import logging
from datetime import datetime
from tigeropen.tiger_open_config import TigerOpenClientConfig
from tigeropen.trade.trade_client import TradeClient
from tigeropen.common.util.contract_utils import stock_contract
from tigeropen.common.util.order_utils import market_order, limit_order, limit_order_with_legs, order_leg
from tigeropen.common.consts import Language, Market, Currency, TradingSessionType, SecurityType
from tigeropen.common.util.signature_utils import read_private_key
from models import OrderType, Side
from config import get_config

logger = logging.getLogger(__name__)

class TigerClient:
    def __init__(self):
        self.client = None
        self.client_config = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Tiger OpenAPI client using config file"""
        try:
            # Create client config
            self.client_config = TigerOpenClientConfig()
            
            # Read configuration from tiger_openapi_config.properties
            config_path = './tiger_openapi_config.properties'
            if os.path.exists(config_path):
                config_data = {}
                with open(config_path, 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            config_data[key] = value
                
                # Set configuration from file
                self.client_config.tiger_id = config_data.get('tiger_id')
                self.client_config.account = config_data.get('account')
                
                # Set private key - use pk8 format
                private_key_pk8 = config_data.get('private_key_pk8')
                if private_key_pk8:
                    self.client_config.private_key = private_key_pk8
                
                # Set other config
                self.client_config.language = Language.zh_CN
                
                logger.info(f"Config loaded - Tiger ID: {self.client_config.tiger_id}, Account: {self.client_config.account}")
                
            else:
                logger.error("Config file not found")
                return
            
            # Override account if set in database config
            account_override = get_config('TIGER_ACCOUNT')
            if account_override:
                self.client_config.account = account_override
                logger.info(f"Account overridden to: {account_override}")
            
            # Validate required fields
            if not all([self.client_config.tiger_id, self.client_config.private_key, self.client_config.account]):
                logger.error(f"Missing required config: tiger_id={bool(self.client_config.tiger_id)}, private_key={bool(self.client_config.private_key)}, account={bool(self.client_config.account)}")
                return
            
            # Initialize trade client
            self.client = TradeClient(self.client_config)
            
            logger.info(f"Tiger client initialized successfully with account: {self.client_config.account}")
                
        except Exception as e:
            logger.error(f"Failed to initialize Tiger client: {str(e)}")
            self.client = None
            self.client_config = None
    
    def place_order(self, trade):
        """Place an order through Tiger API with optional stop loss and take profit"""
        if not self.client or not self.client_config:
            return {
                'success': False,
                'error': 'Tiger client not initialized'
            }
        
        try:
            # Check if trading is enabled
            if get_config('TRADING_ENABLED', 'true').lower() != 'true':
                return {
                    'success': False,
                    'error': 'Trading is currently disabled'
                }
            
            # Create contract
            contract = stock_contract(symbol=trade.symbol, currency='USD')
            action = 'BUY' if trade.side == Side.BUY else 'SELL'
            
            # Determine trading session type
            session_map = {
                'regular': None,  # Default - no special session type needed
                'extended': None,  # Extended hours handled by outside_rth flag
                'overnight': TradingSessionType.OVERNIGHT,
                'full': TradingSessionType.FULL
            }
            
            trading_session_type = session_map.get(trade.trading_session)
            outside_rth = getattr(trade, 'outside_rth', False)
            
            logger.info(f"Trading session: {trade.trading_session}, outside_rth: {outside_rth}")
            
            # Check if we have stop loss or take profit
            has_stop_loss = trade.stop_loss_price is not None
            has_take_profit = trade.take_profit_price is not None
            
            order = None
            
            # Create order with or without attached orders (止损止盈)
            if has_stop_loss or has_take_profit:
                # Use limit order for orders with attached orders (market orders don't support attachments)
                if trade.order_type == OrderType.MARKET:
                    # For market orders with attachments, convert to limit order at market price
                    logger.warning("Market orders don't support attachments, converting to limit order")
                    # For market orders with stop/take profit, use reference price from signal
                    reference_price = getattr(trade, 'reference_price', None)
                    if reference_price:
                        # Use reference price as basis for limit order, rounded to 0.01
                        if action == 'BUY':
                            trade.price = round(reference_price * 1.01, 2)  # Buy at 1% above reference, rounded
                        else:
                            trade.price = round(reference_price * 0.99, 2)  # Sell at 1% below reference, rounded
                        logger.info(f"Converted market order to limit order at ${trade.price:.2f} (reference: ${reference_price:.2f})")
                    else:
                        # No reference price available, reject the order
                        logger.error("No reference price available for market order conversion")
                        return {
                            'success': False,
                            'error': 'Market orders with stop/take profit require reference price. Please use limit orders.'
                        }
                
                # Create order legs for stop loss and take profit
                order_legs = []
                
                if has_stop_loss:
                    # Stop loss order leg
                    stop_loss_leg = order_leg(
                        'LOSS', 
                        trade.stop_loss_price,
                        time_in_force='GTC',
                        outside_rth=outside_rth
                    )
                    order_legs.append(stop_loss_leg)
                    logger.info(f"Adding stop loss at {trade.stop_loss_price}")
                
                if has_take_profit:
                    # Take profit order leg  
                    take_profit_leg = order_leg(
                        'PROFIT',
                        trade.take_profit_price,
                        time_in_force='GTC', 
                        outside_rth=outside_rth
                    )
                    order_legs.append(take_profit_leg)
                    logger.info(f"Adding take profit at {trade.take_profit_price}")
                
                # Create limit order with legs
                order = limit_order_with_legs(
                    account=self.client_config.account,
                    contract=contract,
                    action=action,
                    quantity=int(trade.quantity),
                    limit_price=trade.price,
                    order_legs=order_legs
                )
                
                # Set trading session type and outside_rth
                if trading_session_type:
                    order.trading_session_type = trading_session_type
                order.outside_rth = outside_rth
                
            else:
                # Standard order without attachments
                if trade.order_type == OrderType.MARKET:
                    order = market_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=action,
                        quantity=int(trade.quantity)
                    )
                else:
                    order = limit_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=action,
                        limit_price=trade.price,
                        quantity=int(trade.quantity)
                    )
                
                # Set trading session type and outside_rth for standard orders
                if trading_session_type:
                    order.trading_session_type = trading_session_type
                order.outside_rth = outside_rth
            
            # Place order
            result = self.client.place_order(order)
            
            if result and order.id:
                order_id = str(order.id)
                logger.info(f"Order placed successfully: {order_id}")
                
                response = {
                    'success': True,
                    'order_id': order_id
                }
                
                # If we have attached orders, get their IDs
                if has_stop_loss or has_take_profit:
                    try:
                        # Get attached orders
                        attached_orders = self.client.get_open_orders(
                            account=self.client_config.account,
                            parent_id=order.id
                        )
                        
                        if attached_orders:
                            for attached_order in attached_orders:
                                # Identify stop loss and take profit orders by their type
                                if hasattr(attached_order, 'order_type'):
                                    if 'LOSS' in str(attached_order.order_type):
                                        response['stop_loss_order_id'] = str(attached_order.id)
                                    elif 'PROFIT' in str(attached_order.order_type):
                                        response['take_profit_order_id'] = str(attached_order.id)
                                        
                        logger.info(f"Attached orders created: {len(attached_orders) if attached_orders else 0}")
                        
                    except Exception as e:
                        logger.warning(f"Could not get attached order IDs: {str(e)}")
                
                return response
                
            else:
                logger.error("Order placement failed")
                return {
                    'success': False,
                    'error': 'Order placement failed'
                }
                
        except Exception as e:
            logger.error(f"Error placing order: {str(e)}")
            return {
                'success': False,
                'error': f'Exception: {str(e)}'
            }
    
    def get_order_status(self, order_id):
        """Get order status from Tiger API"""
        if not self.client:
            return {
                'success': False,
                'error': 'Tiger client not initialized'
            }
        
        try:
            # Get single order by ID - use get_order method
            order = self.client.get_order(account=self.client_config.account, id=int(order_id))
            
            if order:
                
                # Map Tiger status to our status
                status_map = {
                    'Initial': 'pending',
                    'Submitted': 'pending', 
                    'Filled': 'filled',
                    'Cancelled': 'cancelled',
                    'Rejected': 'rejected',
                    'PartiallyFilled': 'partially_filled'
                }
                
                # Get status from order object - use correct attribute names
                tiger_status = getattr(order, 'status', 'pending')
                
                # Handle both string and enum status
                if hasattr(tiger_status, 'value'):
                    tiger_status_str = tiger_status.value  # Get value from enum
                else:
                    tiger_status_str = str(tiger_status)   # Convert to string
                
                our_status = status_map.get(tiger_status_str, 'pending')
                
                return {
                    'success': True,
                    'status': our_status,
                    'tiger_status': tiger_status,  # Include original status for debugging
                    'filled_price': getattr(order, 'avg_fill_price', 0) or getattr(order, 'avgFillPrice', 0),
                    'filled_quantity': getattr(order, 'filled_quantity', 0) or getattr(order, 'filledQuantity', 0),
                    'total_quantity': getattr(order, 'total_quantity', 0) or getattr(order, 'totalQuantity', 0)
                }
            else:
                return {
                    'success': False,
                    'error': 'Order not found'
                }
                
        except Exception as e:
            logger.error(f"Error getting order status: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_positions(self, symbol=None):
        """Get current positions"""
        if not self.client:
            return {
                'success': False,
                'error': 'Tiger client not initialized'
            }
        
        try:
            logger.info(f"Getting positions for symbol: {symbol or 'all'}")
            
            # Get positions from Tiger API
            positions = self.client.get_positions(
                account=self.client_config.account,
                sec_type=SecurityType.STK,
                currency=Currency.ALL,
                market=Market.ALL,
                symbol=symbol
            )
            
            position_list = []
            for pos in positions:
                position_data = {
                    'symbol': pos.contract.symbol,
                    'quantity': pos.quantity,
                    'average_cost': pos.average_cost,
                    'market_value': pos.market_value,
                    'unrealized_pnl': pos.unrealized_pnl,
                    'sec_type': pos.contract.sec_type,
                    'currency': pos.contract.currency,
                    'multiplier': getattr(pos.contract, 'multiplier', 1)
                }
                position_list.append(position_data)
                logger.info(f"Position: {pos.contract.symbol}, Qty: {pos.quantity}, Cost: {pos.average_cost}")
            
            return {
                'success': True,
                'positions': position_list,
                'count': len(position_list)
            }
            
        except Exception as e:
            logger.error(f"Error getting positions: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'positions': []
            }
    
    def get_open_orders_for_symbol(self, symbol):
        """Get all open orders for a specific symbol"""
        try:
            if not self.client:
                return {'success': False, 'error': 'Tiger client not initialized'}

            # Get open orders for the specific symbol
            open_orders = self.client.get_open_orders(symbol=symbol)
            logger.info(f"Retrieved {len(open_orders)} open orders for {symbol}")
            
            # Log details of each order
            for order in open_orders:
                order_dict = order.__dict__ if hasattr(order, '__dict__') else order
                order_id = getattr(order, 'id', None) or order_dict.get('id', 'unknown')
                action = getattr(order, 'action', None) or order_dict.get('action', 'unknown')
                quantity = getattr(order, 'totalQuantity', None) or order_dict.get('totalQuantity', 'unknown')
                limit_price = getattr(order, 'limitPrice', None) or order_dict.get('limitPrice', 'MKT')
                status = getattr(order, 'status', None) or order_dict.get('status', 'unknown')
                can_cancel = getattr(order, 'canCancel', None) or order_dict.get('canCancel', False)
                logger.info(f"Open order: {order_id} - {action} {quantity} {symbol} @ {limit_price} - Status: {status} - CanCancel: {can_cancel}")
            
            return {'success': True, 'orders': open_orders}
            
        except Exception as e:
            logger.error(f"Error getting open orders for {symbol}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def force_cancel_all_orders_for_symbol(self, symbol):
        """Force cancel ALL orders for a specific symbol"""
        try:
            if not self.client:
                return {'success': False, 'error': 'Tiger client not initialized'}
            
            # Get ALL open orders for the symbol
            open_orders_result = self.get_open_orders_for_symbol(symbol)
            if not open_orders_result['success']:
                return open_orders_result
            
            open_orders = open_orders_result['orders']
            if not open_orders:
                logger.info(f"No open orders found for {symbol}")
                return {'success': True, 'canceled_count': 0}
            
            canceled_count = 0
            errors = []
            
            for order in open_orders:
                order_dict = order.__dict__ if hasattr(order, '__dict__') else order
                order_id = getattr(order, 'id', None) or order_dict.get('id', 'unknown')
                can_cancel = getattr(order, 'canCancel', None) or order_dict.get('canCancel', False)
                
                logger.info(f"Processing order {order_id} for {symbol} - CanCancel: {can_cancel}")
                
                if can_cancel and order_id != 'unknown':
                    cancel_result = self.cancel_order(order_id)
                    if cancel_result['success']:
                        canceled_count += 1
                        logger.info(f"Successfully canceled order {order_id}")
                    else:
                        error_msg = f"Failed to cancel order {order_id}: {cancel_result.get('error')}"
                        logger.error(error_msg)
                        errors.append(error_msg)
                else:
                    logger.info(f"Order {order_id} cannot be canceled (canCancel={can_cancel})")
            
            logger.info(f"Canceled {canceled_count} out of {len(open_orders)} orders for {symbol}")
            
            return {
                'success': True,
                'canceled_count': canceled_count,
                'total_orders': len(open_orders),
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"Error force canceling orders for {symbol}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def cancel_order(self, order_id):
        """Cancel a specific order"""
        try:
            trade_client = self.get_trade_client()
            if not trade_client:
                return {'success': False, 'error': 'Failed to initialize trade client'}

            # Cancel the order
            result = trade_client.cancel_order(id=order_id)
            logger.info(f"Cancel order {order_id} result: {result}")
            
            return {'success': True, 'result': result}
            
        except Exception as e:
            logger.error(f"Error canceling order {order_id}: {str(e)}")
            return {'success': False, 'error': str(e)}

    def close_position_with_sandbox_fallback(self, symbol, trading_session='regular'):
        """Close position with sandbox environment fallback strategies"""
        logger.info(f"Attempting to close position for {symbol} with sandbox fallback")
        
        # First try normal close
        result = self.close_position(symbol, trading_session)
        
        # If failed due to salable quantity issue, try fallback strategies
        if not result['success'] and 'salable quantity is 0' in result.get('error', ''):
            logger.info(f"Normal close failed for {symbol} due to salable quantity issue, trying sandbox fallback strategies")
            
            # Strategy 1: Try limit order at current market price - 1%
            try:
                position_result = self.get_positions(symbol=symbol)
                if position_result['success'] and position_result['positions']:
                    position = position_result['positions'][0]
                    current_quantity = position['quantity']
                    
                    # Get current market price and create limit order slightly below market
                    current_price = position.get('latest_price', 0)
                    if current_price > 0:
                        limit_price = round(current_price * 0.99, 2)  # 1% below market
                        logger.info(f"Trying fallback limit order: SELL {current_quantity} {symbol} @ ${limit_price}")
                        
                        # Create limit order directly 
                        contract = stock_contract(symbol=symbol, currency='USD')
                        order = limit_order(
                            account=self.client_config.account,
                            contract=contract,
                            action='SELL',
                            quantity=abs(int(current_quantity)),
                            limit_price=limit_price
                        )
                        order.outside_rth = trading_session != 'regular'
                        
                        fallback_result = self.client.place_order(order)
                        if fallback_result and order.id:
                            logger.info(f"Sandbox fallback limit order successful: {order.id}")
                            return {
                                'success': True,
                                'message': f'Sandbox fallback: Limit order placed for {symbol}',
                                'order_id': str(order.id),
                                'fallback_strategy': 'limit_order',
                                'limit_price': limit_price
                            }
                        
            except Exception as e:
                logger.error(f"Fallback limit order failed: {str(e)}")
            
            # Strategy 2: Return sandbox-specific error message
            return {
                'success': False,
                'error': f'Unable to close {symbol} position - Sandbox environment limitation (salableQty=0). In production, this position would be closeable.',
                'sandbox_limitation': True,
                'original_error': result.get('error')
            }
        
        return result

    def close_position(self, symbol, trading_session='regular'):
        """Close existing position for a symbol"""
        if not self.client:
            return {
                'success': False,
                'error': 'Tiger client not initialized'
            }
        
        try:
            logger.info(f"Attempting to close position for {symbol}")
            
            # Get current position for this symbol
            position_result = self.get_positions(symbol=symbol)
            if not position_result['success'] or not position_result['positions']:
                return {
                    'success': False,
                    'error': f'No position found for {symbol}'
                }
            
            position = position_result['positions'][0]  # Get first matching position
            current_quantity = position['quantity']
            salable_quantity = position.get('salable_qty', current_quantity)  # Get salable quantity
            
            if current_quantity == 0:
                return {
                    'success': False,
                    'error': f'No position to close for {symbol} (quantity is 0)'
                }
            
            logger.info(f"Current position for {symbol}: {current_quantity} shares, salable: {salable_quantity} shares")
            
            # CRITICAL: Force cancel ALL open orders for this symbol before closing position
            logger.info(f"Force canceling ALL open orders for {symbol} before attempting to close position")
            
            cancel_result = self.force_cancel_all_orders_for_symbol(symbol)
            if cancel_result['success']:
                canceled_count = cancel_result['canceled_count']
                total_orders = cancel_result['total_orders']
                logger.info(f"Force canceled {canceled_count} out of {total_orders} orders for {symbol}")
                
                if canceled_count > 0:
                    # Wait for cancellations to process
                    import time
                    time.sleep(2)  # Longer wait for processing
                    
                    # Refresh position info after canceling orders
                    logger.info(f"Refreshing position info for {symbol} after canceling orders")
                    position_result = self.get_positions(symbol=symbol)
                    if position_result['success'] and position_result['positions']:
                        position = position_result['positions'][0]
                        current_quantity = position['quantity']
                        salable_quantity = position.get('salable_qty', current_quantity)
                        logger.info(f"After canceling orders - {symbol}: {current_quantity} shares, salable: {salable_quantity} shares")
            else:
                logger.error(f"Failed to cancel orders for {symbol}: {cancel_result.get('error')}")
            
            # Final check - if still no salable quantity, return error
            if salable_quantity == 0:
                return {
                    'success': False,
                    'error': f'No salable position for {symbol} (salable quantity is 0 even after canceling open orders)'
                }
            
            # Determine action based on current position
            if current_quantity > 0:
                action = 'SELL'  # Close long position
                close_quantity = abs(current_quantity)
            else:
                action = 'BUY'   # Close short position  
                close_quantity = abs(current_quantity)
            
            # Create contract
            contract = stock_contract(symbol=symbol, currency='USD')
            
            # Determine trading session settings
            session_map = {
                'regular': None,
                'extended': None,
                'overnight': TradingSessionType.OVERNIGHT,
                'full': TradingSessionType.FULL
            }
            
            trading_session_type = session_map.get(trading_session)
            outside_rth = trading_session != 'regular'
            
            logger.info(f"Closing position: {action} {close_quantity} shares of {symbol}")
            logger.info(f"Trading session: {trading_session}, outside_rth: {outside_rth}")
            
            # Create market order to close position quickly
            order = market_order(
                account=self.client_config.account,
                contract=contract,
                action=action,
                quantity=int(close_quantity)
            )
            
            # Set trading session type and outside_rth
            if trading_session_type:
                order.trading_session_type = trading_session_type
            order.outside_rth = outside_rth
            
            # Place order
            result = self.client.place_order(order)
            
            if result and order.id:
                order_id = str(order.id)
                logger.info(f"Close position order placed successfully: {order_id}")
                return {
                    'success': True,
                    'message': f'Close position order placed for {symbol}',
                    'order_id': order_id,
                    'action': action.lower(),
                    'quantity': close_quantity,
                    'original_position': current_quantity
                }
            else:
                logger.error(f"Failed to place close position order: {order_id if 'order_id' in locals() else 'Unknown ID'}")
                return {
                    'success': False,
                    'error': 'Failed to place close position order',
                    'details': str(result) if result else 'No result returned'
                }
                
        except Exception as e:
            logger.error(f"Error closing position for {symbol}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def test_connection(self):
        """Test Tiger API connection"""
        if not self.client:
            return False
        
        try:
            # Try to get accounts
            accounts = self.client.get_accounts()
            return accounts is not None and len(accounts) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False