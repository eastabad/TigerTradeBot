import os
import logging
from datetime import datetime
from tigeropen.tiger_open_config import TigerOpenClientConfig
from tigeropen.trade.trade_client import TradeClient
from tigeropen.common.util.contract_utils import stock_contract
from tigeropen.common.util.order_utils import market_order, limit_order, limit_order_with_legs, order_leg
from tigeropen.common.consts import Language, Market, Currency, TradingSessionType
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
                    # For market orders with attachments, we need to use limit order at a reasonable price
                    # This is a limitation - attachments only work with limit orders
                    logger.warning("Market orders don't support attachments, converting to limit order")
                    return {
                        'success': False,
                        'error': 'Market orders with stop loss/take profit not supported. Use limit orders.'
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