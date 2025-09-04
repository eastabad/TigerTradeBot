import os
import logging
from datetime import datetime
from tigeropen.tiger_open_config import TigerOpenClientConfig
from tigeropen.trade.trade_client import TradeClient
from tigeropen.common.util.contract_utils import stock_contract
from tigeropen.common.util.order_utils import market_order, limit_order
from tigeropen.common.consts import Language, Market, Currency
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
        """Place an order through Tiger API"""
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
            
            # Create order based on type
            if trade.order_type == OrderType.MARKET:
                order = market_order(
                    account=self.client_config.account,
                    contract=contract,
                    action='BUY' if trade.side == Side.BUY else 'SELL',
                    quantity=int(trade.quantity)
                )
            else:
                order = limit_order(
                    account=self.client_config.account,
                    contract=contract,
                    action='BUY' if trade.side == Side.BUY else 'SELL',
                    limit_price=trade.price,
                    quantity=int(trade.quantity)
                )
            
            # Place order
            result = self.client.place_order(order)
            
            if result and order.id:
                order_id = str(order.id)
                logger.info(f"Order placed successfully: {order_id}")
                return {
                    'success': True,
                    'order_id': order_id
                }
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
            # Get orders by ID
            orders = self.client.get_orders(order_id=int(order_id))
            
            if orders and len(orders) > 0:
                order = orders[0]
                
                # Map Tiger status to our status
                status_map = {
                    'Submitted': 'pending',
                    'Filled': 'filled',
                    'Cancelled': 'cancelled',
                    'Rejected': 'rejected',
                    'PartiallyFilled': 'partially_filled'
                }
                
                tiger_status = getattr(order, 'status', 'pending')
                our_status = status_map.get(tiger_status, 'pending')
                
                return {
                    'success': True,
                    'status': our_status,
                    'filled_price': getattr(order, 'avg_fill_price', None),
                    'filled_quantity': getattr(order, 'filled_quantity', None)
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