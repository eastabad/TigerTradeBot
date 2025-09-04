import os
import logging
from datetime import datetime
from tigeropen.tiger_open_config import get_client_config
from tigeropen.trade.trade_client import TradeClient
from tigeropen.common.util.contract_utils import stock_contract
from tigeropen.common.util.order_utils import market_order, limit_order
from tigeropen.common.consts import Market, Currency
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
            # Use TigerOpenClientConfig with props_path pointing to current directory
            from tigeropen.tiger_open_config import TigerOpenClientConfig
            
            # Initialize client config using the config file in current directory
            self.client_config = TigerOpenClientConfig(props_path='./')
            
            # Override account if set in database config
            account_override = get_config('TIGER_ACCOUNT')
            if account_override:
                self.client_config.account = account_override
            
            # Set additional configuration
            self.client_config.language = 'zh_CN'
            self.client_config.timeout = 15
            
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
            
            # Check if account is configured
            if not self.client_config.account:
                return {
                    'success': False,
                    'error': 'Trading account not configured'
                }
            
            # Create contract
            contract = stock_contract(
                symbol=trade.symbol,
                currency=Currency.USD.value
            )
            
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
            response = self.client.place_order(order)
            
            if response:
                order_id = str(response)
                logger.info(f"Order placed successfully: {order_id}")
                return {
                    'success': True,
                    'order_id': order_id
                }
            else:
                logger.error("Order placement failed: No response")
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