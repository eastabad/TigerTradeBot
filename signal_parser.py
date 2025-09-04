import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class SignalParser:
    def __init__(self):
        self.required_fields = ['symbol', 'side', 'quantity']
        self.optional_fields = ['price', 'order_type', 'time_in_force']
    
    def parse(self, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse TradingView signal data"""
        try:
            # Normalize the signal data
            normalized = self._normalize_signal(signal_data)
            
            # Validate required fields
            self._validate_signal(normalized)
            
            # Apply default values
            normalized = self._apply_defaults(normalized)
            
            logger.info(f"Signal parsed successfully: {normalized}")
            return normalized
            
        except Exception as e:
            logger.error(f"Error parsing signal: {str(e)}")
            raise
    
    def _normalize_signal(self, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize signal data to standard format"""
        normalized = {}
        
        # Handle different TradingView signal formats
        # Format 1: Direct format
        if 'symbol' in signal_data:
            normalized['symbol'] = str(signal_data['symbol']).upper()
        elif 'ticker' in signal_data:
            normalized['symbol'] = str(signal_data['ticker']).upper()
        
        # Side (buy/sell)
        side = signal_data.get('side', signal_data.get('action', '')).lower()
        if side in ['buy', 'long']:
            normalized['side'] = 'buy'
        elif side in ['sell', 'short']:
            normalized['side'] = 'sell'
        else:
            raise ValueError(f"Invalid side: {side}")
        
        # Quantity
        if 'quantity' in signal_data:
            normalized['quantity'] = float(signal_data['quantity'])
        elif 'qty' in signal_data:
            normalized['quantity'] = float(signal_data['qty'])
        elif 'size' in signal_data:
            normalized['quantity'] = float(signal_data['size'])
        else:
            # Default quantity if not specified
            normalized['quantity'] = 1.0
        
        # Price (for limit orders)
        if 'price' in signal_data:
            normalized['price'] = float(signal_data['price'])
        elif 'limit_price' in signal_data:
            normalized['price'] = float(signal_data['limit_price'])
        
        # Order type
        order_type = signal_data.get('order_type', signal_data.get('type', 'market')).lower()
        if order_type in ['market', 'mkt']:
            normalized['order_type'] = 'market'
        elif order_type in ['limit', 'lmt']:
            normalized['order_type'] = 'limit'
        else:
            normalized['order_type'] = 'market'  # Default
        
        return normalized
    
    def _validate_signal(self, signal: Dict[str, Any]) -> None:
        """Validate parsed signal"""
        # Check required fields
        for field in self.required_fields:
            if field not in signal:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate symbol format
        if not signal['symbol'] or len(signal['symbol']) > 20:
            raise ValueError("Invalid symbol")
        
        # Validate side
        if signal['side'] not in ['buy', 'sell']:
            raise ValueError(f"Invalid side: {signal['side']}")
        
        # Validate quantity
        if signal['quantity'] <= 0:
            raise ValueError("Quantity must be positive")
        
        # Validate price for limit orders
        if signal.get('order_type') == 'limit':
            if 'price' not in signal or signal['price'] <= 0:
                raise ValueError("Limit orders require a positive price")
    
    def _apply_defaults(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Apply default values to signal"""
        # Default order type
        if 'order_type' not in signal:
            signal['order_type'] = 'market'
        
        # Default time in force
        if 'time_in_force' not in signal:
            signal['time_in_force'] = 'day'
        
        return signal
    
    @staticmethod
    def create_test_signal(symbol: str = "AAPL", side: str = "buy", quantity: float = 1.0, 
                          order_type: str = "market", price = None) -> Dict[str, Any]:
        """Create a test signal for debugging"""
        signal = {
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'order_type': order_type
        }
        
        if price and order_type == 'limit':
            signal['price'] = price
        
        return signal
