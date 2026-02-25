import json
import logging
from typing import Dict, Any
from datetime import datetime, time
import pytz

logger = logging.getLogger(__name__)

class SignalParser:
    def __init__(self):
        self.required_fields = ['symbol', 'side', 'quantity']
        self.optional_fields = ['price', 'order_type', 'time_in_force']
    
    def _is_regular_trading_hours(self) -> bool:
        """
        检测当前时间是否在美股常规交易时间内 (9:30 AM - 4:00 PM ET)
        Returns True if in regular trading hours, False otherwise
        """
        try:
            # Get current time in Eastern Time (US stock market timezone)
            et_tz = pytz.timezone('America/New_York')
            now_et = datetime.now(et_tz)
            
            # Get current weekday (0=Monday, 6=Sunday)
            weekday = now_et.weekday()
            
            # Check if it's a trading day (Monday to Friday)
            if weekday > 4:  # Saturday (5) or Sunday (6)
                return False
                
            # Define regular trading hours (9:30 AM - 4:00 PM ET)
            market_open = time(9, 30)  # 9:30 AM
            market_close = time(16, 0)  # 4:00 PM
            current_time = now_et.time()
            
            # Check if current time is within regular trading hours
            is_regular_hours = market_open <= current_time <= market_close
            
            logger.info(f"Market time check: {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}, "
                       f"Regular hours: {is_regular_hours}")
            
            return is_regular_hours
            
        except Exception as e:
            logger.error(f"Error checking trading hours: {str(e)}")
            # Default to False (assume outside regular hours) for safety
            return False
    
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
        
        if 'symbol' in signal_data:
            normalized['symbol'] = str(signal_data['symbol']).upper()
        elif 'ticker' in signal_data:
            normalized['symbol'] = str(signal_data['ticker']).upper()
            logger.debug(f"Used 'ticker' field as symbol: {normalized['symbol']}")
        
        # Check for flat/close signal first
        # Multiple ways to identify a close signal:
        # 1. sentiment == 'flat'
        # 2. closePosition == true
        # 3. ratingstatus contains 'Exit'
        sentiment = signal_data.get('sentiment', '').lower()
        close_position_flag = signal_data.get('closePosition', False)
        rating_status = signal_data.get('data', {}).get('ratingstatus', '') if isinstance(signal_data.get('data'), dict) else ''
        
        is_close = (
            sentiment == 'flat' or 
            close_position_flag == True or 
            (isinstance(close_position_flag, str) and close_position_flag.lower() == 'true') or
            'exit' in rating_status.lower()
        )
        
        if is_close:
            normalized['is_close_signal'] = True
            normalized['close_type'] = 'exit' if 'exit' in rating_status.lower() else 'flat'
            normalized['side'] = signal_data.get('side', signal_data.get('action', 'sell')).lower()
            logger.info(f"🔴 Detected CLOSE signal for {normalized.get('symbol', 'unknown')} "
                       f"(sentiment={sentiment}, closePosition={close_position_flag}, rating={rating_status}, "
                       f"close_type={normalized['close_type']})")
        else:
            normalized['is_close_signal'] = False
            
        if not normalized.get('is_close_signal'):
            side = signal_data.get('side', signal_data.get('action', '')).lower()
            if side in ['buy', 'long']:
                normalized['side'] = 'buy'
            elif side in ['sell', 'short']:
                normalized['side'] = 'sell'
            else:
                logger.error(f"❌ Invalid side value: '{side}' for {normalized.get('symbol', 'unknown')}")
                raise ValueError(f"Invalid side: {side}")
        else:
            # For close signals, side is determined by current position
            normalized['side'] = signal_data.get('side', signal_data.get('action', 'sell')).lower()
        
        # Quantity
        quantity_str = signal_data.get('quantity', signal_data.get('qty', signal_data.get('size', '')))
        if quantity_str:
            quantity_str = str(quantity_str).lower().strip()
            if quantity_str == 'all':
                normalized['quantity'] = 'all'
                normalized['close_all'] = True
            elif normalized.get('is_close_signal'):
                try:
                    qty_num = float(quantity_str)
                    if qty_num > 0:
                        normalized['quantity'] = qty_num
                        normalized['close_all'] = False
                    else:
                        normalized['quantity'] = 'all'
                        normalized['close_all'] = True
                except ValueError:
                    normalized['quantity'] = 'all'
                    normalized['close_all'] = True
            else:
                try:
                    normalized['quantity'] = float(quantity_str)
                except ValueError:
                    normalized['quantity'] = 1.0
        else:
            normalized['quantity'] = 1.0
        
        # Price (for limit orders)
        if 'price' in signal_data:
            price_value = signal_data['price']
            if str(price_value).lower() != 'market':
                normalized['price'] = float(price_value)
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
        
        if 'stopLoss' in signal_data and signal_data['stopLoss']:
            stop_loss_data = signal_data['stopLoss']
            if 'stopPrice' in stop_loss_data:
                normalized['stop_loss'] = round(float(stop_loss_data['stopPrice']), 2)
        
        if 'takeProfit' in signal_data and signal_data['takeProfit']:
            take_profit_data = signal_data['takeProfit']
            if 'limitPrice' in take_profit_data:
                normalized['take_profit'] = round(float(take_profit_data['limitPrice']), 2)
        
        if 'stop_loss' in signal_data:
            normalized['stop_loss'] = round(float(signal_data['stop_loss']), 2)
        if 'take_profit' in signal_data:
            normalized['take_profit'] = round(float(signal_data['take_profit']), 2)
        
        if normalized.get('stop_loss') or normalized.get('take_profit'):
            logger.info(f"📊 SL/TP parsed: SL=${normalized.get('stop_loss')}, TP=${normalized.get('take_profit')}")
        
        # Reference price for market order conversion (参考价格)
        if 'extras' in signal_data and 'referencePrice' in signal_data['extras']:
            normalized['reference_price'] = float(signal_data['extras']['referencePrice'])
        elif 'reference_price' in signal_data:
            normalized['reference_price'] = float(signal_data['reference_price'])
        elif 'referencePrice' in signal_data:
            normalized['reference_price'] = float(signal_data['referencePrice'])
        
        # Trading session type (交易时段)
        session_type = signal_data.get('trading_session', signal_data.get('session', 'regular')).lower()
        if session_type in ['regular', 'rth']:
            normalized['trading_session'] = 'regular'
        elif session_type in ['extended', 'extended_hours']:
            normalized['trading_session'] = 'extended'  
        elif session_type in ['overnight', 'night']:
            normalized['trading_session'] = 'overnight'
        elif session_type in ['full', 'full_time', '24h']:
            normalized['trading_session'] = 'full'
        else:
            normalized['trading_session'] = 'regular'  # Default
        
        # Outside regular trading hours flag - Auto-determine based on current time
        if 'outside_rth' in signal_data:
            # Use explicit setting from signal
            normalized['outside_rth'] = bool(signal_data['outside_rth'])
        else:
            # Smart auto-determination based on current market time
            is_regular_hours = self._is_regular_trading_hours()
            
            if normalized['trading_session'] == 'regular':
                # If signal specifies regular session but we're outside regular hours,
                # automatically enable extended hours trading
                if not is_regular_hours:
                    normalized['outside_rth'] = True
                    normalized['trading_session'] = 'extended'  # Upgrade to extended session
                    logger.info("Auto-detected outside regular hours: enabling extended hours trading")
                    
                    # CRITICAL: Convert market order to limit order for extended hours
                    if normalized['order_type'] == 'market':
                        if 'reference_price' in normalized:
                            normalized['order_type'] = 'limit'
                            normalized['price'] = normalized['reference_price']
                            logger.info(f"Converted market order to limit order at ${normalized['reference_price']:.2f} for extended hours trading")
                        else:
                            logger.warning("Market order in extended hours requires referencePrice - order may be rejected")
                else:
                    normalized['outside_rth'] = False
                    logger.info("Auto-detected regular trading hours: standard session")
            else:
                # For extended, overnight, full sessions, always allow outside RTH
                normalized['outside_rth'] = normalized['trading_session'] != 'regular'
                
                # Also check for market order conversion in explicitly set extended sessions
                if normalized['trading_session'] in ['extended', 'overnight', 'full'] and normalized['order_type'] == 'market':
                    if 'reference_price' in normalized:
                        normalized['order_type'] = 'limit'
                        normalized['price'] = normalized['reference_price']
                        logger.info(f"Converted market order to limit order at ${normalized['reference_price']:.2f} for {normalized['trading_session']} session")
        
        return normalized
    
    def _validate_signal(self, signal: Dict[str, Any]) -> None:
        """Validate parsed signal"""
        for field in self.required_fields:
            if field not in signal:
                logger.error(f"❌ Validation failed: missing required field '{field}' in signal")
                raise ValueError(f"Missing required field: {field}")
        
        if not signal['symbol'] or len(signal['symbol']) > 20:
            logger.error(f"❌ Validation failed: invalid symbol '{signal.get('symbol')}'")
            raise ValueError("Invalid symbol")
        
        if signal['side'] not in ['buy', 'sell']:
            logger.error(f"❌ Validation failed: invalid side '{signal['side']}'")
            raise ValueError(f"Invalid side: {signal['side']}")
        
        if signal.get('close_all') or signal['quantity'] == 'all':
            logger.debug(f"Quantity='all' for close signal, will resolve from position")
        elif isinstance(signal['quantity'], (int, float)) and signal['quantity'] <= 0:
            logger.error(f"❌ Validation failed: quantity must be positive, got {signal['quantity']}")
            raise ValueError("Quantity must be positive")
        
        if signal.get('order_type') == 'limit':
            if 'price' not in signal or signal['price'] <= 0:
                logger.error(f"❌ Validation failed: limit order requires positive price, got {signal.get('price')}")
                raise ValueError("Limit orders require a positive price")
        
        logger.debug(f"✅ Signal validation passed: {signal['symbol']} {signal['side']} qty={signal['quantity']} type={signal.get('order_type')}")
    
    def _apply_defaults(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """Apply default values to signal"""
        # Default order type
        if 'order_type' not in signal:
            signal['order_type'] = 'market'
        
        # Default time in force
        if 'time_in_force' not in signal:
            signal['time_in_force'] = 'day'
        
        # Trading session type (交易时段)
        if 'trading_session' not in signal:
            signal['trading_session'] = 'regular'  # regular, extended, overnight, full
        
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
