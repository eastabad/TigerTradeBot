import os
import logging
from datetime import datetime, time, timedelta, timezone
from typing import Dict, List, Optional
import pytz
from tigeropen.tiger_open_config import TigerOpenClientConfig
from tigeropen.trade.trade_client import TradeClient
from tigeropen.quote.quote_client import QuoteClient
from tigeropen.common.util.contract_utils import stock_contract
from tigeropen.common.util.order_utils import market_order, limit_order, limit_order_with_legs, order_leg, stop_order, oca_order, stop_limit_order
from tigeropen.common.consts import Language, Market, Currency, TradingSessionType, SecurityType, BarPeriod, QuoteRight, TradingSession
from tigeropen.common.util.signature_utils import read_private_key
from models import OrderType, Side
from config import get_config

logger = logging.getLogger(__name__)


class TigerQuoteClient:
    """Tiger Securities Quote Client for market data (实时行情客户端)"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self.quote_client = None
        self.client_config = None
        self._initialize_quote_client()
        self._initialized = True
    
    def _initialize_quote_client(self):
        """Initialize Tiger Quote Client for market data"""
        try:
            self.client_config = TigerOpenClientConfig(sandbox_debug=False)
            
            config_path = './tiger_openapi_config.properties'
            if os.path.exists(config_path):
                config_data = {}
                with open(config_path, 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            config_data[key] = value
                
                self.client_config.tiger_id = config_data.get('tiger_id')
                self.client_config.account = config_data.get('account')
                
                private_key_pk8 = config_data.get('private_key_pk8')
                if private_key_pk8:
                    self.client_config.private_key = private_key_pk8
                
                self.client_config.language = Language.zh_CN
                
                device_id = config_data.get('device_id')
                if device_id:
                    self.client_config._device_id = device_id
                    logger.info(f"Quote client using custom device_id: {device_id}")
                
                logger.info(f"Quote Config loaded - Tiger ID: {self.client_config.tiger_id}")
            else:
                logger.error("Config file not found for quote client")
                return
            
            if not all([self.client_config.tiger_id, self.client_config.private_key]):
                logger.error("Missing required config for quote client")
                return
            
            license_type = config_data.get('license', '')
            if license_type == 'TBUS':
                from tigeropen.common.consts import License
                self.client_config.license = License.TBUS
                us_server = "https://openapi.tradeup.com/gateway"
                self.client_config.server_url = us_server
                self.client_config.quote_server_url = us_server
                self.client_config.socket_host_port = ('ssl', 'openapi.tradeup.com', 9983)
                logger.info(f"Quote client: TBUS license detected, server={us_server}, ws_port=9983")
            
            self.quote_client = QuoteClient(self.client_config)
            
            if license_type == 'TBUS':
                try:
                    self.quote_client._TigerOpenClient__config.server_url = us_server
                    self.quote_client._TigerOpenClient__config.quote_server_url = us_server
                except Exception:
                    pass
            
            logger.info("Tiger Quote Client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Tiger Quote Client: {str(e)}")
            self.quote_client = None
    
    def get_latest_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        批量获取多个股票的最新价格
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            {symbol: price} 字典
        """
        if not self.quote_client:
            logger.error("Quote client not initialized")
            return {}
        
        if not symbols:
            return {}
        
        try:
            clean_symbols = [s.replace('[PAPER]', '').strip() for s in symbols]
            briefs = self.quote_client.get_stock_briefs(clean_symbols)
            
            prices = {}
            if briefs is not None and not briefs.empty:
                for _, row in briefs.iterrows():
                    symbol = row.get('symbol', '')
                    price = row.get('latest_price', 0)
                    if symbol and price:
                        prices[symbol] = float(price)
            
            return prices
            
        except Exception as e:
            logger.error(f"Failed to get latest prices: {str(e)}")
            return {}
    
    def get_latest_trade(self, symbol: str) -> Optional[Dict]:
        """
        获取股票最新交易价格
        
        Returns:
            {
                'symbol': str,
                'price': float,
                'timestamp': datetime
            }
        """
        if not self.quote_client:
            logger.error("Quote client not initialized")
            return None
        
        try:
            clean_symbol = symbol.replace('[PAPER]', '').strip()
            
            briefs = self.quote_client.get_stock_briefs([clean_symbol])
            
            if briefs is not None and not briefs.empty:
                row = briefs.iloc[0]
                latest_price = row.get('latest_price', 0)
                market_time = row.get('latest_time', None)
                
                if market_time:
                    if isinstance(market_time, (int, float)):
                        timestamp = datetime.fromtimestamp(market_time / 1000, tz=timezone.utc)
                    else:
                        timestamp = market_time
                else:
                    timestamp = datetime.now(timezone.utc)
                
                return {
                    'symbol': clean_symbol,
                    'price': float(latest_price),
                    'timestamp': timestamp
                }
            
            logger.warning(f"No quote data returned for {clean_symbol}")
            return None
            
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Error getting latest trade for {symbol}: {str(e)}")
            if 'permission denied' in error_msg or 'code=4' in error_msg or 'code":4' in error_msg:
                raise
            return None
    
    def get_market_session(self) -> str:
        """
        获取当前美股市场时段
        
        Returns:
            'pre_market' - 盘前 (04:00 - 09:30 ET)
            'regular' - 常规交易时段 (09:30 - 16:00 ET)
            'post_market' - 盘后 (16:00 - 20:00 ET)
            'closed' - 休市
        """
        eastern = pytz.timezone('America/New_York')
        now_et = datetime.now(eastern)
        
        hour = now_et.hour
        minute = now_et.minute
        weekday = now_et.weekday()
        
        if weekday >= 5:
            return 'closed'
        
        current_minutes = hour * 60 + minute
        
        if 240 <= current_minutes < 570:
            return 'pre_market'
        elif 570 <= current_minutes < 960:
            return 'regular'
        elif 960 <= current_minutes < 1200:
            return 'post_market'
        else:
            return 'closed'
    
    def get_extended_hours_price(self, symbol: str) -> Optional[Dict]:
        """
        使用get_timeline获取盘前盘后的实时价格
        
        Returns:
            {
                'symbol': str,
                'price': float,
                'timestamp': datetime,
                'session': str ('pre_market' or 'after_hours'),
                'avg_price': float
            }
        """
        if not self.quote_client:
            logger.error("Quote client not initialized")
            return None
        
        try:
            clean_symbol = symbol.replace('[PAPER]', '').strip()
            
            timeline = self.quote_client.get_timeline([clean_symbol], include_hour_trading=True)
            
            if timeline is not None and not timeline.empty:
                last_row = timeline.iloc[-1]
                
                price = float(last_row.get('price', 0))
                timeline_time = last_row.get('time', None)
                session = last_row.get('trading_session', '')
                avg_price = float(last_row.get('avg_price', price))
                
                if timeline_time:
                    if isinstance(timeline_time, (int, float)):
                        timestamp = datetime.fromtimestamp(timeline_time / 1000, tz=timezone.utc)
                    else:
                        timestamp = timeline_time
                else:
                    timestamp = datetime.now(timezone.utc)
                
                return {
                    'symbol': clean_symbol,
                    'price': price,
                    'timestamp': timestamp,
                    'session': session,
                    'avg_price': avg_price
                }
            
            logger.warning(f"No timeline data returned for {clean_symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting extended hours price for {symbol}: {str(e)}")
            return None
    
    def get_overnight_price(self, symbol: str) -> Optional[Dict]:
        """
        获取夜盘价格 - 使用get_trade_ticks指定OverNight时段
        
        夜盘时间: 周日18:00 ET - 周一04:00 ET (以及工作日20:00-04:00)
        
        Returns:
            {
                'symbol': str,
                'price': float,
                'volume': int,
                'timestamp': datetime,
                'session': 'overnight'
            }
        """
        if not self.quote_client:
            logger.error("Quote client not initialized")
            return None
        
        try:
            clean_symbol = symbol.replace('[PAPER]', '').strip()
            
            ticks = self.quote_client.get_trade_ticks(
                symbols=[clean_symbol],
                trade_session=TradingSession.OverNight,
                limit=10
            )
            
            if ticks is not None and not ticks.empty:
                last_tick = ticks.iloc[-1]
                
                price = float(last_tick.get('price', 0))
                volume = int(last_tick.get('volume', 0))
                tick_time = last_tick.get('time', None)
                
                if tick_time:
                    if isinstance(tick_time, (int, float)):
                        timestamp = datetime.fromtimestamp(tick_time / 1000, tz=timezone.utc)
                    else:
                        timestamp = tick_time
                else:
                    timestamp = datetime.now(timezone.utc)
                
                logger.info(f"Overnight tick for {clean_symbol}: price={price}, volume={volume}, time={timestamp}")
                
                return {
                    'symbol': clean_symbol,
                    'price': price,
                    'volume': volume,
                    'timestamp': timestamp,
                    'session': 'overnight'
                }
            
            logger.debug(f"No overnight tick data for {clean_symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting overnight price for {symbol}: {str(e)}")
            return None
    
    def get_session_ticks(self, symbol: str, session: str = 'overnight', limit: int = 10) -> Optional[List[Dict]]:
        """
        获取指定时段的tick数据
        
        Args:
            symbol: 股票代码
            session: 交易时段 ('overnight', 'premarket', 'afterhours', 'regular')
            limit: 返回的tick数量
        
        Returns:
            List of tick data dicts
        """
        if not self.quote_client:
            logger.error("Quote client not initialized")
            return None
        
        session_map = {
            'overnight': TradingSession.OverNight,
            'premarket': TradingSession.PreMarket,
            'afterhours': TradingSession.AfterHours,
            'regular': TradingSession.Regular,
            'all': TradingSession.All
        }
        
        trading_session = session_map.get(session.lower(), TradingSession.All)
        
        try:
            clean_symbol = symbol.replace('[PAPER]', '').strip()
            
            ticks = self.quote_client.get_trade_ticks(
                symbols=[clean_symbol],
                trade_session=trading_session,
                limit=limit
            )
            
            if ticks is not None and not ticks.empty:
                result = []
                for _, row in ticks.iterrows():
                    tick_time = row.get('time', None)
                    if tick_time:
                        if isinstance(tick_time, (int, float)):
                            timestamp = datetime.fromtimestamp(tick_time / 1000, tz=timezone.utc)
                        else:
                            timestamp = tick_time
                    else:
                        timestamp = datetime.now(timezone.utc)
                    
                    result.append({
                        'symbol': clean_symbol,
                        'price': float(row.get('price', 0)),
                        'volume': int(row.get('volume', 0)),
                        'timestamp': timestamp,
                        'session': session
                    })
                
                return result
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting {session} ticks for {symbol}: {str(e)}")
            return None
    
    def get_smart_price(self, symbol: str) -> Optional[Dict]:
        """
        智能获取价格 - 根据当前市场时段选择合适的API
        
        盘前盘后时段: 使用get_timeline获取真实的盘前盘后价格
        常规交易时段: 使用get_stock_briefs获取常规价格
        
        Returns:
            {
                'symbol': str,
                'price': float,
                'timestamp': datetime,
                'session': str ('pre_market', 'regular', 'post_market', 'closed'),
                'source': str ('timeline' or 'briefs')
            }
        """
        session = self.get_market_session()
        
        if session in ['pre_market', 'post_market']:
            extended_result = self.get_extended_hours_price(symbol)
            if extended_result and extended_result['price'] > 0:
                return {
                    'symbol': extended_result['symbol'],
                    'price': extended_result['price'],
                    'timestamp': extended_result['timestamp'],
                    'session': session,
                    'source': 'timeline'
                }
            logger.warning(f"Extended hours price not available for {symbol}, falling back to briefs")
        
        regular_result = self.get_latest_trade(symbol)
        if regular_result:
            return {
                'symbol': regular_result['symbol'],
                'price': regular_result['price'],
                'timestamp': regular_result['timestamp'],
                'session': session,
                'source': 'briefs'
            }
        
        return None
    
    def get_batch_smart_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        批量智能获取多个股票价格 - 一次API调用获取所有symbols
        
        Args:
            symbols: 股票代码列表
            
        Returns:
            {symbol: {'price': float, 'session': str, 'source': str}} 字典
        """
        if not symbols:
            return {}
        
        session = self.get_market_session()
        clean_symbols = [s.replace('[PAPER]', '').strip() for s in symbols if s and s.replace('[PAPER]', '').strip()]
        results = {}
        
        if not clean_symbols:
            return {}
        
        if session in ['pre_market', 'post_market']:
            try:
                timeline = self.quote_client.get_timeline(clean_symbols, include_hour_trading=True)
                if timeline is not None and not timeline.empty:
                    for symbol in clean_symbols:
                        symbol_data = timeline[timeline['symbol'] == symbol] if 'symbol' in timeline.columns else timeline
                        if not symbol_data.empty:
                            last_row = symbol_data.iloc[-1]
                            price = float(last_row.get('price', 0))
                            if price > 0:
                                results[symbol] = {
                                    'price': price,
                                    'session': session,
                                    'source': 'timeline_batch'
                                }
            except Exception as e:
                logger.warning(f"Batch timeline fetch failed: {e}")
            
            missing = [s for s in clean_symbols if s not in results]
            if missing:
                try:
                    briefs = self.quote_client.get_stock_briefs(missing)
                    if briefs is not None and not briefs.empty:
                        for _, row in briefs.iterrows():
                            sym = row.get('symbol', '')
                            price = float(row.get('latest_price', 0))
                            if sym and price > 0:
                                results[sym] = {
                                    'price': price,
                                    'session': session,
                                    'source': 'briefs_batch'
                                }
                except Exception as e:
                    logger.warning(f"Batch briefs fallback failed: {e}")
        else:
            try:
                briefs = self.quote_client.get_stock_briefs(clean_symbols)
                if briefs is not None and not briefs.empty:
                    for _, row in briefs.iterrows():
                        sym = row.get('symbol', '')
                        price = float(row.get('latest_price', 0))
                        if sym and price > 0:
                            results[sym] = {
                                'price': price,
                                'session': session,
                                'source': 'briefs_batch'
                            }
            except Exception as e:
                error_msg = str(e).lower()
                logger.error(f"Batch smart price fetch failed: {str(e)}")
                if 'permission denied' in error_msg or 'code=4' in error_msg or 'code":4' in error_msg:
                    raise
        
        return results
    
    def get_bars(self, symbol: str, timeframe: str = 'day', limit: int = 20) -> List[Dict]:
        """
        获取历史K线数据
        
        Args:
            symbol: 股票代码
            timeframe: K线周期 ('1min', '5min', '15min', '30min', '1hour', 'day')
            limit: 返回的K线数量
            
        Returns:
            List of bars: [{'timestamp': datetime, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': int}]
        """
        if not self.quote_client:
            logger.error("Quote client not initialized")
            return []
        
        try:
            clean_symbol = symbol.replace('[PAPER]', '').strip()
            
            period_map = {
                '1min': BarPeriod.ONE_MINUTE,
                '3min': BarPeriod.THREE_MINUTES,
                '5min': BarPeriod.FIVE_MINUTES,
                '10min': BarPeriod.TEN_MINUTES,
                '15min': BarPeriod.FIFTEEN_MINUTES,
                '30min': BarPeriod.HALF_HOUR,
                '45min': BarPeriod.FORTY_FIVE_MINUTES,
                '1hour': BarPeriod.ONE_HOUR,
                'day': BarPeriod.DAY,
                'week': BarPeriod.WEEK,
            }
            
            period = period_map.get(timeframe, BarPeriod.DAY)
            
            bars_df = self.quote_client.get_bars(
                symbols=[clean_symbol],
                period=period,
                begin_time=-1,
                end_time=-1,
                right=QuoteRight.BR,
                limit=limit
            )
            
            if bars_df is None or bars_df.empty:
                logger.warning(f"No bar data returned for {clean_symbol}")
                return []
            
            bars_list = []
            for _, row in bars_df.iterrows():
                bar_time = row.get('time', 0)
                if isinstance(bar_time, (int, float)):
                    timestamp = datetime.fromtimestamp(bar_time / 1000, tz=timezone.utc)
                else:
                    timestamp = bar_time
                
                bars_list.append({
                    'timestamp': timestamp,
                    'open': float(row.get('open', 0)),
                    'high': float(row.get('high', 0)),
                    'low': float(row.get('low', 0)),
                    'close': float(row.get('close', 0)),
                    'volume': int(row.get('volume', 0))
                })
            
            bars_list.sort(key=lambda x: x['timestamp'])
            
            logger.info(f"Retrieved {len(bars_list)} bars for {clean_symbol} ({timeframe})")
            return bars_list
            
        except Exception as e:
            logger.error(f"Error getting bars for {symbol}: {str(e)}")
            return []


def get_tiger_quote_client() -> TigerQuoteClient:
    """Get singleton instance of Tiger Quote Client"""
    return TigerQuoteClient()


class TigerClient:
    def __init__(self):
        self.client = None
        self.client_config = None
        self.is_paper = False  # Real account
        self._account_type = 'real'
        self._initialize_client()
    
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
    
    def _initialize_client(self):
        """Initialize Tiger OpenAPI client using config file"""
        try:
            # Create client config with sandbox_debug=False to use production server
            self.client_config = TigerOpenClientConfig(sandbox_debug=False)
            
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
                
                device_id = config_data.get('device_id')
                if device_id:
                    self.client_config._device_id = device_id
                    logger.info(f"Trade client using custom device_id: {device_id}")
                
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
            
            license_type = config_data.get('license', '')
            if license_type == 'TBUS':
                from tigeropen.common.consts import License
                self.client_config.license = License.TBUS
                us_server = "https://openapi.tradeup.com/gateway"
                self.client_config.server_url = us_server
                self.client_config.quote_server_url = us_server
                self.client_config.socket_host_port = ('ssl', 'openapi.tradeup.com', 9983)
                logger.info(f"TBUS license detected, server={us_server}, ws_port=9983")
            
            self.client = TradeClient(self.client_config)
            
            if license_type == 'TBUS':
                try:
                    self.client._TigerOpenClient__config.server_url = us_server
                    self.client._TigerOpenClient__config.quote_server_url = us_server
                    logger.info(f"Trade client server URL confirmed: {self.client._TigerOpenClient__config.server_url}")
                except Exception as e:
                    logger.warning(f"Could not set internal server URL: {e}")
            
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
            
            # Handle position increase scenario - check if we need to cancel existing orders
            position_increase_result = self._handle_position_increase(trade)
            if not position_increase_result['success']:
                return position_increase_result
            
            # Check if this is a position increase or close
            is_position_increase = position_increase_result.get('is_position_increase', False)
            is_position_close = position_increase_result.get('is_position_close', False)
            protection_info = position_increase_result.get('protection_info', {})
            
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
            
            # Smart auto-determination based on current market time
            is_regular_hours = self._is_regular_trading_hours()
            
            if trade.trading_session == 'regular':
                # If signal specifies regular session but we're outside regular hours,
                # automatically enable extended hours trading
                if not is_regular_hours:
                    outside_rth = True
                    trade.trading_session = 'extended'  # Upgrade to extended session
                    logger.info("Place order: Auto-detected outside regular hours, enabling extended hours trading")
                else:
                    outside_rth = False
                    logger.info("Place order: Auto-detected regular trading hours, standard session")
            else:
                # For extended, overnight, full sessions, always allow outside RTH
                outside_rth = trade.trading_session != 'regular'
            
            logger.info(f"Trading session: {trade.trading_session}, outside_rth: {outside_rth}")
            
            # Check if we have stop loss or take profit
            # For position increases or closes, skip attaching stop loss/take profit to main order
            # Tiger API doesn't allow child orders on close position orders
            if is_position_increase:
                has_stop_loss = False
                has_take_profit = False
                logger.info(f"Position increase detected: will place main order without attachments, then set protection for entire position")
            elif is_position_close:
                has_stop_loss = False
                has_take_profit = False
                logger.info(f"Position close/reduce detected: Tiger API doesn't allow child orders on close position orders")
            else:
                has_stop_loss = trade.stop_loss_price is not None
                has_take_profit = trade.take_profit_price is not None
            
            order = None
            
            # Round stop loss and take profit prices to 2 decimal places
            if has_stop_loss and trade.stop_loss_price:
                trade.stop_loss_price = round(trade.stop_loss_price, 2)
            if has_take_profit and trade.take_profit_price:
                trade.take_profit_price = round(trade.take_profit_price, 2)
            
            if has_stop_loss or has_take_profit:
                account_label = 'Paper' if self.is_paper else 'Real'
                
                if trade.order_type == OrderType.MARKET:
                    if self.is_paper:
                        reference_price = getattr(trade, 'reference_price', None)
                        if reference_price and reference_price > 0:
                            if action == 'BUY':
                                trade.price = round(reference_price * 1.005, 2)
                            else:
                                trade.price = round(reference_price * 0.995, 2)
                            logger.info(f"[Paper] Converting MARKET to aggressive LIMIT at ${trade.price:.2f} for bracket (ref: ${reference_price:.2f})")
                        else:
                            prices = self.get_latest_prices([trade.symbol])
                            latest = prices.get(trade.symbol, 0)
                            if latest and latest > 0:
                                if action == 'BUY':
                                    trade.price = round(latest * 1.005, 2)
                                else:
                                    trade.price = round(latest * 0.995, 2)
                                logger.info(f"[Paper] Converting MARKET to aggressive LIMIT at ${trade.price:.2f} for bracket (latest: ${latest:.2f})")
                            else:
                                logger.warning(f"[Paper] No reference/latest price for market→limit conversion, will use market order + soft stop only")
                    elif outside_rth:
                        reference_price = getattr(trade, 'reference_price', None)
                        if reference_price and reference_price > 0:
                            if action == 'BUY':
                                trade.price = round(reference_price * 1.02, 2)
                            else:
                                trade.price = round(reference_price * 0.98, 2)
                            logger.info(f"[Real] Extended hours: Converting MARKET to LIMIT at ${trade.price:.2f} (ref: ${reference_price:.2f})")
                        else:
                            logger.error("No reference price for market order conversion in extended hours")
                            return {
                                'success': False,
                                'error': 'Extended hours requires referencePrice for market orders.'
                            }
                
                use_bracket = self.is_paper and trade.price is not None
                
                if use_bracket:
                    legs = []
                    is_buy = action == 'BUY'
                    if has_stop_loss and trade.stop_loss_price:
                        sl_valid = True
                        if is_buy and trade.stop_loss_price >= trade.price:
                            logger.warning(f"⚠️ [{trade.symbol}] Bracket SL ${trade.stop_loss_price:.2f} >= entry ${trade.price:.2f} for BUY, skipping SL leg")
                            sl_valid = False
                        elif not is_buy and trade.stop_loss_price <= trade.price:
                            logger.warning(f"⚠️ [{trade.symbol}] Bracket SL ${trade.stop_loss_price:.2f} <= entry ${trade.price:.2f} for SELL, skipping SL leg")
                            sl_valid = False
                        if sl_valid:
                            loss_leg = order_leg('LOSS', price=round(trade.stop_loss_price, 2), time_in_force='DAY', outside_rth=False)
                            legs.append(loss_leg)
                    if has_take_profit and trade.take_profit_price:
                        tp_valid = True
                        if is_buy and trade.take_profit_price <= trade.price:
                            logger.warning(f"⚠️ [{trade.symbol}] Bracket TP ${trade.take_profit_price:.2f} <= entry ${trade.price:.2f} for BUY, skipping TP leg")
                            tp_valid = False
                        elif not is_buy and trade.take_profit_price >= trade.price:
                            logger.warning(f"⚠️ [{trade.symbol}] Bracket TP ${trade.take_profit_price:.2f} >= entry ${trade.price:.2f} for SELL, skipping TP leg")
                            tp_valid = False
                        if tp_valid:
                            profit_leg = order_leg('PROFIT', price=round(trade.take_profit_price, 2), time_in_force='DAY', outside_rth=True)
                            legs.append(profit_leg)
                    
                    if legs:
                        order = limit_order_with_legs(
                            account=self.client_config.account,
                            contract=contract,
                            action=action,
                            quantity=int(trade.quantity),
                            limit_price=round(trade.price, 2),
                            order_legs=legs,
                            time_in_force='DAY',
                        )
                        if outside_rth:
                            order.outside_rth = True
                            if trading_session_type:
                                order.trading_session_type = trading_session_type
                        session_label = "Extended hours" if outside_rth else "RTH"
                        logger.info(f"[Paper] {session_label} BRACKET entry at ${round(trade.price, 2):.2f} "
                                   f"SL=${trade.stop_loss_price if has_stop_loss else 'N/A'} "
                                   f"TP=${trade.take_profit_price if has_take_profit else 'N/A'}")
                    else:
                        order = limit_order(
                            account=self.client_config.account,
                            contract=contract,
                            action=action,
                            limit_price=round(trade.price, 2),
                            quantity=int(trade.quantity)
                        )
                        if outside_rth:
                            order.outside_rth = True
                            if trading_session_type:
                                order.trading_session_type = trading_session_type
                        logger.info(f"[Paper] LIMIT entry at ${round(trade.price, 2):.2f}, no SL/TP legs")
                else:
                    if trade.order_type == OrderType.MARKET and not outside_rth:
                        order = market_order(
                            account=self.client_config.account,
                            contract=contract,
                            action=action,
                            quantity=int(trade.quantity)
                        )
                        if self.is_paper:
                            logger.info(f"[Paper] RTH MARKET entry (no bracket price avail), soft stop will protect")
                        else:
                            logger.info(f"[Real] RTH MARKET entry, OCA after fill")
                    else:
                        order = limit_order(
                            account=self.client_config.account,
                            contract=contract,
                            action=action,
                            limit_price=round(trade.price, 2),
                            quantity=int(trade.quantity)
                        )
                        if self.is_paper:
                            logger.info(f"[Paper] LIMIT entry at ${round(trade.price, 2):.2f}, soft stop will protect (no bracket legs)")
                        else:
                            logger.info(f"[Real] LIMIT entry at ${round(trade.price, 2):.2f}, OCA after fill")
                
                if not hasattr(order, 'order_legs') or not order.order_legs:
                    if trading_session_type:
                        order.trading_session_type = trading_session_type
                    order.outside_rth = outside_rth
                
            else:
                # Standard order without attachments
                if trade.order_type == OrderType.MARKET:
                    # CRITICAL: In extended hours, market orders are NOT supported
                    # Must convert to limit order for outside_rth to work
                    if outside_rth:
                        reference_price = getattr(trade, 'reference_price', None)
                        if reference_price and reference_price > 0:
                            # Convert market order to limit order for extended hours
                            if action == 'BUY':
                                limit_price = round(reference_price * 1.02, 2)  # Buy 2% above reference
                            else:
                                limit_price = round(reference_price * 0.98, 2)  # Sell 2% below reference
                            logger.info(f"Extended hours: Converting MARKET to LIMIT order at ${limit_price:.2f} (reference: ${reference_price:.2f})")
                            order = limit_order(
                                account=self.client_config.account,
                                contract=contract,
                                action=action,
                                limit_price=limit_price,
                                quantity=int(trade.quantity)
                            )
                        else:
                            # No reference price, return error
                            logger.error(f"Extended hours requires reference_price for market order conversion")
                            return {
                                'success': False,
                                'error': 'Extended hours trading requires referencePrice. Market orders are not supported outside regular trading hours.'
                            }
                    else:
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
                        limit_price=round(trade.price, 2),
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
                
                if is_position_increase and (protection_info.get('stop_loss_price') or protection_info.get('take_profit_price')):
                    if self.is_paper:
                        logger.info(f"[Paper] Position increase order placed. Bracket legs handle protection, soft stop fallback.")
                    else:
                        logger.info(f"Position increase order placed successfully. Protection info saved for later application.")
                        response['needs_auto_protection'] = True
                        response['protection_info'] = protection_info
                        response['symbol'] = trade.symbol
                
                is_bracket = hasattr(order, 'order_legs') and order.order_legs
                if is_bracket:
                    account_label = 'Paper' if self.is_paper else 'Real'
                    sub_ids = getattr(order, 'sub_ids', None) or []
                    orders_data = getattr(order, 'orders', None) or []
                    sl_sub_id = None
                    tp_sub_id = None
                    if orders_data:
                        for o in orders_data:
                            if o.get('parentId') and o.get('orderType') == 'STP':
                                sl_sub_id = str(o['id'])
                            elif o.get('parentId') and o.get('orderType') == 'LMT':
                                tp_sub_id = str(o['id'])
                    if not sl_sub_id and not tp_sub_id and sub_ids:
                        if len(sub_ids) >= 2:
                            sl_sub_id = str(sub_ids[0])
                            tp_sub_id = str(sub_ids[1])
                        elif len(sub_ids) == 1:
                            if has_stop_loss:
                                sl_sub_id = str(sub_ids[0])
                            else:
                                tp_sub_id = str(sub_ids[0])
                    
                    response['bracket_order'] = True
                    response['stop_loss_order_id'] = sl_sub_id
                    response['take_profit_order_id'] = tp_sub_id
                    response['protection_info'] = {
                        'stop_loss_price': trade.stop_loss_price if has_stop_loss else None,
                        'take_profit_price': trade.take_profit_price if has_take_profit else None,
                        'symbol': trade.symbol,
                        'quantity': int(trade.quantity),
                        'side': action,
                        'stop_loss_order_id': sl_sub_id,
                        'take_profit_order_id': tp_sub_id,
                    }
                    response['symbol'] = trade.symbol
                    logger.info(f"[{account_label}] BRACKET order placed: parent={order_id}, SL={sl_sub_id}, TP={tp_sub_id}")
                elif has_stop_loss or has_take_profit:
                    if self.is_paper:
                        logger.info(f"[Paper] Non-bracket order with SL/TP, soft stop will protect (no OCA)")
                    else:
                        logger.info(f"[Real] Setting needs_auto_protection for OCA creation after fill")
                        response['needs_auto_protection'] = True
                        response['protection_info'] = {
                            'stop_loss_price': trade.stop_loss_price if has_stop_loss else None,
                            'take_profit_price': trade.take_profit_price if has_take_profit else None,
                            'symbol': trade.symbol,
                            'quantity': int(trade.quantity),
                            'side': action
                        }
                    response['symbol'] = trade.symbol
                
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
                    'PartiallyFilled': 'partially_filled',
                    'Invalid': 'invalid',
                    'Expired': 'expired',
                    'Inactive': 'expired'
                }
                
                # Get status from order object - use correct attribute names
                tiger_status = getattr(order, 'status', 'pending')
                
                # Handle both string and enum status
                if hasattr(tiger_status, 'value'):
                    tiger_status_str = tiger_status.value  # Get value from enum
                else:
                    tiger_status_str = str(tiger_status)   # Convert to string
                
                our_status = status_map.get(tiger_status_str, 'pending')
                
                # Extract actual values from Tiger API response
                # Try different attribute names to find the correct ones
                avg_fill_price = (getattr(order, 'avg_fill_price', 0) or 
                                 getattr(order, 'avgFillPrice', 0) or
                                 getattr(order, 'average_fill_price', 0))
                
                filled_quantity = (getattr(order, 'filled', 0) or 
                                  getattr(order, 'filled_quantity', 0) or
                                  getattr(order, 'filledQuantity', 0))
                
                total_quantity = (getattr(order, 'quantity', 0) or
                                 getattr(order, 'total_quantity', 0) or 
                                 getattr(order, 'totalQuantity', 0))
                
                # Get realized_pnl and commission from Tiger API
                realized_pnl = (getattr(order, 'realized_pnl', 0) or
                               getattr(order, 'realizedPnl', 0) or 0)
                commission = (getattr(order, 'commission', 0) or 0)
                
                # Get reject/cancel reason from Tiger API
                reason = (getattr(order, 'reason', None) or 
                         getattr(order, 'cancel_reason', None) or
                         getattr(order, 'reject_reason', None) or
                         getattr(order, 'message', None) or '')
                
                # Get order type for identifying stop loss vs take profit
                order_type = getattr(order, 'order_type', '')
                if hasattr(order_type, 'value'):
                    order_type = order_type.value
                else:
                    order_type = str(order_type)
                
                # Get outside_rth flag for extended hours support detection
                outside_rth = getattr(order, 'outside_rth', False) or False
                
                # Log all available attributes for debugging
                order_attrs = [attr for attr in dir(order) if not attr.startswith('_')]
                logger.info(f"Order {order_id} available attributes: {order_attrs}")
                logger.info(f"Order {order_id} Tiger data: avgFillPrice={avg_fill_price}, filledQuantity={filled_quantity}, totalQuantity={total_quantity}, realizedPnl={realized_pnl}, commission={commission}, reason={reason}, order_type={order_type}, outside_rth={outside_rth}")
                
                return {
                    'success': True,
                    'status': our_status,
                    'tiger_status': tiger_status,  # Include original status for debugging
                    'filled_price': avg_fill_price,
                    'filled_quantity': filled_quantity,
                    'total_quantity': total_quantity,
                    'realized_pnl': realized_pnl,
                    'commission': commission,
                    'reason': reason,  # Reject/cancel reason from Tiger
                    'order_type': order_type,  # Order type: LMT, STP, STP_LMT, etc.
                    'outside_rth': outside_rth  # Extended hours flag
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
    
    def get_positions(self, symbol=None, max_retries=3):
        """Get current positions with rate limit retry"""
        if not self.client or not self.client_config:
            return {
                'success': False,
                'error': 'Tiger client not initialized'
            }
        
        import time
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Getting positions for symbol: {symbol or 'all'}" + (f" (retry {attempt})" if attempt > 0 else ""))
                
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
                        'multiplier': getattr(pos.contract, 'multiplier', 1),
                        'salable_qty': getattr(pos, 'salable_qty', pos.quantity)
                    }
                    position_list.append(position_data)
                    logger.info(f"Position: {pos.contract.symbol}, Qty: {pos.quantity}, Cost: {pos.average_cost}")
                
                return {
                    'success': True,
                    'positions': position_list,
                    'count': len(position_list)
                }
                
            except Exception as e:
                error_msg = str(e)
                is_rate_limit = 'rate limit' in error_msg.lower() or 'code=4' in error_msg
                
                if is_rate_limit and attempt < max_retries - 1:
                    wait_time = 1 * (attempt + 1)
                    logger.warning(f"Rate limited on get_positions (attempt {attempt+1}/{max_retries}), waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                
                logger.error(f"Error getting positions: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'rate_limited': is_rate_limit,
                    'positions': []
                }
    
    def get_filled_orders(self, start_date=None, end_date=None, symbol=None, limit=100):
        """Get filled orders with realized PnL (已平仓订单)
        
        Args:
            start_date: Start date string 'YYYY-MM-DD' or None for last 30 days
            end_date: End date string 'YYYY-MM-DD' or None for today
            symbol: Filter by symbol or None for all
            limit: Maximum number of orders to return
            
        Returns:
            dict with success status and list of filled orders
        """
        if not self.client or not self.client_config:
            return {
                'success': False,
                'error': 'Tiger client not initialized',
                'orders': []
            }
        
        try:
            import datetime
            import time as time_module
            
            # Calculate time range (default: last 30 days)
            if end_date:
                end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end_dt = datetime.datetime.now()
            
            if start_date:
                start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            else:
                start_dt = end_dt - datetime.timedelta(days=30)
            
            # Convert to milliseconds timestamp
            # start_date: beginning of day (00:00:00)
            start_time = int(start_dt.timestamp() * 1000)
            # end_date: end of day (23:59:59) to include all orders on that day
            end_dt = end_dt.replace(hour=23, minute=59, second=59)
            end_time = int(end_dt.timestamp() * 1000)
            
            logger.info(f"Getting filled orders from {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
            
            # Get filled orders from Tiger API
            filled_orders = self.client.get_filled_orders(
                account=self.client_config.account,
                sec_type=SecurityType.STK,
                market=Market.ALL,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time
            )
            
            order_list = []
            
            # First pass: collect all orders and group by symbol for P&L calculation
            symbol_orders = {}  # {symbol: [orders]}
            raw_orders = []
            
            for order in filled_orders:
                try:
                    symbol = order.contract.symbol if order.contract else 'N/A'
                    action = str(order.action) if order.action else 'N/A'
                    is_open = getattr(order, 'is_open', True)  # True=开仓, False=平仓
                    
                    order_data = {
                        'order_id': str(order.id),
                        'symbol': symbol,
                        'action': action,
                        'quantity': order.quantity or 0,
                        'filled': order.filled or 0,
                        'avg_fill_price': order.avg_fill_price or 0,
                        'latest_price': getattr(order, 'latest_price', 0) or 0,
                        'realized_pnl': getattr(order, 'realized_pnl', 0) or 0,
                        'commission': getattr(order, 'commission', 0) or 0,
                        'order_time': order.order_time,
                        'trade_time': getattr(order, 'trade_time', order.order_time),
                        'status': str(order.status) if order.status else 'N/A',
                        'order_type': str(order.order_type) if order.order_type else 'N/A',
                        'limit_price': getattr(order, 'limit_price', 0) or 0,
                        'outside_rth': getattr(order, 'outside_rth', False),
                        'is_open': is_open,
                        'parent_id': getattr(order, 'parent_id', None)
                    }
                    
                    raw_orders.append(order_data)
                    
                    if symbol not in symbol_orders:
                        symbol_orders[symbol] = []
                    symbol_orders[symbol].append(order_data)
                    
                except Exception as e:
                    logger.warning(f"Error processing order: {str(e)}")
                    continue
            
            # Second pass: calculate P&L for orders where realized_pnl=0
            for order_data in raw_orders:
                symbol = order_data['symbol']
                action = order_data['action']
                realized_pnl = order_data['realized_pnl']
                
                # If realized_pnl is 0 and this is a closing order (SELL for long, BUY for short)
                if realized_pnl == 0 and not order_data.get('is_open', True):
                    # Find matching opening orders for this symbol
                    symbol_order_list = symbol_orders.get(symbol, [])
                    
                    # For SELL closing order, find BUY opening orders
                    if 'SELL' in action.upper():
                        opening_orders = [o for o in symbol_order_list 
                                         if 'BUY' in o['action'].upper() and o.get('is_open', True)]
                        if opening_orders:
                            # Use weighted average of opening prices
                            total_qty = sum(o['filled'] for o in opening_orders)
                            if total_qty > 0:
                                avg_entry = sum(o['avg_fill_price'] * o['filled'] for o in opening_orders) / total_qty
                                # P&L = (sell_price - avg_entry) * quantity
                                calculated_pnl = (order_data['avg_fill_price'] - avg_entry) * order_data['filled']
                                order_data['realized_pnl'] = round(calculated_pnl, 2)
                                order_data['pnl_calculated'] = True
                                logger.debug(f"Calculated P&L for {symbol} SELL: entry={avg_entry:.2f}, exit={order_data['avg_fill_price']:.2f}, pnl={calculated_pnl:.2f}")
                    
                    # For BUY closing order (short position), find SELL opening orders
                    elif 'BUY' in action.upper():
                        opening_orders = [o for o in symbol_order_list 
                                         if 'SELL' in o['action'].upper() and o.get('is_open', True)]
                        if opening_orders:
                            total_qty = sum(o['filled'] for o in opening_orders)
                            if total_qty > 0:
                                avg_entry = sum(o['avg_fill_price'] * o['filled'] for o in opening_orders) / total_qty
                                # P&L for short = (avg_entry - buy_price) * quantity
                                calculated_pnl = (avg_entry - order_data['avg_fill_price']) * order_data['filled']
                                order_data['realized_pnl'] = round(calculated_pnl, 2)
                                order_data['pnl_calculated'] = True
                                logger.debug(f"Calculated P&L for {symbol} BUY (short close): entry={avg_entry:.2f}, exit={order_data['avg_fill_price']:.2f}, pnl={calculated_pnl:.2f}")
            
            # Third pass: format timestamps and build final list
            import pytz
            eastern_tz = pytz.timezone('America/New_York')
            
            for order_data in raw_orders:
                try:
                    # Convert timestamps to US Eastern Time (matching exchange time)
                    if order_data['order_time']:
                        utc_dt = datetime.datetime.utcfromtimestamp(order_data['order_time'] / 1000)
                        utc_dt = pytz.utc.localize(utc_dt)
                        eastern_dt = utc_dt.astimezone(eastern_tz)
                        order_data['order_time_str'] = eastern_dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        order_data['order_time_str'] = 'N/A'
                    
                    if order_data['trade_time']:
                        utc_dt = datetime.datetime.utcfromtimestamp(order_data['trade_time'] / 1000)
                        utc_dt = pytz.utc.localize(utc_dt)
                        eastern_dt = utc_dt.astimezone(eastern_tz)
                        order_data['trade_time_str'] = eastern_dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        order_data['trade_time_str'] = 'N/A'
                    
                    order_list.append(order_data)
                    logger.debug(f"Filled order: {order_data['symbol']} {order_data['action']} {order_data['filled']}@{order_data['avg_fill_price']} PnL: {order_data['realized_pnl']}")
                    
                except Exception as e:
                    logger.warning(f"Error formatting order: {str(e)}")
                    order_list.append(order_data)  # Still add the order even if formatting fails
            
            # Sort by trade_time descending (most recent first)
            order_list.sort(key=lambda x: x.get('trade_time', 0) or 0, reverse=True)
            
            # Apply limit
            order_list = order_list[:limit]
            
            logger.info(f"Retrieved {len(order_list)} filled orders")
            
            return {
                'success': True,
                'orders': order_list,
                'count': len(order_list)
            }
            
        except Exception as e:
            logger.error(f"Error getting filled orders: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'orders': []
            }
    
    def get_open_orders_for_symbol(self, symbol):
        """Get all open orders for a specific symbol"""
        try:
            if not self.client:
                return {'success': False, 'error': 'Tiger client not initialized'}

            # Get open orders for the specific symbol
            open_orders = self.client.get_open_orders(symbol=symbol)
            logger.info(f"Retrieved {len(open_orders)} open orders for {symbol}")
            
            # Log details of each order - handling proper attribute access
            for order in open_orders:
                try:
                    order_id = order.id
                    action = order.action
                    quantity = order.quantity
                    limit_price = getattr(order, 'limit_price', getattr(order, 'aux_price', 'MKT'))
                    status = order.status
                    can_cancel = order.can_modify  # Fix: use can_modify instead of can_cancel
                    logger.info(f"Open order: {order_id} - {action} {quantity} {symbol} @ {limit_price} - Status: {status} - CanCancel: {can_cancel}")
                except Exception as e:
                    logger.error(f"Error accessing order attributes: {e}")
                    logger.info(f"Order attributes: {dir(order)}")
            
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
                try:
                    order_id = order.id
                    can_cancel = order.can_modify  # Fix: use can_modify instead of can_cancel
                    
                    logger.info(f"Processing order {order_id} for {symbol} - CanCancel: {can_cancel}")
                    
                    if can_cancel and order_id:
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
                except Exception as e:
                    logger.error(f"Error processing order for cancellation: {e}")
                    logger.info(f"Order attributes: {dir(order)}")
            
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
            if not self.client:
                return {'success': False, 'error': 'Tiger client not initialized'}

            # Cancel the order directly using the Tiger client
            result = self.client.cancel_order(id=order_id)
            logger.info(f"Cancel order {order_id} result: {result}")
            
            # Tiger API returns the order ID as confirmation of successful cancellation
            if result:
                return {'success': True, 'result': result}
            else:
                return {'success': False, 'error': 'Cancel request failed'}
            
        except Exception as e:
            logger.error(f"Error canceling order {order_id}: {str(e)}")
            return {'success': False, 'error': str(e)}

    def modify_stop_loss_price(self, old_order_id: str, symbol: str, quantity: float, 
                                new_stop_price: float, side: str = 'sell') -> dict:
        """
        Modify stop loss order price using Tiger API's modify_order method.
        
        First attempts to use modify_order directly (more efficient).
        Falls back to cancel+create if modify fails.
        
        Args:
            old_order_id: The existing stop loss order ID to modify
            symbol: Stock symbol (without [PAPER] prefix for real account)
            quantity: Number of shares
            new_stop_price: New stop loss price
            side: 'sell' for long positions, 'buy' for short positions
            
        Returns:
            dict with success status and order ID
        """
        try:
            if not self.client or not self.client_config:
                return {'success': False, 'error': 'Tiger client not initialized'}
            
            clean_symbol = symbol.replace('[PAPER]', '')
            
            logger.info(f"📈 Modifying stop loss: {clean_symbol} order {old_order_id} to ${new_stop_price:.2f}")
            
            # First, get the order to check its type
            force_cancel_create = False
            try:
                order = self.client.get_order(id=int(old_order_id))
                if order:
                    order_type = getattr(order, 'order_type', '')
                    order_type_str = str(order_type).upper()
                    
                    # If order is STP (not STP_LMT), we MUST use cancel+create to convert to STP_LMT
                    # STP does not support outside_rth, STP_LMT does
                    if order_type_str == 'STP' or (order_type_str.startswith('STP') and 'LMT' not in order_type_str):
                        logger.info(f"📋 Order {old_order_id} is {order_type_str} type, forcing cancel+create to convert to STP_LMT")
                        force_cancel_create = True
            except Exception as check_err:
                logger.warning(f"⚠️ Could not check order type for {old_order_id}: {str(check_err)}")
            
            # Method 1: Try to use modify_order directly (only for STP_LMT orders)
            if not force_cancel_create:
                try:
                    order = self.client.get_order(id=int(old_order_id))
                    
                    if order and hasattr(order, 'aux_price'):
                        # Check if order can be modified
                        can_modify = getattr(order, 'can_modify', True)
                        order_status = getattr(order, 'status', None)
                        
                        if can_modify or str(order_status) in ['NEW', 'SUBMITTED', 'HELD', 'OrderStatus.NEW', 'OrderStatus.SUBMITTED', 'OrderStatus.HELD']:
                            # Modify the stop price directly
                            order.aux_price = round(new_stop_price, 2)
                            
                            # Also update limit_price for STP_LMT orders
                            if hasattr(order, 'limit_price'):
                                if side == 'sell':
                                    order.limit_price = round(new_stop_price * 0.995, 2)
                                else:
                                    order.limit_price = round(new_stop_price * 1.005, 2)
                            
                            result = self.client.modify_order(order)
                            
                            if result:
                                logger.info(f"✅ Modified stop loss order {old_order_id} to ${new_stop_price:.2f} (direct modify)")
                                return {
                                    'success': True,
                                    'old_order_id': old_order_id,
                                    'new_order_id': old_order_id,  # Same order ID when using modify
                                    'new_stop_price': new_stop_price,
                                    'method': 'modify_order'
                                }
                            else:
                                logger.warning(f"⚠️ modify_order returned False for {old_order_id}, falling back to cancel+create")
                        else:
                            logger.warning(f"⚠️ Order {old_order_id} cannot be modified (status={order_status}), falling back to cancel+create")
                    else:
                        logger.warning(f"⚠️ Could not get order {old_order_id} for modify, falling back to cancel+create")
                        
                except Exception as modify_error:
                    logger.warning(f"⚠️ modify_order failed for {old_order_id}: {str(modify_error)}, falling back to cancel+create")
            
            # Method 2: Fall back to cancel + create new order
            cancel_result = self.cancel_order(old_order_id)
            if not cancel_result['success']:
                try:
                    order_check = self.client.get_order(id=int(old_order_id))
                    check_status = str(getattr(order_check, 'status', '')).upper() if order_check else ''
                    if 'EXPIRED' in check_status or 'CANCELLED' in check_status or 'FILLED' in check_status:
                        logger.info(f"📋 Old stop order {old_order_id} already {check_status}, proceeding to create new order")
                    else:
                        logger.error(f"Failed to cancel old stop loss order {old_order_id}: {cancel_result.get('error')} (status={check_status})")
                        return {'success': False, 'error': f"Cancel failed: {cancel_result.get('error')}"}
                except Exception as status_err:
                    logger.error(f"Failed to cancel old stop loss order {old_order_id}: {cancel_result.get('error')}")
                    return {'success': False, 'error': f"Cancel failed: {cancel_result.get('error')}"}
            else:
                logger.info(f"✅ Cancelled old stop loss order {old_order_id}")
            
            import time
            time.sleep(0.5)
            
            contract = stock_contract(symbol=clean_symbol, currency='USD')
            action = 'SELL' if side == 'sell' else 'BUY'
            
            # Use STOP LIMIT order instead of STOP order for extended hours support
            # Tiger API: Stop Order (STP) does NOT support pre/post market
            # Tiger API: Stop Limit Order (STP_LMT) DOES support pre/post market
            # Paper accounts don't support GTC, use DAY instead
            tif = 'DAY' if self.is_paper else 'GTC'
            stop_limit_price = round(new_stop_price * 0.995, 2)  # 0.5% below stop price
            order = stop_limit_order(
                account=self.client_config.account,
                contract=contract,
                action=action,
                quantity=abs(int(quantity)),  # Always use positive quantity
                limit_price=stop_limit_price,
                aux_price=round(new_stop_price, 2),
                time_in_force=tif  # DAY for paper, GTC for real
            )
            
            # Paper accounts: STP_LMT does NOT support outside_rth (Tiger rejects with "仅支持限价单")
            # Real accounts: STP_LMT supports outside_rth for extended hours protection
            if self.is_paper:
                order.outside_rth = False
                logger.info(f"📅 [Paper] Stop LIMIT order: outside_rth=False (trigger=${round(new_stop_price, 2)}, limit=${stop_limit_price})")
            else:
                order.outside_rth = True
                logger.info(f"📅 [Real] Stop LIMIT order: outside_rth=True (trigger=${round(new_stop_price, 2)}, limit=${stop_limit_price})")
            
            max_retries = 2
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    result = self.client.place_order(order)
                    
                    if result:
                        new_order_id = str(result)
                        logger.info(f"✅ Created new stop loss order {new_order_id} at ${new_stop_price:.2f} (cancel+create)")
                        return {
                            'success': True,
                            'old_order_id': old_order_id,
                            'new_order_id': new_order_id,
                            'new_stop_price': new_stop_price,
                            'method': 'cancel_create'
                        }
                    else:
                        last_error = 'Failed to create new stop loss order (no result)'
                        logger.warning(f"⚠️ Attempt {attempt + 1}/{max_retries} failed to create stop loss order for {clean_symbol}")
                        
                except Exception as retry_error:
                    last_error = str(retry_error)
                    logger.warning(f"⚠️ Attempt {attempt + 1}/{max_retries} error: {last_error}")
                
                if attempt < max_retries - 1:
                    time.sleep(1.0)
            
            logger.error(f"🚨 CRITICAL: Failed to create new stop loss order after {max_retries} attempts for {clean_symbol}. Position unprotected!")
            return {
                'success': False, 
                'error': f'Failed after {max_retries} retries: {last_error}',
                'critical': True,
                'old_order_cancelled': True
            }
            
        except Exception as e:
            logger.error(f"Error modifying stop loss order: {str(e)}")
            return {'success': False, 'error': str(e)}

    def place_stop_limit_order(self, symbol: str, action: str, quantity: float,
                               stop_price: float, limit_price: float = None,
                               outside_rth: bool = True) -> dict:
        """
        Place a standalone stop limit order (STP_LMT).
        
        Used to create protective stop loss orders with extended hours support.
        
        Args:
            symbol: Stock symbol (without [PAPER] prefix)
            action: 'SELL' for long positions, 'BUY' for short positions
            quantity: Number of shares
            stop_price: Stop trigger price (aux_price)
            limit_price: Limit price (if None, calculated with 0.5% slippage)
            outside_rth: Whether to allow extended hours trading (default True)
            
        Returns:
            dict with success status and order_id
        """
        try:
            if not self.client or not self.client_config:
                return {'success': False, 'error': 'Tiger client not initialized'}
            
            clean_symbol = symbol.replace('[PAPER]', '').strip()
            
            stop_price = round(stop_price, 2)
            if limit_price is None:
                if action.upper() == 'SELL':
                    limit_price = round(stop_price * 0.995, 2)
                else:
                    limit_price = round(stop_price * 1.005, 2)
            else:
                limit_price = round(limit_price, 2)
            
            if self.is_paper:
                outside_rth = False
            
            try:
                from tigeropen.quote.quote_client import QuoteClient
                quote_client = QuoteClient(self.client_config)
                briefs = quote_client.get_stock_briefs([clean_symbol])
                if briefs is not None and not briefs.empty:
                    latest_price = float(briefs.iloc[0].get('latest_price', 0))
                    if latest_price > 0:
                        if action.upper() == 'SELL' and stop_price >= latest_price:
                            logger.error(f"❌ STP_LMT validation failed: SELL stop ${stop_price:.2f} >= latest ${latest_price:.2f} for {clean_symbol}, broker will reject")
                            return {'success': False, 'error': f'止损价${stop_price:.2f}必须小于最新价${latest_price:.2f}(卖出止损)'}
                        elif action.upper() == 'BUY' and stop_price <= latest_price:
                            logger.error(f"❌ STP_LMT validation failed: BUY stop ${stop_price:.2f} <= latest ${latest_price:.2f} for {clean_symbol}, broker will reject")
                            return {'success': False, 'error': f'止损价${stop_price:.2f}必须大于最新价${latest_price:.2f}(买入止损)'}
            except Exception as price_check_err:
                logger.debug(f"Price check skipped for {clean_symbol}: {price_check_err}")
            
            logger.info(f"📋 Creating STP_LMT order: {action} {quantity} {clean_symbol} @ stop=${stop_price:.2f} limit=${limit_price:.2f} outside_rth={outside_rth}")
            
            contract = stock_contract(symbol=clean_symbol, currency='USD')
            
            tif = 'DAY' if self.is_paper else 'GTC'
            
            order = stop_limit_order(
                account=self.client_config.account,
                contract=contract,
                action=action.upper(),
                quantity=abs(int(quantity)),
                limit_price=limit_price,
                aux_price=stop_price,
                time_in_force=tif
            )
            
            order.outside_rth = outside_rth
            
            result = self.client.place_order(order)
            
            if result:
                order_id = str(result)
                logger.info(f"✅ Created STP_LMT order {order_id}: {action} {quantity} {clean_symbol} @ stop=${stop_price:.2f} outside_rth={outside_rth}")
                return {
                    'success': True,
                    'order_id': order_id,
                    'stop_price': stop_price,
                    'limit_price': limit_price,
                    'outside_rth': outside_rth
                }
            else:
                logger.error(f"❌ Failed to create STP_LMT order for {clean_symbol}")
                return {'success': False, 'error': 'Failed to create order (no result)'}
                
        except Exception as e:
            logger.error(f"Error placing stop limit order: {str(e)}")
            return {'success': False, 'error': str(e)}

    def _handle_position_increase(self, trade):
        """Handle position increase scenario - cancel existing orders if needed"""
        try:
            symbol = trade.symbol
            action = 'BUY' if trade.side == Side.BUY else 'SELL'
            
            logger.info(f"🔍 Position Increase Check: {action} {trade.quantity} {symbol}")
            
            # Check current position with enhanced logging
            position_result = self.get_positions(symbol=symbol)
            logger.info(f"📊 Position query result for {symbol}: success={position_result['success']}")
            
            if not position_result['success']:
                logger.warning(f"❌ Position query failed for {symbol}: {position_result.get('error', 'Unknown error')} - treating as new order")
                return {'success': True, 'is_position_increase': False}
            
            logger.info(f"📈 Position query returned {len(position_result.get('positions', []))} positions for {symbol}")
            
            if not position_result['positions']:
                logger.info(f"📍 No existing position for {symbol}, proceeding with new order")
                return {'success': True, 'is_position_increase': False}
            
            position = position_result['positions'][0]
            current_quantity = position['quantity']
            
            logger.info(f"📋 Current position for {symbol}: quantity={current_quantity}, cost={position.get('average_cost', 'N/A')}, value={position.get('market_value', 'N/A')}")
            
            if current_quantity == 0:
                logger.info(f"📍 Current position quantity is 0 for {symbol}, proceeding with new order")
                return {'success': True, 'is_position_increase': False}
            
            # Determine if this is a position increase with detailed logging
            is_position_increase = False
            
            logger.info(f"🧮 Position increase logic check: current_qty={current_quantity}, action={action}")
            
            if current_quantity > 0 and action == 'BUY':
                # Adding to long position
                is_position_increase = True
                logger.info(f"✅ Detected LONG position increase: current {current_quantity} shares, adding {trade.quantity} shares")
            elif current_quantity < 0 and action == 'SELL':
                # Adding to short position  
                is_position_increase = True
                logger.info(f"✅ Detected SHORT position increase: current {current_quantity} shares, adding {trade.quantity} shares")
            else:
                is_position_close = False
                if current_quantity > 0 and action == 'SELL':
                    logger.info(f"📉 Position close/reduce scenario: current LONG {current_quantity} shares, SELL action")
                    is_position_close = True
                elif current_quantity < 0 and action == 'BUY':
                    logger.info(f"📈 Position close/reduce scenario: current SHORT {current_quantity} shares, BUY action")
                    is_position_close = True
                elif current_quantity == 0:
                    logger.info(f"🆕 New position scenario: no existing position, {action} action")
                else:
                    logger.info(f"❓ Unexpected scenario: current {current_quantity} shares, action {action}")
                
                logger.info(f"❌ Not a position increase scenario: current {current_quantity} shares, action {action}")
                return {'success': True, 'is_position_increase': False, 'is_position_close': is_position_close}
            
            if is_position_increase:
                logger.info(f"Position increase detected for {symbol}. Checking for existing orders to cancel...")
                
                # Store protection info for later use
                protection_info = {
                    'stop_loss_price': getattr(trade, 'stop_loss_price', None),
                    'take_profit_price': getattr(trade, 'take_profit_price', None)
                }
                
                # Get open orders for this symbol
                try:
                    open_orders = self.client.get_open_orders(
                        account=self.client_config.account,
                        symbol=symbol
                    )
                    
                    if open_orders and len(open_orders) > 0:
                        logger.info(f"Found {len(open_orders)} open orders for {symbol}. Tiger Securities restriction requires canceling them before adding to position.")
                        
                        # Cancel all existing orders for this symbol
                        cancel_result = self.force_cancel_all_orders_for_symbol(symbol)
                        
                        if cancel_result['success']:
                            canceled_count = cancel_result.get('canceled_count', 0)
                            logger.info(f"Successfully canceled {canceled_count} orders for {symbol} before position increase")
                            
                            if canceled_count > 0:
                                # Poll to confirm cancellations are processed
                                if not self._wait_for_order_cancellation(symbol):
                                    return {
                                        'success': False,
                                        'error': f'Cannot increase position for {symbol}: orders were canceled but still showing as active after waiting'
                                    }
                        else:
                            logger.error(f"Failed to cancel existing orders for {symbol}: {cancel_result.get('error')}")
                            return {
                                'success': False,
                                'error': f'Cannot increase position for {symbol}: failed to cancel existing orders - {cancel_result.get("error")}'
                            }
                    else:
                        logger.info(f"No open orders found for {symbol}, proceeding with position increase")
                    
                    # Return success with position increase flag and protection info
                    return {
                        'success': True, 
                        'is_position_increase': True,
                        'protection_info': protection_info
                    }
                        
                except Exception as e:
                    logger.error(f"Error checking open orders for {symbol}: {str(e)}")
                    return {
                        'success': False,
                        'error': f'Error checking open orders for {symbol}: {str(e)}'
                    }
            
            # Not a position increase, return normal success
            return {'success': True, 'is_position_increase': False}
                        
        except Exception as e:
            logger.error(f"Error in _handle_position_increase: {str(e)}")
            return {'success': False, 'error': str(e)}

    def set_position_protection(self, symbol, stop_loss_price=None, take_profit_price=None):
        """Set protection orders (stop loss / take profit) for entire position"""
        try:
            if not self.client or not self.client_config:
                return {'success': False, 'error': 'Tiger client not initialized'}
            
            logger.info(f"Setting position protection for {symbol}: SL={stop_loss_price}, TP={take_profit_price}")
            
            # Get current position to determine quantity and direction
            position_result = self.get_positions(symbol=symbol)
            if not position_result['success'] or not position_result['positions']:
                return {'success': False, 'error': f'No position found for {symbol}'}
            
            position = position_result['positions'][0]
            current_quantity = position['quantity']
            
            if current_quantity == 0:
                return {'success': False, 'error': f'No position to protect for {symbol}'}
            
            # Determine if long or short position
            is_long = current_quantity > 0
            protection_quantity = abs(current_quantity)
            
            logger.info(f"Setting protection for {'LONG' if is_long else 'SHORT'} position of {protection_quantity} shares")
            
            # Create contract
            contract = stock_contract(symbol=symbol, currency='USD')
            response = {}
            
            # Set stop loss if provided
            if stop_loss_price:
                try:
                    # For long positions, stop loss is SELL order below current price
                    # For short positions, stop loss is BUY order above current price
                    sl_action = 'SELL' if is_long else 'BUY'
                    
                    # Create stop LIMIT order (not stop order) for extended hours support
                    # Tiger API: Stop Order (STP) does NOT support pre/post market
                    # Tiger API: Stop Limit Order (STP_LMT) DOES support pre/post market
                    rounded_sl = round(stop_loss_price, 2)
                    stop_limit_price = round(rounded_sl * 0.995, 2)  # 0.5% below stop price
                    # Paper accounts don't support GTC, use DAY instead
                    tif = 'DAY' if self.is_paper else 'GTC'
                    stop_loss_order = stop_limit_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=sl_action,
                        quantity=int(protection_quantity),
                        limit_price=stop_limit_price,
                        aux_price=rounded_sl,  # Stop trigger price
                        time_in_force=tif  # DAY for paper, GTC for real
                    )
                    stop_loss_order.outside_rth = not self.is_paper
                    logger.info(f"Stop LIMIT order: trigger=${stop_loss_price}, limit=${stop_limit_price}, TIF={tif}, outside_rth={not self.is_paper}")
                    
                    sl_result = self.client.place_order(stop_loss_order)
                    if sl_result and stop_loss_order.id:
                        response['stop_loss_order_id'] = str(stop_loss_order.id)
                        logger.info(f"Stop loss order placed: {stop_loss_order.id} at ${stop_loss_price}")
                    else:
                        logger.error(f"Failed to place stop loss order for {symbol}")
                        return {'success': False, 'error': 'Failed to place stop loss order'}
                        
                except Exception as e:
                    logger.error(f"Error placing stop loss order: {str(e)}")
                    return {'success': False, 'error': f'Stop loss order failed: {str(e)}'}
            
            # Set take profit if provided
            if take_profit_price:
                try:
                    # For long positions, take profit is SELL order above current price
                    # For short positions, take profit is BUY order below current price
                    tp_action = 'SELL' if is_long else 'BUY'
                    
                    # Create take profit limit order
                    # Paper accounts don't support GTC, use DAY instead
                    tif = 'DAY' if self.is_paper else 'GTC'
                    take_profit_order = limit_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=tp_action,
                        quantity=int(protection_quantity),
                        limit_price=round(take_profit_price, 2),
                        time_in_force=tif  # DAY for paper, GTC for real
                    )
                    take_profit_order.outside_rth = True  # Enable extended hours trading
                    
                    tp_result = self.client.place_order(take_profit_order)
                    if tp_result and take_profit_order.id:
                        response['take_profit_order_id'] = str(take_profit_order.id)
                        logger.info(f"Take profit order placed: {take_profit_order.id} at ${take_profit_price}")
                    else:
                        logger.error(f"Failed to place take profit order for {symbol}")
                        return {'success': False, 'error': 'Failed to place take profit order'}
                        
                except Exception as e:
                    logger.error(f"Error placing take profit order: {str(e)}")
                    return {'success': False, 'error': f'Take profit order failed: {str(e)}'}
            
            response['success'] = True
            return response
            
        except Exception as e:
            logger.error(f"Error setting position protection for {symbol}: {str(e)}")
            return {'success': False, 'error': str(e)}

    def _wait_for_order_cancellation(self, symbol, max_attempts=20, sleep_interval=0.5):
        """Wait for order cancellations to be processed by polling for open orders"""
        import time
        
        logger.info(f"Polling to confirm order cancellations for {symbol} (max {max_attempts} attempts)")
        
        for attempt in range(max_attempts):
            try:
                # Check if there are still open orders for this symbol
                open_orders = self.client.get_open_orders(
                    account=self.client_config.account,
                    symbol=symbol
                )
                
                if not open_orders or len(open_orders) == 0:
                    logger.info(f"Confirmed: no open orders remaining for {symbol} after {attempt + 1} attempts")
                    return True
                
                logger.info(f"Attempt {attempt + 1}: still {len(open_orders)} open orders for {symbol}, waiting...")
                time.sleep(sleep_interval)
                
            except Exception as e:
                logger.error(f"Error checking open orders during cancellation wait: {str(e)}")
                # If we can't check, return failure to be safe
                logger.error("Cannot verify order cancellation due to exception, returning failure")
                return False
        
        logger.error(f"Timeout: still have open orders for {symbol} after {max_attempts} attempts")
        return False

    def close_position_with_sandbox_fallback(self, symbol, trading_session='regular', reference_price=None, signal_side=None, signal_quantity=None):
        """Close position with sandbox environment fallback strategies
        
        Args:
            symbol: Stock symbol
            trading_session: Trading session type
            reference_price: Reference price for limit orders
            signal_side: The side from the flat signal ('buy' or 'sell')
                        - 'sell' means exit long position (平多仓)
                        - 'buy' means exit short position (平空仓)
            signal_quantity: Explicit quantity from signal (None or 'all' = close all)
        """
        logger.info(f"Attempting to close position for {symbol} with sandbox fallback, signal_side={signal_side}, signal_quantity={signal_quantity}")
        
        # First try normal close
        result = self.close_position(symbol, trading_session, reference_price=reference_price, signal_side=signal_side, signal_quantity=signal_quantity)
        
        # If failed due to salable quantity issue, try fallback strategies
        if not result['success'] and 'salable quantity is 0' in result.get('error', ''):
            logger.info(f"Normal close failed for {symbol} due to salable quantity issue, trying sandbox fallback strategies")
            
            # Strategy 1: Try limit order at current market price - 1%
            try:
                position_result = self.get_positions(symbol=symbol)
                if position_result['success'] and position_result['positions']:
                    position = position_result['positions'][0]
                    current_quantity = position['quantity']
                    
                    if current_quantity > 0:
                        fallback_action = 'SELL'
                        fallback_offset = 0.99
                    elif current_quantity < 0:
                        fallback_action = 'BUY'
                        fallback_offset = 1.01
                    else:
                        logger.info(f"Sandbox fallback: position quantity is 0 for {symbol}, nothing to close")
                        return {
                            'success': True,
                            'no_action': True,
                            'message': f'Position already closed for {symbol}'
                        }
                    
                    current_price = position.get('latest_price', 0)
                    if current_price > 0:
                        limit_price = round(current_price * fallback_offset, 2)
                        logger.info(f"Trying fallback limit order: {fallback_action} {abs(current_quantity)} {symbol} @ ${limit_price}")
                        
                        contract = stock_contract(symbol=symbol, currency='USD')
                        order = limit_order(
                            account=self.client_config.account,
                            contract=contract,
                            action=fallback_action,
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
                                'action': fallback_action.lower(),
                                'quantity': abs(current_quantity),
                                'fallback_strategy': 'limit_order',
                                'limit_price': limit_price,
                                'order_type': 'limit',
                                'order_price': limit_price,
                                'outside_rth': trading_session != 'regular',
                                'trading_session': trading_session
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

    def close_position(self, symbol, trading_session='regular', reference_price=None, signal_side=None, signal_quantity=None):
        """Close existing position for a symbol.
        
        Strategy: Try Tiger API first for position info, fallback to database records,
        then submit order directly to Tiger and let Tiger validate.
        signal_quantity: Explicit quantity from signal (None or 'all' = close all, number = partial close)
        
        Args:
            symbol: Stock symbol
            trading_session: Trading session type
            reference_price: Reference price for limit orders
            signal_side: The side from the flat signal ('buy' or 'sell')
                        - 'sell' means exit long position (平多仓)
                        - 'buy' means exit short position (平空仓)
        """
        if not self.client:
            return {
                'success': False,
                'error': 'Tiger client not initialized'
            }
        
        try:
            logger.info(f"Attempting to close position for {symbol}, signal_side={signal_side}")
            
            try:
                cancel_result = self.force_cancel_all_orders_for_symbol(symbol)
                if cancel_result.get('success') and cancel_result.get('canceled_count', 0) > 0:
                    logger.info(f"Pre-close: Cancelled {cancel_result['canceled_count']} pending orders for {symbol}")
                    import time
                    time.sleep(0.5)
            except Exception as cancel_err:
                logger.warning(f"Pre-close: Failed to cancel pending orders for {symbol}: {cancel_err}")
            
            current_quantity = None
            position = None
            position_source = None
            close_quantity = None
            
            account_type = getattr(self, '_account_type', 'real')
            
            position_result = self.get_positions(symbol=symbol)
            if position_result.get('success') and position_result.get('positions'):
                position = position_result['positions'][0]
                current_quantity = position['quantity']
                position_source = 'tiger_api'
                logger.info(f"Got position from Tiger API: {symbol} = {current_quantity} shares")
            else:
                if position_result.get('rate_limited'):
                    logger.warning(f"Tiger API rate limited for {symbol}, no fallback for close orders")
                else:
                    logger.warning(f"No position from Tiger API for {symbol}, position likely already closed")

            
            if current_quantity is None or current_quantity == 0:
                logger.warning(f"No position found for {symbol} at broker (API + cache + DB all returned 0/None). "
                              f"Aborting close to prevent opening reverse position.")
                return {
                    'success': True,
                    'no_action': True,
                    'message': f'No position found for {symbol} - position may have been already closed by trailing stop or other exit'
                }
            
            if current_quantity is not None and current_quantity != 0:
                if signal_side:
                    is_long_position = current_quantity > 0
                    is_short_position = current_quantity < 0
                    
                    if signal_side.lower() == 'sell' and is_short_position:
                        logger.info(f"No action: Received exit-long signal (sell) but holding SHORT position for {symbol}")
                        return {
                            'success': True,
                            'no_action': True,
                            'message': f'No action needed: exit-long signal received but currently holding SHORT position for {symbol}',
                            'position_type': 'short',
                            'signal_type': 'exit_long'
                        }
                    
                    if signal_side.lower() == 'buy' and is_long_position:
                        logger.info(f"No action: Received exit-short signal (buy) but holding LONG position for {symbol}")
                        return {
                            'success': True,
                            'no_action': True,
                            'message': f'No action needed: exit-short signal received but currently holding LONG position for {symbol}',
                            'position_type': 'long',
                            'signal_type': 'exit_short'
                        }
                
                if current_quantity > 0:
                    action = 'SELL'
                    close_quantity = abs(current_quantity)
                else:
                    action = 'BUY'
                    close_quantity = abs(current_quantity)
            
            logger.info(f"Close position: {action} {close_quantity} {symbol} (source: {position_source or 'signal'})")
            
            # Create contract
            contract = stock_contract(symbol=symbol, currency='USD')
            
            # Determine trading session settings with smart auto-detection
            session_map = {
                'regular': None,
                'extended': None,
                'overnight': TradingSessionType.OVERNIGHT,
                'full': TradingSessionType.FULL
            }
            
            trading_session_type = session_map.get(trading_session)
            
            # Smart auto-determination based on current market time
            is_regular_hours = self._is_regular_trading_hours()
            
            if trading_session == 'regular':
                # If signal specifies regular session but we're outside regular hours,
                # automatically enable extended hours trading for closing positions
                if not is_regular_hours:
                    outside_rth = True
                    trading_session = 'extended'  # Upgrade to extended session
                    logger.info("Close position: Auto-detected outside regular hours, enabling extended hours trading")
                else:
                    outside_rth = False
                    logger.info("Close position: Auto-detected regular trading hours, standard session")
            else:
                # For extended, overnight, full sessions, always allow outside RTH
                outside_rth = trading_session != 'regular'
            
            logger.info(f"Closing position: {action} {close_quantity} shares of {symbol}")
            logger.info(f"Trading session: {trading_session}, outside_rth: {outside_rth}")
            
            # Create order - use limit order for extended hours, market order for regular hours
            if outside_rth:
                # Extended hours: use limit order to ensure execution
                # Use reference price if provided, otherwise try to get from position data
                try:
                    current_price = None
                    
                    # First priority: use reference_price if provided
                    if reference_price and reference_price > 0:
                        current_price = reference_price
                        logger.info(f"Using provided reference_price: ${reference_price:.2f} for extended hours limit order")
                    else:
                        # Fallback: try to get price from current position data
                        try:
                            latest_price = position.get('latest_price', 0)
                            market_value = position.get('market_value', 0)
                            quantity = position.get('quantity', 0)
                            
                            if latest_price and latest_price > 0:
                                current_price = latest_price
                                logger.info(f"Using position latest_price: ${latest_price:.2f}")
                            elif market_value and quantity and quantity != 0:
                                current_price = abs(market_value / quantity)
                                logger.info(f"Calculated price from market_value/quantity: ${current_price:.2f}")
                            else:
                                # Try average_cost as last resort
                                avg_cost = position.get('average_cost', 0)
                                if avg_cost and avg_cost > 0:
                                    current_price = avg_cost
                                    logger.info(f"Using position average_cost: ${avg_cost:.2f} as fallback")
                        except Exception as e:
                            logger.error(f"Error getting price from position data: {str(e)}")
                    
                    if not current_price or current_price <= 0:
                        # If no current price available, return error with detailed message
                        logger.error(f"Cannot create limit order for {symbol} in extended hours without current price")
                        return {
                            'success': False,
                            'error': f'Extended hours trading requires reference price for limit order. No valid price found for {symbol}. Please provide reference_price parameter.'
                        }
                    
                    # Create limit order with reasonable buffer for execution
                    # Use smaller buffer (2%) for better fill probability
                    if action == 'SELL':
                        limit_price = round(current_price * 0.98, 2)  # Sell 2% below reference for better fill
                    else:
                        limit_price = round(current_price * 1.02, 2)  # Buy 2% above reference for better fill
                    
                    order = limit_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=action,
                        limit_price=limit_price,
                        quantity=int(close_quantity)
                    )
                    logger.info(f"Created limit order for extended hours at ${limit_price:.2f} (reference: ${current_price:.2f})")
                    
                except Exception as e:
                    logger.error(f"Error creating limit order for extended hours: {str(e)}")
                    return {
                        'success': False,
                        'error': f'Failed to create extended hours limit order: {str(e)}'
                    }
            else:
                # Regular hours: use market order for quick execution
                order = market_order(
                    account=self.client_config.account,
                    contract=contract,
                    action=action,
                    quantity=int(close_quantity)
                )
                logger.info("Created market order for regular hours")
            
            # CRITICAL: Set trading session type and outside_rth for extended hours
            if trading_session_type:
                order.trading_session_type = trading_session_type
                logger.info(f"Set order.trading_session_type = {trading_session_type}")
            
            order.outside_rth = outside_rth
            logger.info(f"Set order.outside_rth = {outside_rth}")
            
            # Additional extended hours validation
            if outside_rth:
                logger.info(f"EXTENDED HOURS ORDER VALIDATION:")
                logger.info(f"  - Symbol: {symbol}")
                logger.info(f"  - Action: {action}")
                logger.info(f"  - Quantity: {close_quantity}")
                logger.info(f"  - Order type: {'LIMIT' if hasattr(order, 'limit_price') else 'MARKET'}")
                if hasattr(order, 'limit_price'):
                    logger.info(f"  - Limit price: ${order.limit_price:.2f}")
                logger.info(f"  - Trading session: {trading_session}")
                logger.info(f"  - Outside RTH: {order.outside_rth}")
                logger.info(f"  - Session type: {getattr(order, 'trading_session_type', 'None')}")
                logger.info(f"  - Account: {self.client_config.account}")
            
            # Place order
            result = self.client.place_order(order)
            
            if result and order.id:
                order_id = str(order.id)
                logger.info(f"Close position order placed successfully: {order_id}")
                
                # Determine order type and price based on what was actually used
                order_type_used = 'limit' if outside_rth else 'market'
                order_price_used = getattr(order, 'limit_price', None) if outside_rth else None
                
                return {
                    'success': True,
                    'message': f'Close position order placed for {symbol}',
                    'order_id': order_id,
                    'action': action.lower(),
                    'quantity': close_quantity,
                    'original_position': current_quantity,
                    'order_type': order_type_used,
                    'order_price': order_price_used,
                    'outside_rth': outside_rth,
                    'trading_session': trading_session
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
    
    def place_limit_order(self, symbol, action, quantity, limit_price, outside_rth=True, time_in_force='DAY'):
        """Place a standalone limit order (used by OCA soft stop and other direct order needs)"""
        if not self.client or not self.client_config:
            return {'success': False, 'error': 'Tiger client not initialized'}
        
        try:
            contract = stock_contract(symbol=symbol, currency='USD')
            order = limit_order(
                account=self.client_config.account,
                contract=contract,
                action=action,
                limit_price=round(limit_price, 2),
                quantity=int(quantity)
            )
            order.outside_rth = outside_rth
            if time_in_force == 'GTC':
                order.time_in_force = 'GTC'
            
            result = self.client.place_order(order)
            
            if result and order.id:
                order_id = str(order.id)
                logger.info(f"Limit order placed: {action} {quantity} {symbol} @ ${limit_price:.2f}, order_id={order_id}")
                return {
                    'success': True,
                    'order_id': order_id,
                    'action': action,
                    'quantity': quantity,
                    'limit_price': limit_price
                }
            else:
                return {'success': False, 'error': 'Failed to place limit order, no order ID returned'}
        except Exception as e:
            logger.error(f"Error placing limit order for {symbol}: {str(e)}")
            return {'success': False, 'error': str(e)}

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
    
    def create_oca_orders_for_position(self, symbol, quantity, stop_loss_price, take_profit_price, 
                                        entry_price=None, trade_id=None, trailing_stop_id=None,
                                        cancel_existing=True, skip_position_check=False, position_side='long'):
        """Create OCA (One-Cancels-All) orders for existing position.
        
        Account-specific configurations:
        - Real account: GTC, outside_rth=True for both stop and take profit
        - Paper account: DAY, outside_rth=True for take profit, outside_rth=False for stop loss
          (Paper requires soft protection via TrailingStop for extended hours stop)
        
        Args:
            symbol: Stock symbol
            quantity: Number of shares to protect
            stop_loss_price: Stop loss trigger price
            take_profit_price: Take profit limit price
            entry_price: Entry price for reference (optional)
            trade_id: Related Trade record ID (optional)
            trailing_stop_id: Related TrailingStopPosition ID (optional)
            cancel_existing: Whether to cancel existing orders first (default True)
            skip_position_check: Skip position verification (for fresh fills, default False)
            position_side: 'long' or 'short' (used when skip_position_check=True)
            
        Returns:
            dict with success, order IDs, and OCAGroup record ID
        """
        if not self.client or not self.client_config:
            return {
                'success': False,
                'error': 'Tiger client not initialized'
            }
            
        try:
            if get_config('TRADING_ENABLED', 'true').lower() != 'true':
                return {
                    'success': False,
                    'error': 'Trading is currently disabled'
                }
            
            quantity = abs(int(quantity)) if quantity else 0
            if quantity <= 0:
                return {
                    'success': False,
                    'error': f'Invalid quantity={quantity}, must be > 0'
                }
            
            if skip_position_check:
                logger.info(f"Skipping position check for {symbol}, using provided side={position_side}, qty={quantity}")
                if position_side == 'long':
                    action = 'SELL'
                    position_type = 'LONG'
                    side = 'long'
                else:
                    action = 'BUY'
                    position_type = 'SHORT'
                    side = 'short'
                current_qty = abs(quantity)
                salable_qty = abs(quantity)
            else:
                positions_result = self.get_positions(symbol)
                if not positions_result['success'] or not positions_result['positions']:
                    return {
                        'success': False,
                        'error': f'No position found for {symbol}'
                    }
                
                position = positions_result['positions'][0]
                current_qty = position['quantity']
                salable_qty = current_qty
                
                if current_qty > 0:
                    action = 'SELL'
                    position_type = 'LONG'
                    side = 'long'
                else:
                    action = 'BUY'
                    position_type = 'SHORT'
                    side = 'short'
                    current_qty = abs(current_qty)
                    salable_qty = abs(salable_qty)
            
            if quantity > salable_qty:
                return {
                    'success': False,
                    'error': f'Cannot set protection for {quantity} shares, only {salable_qty} shares available'
                }
            
            logger.info(f"Creating OCA orders for {position_type} position: {quantity} shares of {symbol}")
            
            if stop_loss_price:
                stop_loss_price = round(stop_loss_price, 2)
            if take_profit_price:
                take_profit_price = round(take_profit_price, 2)
            
            existing_sl_id = None
            existing_sl_price = None
            existing_tp_id = None
            existing_tp_price = None
            existing_order_qty = None
            try:
                open_orders_result = self.get_open_orders_for_symbol(symbol)
                if open_orders_result.get('success'):
                    for order in open_orders_result.get('orders', []):
                        order_action = getattr(order, 'action', '')
                        order_type = str(getattr(order, 'order_type', ''))
                        order_qty = getattr(order, 'quantity', None)
                        is_exit = (side == 'long' and order_action == 'SELL') or (side == 'short' and order_action == 'BUY')
                        if is_exit:
                            existing_order_qty = order_qty
                            if 'STP' in order_type:
                                existing_sl_id = str(order.id)
                                existing_sl_price = getattr(order, 'aux_price', None) or getattr(order, 'limit_price', None)
                                logger.info(f"Found existing SL order {existing_sl_id}: {order_action} {order_qty} @ ${existing_sl_price}")
                            elif order_type == 'LMT':
                                existing_tp_id = str(order.id)
                                existing_tp_price = getattr(order, 'limit_price', None)
                                logger.info(f"Found existing TP order {existing_tp_id}: {order_action} {order_qty} @ ${existing_tp_price}")
            except Exception as e:
                logger.warning(f"Could not check existing orders for {symbol}: {e}")
            
            sl_match = (existing_sl_price == stop_loss_price) if stop_loss_price else (existing_sl_id is None)
            tp_match = (existing_tp_price == take_profit_price) if take_profit_price else (existing_tp_id is None)
            qty_match = (existing_order_qty == quantity) if existing_order_qty else False
            
            if sl_match and tp_match and qty_match and (existing_sl_id or existing_tp_id):
                logger.info(f"OCA orders already exist for {symbol} with matching prices and quantity, "
                           f"skipping duplicate placement (SL={existing_sl_id}, TP={existing_tp_id})")
                
                from models import OCAGroup, OCAStatus
                existing_oca = OCAGroup.query.filter_by(
                    symbol=symbol, account_type='paper' if self.is_paper else 'real', status=OCAStatus.ACTIVE
                ).first()
                if existing_oca:
                    return {
                        'success': True,
                        'stop_loss_order_id': existing_sl_id,
                        'take_profit_order_id': existing_tp_id,
                        'oca_group_record_id': existing_oca.id,
                        'orders_created': [],
                        'skipped_duplicate': True
                    }
            
            import time as time_module
            
            if cancel_existing:
                has_existing = existing_sl_id or existing_tp_id
                if has_existing:
                    try:
                        cancel_result = self.force_cancel_all_orders_for_symbol(symbol)
                        if cancel_result.get('success'):
                            cancelled_count = cancel_result.get('canceled_count', 0)
                            logger.info(f"Cancelled existing orders for {symbol}")
                            if cancelled_count > 0:
                                time_module.sleep(2)
                                logger.info(f"Waited 2s after cancelling {cancelled_count} orders for position hold release")
                    except Exception as e:
                        logger.warning(f"Could not cancel existing orders: {str(e)}")
                else:
                    logger.info(f"No existing orders to cancel for {symbol}")
            
            contract = stock_contract(symbol=symbol, currency='USD')
            
            oca_group_id_str = f"{symbol}-{int(time_module.time())}"
            orders_created = []
            stop_loss_id = None
            take_profit_id = None
            
            is_paper = getattr(self.client_config, '_is_paper', False) or self.is_paper
            
            if is_paper:
                tif = 'DAY'
                outside_rth_stop = False
                outside_rth_tp = True
                logger.info(f"Paper account: TIF=DAY, stop outside_rth=False (soft protection), TP outside_rth=True")
            else:
                tif = 'GTC'
                outside_rth_stop = True
                outside_rth_tp = True
                logger.info(f"Real account: TIF=GTC, both orders outside_rth=True")
            
            if stop_loss_price and take_profit_price:
                try:
                    from tigeropen.common.util.order_utils import order_leg, oca_order
                    
                    order_legs = []
                    
                    rounded_sl = round(stop_loss_price, 2)
                    stop_limit_price = round(rounded_sl * 0.995, 2)
                    stop_loss_leg = order_leg(
                        'STP_LMT',
                        price=rounded_sl,
                        limit_price=stop_limit_price,
                        outside_rth=outside_rth_stop,
                        time_in_force=tif
                    )
                    order_legs.append(stop_loss_leg)
                    logger.info(f"OCA stop leg: trigger=${rounded_sl}, limit=${stop_limit_price}, outside_rth={outside_rth_stop}")
                    
                    take_profit_leg = order_leg(
                        'LMT',
                        limit_price=round(take_profit_price, 2),
                        outside_rth=outside_rth_tp,
                        time_in_force=tif
                    )
                    order_legs.append(take_profit_leg)
                    logger.info(f"OCA TP leg: price=${take_profit_price}, outside_rth={outside_rth_tp}")
                    
                    oca_order_obj = oca_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=action,
                        order_legs=order_legs,
                        quantity=int(quantity)
                    )
                    
                    oca_result = self.client.place_order(oca_order_obj)
                    if oca_result and oca_order_obj.id:
                        oca_main_id = str(oca_order_obj.id)
                        orders_created.append(f"OCA Group: {oca_main_id}")
                        logger.info(f"OCA order group placed: {oca_main_id}")
                        
                        raw_sub_ids = getattr(oca_order_obj, 'sub_ids', None)
                        logger.info(f"OCA order {oca_main_id} sub_ids raw: {raw_sub_ids} (type={type(raw_sub_ids).__name__})")
                        
                        sub_ids = list(raw_sub_ids) if raw_sub_ids else []
                        
                        if not sub_ids:
                            raw_orders = getattr(oca_order_obj, 'orders', None)
                            if raw_orders and isinstance(raw_orders, list):
                                logger.info(f"sub_ids empty, extracting from orders attribute ({len(raw_orders)} orders found)")
                                for raw_order in raw_orders:
                                    if isinstance(raw_order, dict):
                                        order_id = raw_order.get('id')
                                        order_type = raw_order.get('orderType', '')
                                    else:
                                        order_id = getattr(raw_order, 'id', None)
                                        order_type = str(getattr(raw_order, 'order_type', '') or getattr(raw_order, 'orderType', ''))
                                    
                                    if order_id:
                                        if 'STP' in str(order_type):
                                            stop_loss_id = str(order_id)
                                            logger.info(f"  SL sub-order from orders attr: id={stop_loss_id}, type={order_type}")
                                        elif str(order_type) == 'LMT':
                                            take_profit_id = str(order_id)
                                            logger.info(f"  TP sub-order from orders attr: id={take_profit_id}, type={order_type}")
                                        else:
                                            logger.warning(f"  Unknown sub-order type: id={order_id}, type={order_type}")
                            else:
                                logger.warning(f"Both sub_ids and orders are empty for OCA {oca_main_id}")
                        
                        if sub_ids and not stop_loss_id and not take_profit_id:
                            if len(sub_ids) == 2:
                                try:
                                    first_order = self.client.get_order(account=self.client_config.account, id=sub_ids[0])
                                    if first_order:
                                        first_type = str(getattr(first_order, 'order_type', ''))
                                        if 'STP' in first_type:
                                            stop_loss_id = str(sub_ids[0])
                                            take_profit_id = str(sub_ids[1])
                                        else:
                                            take_profit_id = str(sub_ids[0])
                                            stop_loss_id = str(sub_ids[1])
                                        logger.info(f"OCA sub-orders identified via sub_ids: SL={stop_loss_id}, TP={take_profit_id} "
                                                   f"(first sub-order type={first_type})")
                                    else:
                                        stop_loss_id = str(sub_ids[0])
                                        take_profit_id = str(sub_ids[1])
                                        logger.info(f"OCA sub-orders assigned by position (query returned None): "
                                                   f"SL={stop_loss_id}, TP={take_profit_id}")
                                except Exception as sub_e:
                                    stop_loss_id = str(sub_ids[0])
                                    take_profit_id = str(sub_ids[1])
                                    logger.warning(f"Could not verify sub-order type, assigning by position: "
                                                  f"SL={stop_loss_id}, TP={take_profit_id}, error: {sub_e}")
                            else:
                                for sub_id in sub_ids:
                                    try:
                                        sub_order = self.client.get_order(account=self.client_config.account, id=sub_id)
                                        if sub_order:
                                            sub_order_type = str(getattr(sub_order, 'order_type', ''))
                                            if 'STP' in sub_order_type:
                                                stop_loss_id = str(sub_id)
                                            elif sub_order_type == 'LMT':
                                                take_profit_id = str(sub_id)
                                            logger.info(f"OCA sub-order {sub_id}: type={sub_order_type}")
                                    except Exception as sub_e:
                                        logger.warning(f"Could not query sub order {sub_id}: {sub_e}")
                        
                        if stop_loss_id:
                            orders_created.append(f"Stop Loss: {stop_loss_id}")
                        if take_profit_id:
                            orders_created.append(f"Take Profit: {take_profit_id}")
                        
                        if (stop_loss_price and not stop_loss_id) or (take_profit_price and not take_profit_id):
                            logger.error(f"❌ OCA placed (id={oca_main_id}) but could not identify sub-order IDs: "
                                        f"SL={stop_loss_id}, TP={take_profit_id}. "
                                        f"sub_ids={raw_sub_ids}, orders={getattr(oca_order_obj, 'orders', None)}")
                    else:
                        logger.error(f"Failed to place OCA order group")
                        raise Exception("OCA order placement failed")
                        
                except Exception as e:
                    error_str = str(e).lower()
                    is_bad_request = '当前时段不支持下单' in str(e) or 'bad_request' in error_str
                    is_true_rate_limit = ('code=4' in error_str or '请稍后' in str(e)) and not is_bad_request
                    is_rate_limited = is_true_rate_limit or ('code=1200' in error_str and 'forbidden' in error_str and not is_bad_request)
                    
                    if is_rate_limited:
                        logger.warning(f"OCA creation rate-limited for {symbol}, skipping fallback orders to avoid further throttling")
                        return {
                            'success': False,
                            'error': f'Rate limited: {str(e)}',
                            'rate_limited': True,
                            'orders_created': []
                        }
                    
                    if is_bad_request:
                        logger.warning(f"OCA group order not supported for {symbol} ({str(e)}), falling back to individual orders")
                    else:
                        logger.error(f"OCA creation failed: {type(e).__name__}: {str(e)}")
                    logger.info("Falling back to individual orders")
                    
                    if stop_loss_price:
                        try:
                            rounded_sl = round(stop_loss_price, 2)
                            stop_limit_price = round(rounded_sl * 0.995, 2)
                            stop_loss_order = stop_limit_order(
                                account=self.client_config.account,
                                contract=contract,
                                action=action,
                                quantity=quantity,
                                limit_price=stop_limit_price,
                                aux_price=rounded_sl,
                                time_in_force=tif
                            )
                            stop_loss_order.outside_rth = outside_rth_stop
                            
                            stop_result = self.client.place_order(stop_loss_order)
                            if stop_result and stop_loss_order.id:
                                stop_loss_id = str(stop_loss_order.id)
                                orders_created.append(f"Stop Loss: {stop_loss_id}")
                                logger.info(f"Fallback stop order: {stop_loss_id}")
                        except Exception as fallback_e:
                            fallback_str = str(fallback_e).lower()
                            if 'code=1200' in fallback_str or 'code=4' in fallback_str or 'forbidden' in fallback_str or '请稍后' in str(fallback_e):
                                logger.warning(f"Fallback stop loss rate-limited for {symbol}, aborting all fallback orders")
                                return {
                                    'success': False,
                                    'error': f'Rate limited during fallback: {str(fallback_e)}',
                                    'rate_limited': True,
                                    'orders_created': []
                                }
                            logger.error(f"Fallback stop loss failed: {fallback_e}")
                    
                    if take_profit_price:
                        try:
                            take_profit_order = limit_order(
                                account=self.client_config.account,
                                contract=contract,
                                action=action,
                                quantity=quantity,
                                limit_price=round(take_profit_price, 2),
                                time_in_force=tif
                            )
                            take_profit_order.outside_rth = outside_rth_tp
                            
                            profit_result = self.client.place_order(take_profit_order)
                            if profit_result and take_profit_order.id:
                                take_profit_id = str(take_profit_order.id)
                                orders_created.append(f"Take Profit: {take_profit_id}")
                                logger.info(f"Fallback TP order: {take_profit_id}")
                        except Exception as fallback_e:
                            fallback_str = str(fallback_e).lower()
                            if 'code=1200' in fallback_str or 'code=4' in fallback_str or 'forbidden' in fallback_str or '请稍后' in str(fallback_e):
                                logger.warning(f"Fallback TP rate-limited for {symbol}, aborting")
                                return {
                                    'success': False,
                                    'error': f'Rate limited during fallback: {str(fallback_e)}',
                                    'rate_limited': True,
                                    'orders_created': list(orders_created)
                                }
                            logger.error(f"Fallback take profit failed: {fallback_e}")
                            
            elif stop_loss_price:
                try:
                    rounded_sl = round(stop_loss_price, 2)
                    stop_limit_price = round(rounded_sl * 0.995, 2)
                    stop_loss_order = stop_limit_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=action,
                        quantity=quantity,
                        limit_price=stop_limit_price,
                        aux_price=rounded_sl,
                        time_in_force=tif
                    )
                    stop_loss_order.outside_rth = outside_rth_stop
                    
                    stop_result = self.client.place_order(stop_loss_order)
                    if stop_result and stop_loss_order.id:
                        stop_loss_id = str(stop_loss_order.id)
                        orders_created.append(f"Stop Loss: {stop_loss_id}")
                except Exception as e:
                    logger.error(f"Stop loss order failed: {e}")
                    
            elif take_profit_price:
                try:
                    take_profit_order = limit_order(
                        account=self.client_config.account,
                        contract=contract,
                        action=action,
                        quantity=quantity,
                        limit_price=round(take_profit_price, 2),
                        time_in_force=tif
                    )
                    take_profit_order.outside_rth = outside_rth_tp
                    
                    profit_result = self.client.place_order(take_profit_order)
                    if profit_result and take_profit_order.id:
                        take_profit_id = str(take_profit_order.id)
                        orders_created.append(f"Take Profit: {take_profit_id}")
                except Exception as e:
                    logger.error(f"Take profit order failed: {e}")
            
            oca_group_record = None
            if orders_created:
                try:
                    from app import db
                    from models import OCAGroup, OCAStatus
                    
                    existing_oca = OCAGroup.query.filter_by(oca_group_id=oca_group_id_str).first()
                    if existing_oca:
                        existing_oca.stop_order_id = stop_loss_id or existing_oca.stop_order_id
                        existing_oca.take_profit_order_id = take_profit_id or existing_oca.take_profit_order_id
                        existing_oca.stop_price = stop_loss_price or existing_oca.stop_price
                        existing_oca.stop_limit_price = round(stop_loss_price * 0.995, 2) if stop_loss_price else existing_oca.stop_limit_price
                        existing_oca.take_profit_price = take_profit_price or existing_oca.take_profit_price
                        existing_oca.quantity = quantity
                        existing_oca.entry_price = entry_price
                        existing_oca.status = OCAStatus.ACTIVE
                        existing_oca.trade_id = trade_id or existing_oca.trade_id
                        existing_oca.trailing_stop_id = trailing_stop_id or existing_oca.trailing_stop_id
                        db.session.commit()
                        oca_group_record = existing_oca
                        logger.info(f"OCAGroup record updated (already existed): group={oca_group_id_str}")
                    else:
                        oca_group_record = OCAGroup(
                            oca_group_id=oca_group_id_str,
                            symbol=symbol,
                            account=self.client_config.account,
                            account_type='paper' if is_paper else 'real',
                            side=side,
                            quantity=quantity,
                            entry_price=entry_price,
                            stop_order_id=stop_loss_id,
                            take_profit_order_id=take_profit_id,
                            stop_price=stop_loss_price,
                            stop_limit_price=round(stop_loss_price * 0.995, 2) if stop_loss_price else None,
                            take_profit_price=take_profit_price,
                            time_in_force=tif,
                            outside_rth_stop=outside_rth_stop,
                            outside_rth_tp=outside_rth_tp,
                            status=OCAStatus.ACTIVE,
                            trade_id=trade_id,
                            trailing_stop_id=trailing_stop_id
                        )
                        db.session.add(oca_group_record)
                        db.session.commit()
                        logger.info(f"OCAGroup record created: ID={oca_group_record.id}, group={oca_group_id_str}")
                    
                    from order_tracker_service import register_order
                    if stop_loss_id:
                        register_order(
                            tiger_order_id=stop_loss_id,
                            symbol=symbol,
                            account_type='paper' if is_paper else 'real',
                            role='stop_loss',
                            side=action,
                            quantity=quantity,
                            order_type='STP_LMT',
                            limit_price=round(stop_loss_price * 0.995, 2) if stop_loss_price else None,
                            stop_price=round(stop_loss_price, 2) if stop_loss_price else None,
                            trade_id=trade_id,
                            trailing_stop_id=trailing_stop_id
                        )
                    if take_profit_id:
                        register_order(
                            tiger_order_id=take_profit_id,
                            symbol=symbol,
                            account_type='paper' if is_paper else 'real',
                            role='take_profit',
                            side=action,
                            quantity=quantity,
                            order_type='LMT',
                            limit_price=round(take_profit_price, 2) if take_profit_price else None,
                            trade_id=trade_id,
                            trailing_stop_id=trailing_stop_id
                        )
                except Exception as db_e:
                    db.session.rollback()
                    logger.error(f"Failed to save OCAGroup record: {db_e}")
            
            logger.info(f"OCA group '{oca_group_id_str}' creation completed with {len(orders_created)} orders")
            
            if orders_created:
                main_order_id = stop_loss_id or take_profit_id
                warnings = []
                if stop_loss_price and not stop_loss_id:
                    warnings.append("Stop loss order could not be created")
                if take_profit_price and not take_profit_id:
                    warnings.append("Take profit order could not be created")
                
                if is_paper and stop_loss_id:
                    warnings.append("Paper account: stop loss only active during RTH. Soft protection active for extended hours.")
                
                result = {
                    'success': True,
                    'order_id': main_order_id,
                    'stop_loss_order_id': stop_loss_id,
                    'take_profit_order_id': take_profit_id,
                    'oca_group': oca_group_id_str,
                    'oca_group_record_id': oca_group_record.id if oca_group_record else None,
                    'time_in_force': tif,
                    'outside_rth_stop': outside_rth_stop,
                    'outside_rth_tp': outside_rth_tp,
                    'message': f'OCA protection created for {quantity} shares of {symbol}: {", ".join(orders_created)}'
                }
                
                if warnings:
                    result['warnings'] = warnings
                    
                return result
            else:
                return {
                    'success': False,
                    'error': 'Failed to create any protection orders'
                }
                
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(f"Error creating position protection orders: {error_type}: {error_msg}")
            
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            
            return {
                'success': False,
                'error': f'{error_type}: {error_msg}'
            }


class TigerPaperClient(TigerClient):
    """
    Tiger Securities Paper Trading Client (模拟账户客户端)
    Uses the same tiger_id but connects to paper trading account
    """
    
    def __init__(self):
        self.client = None
        self.client_config = None
        self.is_paper = True  # Paper trading account
        self._account_type = 'paper'
        self._initialize_paper_client()
    
    def _initialize_paper_client(self):
        """Initialize Tiger OpenAPI client for paper trading account"""
        try:
            # Create client config with sandbox_debug=False to use production server
            self.client_config = TigerOpenClientConfig(sandbox_debug=False)
            # Set is_paper flag to indicate paper trading account
            self.client_config._is_paper = True
            
            # Read configuration from tiger_openapi_config.properties
            config_path = './tiger_openapi_config.properties'
            if os.path.exists(config_path):
                config_data = {}
                with open(config_path, 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            config_data[key] = value
                
                # Set tiger_id from config file (same as real account)
                self.client_config.tiger_id = config_data.get('tiger_id')
                
                # Use paper trading account from database config or default
                paper_account = get_config('TIGER_PAPER_ACCOUNT', '21994480083284213')
                self.client_config.account = paper_account
                
                # Set private key - use pk8 format (same key for both accounts)
                private_key_pk8 = config_data.get('private_key_pk8')
                if private_key_pk8:
                    self.client_config.private_key = private_key_pk8
                
                # Set other config
                self.client_config.language = Language.zh_CN
                
                device_id = config_data.get('device_id')
                if device_id:
                    self.client_config._device_id = device_id
                    logger.info(f"📝 Paper Trading using custom device_id: {device_id}")
                
                logger.info(f"📝 Paper Trading Config loaded (Production Server) - Tiger ID: {self.client_config.tiger_id}, Paper Account: {self.client_config.account}")
                
            else:
                logger.error("Config file not found for paper trading")
                return
            
            # Validate required fields
            if not all([self.client_config.tiger_id, self.client_config.private_key, self.client_config.account]):
                logger.error(f"Missing required config for paper trading: tiger_id={bool(self.client_config.tiger_id)}, private_key={bool(self.client_config.private_key)}, account={bool(self.client_config.account)}")
                return
            
            # Determine server URL based on license type from config file
            license_type = config_data.get('license', '')
            if license_type == 'TBUS':
                production_server = "https://openapi.tradeup.com/gateway"
            else:
                production_server = "https://openapi.tigerfintech.com/hkg/gateway"
            self.client_config._server_url = production_server
            
            # Initialize trade client
            self.client = TradeClient(self.client_config)
            
            # Force server URL after client creation
            # SDK stores config in private attribute _TigerOpenClient__config
            # We must override the internal copy to ensure correct server is used
            try:
                self.client._TigerOpenClient__config._server_url = production_server
                actual_url = self.client._TigerOpenClient__config._server_url
                logger.info(f"📝 Paper Trading server URL set to: {actual_url}")
            except Exception as e:
                logger.warning(f"Could not set internal server URL: {e}")
            
            logger.info(f"📝 Tiger Paper Trading client initialized successfully with account: {self.client_config.account}")
                
        except Exception as e:
            logger.error(f"Failed to initialize Tiger Paper Trading client: {str(e)}")
            self.client = None
            self.client_config = None
    
    def is_paper_trading(self):
        """Return True to indicate this is paper trading"""
        return True