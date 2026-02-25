"""
Tiger Securities WebSocket Push Client
Handles real-time subscriptions for quotes, orders, and positions
"""
import os
import logging
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Callable
from collections import defaultdict

from tigeropen.tiger_open_config import TigerOpenClientConfig
from tigeropen.push.push_client import PushClient
from tigeropen.common.consts import Language
from tigeropen.common.util.signature_utils import read_private_key

logger = logging.getLogger(__name__)


class TigerPushManager:
    """
    Manages Tiger WebSocket push subscriptions for:
    - Real-time quotes (including pre/post market)
    - Order status changes
    - Position changes
    """
    
    _instance = None
    _instance_lock = threading.Lock()
    
    def __new__(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.push_client: Optional[PushClient] = None
        self.client_config: Optional[TigerOpenClientConfig] = None
        
        self._state_lock = threading.RLock()
        self._is_connected = False
        self._subscribed_symbols: List[str] = []
        
        self._quote_cache: Dict[str, Dict] = {}
        self._quote_cache_lock = threading.Lock()
        
        self._quote_callbacks: List[Callable] = []
        self._order_callbacks: List[Callable] = []
        self._position_callbacks: List[Callable] = []
        
        self._reconnect_thread: Optional[threading.Thread] = None
        self._should_reconnect = True
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 20
        
        self._api_call_count = 0
        self._api_call_window_start = time.time()
        self._api_call_lock = threading.Lock()
        
        self._symbol_api_fallback_count: Dict[str, int] = {}
        self._symbol_fallback_lock = threading.Lock()
        
        self._initialized = True
        logger.info("TigerPushManager initialized")
    
    @property
    def is_connected(self) -> bool:
        with self._state_lock:
            return self._is_connected
    
    @is_connected.setter
    def is_connected(self, value: bool):
        with self._state_lock:
            self._is_connected = value
    
    @property
    def subscribed_symbols(self) -> List[str]:
        with self._state_lock:
            return self._subscribed_symbols.copy()
    
    def _add_subscribed_symbol(self, symbol: str):
        with self._state_lock:
            if symbol not in self._subscribed_symbols:
                self._subscribed_symbols.append(symbol)
    
    def _remove_subscribed_symbol(self, symbol: str):
        with self._state_lock:
            if symbol in self._subscribed_symbols:
                self._subscribed_symbols.remove(symbol)
    
    def _load_config(self) -> bool:
        """Load Tiger API configuration"""
        try:
            self.client_config = TigerOpenClientConfig(sandbox_debug=False)
            
            config_path = './tiger_openapi_config.properties'
            if not os.path.exists(config_path):
                logger.error("Config file not found for push client")
                return False
            
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
            
            if not all([self.client_config.tiger_id, self.client_config.private_key]):
                logger.error("Missing required config for push client")
                return False
            
            license_type = config_data.get('license', '')
            if license_type == 'TBUS':
                from tigeropen.common.consts import License
                self.client_config.license = License.TBUS
                us_server = "https://openapi.tradeup.com/gateway"
                self.client_config.server_url = us_server
                self.client_config.quote_server_url = us_server
                self.client_config.socket_host_port = ('ssl', 'openapi.tradeup.com', 9983)
                logger.info(f"Push client: TBUS license detected, server={us_server}, ws=openapi.tradeup.com:9983")
            
            logger.info(f"Push Config loaded - Tiger ID: {self.client_config.tiger_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load push client config: {str(e)}")
            return False
    
    def connect(self) -> bool:
        """Establish WebSocket connection to Tiger"""
        try:
            if not self.client_config and not self._load_config():
                return False
            
            protocol, host, port = self.client_config.socket_host_port
            use_ssl = (protocol == 'ssl')
            
            self.push_client = PushClient(host, port, use_ssl=use_ssl)
            
            self.push_client.quote_changed = self._on_quote_changed
            self.push_client.order_changed = self._on_order_changed
            self.push_client.position_changed = self._on_position_changed
            self.push_client.asset_changed = self._on_asset_changed
            self.push_client.subscribe_callback = self._on_subscribe_callback
            self.push_client.disconnect_callback = self._on_disconnect
            self.push_client.error_callback = self._on_error
            
            self.push_client.connect(
                self.client_config.tiger_id,
                self.client_config.private_key
            )
            
            self.is_connected = True
            self._reconnect_attempts = 0
            logger.info("✅ Tiger Push Client connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect Tiger Push Client: {str(e)}")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """Disconnect from Tiger WebSocket"""
        try:
            self._should_reconnect = False
            if self.push_client:
                if self.subscribed_symbols:
                    self.push_client.unsubscribe_quote(self.subscribed_symbols)
                self.push_client.disconnect()
            self.is_connected = False
            logger.info("Tiger Push Client disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting push client: {str(e)}")
    
    def subscribe_quotes(self, symbols: List[str]) -> bool:
        """Subscribe to real-time quotes for symbols (thread-safe)"""
        if not self.is_connected or not self.push_client:
            logger.warning("Push client not connected, cannot subscribe quotes")
            return False
        
        try:
            current_symbols = self.subscribed_symbols
            new_symbols = [s for s in symbols if s not in current_symbols]
            if new_symbols:
                self.push_client.subscribe_quote(new_symbols)
                for s in new_symbols:
                    self._add_subscribed_symbol(s)
                logger.info(f"📊 Subscribed to quotes: {new_symbols}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe quotes: {str(e)}")
            return False
    
    def unsubscribe_quotes(self, symbols: List[str]) -> bool:
        """Unsubscribe from real-time quotes (thread-safe)"""
        if not self.is_connected or not self.push_client:
            return False
        
        try:
            current_symbols = self.subscribed_symbols
            existing = [s for s in symbols if s in current_symbols]
            if existing:
                self.push_client.unsubscribe_quote(existing)
                for s in existing:
                    self._remove_subscribed_symbol(s)
                logger.info(f"📊 Unsubscribed from quotes: {existing}")
            return True
        except Exception as e:
            logger.error(f"Failed to unsubscribe quotes: {str(e)}")
            return False
    
    def subscribe_orders(self, account: str = None) -> bool:
        """
        Subscribe to order status changes
        
        Args:
            account: Optional single account ID. If None, subscribes to ALL associated accounts.
        """
        if not self.is_connected or not self.push_client:
            logger.warning("Push client not connected, cannot subscribe orders")
            return False
        
        try:
            # Per Tiger docs: "all associated accounts if not passed"
            result = self.push_client.subscribe_order(account=account)
            if account:
                logger.info(f"📊 Subscribed to order changes for account {account} (request_id={result})")
            else:
                logger.info(f"📊 Subscribed to order changes for ALL accounts (request_id={result})")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe orders: {str(e)}")
            return False
    
    def subscribe_positions(self, account: str = None) -> bool:
        """
        Subscribe to position changes
        
        Args:
            account: Optional single account ID. If None, subscribes to ALL associated accounts.
        """
        if not self.is_connected or not self.push_client:
            logger.warning("Push client not connected, cannot subscribe positions")
            return False
        
        try:
            # Per Tiger docs: "all associated accounts if not passed"
            result = self.push_client.subscribe_position(account=account)
            if account:
                logger.info(f"📊 Subscribed to position changes for account {account} (request_id={result})")
            else:
                logger.info(f"📊 Subscribed to position changes for ALL accounts (request_id={result})")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe positions: {str(e)}")
            return False
    
    def subscribe_assets(self) -> bool:
        """Subscribe to asset changes"""
        if not self.is_connected or not self.push_client:
            return False
        
        try:
            self.push_client.subscribe_asset()
            logger.info("📊 Subscribed to asset changes")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe assets: {str(e)}")
            return False
    
    def get_cached_quote(self, symbol: str) -> Optional[Dict]:
        """Get cached quote for a symbol"""
        with self._quote_cache_lock:
            return self._quote_cache.get(symbol)
    
    def get_cached_price(self, symbol: str) -> Optional[float]:
        """Get cached latest price for a symbol"""
        quote = self.get_cached_quote(symbol)
        if quote:
            return quote.get('latest_price')
        return None
    
    def get_cached_quote_if_fresh(self, symbol: str, max_age_seconds: float = 15.0) -> Optional[Dict]:
        """Get cached quote only if it's fresh (within max_age_seconds).
        Returns None if cache is stale or missing — caller should use API fallback.
        """
        with self._quote_cache_lock:
            quote = self._quote_cache.get(symbol)
            if not quote:
                return None
            ts = quote.get('timestamp')
            if not ts:
                return None
            age = (datetime.utcnow() - ts).total_seconds()
            if age > max_age_seconds:
                logger.debug(f"📊 {symbol} WebSocket cache stale ({age:.1f}s > {max_age_seconds}s)")
                return None
            return quote.copy()
    
    def get_stale_symbols(self, symbols: list, max_age_seconds: float = 15.0) -> list:
        """Return symbols whose WebSocket cache is stale or missing."""
        stale = []
        with self._quote_cache_lock:
            for symbol in symbols:
                quote = self._quote_cache.get(symbol)
                if not quote or not quote.get('timestamp'):
                    stale.append(symbol)
                    continue
                age = (datetime.utcnow() - quote['timestamp']).total_seconds()
                if age > max_age_seconds:
                    stale.append(symbol)
        return stale
    
    def update_cache_from_api(self, symbol: str, price: float, session: str = 'regular'):
        """Write an API-fetched price into the cache so trailing stop engine can use it."""
        with self._quote_cache_lock:
            self._quote_cache[symbol] = {
                'symbol': symbol,
                'latest_price': price,
                'session': session,
                'hour_trading': session != 'regular',
                'timestamp': datetime.utcnow(),
                'source': 'api_fallback'
            }
    
    def clear_cache(self):
        """Clear all cached quotes (used on reconnection)."""
        with self._quote_cache_lock:
            count = len(self._quote_cache)
            self._quote_cache.clear()
            if count:
                logger.info(f"📊 Cleared {count} cached quotes on reconnection")
    
    def record_api_call(self, count: int = 1):
        """Record API calls for budget tracking."""
        with self._api_call_lock:
            now = time.time()
            if now - self._api_call_window_start >= 60:
                self._api_call_count = 0
                self._api_call_window_start = now
            self._api_call_count += count
    
    def get_api_calls_in_window(self) -> int:
        """Get API call count in current 60-second window."""
        with self._api_call_lock:
            now = time.time()
            if now - self._api_call_window_start >= 60:
                self._api_call_count = 0
                self._api_call_window_start = now
            return self._api_call_count
    
    def get_adaptive_cache_max_age(self) -> float:
        """Get adaptive cache max age based on API call budget.
        Normal: 15s. If API calls high: extend to 30s or 60s.
        """
        api_calls = self.get_api_calls_in_window()
        if api_calls >= 100:
            return 60.0
        elif api_calls >= 60:
            return 30.0
        return 15.0
    
    def record_symbol_api_fallback(self, symbol: str):
        """Record that a symbol used API fallback instead of WebSocket."""
        with self._symbol_fallback_lock:
            self._symbol_api_fallback_count[symbol] = self._symbol_api_fallback_count.get(symbol, 0) + 1
    
    def reset_symbol_api_fallback(self, symbol: str):
        """Reset fallback counter when WebSocket data resumes for a symbol."""
        with self._symbol_fallback_lock:
            self._symbol_api_fallback_count.pop(symbol, None)
    
    def get_symbols_needing_resubscribe(self, threshold: int = 36) -> list:
        """Get symbols that have been using API fallback for too long (default: 36 cycles = ~3 min at 5s interval).
        These symbols likely lost their WebSocket subscription.
        """
        result = []
        with self._symbol_fallback_lock:
            for symbol, count in list(self._symbol_api_fallback_count.items()):
                if count >= threshold:
                    result.append(symbol)
                    self._symbol_api_fallback_count[symbol] = 0
        return result[:5]
    
    def register_quote_callback(self, callback: Callable):
        """Register a callback for quote updates"""
        if callback not in self._quote_callbacks:
            self._quote_callbacks.append(callback)
    
    def register_order_callback(self, callback: Callable):
        """Register a callback for order updates"""
        if callback not in self._order_callbacks:
            self._order_callbacks.append(callback)
    
    def register_position_callback(self, callback: Callable):
        """Register a callback for position updates"""
        if callback not in self._position_callbacks:
            self._position_callbacks.append(callback)
    
    def _on_quote_changed(self, quote_data_raw):
        """Handle quote update from Tiger (SDK 3.4.6+ new signature)
        
        SDK 3.4.6 changed the callback signature from (symbol, items, hour_trading)
        to just (quoteData) where quoteData is a protobuf object or dict.
        """
        try:
            if hasattr(quote_data_raw, 'symbol'):
                symbol = quote_data_raw.symbol
                hour_trading = getattr(quote_data_raw, 'hourTrading', False) or getattr(quote_data_raw, 'hour_trading', False)
                
                quote_data = {
                    'symbol': symbol,
                    'hour_trading': hour_trading,
                    'session': 'extended' if hour_trading else 'regular',
                    'timestamp': datetime.utcnow()
                }
                
                for attr in ['latestPrice', 'latest_price', 'volume', 'open', 'high', 'low', 'close', 
                             'preClose', 'pre_close', 'askPrice', 'ask_price', 'bidPrice', 'bid_price',
                             'askSize', 'ask_size', 'bidSize', 'bid_size']:
                    val = getattr(quote_data_raw, attr, None)
                    if val is not None:
                        key = attr.replace('Price', '_price').replace('Size', '_size')
                        key = key[0].lower() + key[1:]
                        if key == 'latestPrice' or key == 'latest_price':
                            quote_data['latest_price'] = val
                        elif key == 'preClose' or key == 'pre_close':
                            quote_data['pre_close'] = val
                        else:
                            quote_data[key] = val
                
            elif isinstance(quote_data_raw, dict):
                symbol = quote_data_raw.get('symbol', '')
                hour_trading = quote_data_raw.get('hourTrading', False) or quote_data_raw.get('hour_trading', False)
                
                quote_data = {
                    'symbol': symbol,
                    'hour_trading': hour_trading,
                    'session': 'extended' if hour_trading else 'regular',
                    'timestamp': datetime.utcnow(),
                    'latest_price': quote_data_raw.get('latestPrice') or quote_data_raw.get('latest_price'),
                    'volume': quote_data_raw.get('volume'),
                    'open': quote_data_raw.get('open'),
                    'high': quote_data_raw.get('high'),
                    'low': quote_data_raw.get('low'),
                    'close': quote_data_raw.get('close'),
                    'pre_close': quote_data_raw.get('preClose') or quote_data_raw.get('pre_close'),
                    'ask_price': quote_data_raw.get('askPrice') or quote_data_raw.get('ask_price'),
                    'bid_price': quote_data_raw.get('bidPrice') or quote_data_raw.get('bid_price'),
                }
            else:
                logger.warning(f"Unknown quote data format: {type(quote_data_raw)}")
                return
            
            if not symbol:
                logger.warning("Quote update without symbol, skipping")
                return
            
            logger.debug(f"📊 Quote update: {symbol} price={quote_data.get('latest_price')} session={quote_data.get('session')}")
            
            with self._quote_cache_lock:
                self._quote_cache[symbol] = quote_data
            
            self.reset_symbol_api_fallback(symbol)
            
            for callback in self._quote_callbacks:
                try:
                    callback(symbol, quote_data)
                except Exception as cb_err:
                    logger.error(f"Quote callback error: {str(cb_err)}")
                    
        except Exception as e:
            logger.error(f"Error processing quote update: {str(e)}")
    
    def _on_order_changed(self, order_data):
        """Handle order status change from Tiger"""
        try:
            logger.info(f"📋 Order update received: {order_data}")
            
            for callback in self._order_callbacks:
                try:
                    callback(order_data)
                except Exception as cb_err:
                    logger.error(f"Order callback error: {str(cb_err)}")
                    
        except Exception as e:
            logger.error(f"Error processing order update: {str(e)}")
    
    def _on_position_changed(self, position_data):
        """Handle position change from Tiger"""
        try:
            logger.info(f"📋 Position update received: {position_data}")
            
            for callback in self._position_callbacks:
                try:
                    callback(position_data)
                except Exception as cb_err:
                    logger.error(f"Position callback error: {str(cb_err)}")
                    
        except Exception as e:
            logger.error(f"Error processing position update: {str(e)}")
    
    def _on_asset_changed(self, asset_data):
        """Handle asset change from Tiger"""
        logger.info(f"📋 Asset update received: {asset_data}")
    
    def _on_subscribe_callback(self, frame):
        """Handle subscription confirmation"""
        logger.info(f"📊 Subscribe callback: {frame}")
    
    def _on_disconnect(self):
        """Handle disconnect event"""
        logger.warning("⚠️ Tiger Push Client disconnected")
        self.is_connected = False
        
        if self._should_reconnect:
            self._start_reconnect()
    
    def _on_error(self, error):
        """Handle error event"""
        logger.error(f"❌ Tiger Push Client error: {error}")
    
    def _start_reconnect(self):
        """Start reconnection thread"""
        if self._reconnect_thread and self._reconnect_thread.is_alive():
            return
        
        self._reconnect_thread = threading.Thread(
            target=self._reconnect_loop,
            daemon=True,
            name="TigerPushReconnect"
        )
        self._reconnect_thread.start()
    
    def _reconnect_loop(self):
        """Reconnection loop with exponential backoff"""
        while self._should_reconnect and self._reconnect_attempts < self._max_reconnect_attempts:
            self._reconnect_attempts += 1
            wait_time = min(2 ** self._reconnect_attempts, 60)
            
            logger.info(f"🔄 Reconnecting attempt {self._reconnect_attempts}/{self._max_reconnect_attempts} in {wait_time}s...")
            time.sleep(wait_time)
            
            try:
                if self.connect():
                    self.clear_cache()
                    
                    REAL_ACCOUNT = "50904193"
                    PAPER_ACCOUNT = "21994480083284213"
                    self.subscribe_orders()
                    self.subscribe_positions()
                    self.subscribe_orders(account=PAPER_ACCOUNT)
                    self.subscribe_positions(account=PAPER_ACCOUNT)
                    
                    _subscribe_active_trailing_stop_symbols(self)
                    
                    logger.info("✅ Reconnected successfully with full re-subscription")
                    return
            except Exception as e:
                logger.error(f"Reconnect failed: {str(e)}")
        
        logger.error("❌ Max reconnect attempts reached, giving up")


_push_manager: Optional[TigerPushManager] = None


def get_push_manager() -> TigerPushManager:
    """Get the singleton TigerPushManager instance"""
    global _push_manager
    if _push_manager is None:
        _push_manager = TigerPushManager()
    return _push_manager


def initialize_push_client(register_handlers: bool = True) -> bool:
    """
    Initialize and connect the push client
    
    Args:
        register_handlers: Whether to register default event handlers
        
    Returns:
        True if connected successfully
    """
    manager = get_push_manager()
    if manager.connect():
        if register_handlers:
            try:
                from push_event_handlers import handle_order_fill, handle_position_change
                manager.register_order_callback(handle_order_fill)
                manager.register_position_callback(handle_position_change)
                logger.info("📊 Registered WebSocket event handlers")
            except ImportError as e:
                logger.warning(f"Could not import event handlers: {e}")
        
        # Subscribe to both Real and Paper accounts explicitly
        # Tiger may only push to the default connected account, so we subscribe each explicitly
        # Read accounts dynamically from config file and database
        try:
            config_path = './tiger_openapi_config.properties'
            config_data = {}
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    for line in f:
                        if '=' in line and not line.strip().startswith('#'):
                            key, value = line.strip().split('=', 1)
                            config_data[key] = value
            REAL_ACCOUNT = config_data.get('account', '')
            from config import get_config
            PAPER_ACCOUNT = get_config('TIGER_PAPER_ACCOUNT', '')
        except Exception:
            REAL_ACCOUNT = ''
            PAPER_ACCOUNT = ''
        
        # First try subscribing all accounts (no parameter)
        manager.subscribe_orders()
        manager.subscribe_positions()
        
        # Also explicitly subscribe Paper account to ensure it receives pushes
        if PAPER_ACCOUNT:
            manager.subscribe_orders(account=PAPER_ACCOUNT)
            manager.subscribe_positions(account=PAPER_ACCOUNT)
            logger.info(f"📊 Subscribed Paper account {PAPER_ACCOUNT} for push events")
        
        _subscribe_active_trailing_stop_symbols(manager)
        
        return True
    return False


def _subscribe_active_trailing_stop_symbols(manager: TigerPushManager) -> None:
    """Subscribe to quotes for all positions + watchlist symbols (from Tiger API + TrailingStopPosition + Alpaca + Watchlist)"""
    try:
        from models import TrailingStopPosition
        from app import app
        
        all_symbols = set()
        
        with app.app_context():
            # 1. Get symbols from active TrailingStopPosition records
            active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
            for p in active_positions:
                all_symbols.add(p.symbol)
            
            # 2. Get actual positions from Tiger API (real + paper accounts)
            try:
                from tiger_client import get_tiger_quote_client, TigerClient, TigerPaperClient
                
                from push_event_handlers import update_position_cache
                
                try:
                    real_client = TigerClient()
                    real_positions = real_client.get_positions()
                    if real_positions.get('success') and real_positions.get('positions'):
                        for pos in real_positions['positions']:
                            if pos.get('quantity', 0) != 0:
                                all_symbols.add(pos['symbol'])
                                update_position_cache(pos['symbol'], 'real', pos['quantity'], pos.get('average_cost', 0))
                        logger.info(f"📊 Found {len(real_positions['positions'])} real account positions, cached for fallback")
                except Exception as e:
                    logger.warning(f"Could not get real account positions: {e}")
                
                try:
                    paper_client = TigerPaperClient()
                    paper_positions = paper_client.get_positions()
                    if paper_positions.get('success') and paper_positions.get('positions'):
                        for pos in paper_positions['positions']:
                            if pos.get('quantity', 0) != 0:
                                all_symbols.add(pos['symbol'])
                                update_position_cache(pos['symbol'], 'paper', pos['quantity'], pos.get('average_cost', 0))
                        logger.info(f"📊 Found {len(paper_positions['positions'])} paper account positions, cached for fallback")
                except Exception as e:
                    logger.warning(f"Could not get paper account positions: {e}")
                    
            except ImportError as e:
                logger.warning(f"Could not import Tiger clients for position query: {e}")
            
            # 3. Get symbols from Alpaca active trailing stops
            try:
                from alpaca.models import AlpacaTrailingStopPosition
                alpaca_positions = AlpacaTrailingStopPosition.query.filter_by(is_active=True).all()
                for p in alpaca_positions:
                    all_symbols.add(p.symbol)
                if alpaca_positions:
                    logger.info(f"📊 Found {len(alpaca_positions)} Alpaca trailing stop positions")
            except Exception as e:
                logger.debug(f"Could not get Alpaca trailing stop positions: {e}")
            
            # 4. Get symbols from watchlist
            try:
                from watchlist_service import get_active_watchlist_symbols
                watchlist_symbols = get_active_watchlist_symbols()
                for s in watchlist_symbols:
                    all_symbols.add(s)
                if watchlist_symbols:
                    logger.info(f"📊 Watchlist has {len(watchlist_symbols)} active symbols")
            except Exception as e:
                logger.debug(f"Could not get watchlist symbols: {e}")
            
            # Subscribe to all collected symbols
            if all_symbols:
                symbols_list = list(all_symbols)
                manager.subscribe_quotes(symbols_list)
                logger.info(f"📊 Subscribed to quotes for {len(symbols_list)} symbols (positions + watchlist): {symbols_list}")
            else:
                logger.info("📊 No symbols found to subscribe")
                
    except Exception as e:
        logger.error(f"Failed to subscribe position symbols: {e}")


def get_websocket_price(symbol: str) -> Optional[float]:
    """Get cached price from WebSocket, returns None if not available"""
    manager = get_push_manager()
    return manager.get_cached_price(symbol)


def subscribe_trailing_stop_symbols(symbols: List[str]) -> bool:
    """Subscribe to quotes for trailing stop monitoring"""
    manager = get_push_manager()
    return manager.subscribe_quotes(symbols)
