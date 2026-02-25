"""
EODHD WebSocket Real-Time Market Data Client for TBUS.

Provides real-time US stock prices via EODHD WebSocket API as an independent
market data source for TBUS accounts (Tiger US Standard doesn't provide
adequate real-time quotes).

Features:
- WebSocket connection management with auto-reconnect (exponential backoff)
- Price cache with freshness tracking
- Subscribe/unsubscribe symbols dynamically
- Supports pre-market and post-market hours (4am-8pm ET)
- Thread-safe price access for trailing stop engine
- Unified get_realtime_price() interface matching Tiger WebSocket cache pattern

EODHD API: wss://ws.eodhistoricaldata.com/ws/us?api_token=KEY
- Trade stream: last price, volume, market status
- <50ms latency, up to 50 symbols per connection
"""

import os
import json
import logging
import threading
import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)

EODHD_WS_URL_TEMPLATE = 'wss://ws.eodhistoricaldata.com/ws/us?api_token={api_token}'

_eodhd_instance = None
_eodhd_lock = threading.Lock()


class EODHDQuoteWebSocket:
    """EODHD WebSocket client for real-time US stock trade data.
    
    Follows the same singleton + thread pattern as AlpacaTradeStream and TigerPushManager.
    Provides price cache with freshness tracking compatible with trailing stop engine.
    """

    def __init__(self):
        self._api_key = os.environ.get('EODHD_API_KEY', '')
        self._ws_url = EODHD_WS_URL_TEMPLATE.format(api_token=self._api_key) if self._api_key else ''

        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False

        self._price_cache: Dict[str, Dict] = {}
        self._price_cache_lock = threading.Lock()

        self._subscribed_symbols: List[str] = []
        self._symbols_lock = threading.Lock()

        self._pending_subscribes: List[str] = []
        self._pending_unsubscribes: List[str] = []
        self._pending_lock = threading.Lock()

        self._reconnect_delay = 3
        self._max_reconnect_delay = 60
        self._current_reconnect_delay = 3

        self._last_message_time: Optional[datetime] = None
        self._message_count = 0
        self._connect_time: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._reconnect_count = 0

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def is_running(self) -> bool:
        return self._running

    def get_status(self) -> Dict[str, Any]:
        with self._symbols_lock:
            subscribed = list(self._subscribed_symbols)
        return {
            'running': self._running,
            'connected': self._connected,
            'message_count': self._message_count,
            'last_message_time': self._last_message_time,
            'connect_time': self._connect_time,
            'last_error': self._last_error,
            'reconnect_count': self._reconnect_count,
            'subscribed_symbols': subscribed,
            'subscribed_count': len(subscribed),
            'cache_size': len(self._price_cache),
            'thread_alive': self._thread.is_alive() if self._thread else False,
        }

    def start(self):
        if self._running:
            logger.info("EODHD quote WebSocket already running")
            return

        if not self._api_key:
            logger.error("EODHD_API_KEY not set, cannot start quote WebSocket")
            return

        self._running = True
        self._thread = threading.Thread(target=self._run_loop, name='eodhd-quote-ws', daemon=True)
        self._thread.start()
        logger.info("EODHD quote WebSocket thread started")

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self._connected = False
        logger.info("EODHD quote WebSocket stopped")

    MAX_SYMBOLS = 50

    def subscribe(self, symbols: List[str]):
        if not symbols:
            return
        clean_symbols = [s.upper().strip() for s in symbols if s.strip()]
        if not clean_symbols:
            return
        with self._symbols_lock:
            current_count = len(self._subscribed_symbols)
            new_symbols = [s for s in clean_symbols if s not in self._subscribed_symbols]
            if current_count + len(new_symbols) > self.MAX_SYMBOLS:
                allowed = self.MAX_SYMBOLS - current_count
                if allowed <= 0:
                    logger.warning(f"EODHD max {self.MAX_SYMBOLS} symbols reached, cannot subscribe {new_symbols}")
                    return
                logger.warning(f"EODHD truncating subscribe to {allowed}/{len(new_symbols)} symbols (limit {self.MAX_SYMBOLS})")
                new_symbols = new_symbols[:allowed]
            for s in new_symbols:
                self._subscribed_symbols.append(s)
        with self._pending_lock:
            for s in new_symbols:
                if s not in self._pending_subscribes:
                    self._pending_subscribes.append(s)
        logger.info(f"EODHD subscribe queued: {new_symbols} (total: {len(self._subscribed_symbols)})")

    def unsubscribe(self, symbols: List[str]):
        if not symbols:
            return
        clean_symbols = [s.upper().strip() for s in symbols if s.strip()]
        with self._pending_lock:
            for s in clean_symbols:
                if s not in self._pending_unsubscribes:
                    self._pending_unsubscribes.append(s)
        with self._symbols_lock:
            self._subscribed_symbols = [s for s in self._subscribed_symbols if s not in clean_symbols]
        with self._price_cache_lock:
            for s in clean_symbols:
                self._price_cache.pop(s, None)
        logger.info(f"EODHD unsubscribe queued: {clean_symbols}")

    def get_cached_price(self, symbol: str) -> Optional[float]:
        with self._price_cache_lock:
            entry = self._price_cache.get(symbol.upper())
            if entry:
                return entry.get('price')
        return None

    def get_cached_quote_if_fresh(self, symbol: str, max_age_seconds: float = 15.0) -> Optional[Dict]:
        with self._price_cache_lock:
            entry = self._price_cache.get(symbol.upper())
            if not entry:
                return None
            ts = entry.get('timestamp')
            if not ts:
                return None
            age = (datetime.utcnow() - ts).total_seconds()
            if age > max_age_seconds:
                logger.debug(f"EODHD {symbol} cache stale ({age:.1f}s > {max_age_seconds}s)")
                return None
            return entry.copy()

    def get_realtime_price(self, symbol: str, max_age_seconds: float = 30.0) -> Optional[Dict]:
        """Get real-time price from EODHD cache.
        
        Returns dict compatible with trailing stop engine:
        {'price': float, 'session': str, 'source': str}
        
        Returns None if no cached price or cache too stale.
        """
        entry = self.get_cached_quote_if_fresh(symbol.upper(), max_age_seconds)
        if entry and entry.get('price'):
            return {
                'price': entry['price'],
                'session': entry.get('market_status', 'unknown'),
                'source': 'eodhd_ws',
                'volume': entry.get('volume'),
                'timestamp': entry.get('timestamp'),
            }
        return None

    def update_cache_from_external(self, symbol: str, price: float, session: str = 'regular'):
        """Allow external sources (e.g., API fallback) to update the price cache."""
        with self._price_cache_lock:
            self._price_cache[symbol.upper()] = {
                'symbol': symbol.upper(),
                'price': price,
                'volume': 0,
                'market_status': session,
                'timestamp': datetime.utcnow(),
                'source': 'external_fallback',
            }

    def clear_cache(self):
        with self._price_cache_lock:
            count = len(self._price_cache)
            self._price_cache.clear()
        logger.info(f"EODHD price cache cleared ({count} entries)")

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while self._running:
            try:
                loop.run_until_complete(self._connect_and_listen())
            except Exception as e:
                self._last_error = str(e)
                logger.error(f"EODHD WebSocket connection error: {e}")

            self._connected = False

            if not self._running:
                break

            delay = self._current_reconnect_delay
            logger.info(f"EODHD WebSocket reconnecting in {delay}s...")
            time.sleep(delay)
            self._current_reconnect_delay = min(
                self._current_reconnect_delay * 2,
                self._max_reconnect_delay
            )
            self._reconnect_count += 1

        loop.close()
        logger.info("EODHD WebSocket loop ended")

    async def _connect_and_listen(self):
        import websockets

        logger.info(f"Connecting to EODHD WebSocket...")

        async with websockets.connect(self._ws_url) as ws:
            self._connected = True
            self._connect_time = datetime.utcnow()
            self._current_reconnect_delay = self._reconnect_delay
            logger.info("EODHD WebSocket connected")

            with self._symbols_lock:
                initial_symbols = list(self._subscribed_symbols)
            if initial_symbols:
                await self._send_subscribe(ws, initial_symbols)

            with self._pending_lock:
                self._pending_subscribes.clear()
                self._pending_unsubscribes.clear()

            while self._running:
                try:
                    await self._process_pending_commands(ws)

                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    self._handle_message(raw_msg)

                except asyncio.TimeoutError:
                    if not hasattr(self, '_last_ping_time'):
                        self._last_ping_time = time.time()
                    if time.time() - self._last_ping_time > 30:
                        try:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=10)
                            self._last_ping_time = time.time()
                        except Exception:
                            logger.warning("EODHD WebSocket ping failed, reconnecting")
                            break
                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning(f"EODHD WebSocket connection closed: {e}")
                    break

    async def _send_subscribe(self, ws, symbols: List[str]):
        symbols_str = ','.join(symbols)
        msg = json.dumps({"action": "subscribe", "symbols": symbols_str})
        await ws.send(msg)
        logger.info(f"EODHD subscribed to: {symbols_str}")

    async def _send_unsubscribe(self, ws, symbols: List[str]):
        symbols_str = ','.join(symbols)
        msg = json.dumps({"action": "unsubscribe", "symbols": symbols_str})
        await ws.send(msg)
        logger.info(f"EODHD unsubscribed from: {symbols_str}")

    async def _process_pending_commands(self, ws):
        with self._pending_lock:
            to_subscribe = list(self._pending_subscribes)
            to_unsubscribe = list(self._pending_unsubscribes)
            self._pending_subscribes.clear()
            self._pending_unsubscribes.clear()

        if to_unsubscribe:
            await self._send_unsubscribe(ws, to_unsubscribe)
        if to_subscribe:
            await self._send_subscribe(ws, to_subscribe)

    def _handle_message(self, raw_msg):
        try:
            if isinstance(raw_msg, bytes):
                text = raw_msg.decode('utf-8')
            else:
                text = raw_msg

            data = json.loads(text)

            if not isinstance(data, dict):
                return

            status_msg = data.get('status_code')
            if status_msg is not None:
                logger.info(f"EODHD status message: {data}")
                return

            symbol = data.get('s')
            if not symbol:
                return

            price = data.get('p')
            if price is None:
                return

            volume = data.get('v', 0)
            market_status = data.get('ms', 'unknown')
            trade_timestamp = data.get('t', 0)

            with self._price_cache_lock:
                self._price_cache[symbol] = {
                    'symbol': symbol,
                    'price': float(price),
                    'volume': volume,
                    'market_status': market_status,
                    'trade_timestamp': trade_timestamp,
                    'timestamp': datetime.utcnow(),
                    'source': 'eodhd_ws',
                }

            self._message_count += 1
            self._last_message_time = datetime.utcnow()

            if self._message_count <= 5 or self._message_count % 1000 == 0:
                logger.info(f"EODHD trade: {symbol} ${price} vol={volume} ms={market_status} (total: {self._message_count})")

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"EODHD message decode error: {e}")
        except Exception as e:
            logger.error(f"EODHD message handling error: {e}")


def get_eodhd_quote_manager() -> EODHDQuoteWebSocket:
    """Get or create the singleton EODHD quote WebSocket manager."""
    global _eodhd_instance
    with _eodhd_lock:
        if _eodhd_instance is None:
            _eodhd_instance = EODHDQuoteWebSocket()
        return _eodhd_instance


def get_eodhd_price(symbol: str) -> Optional[float]:
    """Convenience function: get cached EODHD price for a symbol."""
    manager = get_eodhd_quote_manager()
    return manager.get_cached_price(symbol)


def get_eodhd_realtime_price(symbol: str, max_age_seconds: float = 30.0) -> Optional[Dict]:
    """Convenience function: get real-time price dict from EODHD.
    
    Returns dict with 'price', 'session', 'source' keys (compatible with
    trailing stop engine's get_realtime_price_with_websocket_fallback return format).
    """
    manager = get_eodhd_quote_manager()
    return manager.get_realtime_price(symbol, max_age_seconds)
