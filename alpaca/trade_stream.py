import os
import json
import logging
import threading
import asyncio
import time
from queue import Queue, Empty
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

ALPACA_PAPER_WS_URL = 'wss://paper-api.alpaca.markets/stream'

_stream_instance = None
_stream_lock = threading.Lock()


class AlpacaTradeStream:

    def __init__(self):
        self._api_key = os.environ.get('ALPACA_API_KEY2', '') or os.environ.get('ALPACA_API_KEY', '')
        self._secret_key = os.environ.get('ALPACA_SECRET_KEY2', '') or os.environ.get('ALPACA_SECRET_KEY', '')
        self._ws_url = ALPACA_PAPER_WS_URL

        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False
        self._authenticated = False

        self._event_queue: Queue = Queue(maxsize=1000)

        self._reconnect_delay = 3
        self._max_reconnect_delay = 60
        self._current_reconnect_delay = 3

        self._last_event_time: Optional[datetime] = None
        self._event_count = 0
        self._connect_time: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._reconnect_count = 0

    @property
    def is_connected(self) -> bool:
        return self._connected and self._authenticated

    @property
    def is_running(self) -> bool:
        return self._running

    def get_status(self) -> Dict[str, Any]:
        return {
            'running': self._running,
            'connected': self._connected,
            'authenticated': self._authenticated,
            'event_count': self._event_count,
            'last_event_time': self._last_event_time,
            'connect_time': self._connect_time,
            'last_error': self._last_error,
            'reconnect_count': self._reconnect_count,
            'queue_size': self._event_queue.qsize(),
            'thread_alive': self._thread.is_alive() if self._thread else False,
        }

    def get_pending_events(self, max_events: int = 50) -> list:
        events = []
        for _ in range(max_events):
            try:
                event = self._event_queue.get_nowait()
                events.append(event)
            except Empty:
                break
        return events

    def start(self):
        if self._running:
            logger.info("Alpaca trade stream already running")
            return

        if not self._api_key or not self._secret_key:
            logger.warning("Alpaca API keys not configured, trade stream not started")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name='alpaca-trade-stream'
        )
        self._thread.start()
        logger.info("Alpaca trade stream thread started")

    def stop(self):
        self._running = False
        self._connected = False
        self._authenticated = False
        logger.info("Alpaca trade stream stopping...")

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while self._running:
            try:
                loop.run_until_complete(self._connect_and_listen())
            except Exception as e:
                self._last_error = str(e)
                logger.error(f"Trade stream connection error: {e}")

            self._connected = False
            self._authenticated = False

            if not self._running:
                break

            delay = self._current_reconnect_delay
            logger.info(f"Trade stream reconnecting in {delay}s...")
            time.sleep(delay)
            self._current_reconnect_delay = min(
                self._current_reconnect_delay * 2,
                self._max_reconnect_delay
            )
            self._reconnect_count += 1

        loop.close()
        logger.info("Alpaca trade stream loop ended")

    async def _connect_and_listen(self):
        import websockets

        logger.info(f"Connecting to Alpaca trade stream: {self._ws_url}")

        async with websockets.connect(self._ws_url) as ws:
            self._connected = True
            self._connect_time = datetime.utcnow()
            logger.info("Alpaca trade stream connected")

            auth_msg = json.dumps({
                "action": "auth",
                "key": self._api_key,
                "secret": self._secret_key,
            })
            await ws.send(auth_msg)

            auth_response = await ws.recv()
            auth_data = self._decode_message(auth_response)

            if not auth_data:
                raise Exception("Failed to decode auth response")

            if isinstance(auth_data, dict):
                status = auth_data.get('data', {}).get('status', '')
                if status != 'authorized':
                    raise Exception(f"Authentication failed: {auth_data}")
            elif isinstance(auth_data, list):
                auth_ok = False
                for item in auth_data:
                    if isinstance(item, dict) and item.get('data', {}).get('status') == 'authorized':
                        auth_ok = True
                        break
                if not auth_ok:
                    raise Exception(f"Authentication failed: {auth_data}")

            self._authenticated = True
            self._current_reconnect_delay = self._reconnect_delay
            logger.info("Alpaca trade stream authenticated")

            listen_msg = json.dumps({
                "action": "listen",
                "data": {
                    "streams": ["trade_updates"]
                }
            })
            await ws.send(listen_msg)

            listen_response = await ws.recv()
            listen_data = self._decode_message(listen_response)
            logger.info(f"Trade stream subscription response: {listen_data}")

            while self._running:
                try:
                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    message = self._decode_message(raw_msg)

                    if message:
                        self._handle_raw_message(message)
                except asyncio.TimeoutError:
                    try:
                        pong = await ws.ping()
                        await asyncio.wait_for(pong, timeout=10)
                    except Exception:
                        logger.warning("Trade stream ping failed, reconnecting")
                        break
                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning(f"Trade stream connection closed: {e}")
                    break

    def _decode_message(self, raw) -> Optional[Any]:
        try:
            if isinstance(raw, bytes):
                text = raw.decode('utf-8')
            else:
                text = raw

            return json.loads(text)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to decode trade stream message: {e}")
            return None

    def _handle_raw_message(self, message):
        if isinstance(message, list):
            for item in message:
                self._process_single_message(item)
        elif isinstance(message, dict):
            self._process_single_message(message)

    def _process_single_message(self, msg: dict):
        stream = msg.get('stream', '')

        if stream == 'trade_updates':
            data = msg.get('data', {})
            event_type = data.get('event', '')
            order_data = data.get('order', {})

            trade_event = {
                'event': event_type,
                'order_id': order_data.get('id', ''),
                'symbol': order_data.get('symbol', ''),
                'status': order_data.get('status', ''),
                'side': order_data.get('side', ''),
                'qty': order_data.get('qty', ''),
                'filled_qty': order_data.get('filled_qty', ''),
                'filled_avg_price': order_data.get('filled_avg_price', ''),
                'order_type': order_data.get('type', ''),
                'order_class': order_data.get('order_class', ''),
                'replaced_by': order_data.get('replaced_by'),
                'replaces': order_data.get('replaces'),
                'client_order_id': order_data.get('client_order_id', ''),
                'legs': order_data.get('legs'),
                'timestamp': data.get('timestamp', ''),
                'price': data.get('price', ''),
                'position_qty': data.get('position_qty', ''),
                'raw_order': order_data,
                'received_at': datetime.utcnow().isoformat(),
            }

            try:
                self._event_queue.put_nowait(trade_event)
                self._event_count += 1
                self._last_event_time = datetime.utcnow()

                logger.info(
                    f"WS trade_update: {event_type} | {trade_event['symbol']} | "
                    f"order={trade_event['order_id'][:8]}... | "
                    f"status={trade_event['status']} | "
                    f"filled={trade_event['filled_qty']}@{trade_event['filled_avg_price']}"
                )
            except Exception:
                logger.warning("Trade event queue full, dropping oldest event")
                try:
                    self._event_queue.get_nowait()
                    self._event_queue.put_nowait(trade_event)
                    self._event_count += 1
                    self._last_event_time = datetime.utcnow()
                except Exception:
                    pass

        elif stream == 'authorization':
            logger.debug(f"Auth stream message: {msg}")
        elif stream == 'listening':
            logger.debug(f"Listening stream message: {msg}")
        else:
            logger.debug(f"Unknown stream message: {msg}")


def get_trade_stream() -> AlpacaTradeStream:
    global _stream_instance
    with _stream_lock:
        if _stream_instance is None:
            _stream_instance = AlpacaTradeStream()
        return _stream_instance


def start_trade_stream():
    stream = get_trade_stream()
    stream.start()
    return stream


def stop_trade_stream():
    global _stream_instance
    with _stream_lock:
        if _stream_instance:
            _stream_instance.stop()


def get_trade_stream_status() -> Dict[str, Any]:
    stream = get_trade_stream()
    return stream.get_status()
