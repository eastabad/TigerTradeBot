import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta
from queue import Queue, Empty


IMPORTANT_SOURCES = {
    'trailing_stop_engine', 'trailing_stop_scheduler',
    'push_event_handlers', 'tiger_push_client',
    'oca_service', 'tiger_client',
    'routes', 'signal_parser',
    'holdings_sync', 'position_service',
    'reconciliation_service', 'order_tracker_service',
    'discord_notifier', 'position_backfill',
}

KEYWORD_CATEGORIES = {
    'order': [r'订单', r'order', r'OCA', r'oca_group', r'cancel', r'取消', r'fill', r'成交'],
    'trailing_stop': [r'止损', r'止盈', r'trailing', r'stop.?loss', r'take.?profit', r'tier', r'progressive'],
    'position': [r'仓位', r'position', r'Position change', r'holding', r'entry_price', r'quantity'],
    'signal': [r'signal', r'webhook', r'信号', r'TradingView'],
    'sync': [r'sync', r'同步', r'Holdings sync', r'reconcil'],
    'error': [r'fail', r'error', r'exception', r'失败', r'异常'],
    'websocket': [r'WebSocket', r'push.*client', r'Subscribe', r'disconnect', r'reconnect'],
}

SYMBOL_PATTERN = re.compile(r'\b([A-Z]{2,5})\b')

NOISE_PATTERNS = [
    r'heart-?beat',
    r'sending frame',
    r'sending a heartbeat',
    r'Outside market hours, skipping check',
    r'Position change:.*qty=.*value=.*realized_pnl=\$0$',
    r'Asset update received',
    r'Position update received.*positionQty',
    r'DEBUG:root:sending',
    r'Handling signal: winch',
    r'heartbeats calculated',
    r'keepalive:',
    r'Raw MAC found',
    r'method cache',
]
NOISE_COMPILED = [re.compile(p, re.IGNORECASE) for p in NOISE_PATTERNS]

COMMON_SYMBOLS = {'AMDG', 'NVDA', 'AAPL', 'MSFT', 'GOOG', 'AMD', 'TSM', 'AVGO',
                   'COIN', 'SOFI', 'QBTS', 'CLSK', 'MSTU', 'ORCX', 'QCML',
                   'SOXL', 'BABA', 'KLAC', 'LSCC', 'IBKR', 'QCOM', 'STX',
                   'CRCL', 'FICO', 'MSFU', 'MUU', 'NNE', 'KGC', 'TRMB', 'PL',
                   'GE', 'IR', 'MU'}


def _is_noise(message):
    for pattern in NOISE_COMPILED:
        if pattern.search(message):
            return True
    return False


def _detect_category(message):
    for category, patterns in KEYWORD_CATEGORIES.items():
        for p in patterns:
            if re.search(p, message, re.IGNORECASE):
                return category
    return 'general'


def _extract_symbol(message):
    matches = SYMBOL_PATTERN.findall(message)
    for m in matches:
        if m in COMMON_SYMBOLS:
            return m
    return None


def _extract_account_type(message):
    msg_upper = message.upper()
    if 'PAPER' in msg_upper or '[PAPER]' in msg_upper:
        return 'paper'
    if 'REAL' in msg_upper:
        return 'real'
    return None


class DatabaseLogHandler(logging.Handler):
    def __init__(self, flush_interval=15):
        super().__init__()
        self._queue = Queue(maxsize=1000)
        self._db_url = os.environ.get("DATABASE_URL")
        self._engine = None
        self._running = True
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._writer_thread.start()

    def _get_engine(self):
        if self._engine is None and self._db_url:
            from sqlalchemy import create_engine
            self._engine = create_engine(self._db_url, pool_size=1, max_overflow=0, pool_recycle=300)
        return self._engine

    def emit(self, record):
        try:
            source = record.name
            if source not in IMPORTANT_SOURCES and source != 'root':
                return

            level = record.levelname
            if level == 'DEBUG':
                return

            message = record.getMessage()

            if _is_noise(message):
                return

            if level == 'INFO':
                has_emoji = any(c in message for c in '📊📋🔔⚠️❌✅🛑💰🔄')
                category = _detect_category(message)
                if not has_emoji and category == 'general':
                    return

            category = _detect_category(message)
            symbol = _extract_symbol(message)
            account_type = _extract_account_type(message)

            log_entry = {
                'timestamp': datetime.utcnow(),
                'level': level,
                'source': source[:50] if source else None,
                'category': category[:30] if category else None,
                'message': message[:2000],
                'symbol': symbol,
                'account_type': account_type,
            }

            try:
                self._queue.put_nowait(log_entry)
            except Exception:
                pass

        except Exception:
            pass

    def _writer_loop(self):
        time.sleep(5)
        while self._running:
            try:
                entries = []
                while len(entries) < 50:
                    try:
                        entry = self._queue.get(timeout=15)
                        entries.append(entry)
                    except Empty:
                        break

                if entries:
                    self._write_to_db(entries)
            except Exception:
                time.sleep(5)

    def _write_to_db(self, entries):
        engine = self._get_engine()
        if not engine:
            return

        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                for entry in entries:
                    conn.execute(
                        text("""INSERT INTO system_log (timestamp, level, source, category, message, symbol, account_type)
                               VALUES (:timestamp, :level, :source, :category, :message, :symbol, :account_type)"""),
                        entry
                    )
                conn.commit()
        except Exception:
            pass


def cleanup_old_logs(days=7):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(db_url, pool_size=1, max_overflow=0)
        cutoff = datetime.utcnow() - timedelta(days=days)
        with engine.connect() as conn:
            result = conn.execute(text("DELETE FROM system_log WHERE timestamp < :cutoff"), {'cutoff': cutoff})
            conn.commit()
            if result.rowcount > 0:
                logging.getLogger(__name__).info(f"📊 Cleaned up {result.rowcount} old system logs (>{days} days)")
        engine.dispose()
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to cleanup old logs: {e}")


def setup_db_logging(app):
    handler = DatabaseLogHandler()
    handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    def _cleanup_loop():
        time.sleep(3600)
        while True:
            cleanup_old_logs()
            time.sleep(86400)

    t = threading.Thread(target=_cleanup_loop, daemon=True)
    t.start()

    return handler
