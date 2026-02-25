import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from app import db
from models import WatchlistSymbol

logger = logging.getLogger(__name__)


def upsert_watchlist_symbol(symbol: str, source: str = 'manual', 
                            signal_time: bool = False, position_time: bool = False,
                            notes: str = None) -> WatchlistSymbol:
    symbol = symbol.upper().strip()
    existing = WatchlistSymbol.query.filter_by(symbol=symbol).first()
    now = datetime.utcnow()
    
    if existing:
        existing.is_active = True
        if signal_time:
            existing.last_signal_time = now
        if position_time:
            existing.last_position_time = now
        if source == 'manual' or existing.source != 'manual':
            existing.source = source
        if notes:
            existing.notes = notes
        db.session.commit()
        return existing
    else:
        entry = WatchlistSymbol(
            symbol=symbol,
            added_at=now,
            last_signal_time=now if signal_time else None,
            last_position_time=now if position_time else None,
            source=source,
            is_active=True,
            notes=notes
        )
        db.session.add(entry)
        db.session.commit()
        return entry


def deactivate_watchlist_symbol(symbol: str) -> bool:
    symbol = symbol.upper().strip()
    entry = WatchlistSymbol.query.filter_by(symbol=symbol).first()
    if entry:
        entry.is_active = False
        db.session.commit()
        return True
    return False


def remove_watchlist_symbol(symbol: str) -> bool:
    symbol = symbol.upper().strip()
    entry = WatchlistSymbol.query.filter_by(symbol=symbol).first()
    if entry:
        db.session.delete(entry)
        db.session.commit()
        return True
    return False


def get_active_watchlist_symbols() -> List[str]:
    entries = WatchlistSymbol.query.filter_by(is_active=True).all()
    return [e.symbol for e in entries]


def get_all_watchlist_entries() -> List[WatchlistSymbol]:
    return WatchlistSymbol.query.order_by(WatchlistSymbol.is_active.desc(), 
                                           WatchlistSymbol.added_at.desc()).all()


def cleanup_inactive_symbols(inactive_days: int = 3) -> List[str]:
    cutoff = datetime.utcnow() - timedelta(days=inactive_days)
    
    active_entries = WatchlistSymbol.query.filter_by(is_active=True).all()
    symbols_to_deactivate = []
    
    for entry in active_entries:
        if entry.source == 'manual':
            continue
        
        last_activity = max(
            entry.last_signal_time or datetime.min,
            entry.last_position_time or datetime.min
        )
        
        if last_activity < cutoff:
            has_active_position = _check_has_active_position(entry.symbol)
            if not has_active_position:
                entry.is_active = False
                symbols_to_deactivate.append(entry.symbol)
    
    if symbols_to_deactivate:
        db.session.commit()
        logger.info(f"🧹 Watchlist cleanup: deactivated {len(symbols_to_deactivate)} symbols: {symbols_to_deactivate}")
    
    return symbols_to_deactivate


def _check_has_active_position(symbol: str) -> bool:
    try:
        from models import TrailingStopPosition
        tiger_pos = TrailingStopPosition.query.filter_by(symbol=symbol, is_active=True).first()
        if tiger_pos:
            return True
    except Exception:
        pass
    
    try:
        from alpaca.models import AlpacaTrailingStopPosition
        alpaca_pos = AlpacaTrailingStopPosition.query.filter_by(symbol=symbol, is_active=True).first()
        if alpaca_pos:
            return True
    except Exception:
        pass
    
    try:
        from models import Position
        pos = Position.query.filter_by(symbol=symbol, status='open').first()
        if pos:
            return True
    except Exception:
        pass
    
    return False


def on_signal_received(symbol: str, source_broker: str = 'tiger') -> None:
    try:
        source_label = f'auto_signal_{source_broker}'
        entry = upsert_watchlist_symbol(symbol, source=source_label, signal_time=True)
        
        try:
            from tiger_push_client import get_push_manager
            manager = get_push_manager()
            if manager.is_connected:
                manager.subscribe_quotes([symbol])
                logger.info(f"📊 Auto-subscribed {symbol} via watchlist (signal from {source_broker})")
        except Exception as e:
            logger.debug(f"Could not auto-subscribe {symbol}: {e}")

        _trigger_kline_fetch_async(symbol)
    except Exception as e:
        logger.warning(f"Failed to update watchlist for signal {symbol}: {e}")
        try:
            db.session.rollback()
        except Exception:
            pass


_kline_fetch_in_flight = set()
_kline_fetch_lock = __import__('threading').Lock()


def _trigger_kline_fetch_async(symbol: str):
    import threading

    with _kline_fetch_lock:
        if symbol in _kline_fetch_in_flight:
            logger.debug(f"[Watchlist] K-line fetch already in progress for {symbol}, skipping")
            return
        _kline_fetch_in_flight.add(symbol)

    def _fetch():
        try:
            from app import app
            with app.app_context():
                from kline_service import check_symbol_data_status, fetch_and_store_bars, TIMEFRAMES, FETCH_LIMIT
                import time
                for timeframe in TIMEFRAMES:
                    info = check_symbol_data_status(symbol, timeframe)
                    if info['status'] == 'ok':
                        logger.debug(f"[Watchlist] {symbol}/{timeframe} K-line data already up-to-date")
                        continue
                    limit = FETCH_LIMIT.get(timeframe, 55) if info['status'] == 'backfill' else 10
                    saved, msg = fetch_and_store_bars(symbol, timeframe, limit=limit)
                    logger.info(f"[Watchlist] Auto-fetched K-line {symbol}/{timeframe}: {msg}")
                    time.sleep(1.1)
        except Exception as e:
            logger.warning(f"[Watchlist] K-line auto-fetch failed for {symbol}: {e}")
        finally:
            with _kline_fetch_lock:
                _kline_fetch_in_flight.discard(symbol)

    t = threading.Thread(target=_fetch, daemon=True, name=f"KlineFetch-{symbol}")
    t.start()


DEFAULT_WATCHLIST_SYMBOLS = [
    'NVDA', 'TSLA', 'VST', 'ALAB', 'RKLB', 'MTSI', 'ASTS', 'SLAB', 'SLB', 'NXPI',
    'LSCC', 'GFS', 'TEM', 'SYNA', 'ON', 'SNDK', 'CRWV', 'MRVL', 'WDC', 'STX',
    'MCHP', 'STM', 'COIN', 'CRCL', 'HOOD', 'MP', 'MSTR', 'OKLO', 'ORCL', 'USAR',
    'AMD', 'ARM', 'AVGO', 'INTC', 'MU', 'QCOM', 'SOXL', 'TSLL', 'TXN', 'AAPL',
    'AMZN', 'GOOG', 'META', 'MSFT', 'ANET', 'CIEN', 'EQIX', 'REMX', 'ATOM', 'AMAT',
    'LRCX', 'TSM', 'CRDO', 'ENTG', 'SOXX', 'SMH', 'NFLX',
]


def init_default_watchlist():
    existing_count = WatchlistSymbol.query.count()
    if existing_count > 0:
        logger.info(f"📊 Watchlist already has {existing_count} entries, skipping default init")
        return
    
    logger.info(f"📊 Initializing default watchlist with {len(DEFAULT_WATCHLIST_SYMBOLS)} symbols")
    now = datetime.utcnow()
    for symbol in DEFAULT_WATCHLIST_SYMBOLS:
        entry = WatchlistSymbol(
            symbol=symbol.upper().strip(),
            added_at=now,
            source='manual',
            is_active=True,
        )
        db.session.add(entry)
    db.session.commit()
    logger.info(f"📊 Default watchlist initialized: {len(DEFAULT_WATCHLIST_SYMBOLS)} symbols added")


def get_watchlist_status() -> Dict:
    entries = get_all_watchlist_entries()
    active_count = sum(1 for e in entries if e.is_active)
    
    try:
        from tiger_push_client import get_push_manager
        manager = get_push_manager()
        subscribed = manager.subscribed_symbols
        ws_connected = manager.is_connected
    except Exception:
        subscribed = []
        ws_connected = False
    
    return {
        'total_entries': len(entries),
        'active_count': active_count,
        'subscribed_count': len(subscribed),
        'ws_connected': ws_connected,
        'entries': entries,
        'subscribed_symbols': subscribed
    }
