import logging
import threading
import time as time_module
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from app import db
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models import BarCache, WatchlistSymbol

logger = logging.getLogger('kline_service')

MAX_BARS_PER_SYMBOL = 50

_kline_rate_lock = threading.Lock()
_kline_request_times = []
KLINE_MAX_PER_MINUTE = 50


def _kline_rate_wait():
    """Global rate limiter for kline API calls - max 50/minute across all threads."""
    with _kline_rate_lock:
        now = time_module.time()
        _kline_request_times[:] = [t for t in _kline_request_times if now - t < 60]
        if len(_kline_request_times) >= KLINE_MAX_PER_MINUTE:
            oldest = _kline_request_times[0]
            wait_time = 60 - (now - oldest) + 0.5
            if wait_time > 0:
                logger.info(f"[KlineRateLimit] Throttling: {len(_kline_request_times)} requests in last 60s, waiting {wait_time:.1f}s")
                time_module.sleep(wait_time)
                now = time_module.time()
                _kline_request_times[:] = [t for t in _kline_request_times if now - t < 60]
        _kline_request_times.append(time_module.time())

TIMEFRAME_MAP = {
    '5min': '5min',
    '15min': '15min',
    '1hour': '1hour',
    '5': '5min',
    '15': '15min',
    '60': '1hour',
}

TIMEFRAMES = ['5min', '15min', '1hour']

TIMEFRAME_MINUTES = {
    '5min': 5,
    '15min': 15,
    '1hour': 60,
}

FETCH_LIMIT = {
    '5min': 60,
    '15min': 55,
    '1hour': 55,
}


def normalize_timeframe(tf: str) -> str:
    return TIMEFRAME_MAP.get(tf, tf)


def get_active_symbols() -> List[str]:
    try:
        symbols = WatchlistSymbol.query.filter_by(is_active=True).all()
        return [s.symbol for s in symbols]
    except Exception as e:
        logger.error(f"Error getting active symbols: {e}")
        return []


def get_last_cached_bar_time(symbol: str, timeframe: str) -> Optional[datetime]:
    try:
        bar = BarCache.query.filter_by(
            symbol=symbol, timeframe=timeframe
        ).order_by(BarCache.timestamp.desc()).first()
        return bar.timestamp if bar else None
    except Exception as e:
        logger.error(f"Error getting last cached bar time for {symbol}/{timeframe}: {e}")
        return None


def get_cached_bar_count(symbol: str, timeframe: str) -> int:
    try:
        return BarCache.query.filter_by(symbol=symbol, timeframe=timeframe).count()
    except Exception:
        return 0


def save_bars_to_cache(symbol: str, timeframe: str, bars: List[Dict]) -> int:
    if not bars:
        return 0

    saved_count = 0
    for bar in bars:
        ts = bar.get('timestamp')
        if ts is None:
            continue

        if isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)

        values = {
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp': ts,
            'open': float(bar['open']),
            'high': float(bar['high']),
            'low': float(bar['low']),
            'close': float(bar['close']),
            'volume': int(bar.get('volume', 0)),
        }

        stmt = pg_insert(BarCache).values(**values)
        stmt = stmt.on_conflict_do_update(
            constraint='uq_bar_cache_symbol_tf_ts',
            set_={
                'open': stmt.excluded.open,
                'high': stmt.excluded.high,
                'low': stmt.excluded.low,
                'close': stmt.excluded.close,
                'volume': stmt.excluded.volume,
            }
        )
        db.session.execute(stmt)
        saved_count += 1

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error saving bars for {symbol}/{timeframe}: {e}")
        return 0

    trim_old_bars(symbol, timeframe)
    return saved_count


def trim_old_bars(symbol: str, timeframe: str):
    try:
        count = BarCache.query.filter_by(symbol=symbol, timeframe=timeframe).count()
        if count > MAX_BARS_PER_SYMBOL:
            excess = count - MAX_BARS_PER_SYMBOL
            oldest = BarCache.query.filter_by(
                symbol=symbol, timeframe=timeframe
            ).order_by(BarCache.timestamp.asc()).limit(excess).all()
            for bar in oldest:
                db.session.delete(bar)
            db.session.commit()
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error trimming bars for {symbol}/{timeframe}: {e}")


def get_cached_bars(symbol: str, timeframe: str, limit: int = MAX_BARS_PER_SYMBOL) -> List[Dict]:
    try:
        tf = normalize_timeframe(timeframe)
        bars = BarCache.query.filter_by(
            symbol=symbol, timeframe=tf
        ).order_by(BarCache.timestamp.desc()).limit(limit).all()

        result = []
        for bar in reversed(bars):
            result.append({
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            })
        return result
    except Exception as e:
        logger.error(f"Error getting cached bars for {symbol}/{timeframe}: {e}")
        return []


def fetch_and_store_bars(symbol: str, timeframe: str, limit: Optional[int] = None) -> Tuple[int, str]:
    from tiger_client import TigerQuoteClient

    if limit is None:
        limit = FETCH_LIMIT.get(timeframe, 55)

    try:
        _kline_rate_wait()
        quote_client = TigerQuoteClient()
        bars = quote_client.get_bars(symbol, timeframe=timeframe, limit=limit)

        if not bars:
            return 0, f"No bars returned from API for {symbol}/{timeframe}"

        saved = save_bars_to_cache(symbol, timeframe, bars)
        return saved, f"OK: {len(bars)} fetched, {saved} new"

    except Exception as e:
        msg = f"Error fetching bars for {symbol}/{timeframe}: {e}"
        logger.error(msg)
        return 0, msg


def check_symbol_data_status(symbol: str, timeframe: str) -> Dict:
    from market_time import check_data_staleness, get_expected_latest_bar
    last_bar_time = get_last_cached_bar_time(symbol, timeframe)
    bar_count = get_cached_bar_count(symbol, timeframe)
    status = check_data_staleness(last_bar_time, bar_count, timeframe, min_bars=20)
    expected = get_expected_latest_bar(timeframe)
    return {
        'symbol': symbol,
        'timeframe': timeframe,
        'status': status,
        'latest_bar_time': last_bar_time,
        'bar_count': bar_count,
        'expected_time': expected['expected_time'] if expected else None,
        'is_trading_now': expected['is_trading_now'] if expected else False,
    }


def update_symbol_incremental(symbol: str, timeframe: str) -> Tuple[int, str]:
    info = check_symbol_data_status(symbol, timeframe)
    status = info['status']

    if status == 'ok':
        return 0, f"OK: data up-to-date for {symbol}/{timeframe} (bars={info['bar_count']})"

    if status == 'backfill':
        limit = FETCH_LIMIT.get(timeframe, 55)
        saved, msg = fetch_and_store_bars(symbol, timeframe, limit=limit)
        return saved, f"BACKFILL: {msg}"

    last_bar_time = info['latest_bar_time']
    expected_time = info['expected_time']

    if last_bar_time and expected_time:
        last_naive = last_bar_time.replace(tzinfo=None) if last_bar_time.tzinfo else last_bar_time
        diff_minutes = (expected_time - last_naive).total_seconds() / 60
        tf_minutes = TIMEFRAME_MINUTES.get(timeframe, 15)
        bars_needed = int(diff_minutes / tf_minutes) + 5
        bars_needed = max(3, min(bars_needed, FETCH_LIMIT.get(timeframe, 55)))
    else:
        bars_needed = 10

    saved, msg = fetch_and_store_bars(symbol, timeframe, limit=bars_needed)
    return saved, f"INCREMENTAL: {msg}"


def update_all_symbols_for_timeframe(timeframe: str) -> Dict:
    from market_time import ensure_calendar_loaded
    ensure_calendar_loaded()

    symbols = get_active_symbols()
    if not symbols:
        logger.info(f"[KlineService] No active symbols for {timeframe} update")
        return {'total': 0, 'updated': 0, 'errors': 0, 'skipped': 0}

    statuses = []
    for symbol in symbols:
        try:
            statuses.append(check_symbol_data_status(symbol, timeframe))
        except Exception as e:
            statuses.append({'symbol': symbol, 'status': 'backfill', 'bar_count': 0,
                             'latest_bar_time': None, 'expected_time': None})

    ok_symbols = [s for s in statuses if s['status'] == 'ok']
    incremental_symbols = [s for s in statuses if s['status'] == 'incremental']
    backfill_symbols = [s for s in statuses if s['status'] == 'backfill']

    results = {
        'total': len(symbols),
        'updated': 0,
        'errors': 0,
        'skipped': len(ok_symbols),
        'details': {},
    }

    if ok_symbols:
        logger.info(f"[KlineService] {timeframe}: {len(ok_symbols)} symbols already up-to-date")

    for info in backfill_symbols:
        symbol = info['symbol']
        try:
            saved, msg = fetch_and_store_bars(symbol, timeframe, limit=FETCH_LIMIT.get(timeframe, 55))
            results['details'][symbol] = f"BACKFILL: {msg}"
            if saved > 0:
                results['updated'] += 1
        except Exception as e:
            results['errors'] += 1
            results['details'][symbol] = f"BACKFILL ERROR: {e}"
            logger.error(f"[KlineService] Backfill error {symbol}/{timeframe}: {e}")

    for info in incremental_symbols:
        symbol = info['symbol']
        try:
            saved, msg = update_symbol_incremental(symbol, timeframe)
            results['details'][symbol] = msg
            if saved > 0:
                results['updated'] += 1
        except Exception as e:
            results['errors'] += 1
            results['details'][symbol] = f"INCREMENTAL ERROR: {e}"
            logger.error(f"[KlineService] Incremental error {symbol}/{timeframe}: {e}")

    logger.info(f"[KlineService] {timeframe} update: {results['updated']} updated, "
                f"{results['skipped']} ok, {results['errors']} errors "
                f"(backfill={len(backfill_symbols)}, incremental={len(incremental_symbols)})")
    return results


def startup_backfill():
    from market_time import ensure_calendar_loaded
    ensure_calendar_loaded()

    symbols = get_active_symbols()
    if not symbols:
        logger.info("[KlineService] No symbols for startup backfill")
        return

    logger.info(f"[KlineService] Starting backfill for {len(symbols)} symbols across {len(TIMEFRAMES)} timeframes")

    for timeframe in TIMEFRAMES:
        needs_update = []
        ok_count = 0
        for symbol in symbols:
            info = check_symbol_data_status(symbol, timeframe)
            if info['status'] == 'ok':
                ok_count += 1
            else:
                needs_update.append(info)

        if not needs_update:
            logger.info(f"[KlineService] {timeframe}: all {len(symbols)} symbols up-to-date")
            continue

        backfill_count = sum(1 for s in needs_update if s['status'] == 'backfill')
        incremental_count = sum(1 for s in needs_update if s['status'] == 'incremental')
        logger.info(f"[KlineService] {timeframe}: {ok_count} ok, {backfill_count} backfill, {incremental_count} incremental")

        for info in needs_update:
            symbol = info['symbol']
            try:
                saved, msg = update_symbol_incremental(symbol, timeframe)
                logger.debug(f"[KlineService] Startup {symbol}/{timeframe}: {msg}")
            except Exception as e:
                logger.error(f"[KlineService] Startup error {symbol}/{timeframe}: {e}")

    logger.info("[KlineService] Startup backfill complete")


def cleanup_old_data(days: int = 7):
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        deleted = BarCache.query.filter(BarCache.timestamp < cutoff).delete()
        db.session.commit()
        logger.info(f"[KlineService] Cleaned up {deleted} bars older than {days} days")
        return deleted
    except Exception as e:
        db.session.rollback()
        logger.error(f"[KlineService] Cleanup error: {e}")
        return 0


def get_kline_stats() -> Dict:
    try:
        from sqlalchemy import func
        stats = db.session.query(
            BarCache.timeframe,
            func.count(func.distinct(BarCache.symbol)).label('symbols'),
            func.count(BarCache.id).label('total_bars'),
            func.min(BarCache.timestamp).label('oldest'),
            func.max(BarCache.timestamp).label('newest')
        ).group_by(BarCache.timeframe).all()

        result = {}
        for row in stats:
            result[row.timeframe] = {
                'symbols': row.symbols,
                'total_bars': row.total_bars,
                'oldest': row.oldest.isoformat() if row.oldest else None,
                'newest': row.newest.isoformat() if row.newest else None,
            }
        return result
    except Exception as e:
        logger.error(f"Error getting kline stats: {e}")
        return {}
