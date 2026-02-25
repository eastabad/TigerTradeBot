import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger('atr_cache_service')

TIMEFRAME_TTL = {
    '5min': 120,
    '15min': 300,
    '1hour': 600,
}

POSITION_TF_MAP = {
    '5': '5min',
    '15': '15min',
    '60': '1hour',
    '30': '15min',
    '240': '1hour',
    'D': '1hour',
    'W': '1hour',
}


class ATRCache:
    def __init__(self):
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()

    def _key(self, symbol: str, timeframe: str) -> str:
        return f"{symbol}_{timeframe}"

    def get(self, symbol: str, timeframe: str) -> Optional[Dict]:
        key = self._key(symbol, timeframe)
        ttl = TIMEFRAME_TTL.get(timeframe, 300)
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                age = (datetime.now() - entry['computed_at']).total_seconds()
                if age < ttl:
                    return entry['data']
                else:
                    del self._cache[key]
        return None

    def set(self, symbol: str, timeframe: str, data: Dict):
        key = self._key(symbol, timeframe)
        with self._lock:
            self._cache[key] = {
                'data': data,
                'computed_at': datetime.now()
            }

    def invalidate(self, symbol: str = None):
        with self._lock:
            if symbol:
                keys_to_del = [k for k in self._cache if k.startswith(f"{symbol}_")]
                for k in keys_to_del:
                    del self._cache[k]
            else:
                self._cache.clear()

    def get_info(self) -> Dict:
        with self._lock:
            now = datetime.now()
            return {
                'total_entries': len(self._cache),
                'entries': {
                    key: {
                        'age_seconds': (now - entry['computed_at']).total_seconds(),
                        'atr': entry['data'].get('atr', 0),
                        'bars_used': entry['data'].get('bars_count', 0),
                    }
                    for key, entry in self._cache.items()
                }
            }


_atr_cache = ATRCache()


def get_atr_cache_info() -> Dict:
    return _atr_cache.get_info()


def invalidate_atr_cache(symbol: str = None):
    _atr_cache.invalidate(symbol)


def resolve_timeframe(position_tf: str) -> str:
    return POSITION_TF_MAP.get(position_tf or '15', '15min')


def calculate_atr_from_bars(bars: List[Dict], period: int = 14) -> float:
    if not bars or len(bars) < period + 1:
        return 0.0

    true_ranges = []
    for i in range(1, len(bars)):
        high = bars[i]['high']
        low = bars[i]['low']
        prev_close = bars[i - 1]['close']
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return 0.0

    atr = sum(true_ranges[:period]) / period

    for i in range(period, len(true_ranges)):
        atr = (atr * (period - 1) + true_ranges[i]) / period

    return atr


def get_atr_and_bars(symbol: str, position_timeframe: str, atr_period: int = 14) -> Tuple[float, List[Dict]]:
    timeframe = resolve_timeframe(position_timeframe)

    cached = _atr_cache.get(symbol, timeframe)
    if cached:
        logger.debug(f"ATR cache hit: {symbol}/{timeframe} = {cached['atr']:.4f}")
        return cached['atr'], cached['bars']

    from kline_service import get_cached_bars
    bars = get_cached_bars(symbol, timeframe, limit=50)

    if not bars or len(bars) < atr_period + 1:
        logger.warning(f"Insufficient cached bars for ATR: {symbol}/{timeframe}, have {len(bars)}, need {atr_period + 1}")
        return 0.0, bars

    atr = calculate_atr_from_bars(bars, atr_period)

    if atr > 0:
        _atr_cache.set(symbol, timeframe, {
            'atr': atr,
            'bars': bars,
            'bars_count': len(bars),
            'timeframe': timeframe,
        })
        logger.debug(f"ATR computed and cached: {symbol}/{timeframe} = {atr:.4f} ({len(bars)} bars)")

    return atr, bars
