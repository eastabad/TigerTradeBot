import os
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_eodhd_cache: Dict[str, Dict] = {}
_eodhd_cache_lock = threading.Lock()
EODHD_CACHE_MAX_AGE = 30


def _get_api_key() -> Optional[str]:
    key = os.environ.get('EODHD_API_KEY', '')
    return key if key else None


def _get_market_session() -> str:
    try:
        import pytz
        et = pytz.timezone('America/New_York')
        now_et = datetime.now(et)
        h, m = now_et.hour, now_et.minute
        weekday = now_et.weekday()
        if weekday >= 5:
            return 'closed'
        mins = h * 60 + m
        if 240 <= mins < 570:
            return 'pre_market'
        elif 570 <= mins < 960:
            return 'regular'
        elif 960 <= mins < 1200:
            return 'post_market'
        return 'closed'
    except Exception:
        return 'unknown'


def _get_cached_price(symbol: str) -> Optional[Dict]:
    with _eodhd_cache_lock:
        cached = _eodhd_cache.get(symbol)
        if cached:
            age = (datetime.utcnow() - cached['fetched_at']).total_seconds()
            if age <= EODHD_CACHE_MAX_AGE:
                return cached
    return None


def _update_cache(symbol: str, price: float, session: str):
    with _eodhd_cache_lock:
        _eodhd_cache[symbol] = {
            'price': price,
            'session': session,
            'source': 'eodhd_api',
            'fetched_at': datetime.utcnow(),
        }


def get_eodhd_price(symbol: str) -> Optional[Dict]:
    cached = _get_cached_price(symbol)
    if cached:
        return {
            'price': cached['price'],
            'session': cached['session'],
            'source': 'eodhd_api_cached',
        }

    api_key = _get_api_key()
    if not api_key:
        return None

    try:
        import requests
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        resp = requests.get(
            'https://eodhd.com/api/real-time/{}.US'.format(clean_symbol),
            params={'api_token': api_key, 'fmt': 'json'},
            timeout=5
        )
        if resp.status_code != 200:
            logger.debug(f"EODHD API returned status {resp.status_code} for {clean_symbol}")
            return None

        data = resp.json()

        close_price = float(data.get('close', 0))
        previous_close = float(data.get('previousClose', 0))
        price = close_price if close_price > 0 else previous_close

        if price <= 0:
            return None

        session = _get_market_session()
        _update_cache(symbol, price, session)

        logger.debug(f"EODHD price for {clean_symbol}: ${price:.2f} (session: {session})")
        return {
            'price': price,
            'session': session,
            'source': 'eodhd_api',
        }
    except Exception as e:
        logger.debug(f"EODHD API error for {symbol}: {e}")
        return None


def get_eodhd_extended_price(symbol: str) -> Optional[Dict]:
    api_key = _get_api_key()
    if not api_key:
        return None

    try:
        import requests
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        resp = requests.get(
            'https://eodhd.com/api/us-quote-delayed',
            params={'s': f'{clean_symbol}.US', 'api_token': api_key, 'fmt': 'json'},
            timeout=5
        )
        if resp.status_code != 200:
            return None

        raw = resp.json()
        qdata = raw.get('data', {}).get(f'{clean_symbol}.US', {})
        if not qdata:
            return None

        eth_price = float(qdata.get('ethPrice', 0))
        last_price = float(qdata.get('lastTradePrice', 0))
        price = eth_price if eth_price > 0 else last_price

        if price <= 0:
            return None

        session = _get_market_session()
        if eth_price > 0:
            if session in ('pre_market', 'post_market'):
                pass
            else:
                session = 'regular'

        _update_cache(symbol, price, session)

        logger.debug(f"EODHD extended price for {clean_symbol}: ${price:.2f} (session: {session}, eth=${eth_price})")
        return {
            'price': price,
            'session': session,
            'source': 'eodhd_extended',
        }
    except Exception as e:
        logger.debug(f"EODHD extended API error for {symbol}: {e}")
        return None


def get_eodhd_smart_price(symbol: str) -> Optional[Dict]:
    session = _get_market_session()

    if session in ('pre_market', 'post_market'):
        result = get_eodhd_extended_price(symbol)
        if result and result['price'] > 0:
            return result

    result = get_eodhd_price(symbol)
    if result and result['price'] > 0:
        return result

    return None


def get_eodhd_batch_prices(symbols: List[str]) -> Dict[str, Dict]:
    if not symbols:
        return {}

    api_key = _get_api_key()
    if not api_key:
        return {}

    results = {}
    uncached = []

    for sym in symbols:
        cached = _get_cached_price(sym)
        if cached:
            results[sym] = {
                'price': cached['price'],
                'session': cached['session'],
                'source': 'eodhd_api_cached',
            }
        else:
            uncached.append(sym)

    if not uncached:
        return results

    session = _get_market_session()

    if session in ('pre_market', 'post_market'):
        for sym in uncached:
            try:
                result = get_eodhd_extended_price(sym)
                if result and result['price'] > 0:
                    results[sym] = result
            except Exception:
                pass
        still_missing = [s for s in uncached if s not in results]
    else:
        still_missing = uncached

    if still_missing:
        try:
            import requests
            clean_symbols = [s.replace('[PAPER]', '').strip() for s in still_missing]
            primary = clean_symbols[0]
            additional = ','.join(f'{s}.US' for s in clean_symbols[1:]) if len(clean_symbols) > 1 else ''

            params = {'api_token': api_key, 'fmt': 'json'}
            if additional:
                params['s'] = additional

            resp = requests.get(
                'https://eodhd.com/api/real-time/{}.US'.format(primary),
                params=params,
                timeout=8
            )

            if resp.status_code == 200:
                raw = resp.json()
                if isinstance(raw, dict) and not isinstance(raw, list):
                    raw = [raw]
                elif isinstance(raw, list):
                    pass
                else:
                    raw = []

                for item in raw:
                    code = item.get('code', '')
                    sym_clean = code.replace('.US', '') if code else ''
                    if not sym_clean:
                        continue

                    original_sym = None
                    for s in still_missing:
                        if s.replace('[PAPER]', '').strip() == sym_clean:
                            original_sym = s
                            break
                    if not original_sym:
                        continue

                    close_price = float(item.get('close', 0))
                    prev_close = float(item.get('previousClose', 0))
                    price = close_price if close_price > 0 else prev_close

                    if price > 0:
                        _update_cache(original_sym, price, session)
                        results[original_sym] = {
                            'price': price,
                            'session': session,
                            'source': 'eodhd_api',
                        }

                fetched = len([s for s in still_missing if s in results])
                if fetched > 0:
                    logger.debug(f"EODHD batch: {fetched}/{len(still_missing)} prices fetched")
        except Exception as e:
            logger.debug(f"EODHD batch API error: {e}")

    return results
