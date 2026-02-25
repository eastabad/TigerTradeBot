import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List

logger = logging.getLogger('market_time')

TIMEFRAME_MINUTES = {
    '5min': 5,
    '15min': 15,
    '1hour': 60,
}

SESSION_START_MINUTES = 4 * 60
SESSION_END_MINUTES = 20 * 60

LAST_BAR_OFFSET = {
    '5min': 19 * 60 + 55,
    '15min': 19 * 60 + 45,
    '1hour': 19 * 60,
}

_trading_calendar: List[str] = []
_calendar_last_fetched: Optional[datetime] = None


def load_trading_calendar():
    global _trading_calendar, _calendar_last_fetched
    try:
        import os
        api_key = os.environ.get('ALPACA_API_KEY')
        secret_key = os.environ.get('ALPACA_SECRET_KEY')

        if not api_key or not secret_key:
            logger.warning("[MarketTime] No Alpaca API keys, using weekend-only calendar")
            return

        import requests
        now = datetime.utcnow()
        start_date = (now - timedelta(days=365)).strftime('%Y-%m-%d')
        end_date = (now + timedelta(days=90)).strftime('%Y-%m-%d')

        url = f"https://paper-api.alpaca.markets/v2/calendar?start={start_date}&end={end_date}"
        resp = requests.get(url, headers={
            'APCA-API-KEY-ID': api_key,
            'APCA-API-SECRET-KEY': secret_key,
        }, timeout=10)

        if resp.status_code == 200:
            data = resp.json()
            _trading_calendar = [d['date'] for d in data]
            _calendar_last_fetched = datetime.utcnow()
            logger.info(f"[MarketTime] Loaded {len(_trading_calendar)} trading days")
        else:
            logger.error(f"[MarketTime] Calendar API error: {resp.status_code}")
    except Exception as e:
        logger.error(f"[MarketTime] Failed to load trading calendar: {e}")


def ensure_calendar_loaded():
    global _calendar_last_fetched
    if (not _trading_calendar or
        not _calendar_last_fetched or
        (datetime.utcnow() - _calendar_last_fetched).days > 30):
        load_trading_calendar()


def is_trading_day(date_str: str) -> bool:
    if _trading_calendar:
        return date_str in _trading_calendar

    from datetime import date
    parts = date_str.split('-')
    d = date(int(parts[0]), int(parts[1]), int(parts[2]))
    return d.weekday() < 5


def get_ny_time(now: Optional[datetime] = None) -> datetime:
    import pytz
    et = pytz.timezone('America/New_York')
    d = now or datetime.utcnow()
    if d.tzinfo is None:
        import pytz as pz
        d = pz.utc.localize(d)
    return d.astimezone(et)


def format_date_str(d) -> str:
    return d.strftime('%Y-%m-%d')


def get_previous_trading_day(from_date_str: str) -> Optional[str]:
    if _trading_calendar:
        earlier = [d for d in _trading_calendar if d < from_date_str]
        earlier.sort(reverse=True)
        return earlier[0] if earlier else None

    from datetime import date
    parts = from_date_str.split('-')
    d = date(int(parts[0]), int(parts[1]), int(parts[2]))
    for i in range(1, 11):
        prev = d - timedelta(days=i)
        prev_str = prev.strftime('%Y-%m-%d')
        if is_trading_day(prev_str):
            return prev_str
    return None


def et_to_utc(date_str: str, total_minutes_et: int) -> datetime:
    import pytz
    et = pytz.timezone('America/New_York')
    hours = total_minutes_et // 60
    minutes = total_minutes_et % 60

    naive = datetime.strptime(f"{date_str} {hours:02d}:{minutes:02d}:00", "%Y-%m-%d %H:%M:%S")
    localized = et.localize(naive)
    return localized.astimezone(pytz.utc).replace(tzinfo=None)


def get_expected_latest_bar(timeframe: str, now: Optional[datetime] = None) -> Optional[Dict]:
    ny = get_ny_time(now)
    today_str = format_date_str(ny)
    current_minutes = ny.hour * 60 + ny.minute
    tf_minutes = TIMEFRAME_MINUTES.get(timeframe)

    if tf_minutes is None:
        return None

    today_is_trading = is_trading_day(today_str)

    if today_is_trading and SESSION_START_MINUTES <= current_minutes < SESSION_END_MINUTES:
        minutes_since_open = current_minutes - SESSION_START_MINUTES
        bars_completed = minutes_since_open // tf_minutes
        latest_bar_minutes = SESSION_START_MINUTES + bars_completed * tf_minutes

        return {
            'expected_time': et_to_utc(today_str, latest_bar_minutes),
            'is_trading_now': True,
        }

    if today_is_trading and current_minutes >= SESSION_END_MINUTES:
        last_trading_date_str = today_str
    else:
        last_trading_date_str = get_previous_trading_day(today_str)

    if not last_trading_date_str:
        return None

    last_bar_minutes = LAST_BAR_OFFSET.get(timeframe)
    if last_bar_minutes is None:
        return None

    return {
        'expected_time': et_to_utc(last_trading_date_str, last_bar_minutes),
        'is_trading_now': False,
    }


def check_data_staleness(
    latest_bar_time: Optional[datetime],
    bar_count: int,
    timeframe: str,
    min_bars: int = 20,
    now: Optional[datetime] = None,
) -> str:
    if not latest_bar_time or bar_count == 0:
        return 'backfill'

    if bar_count < min_bars:
        return 'backfill'

    expected = get_expected_latest_bar(timeframe, now)
    if not expected:
        return 'ok'

    expected_time = expected['expected_time']

    latest_naive = latest_bar_time.replace(tzinfo=None) if latest_bar_time.tzinfo else latest_bar_time

    diff_seconds = (expected_time - latest_naive).total_seconds()

    if diff_seconds <= 0:
        return 'ok'

    one_trading_day_seconds = 16 * 60 * 60
    if diff_seconds > one_trading_day_seconds:
        return 'backfill'

    return 'incremental'


def get_calendar_status() -> Dict:
    return {
        'loaded': len(_trading_calendar) > 0,
        'days': len(_trading_calendar),
        'last_fetched': _calendar_last_fetched.isoformat() if _calendar_last_fetched else None,
    }
