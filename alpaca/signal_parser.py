import json
import logging
from typing import Dict, Any
from datetime import datetime, time
import pytz

logger = logging.getLogger(__name__)


class AlpacaSignalParser:
    def __init__(self):
        self.required_fields = ['symbol']

    def _is_regular_trading_hours(self) -> bool:
        try:
            et_tz = pytz.timezone('America/New_York')
            now_et = datetime.now(et_tz)
            weekday = now_et.weekday()
            if weekday > 4:
                return False
            market_open = time(9, 30)
            market_close = time(16, 0)
            current_time = now_et.time()
            is_regular = market_open <= current_time <= market_close
            logger.info(f"Market time check: {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}, Regular hours: {is_regular}")
            return is_regular
        except Exception as e:
            logger.error(f"Error checking trading hours: {str(e)}")
            return False

    def _is_extended_hours(self) -> Dict[str, Any]:
        try:
            et_tz = pytz.timezone('America/New_York')
            now_et = datetime.now(et_tz)
            weekday = now_et.weekday()
            if weekday > 4:
                return {'is_extended': False, 'session': 'closed'}

            hours = now_et.hour
            minutes = now_et.minute
            total_minutes = hours * 60 + minutes

            pre_market_start = 4 * 60       # 4:00 AM
            market_open = 9 * 60 + 30        # 9:30 AM
            market_close = 16 * 60           # 4:00 PM
            after_hours_end = 20 * 60        # 8:00 PM

            if pre_market_start <= total_minutes < market_open:
                return {'is_extended': True, 'session': 'pre_market'}
            elif market_open <= total_minutes < market_close:
                return {'is_extended': False, 'session': 'regular'}
            elif market_close <= total_minutes < after_hours_end:
                return {'is_extended': True, 'session': 'after_hours'}
            else:
                return {'is_extended': False, 'session': 'closed'}
        except Exception as e:
            logger.error(f"Error checking extended hours: {str(e)}")
            return {'is_extended': False, 'session': 'unknown'}

    def parse(self, signal_data: Dict[str, Any]) -> Dict[str, Any]:
        symbol = (signal_data.get('symbol') or signal_data.get('ticker') or '').upper().strip()
        if not symbol:
            raise ValueError("Missing required field: symbol")

        sentiment = (signal_data.get('sentiment') or '').lower()
        is_close_signal = sentiment == 'flat'

        side_str = (signal_data.get('side') or signal_data.get('action') or '').lower()
        if is_close_signal:
            side_str = side_str or 'sell'

        if side_str in ('buy', 'long'):
            side = 'buy'
        elif side_str in ('sell', 'short'):
            side = 'sell'
        else:
            raise ValueError(f"Invalid or missing side: {side_str}")

        quantity_raw = signal_data.get('quantity') or signal_data.get('qty') or signal_data.get('size') or '1'
        quantity_str = str(quantity_raw).lower()
        if quantity_str == 'all' or is_close_signal:
            quantity = 'all'
        else:
            try:
                quantity = float(quantity_str)
                if quantity <= 0:
                    quantity = 1
            except (ValueError, TypeError):
                quantity = 1

        price = None
        price_raw = signal_data.get('price') or signal_data.get('limit_price')
        if price_raw and str(price_raw).lower() != 'market':
            try:
                price = float(price_raw)
            except (ValueError, TypeError):
                pass

        if not price:
            ref_price = (signal_data.get('referencePrice') or signal_data.get('reference_price'))
            if not ref_price:
                extras_raw = signal_data.get('extras')
                if isinstance(extras_raw, dict):
                    ref_price = extras_raw.get('referencePrice') or extras_raw.get('reference_price')
            if ref_price:
                try:
                    price = float(ref_price)
                    logger.info(f"Using reference price as limit price: {price}")
                except (ValueError, TypeError):
                    pass

        hours_info = self._is_extended_hours()
        is_regular = hours_info['session'] == 'regular'
        is_extended = hours_info['is_extended']

        order_type_str = (signal_data.get('order_type') or signal_data.get('type') or '').lower()

        if is_regular:
            if order_type_str in ('limit', 'lmt') and price:
                order_type = 'limit'
                logger.info(f"Regular hours: keeping explicit limit order at {price}")
            else:
                order_type = 'market'
                logger.info(f"Regular hours: using market order")
        elif is_extended:
            order_type = 'limit'
            if not price:
                reference_price = signal_data.get('reference_price') or signal_data.get('referencePrice') or signal_data.get('close') or signal_data.get('last_price')
                if not reference_price:
                    extras_raw = signal_data.get('extras')
                    if isinstance(extras_raw, dict):
                        reference_price = extras_raw.get('referencePrice') or extras_raw.get('reference_price')
                if reference_price:
                    price = float(reference_price)
                    logger.info(f"Extended hours: using reference price {price} for limit order")
                else:
                    try:
                        from alpaca.client import AlpacaClient
                        client = AlpacaClient()
                        latest = client.get_latest_trade(symbol)
                        if latest and latest.get('trade'):
                            price = float(latest['trade'].get('p', 0))
                            logger.info(f"Extended hours: fetched last trade price {price} for limit order")
                    except Exception as e:
                        logger.warning(f"Extended hours: could not fetch last price for {symbol}: {e}")
            logger.info(f"Extended hours ({hours_info['session']}): using limit order at {price}")
        else:
            if order_type_str in ('market', 'mkt'):
                order_type = 'market'
            elif order_type_str in ('stop',):
                order_type = 'stop'
            elif order_type_str in ('stop_limit',):
                order_type = 'stop_limit'
            elif order_type_str in ('limit', 'lmt'):
                order_type = 'limit'
            else:
                order_type = 'limit'
            logger.info(f"Market closed ({hours_info['session']}): using {order_type} order")

        stop_loss = None
        sl_raw = signal_data.get('stop_loss') or signal_data.get('sl') or signal_data.get('stoploss') or signal_data.get('stopLoss')
        if sl_raw:
            if isinstance(sl_raw, dict):
                sl_val = sl_raw.get('stopPrice') or sl_raw.get('stop_price') or sl_raw.get('price')
                if sl_val:
                    try:
                        stop_loss = float(sl_val)
                    except (ValueError, TypeError):
                        pass
            else:
                try:
                    stop_loss = float(sl_raw)
                except (ValueError, TypeError):
                    pass

        take_profit = None
        tp_raw = signal_data.get('take_profit') or signal_data.get('tp') or signal_data.get('takeprofit') or signal_data.get('takeProfit')
        if tp_raw:
            if isinstance(tp_raw, dict):
                tp_val = tp_raw.get('limitPrice') or tp_raw.get('limit_price') or tp_raw.get('price')
                if tp_val:
                    try:
                        take_profit = float(tp_val)
                    except (ValueError, TypeError):
                        pass
            else:
                try:
                    take_profit = float(tp_raw)
                except (ValueError, TypeError):
                    pass

        tif_str = (signal_data.get('time_in_force') or '').lower()
        if tif_str in ('day', 'ioc', 'fok', 'opg', 'cls', 'gtc'):
            time_in_force = tif_str
        else:
            time_in_force = 'gtc'

        if is_extended and order_type == 'limit':
            time_in_force = 'day'
            logger.info(f"Extended hours: forced time_in_force to 'day' (required for extended_hours)")


        extras = {}
        for key in ['indicator', 'grade', 'score', 'htf_grade', 'htf_score', 'timeframe',
                     'htf_pass', 'trend', 'comment', 'strategy', 'extras']:
            if key in signal_data:
                extras[key] = signal_data[key]

        if 'extras' in signal_data and isinstance(signal_data['extras'], dict):
            extras.update(signal_data['extras'])

        result = {
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'price': price,
            'order_type': order_type,
            'time_in_force': time_in_force,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'is_close': is_close_signal,
            'extended_hours': hours_info['is_extended'],
            'session': hours_info['session'],
            'reference_price': price or signal_data.get('referencePrice') or signal_data.get('reference_price'),
            'extras': extras,
            'raw_signal': signal_data,
        }

        logger.info(f"Parsed Alpaca signal: {symbol} {side} {quantity} @ {price or 'market'} "
                     f"(type={order_type}, session={hours_info['session']}, "
                     f"SL={stop_loss}, TP={take_profit}, close={is_close_signal})")

        return result
