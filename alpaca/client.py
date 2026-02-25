import os
import json
import logging
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

ALPACA_PAPER_BASE_URL = 'https://paper-api.alpaca.markets'
ALPACA_PAPER_DATA_URL = 'https://data.alpaca.markets'


class AlpacaClient:
    def __init__(self):
        self.api_key = os.environ.get('ALPACA_API_KEY2', '') or os.environ.get('ALPACA_API_KEY', '')
        self.secret_key = os.environ.get('ALPACA_SECRET_KEY2', '') or os.environ.get('ALPACA_SECRET_KEY', '')
        self.base_url = ALPACA_PAPER_BASE_URL
        self.data_url = ALPACA_PAPER_DATA_URL

        if not self.api_key or not self.secret_key:
            logger.warning("Alpaca API keys not configured")

    def _headers(self) -> Dict[str, str]:
        return {
            'APCA-API-KEY-ID': self.api_key,
            'APCA-API-SECRET-KEY': self.secret_key,
            'Content-Type': 'application/json',
        }

    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None,
                 params: Optional[Dict] = None, base_url: Optional[str] = None) -> Dict:
        url = f"{base_url or self.base_url}{endpoint}"
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self._headers(),
                json=data,
                params=params,
                timeout=30,
            )
            if response.status_code == 200 or response.status_code == 201:
                return response.json()
            elif response.status_code == 204:
                return {'success': True}
            else:
                error_text = response.text
                logger.error(f"Alpaca API error {response.status_code}: {error_text}")
                raise Exception(f"Alpaca API error {response.status_code}: {error_text}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Alpaca API request failed: {str(e)}")
            raise

    def get_account(self) -> Dict:
        return self._request('GET', '/v2/account')

    def get_positions(self) -> List[Dict]:
        return self._request('GET', '/v2/positions')

    def get_position(self, symbol: str) -> Optional[Dict]:
        try:
            return self._request('GET', f'/v2/positions/{symbol}')
        except Exception as e:
            err_msg = str(e).lower()
            if '404' in err_msg or 'not found' in err_msg or 'no position' in err_msg:
                return {'_no_position': True}
            return None

    def get_orders(self, status: str = 'all', limit: int = 100, 
                   after: Optional[str] = None, until: Optional[str] = None,
                   symbols: Optional[str] = None) -> List[Dict]:
        params = {'status': status, 'limit': limit, 'direction': 'desc'}
        if after:
            params['after'] = after
        if until:
            params['until'] = until
        if symbols:
            params['symbols'] = symbols
        return self._request('GET', '/v2/orders', params=params)

    def get_order(self, order_id: str) -> Dict:
        return self._request('GET', f'/v2/orders/{order_id}')

    def get_order_by_client_id(self, client_order_id: str) -> Dict:
        return self._request('GET', '/v2/orders:by_client_order_id',
                             params={'client_order_id': client_order_id})

    def place_order(self, parsed_signal: Dict[str, Any]) -> Dict[str, Any]:
        symbol = parsed_signal['symbol']
        side = parsed_signal['side']
        quantity = parsed_signal['quantity']
        order_type = parsed_signal.get('order_type', 'limit')
        price = parsed_signal.get('price')
        time_in_force = parsed_signal.get('time_in_force', 'day')
        extended_hours = parsed_signal.get('extended_hours', False)
        stop_loss = parsed_signal.get('stop_loss')
        take_profit = parsed_signal.get('take_profit')

        is_close = parsed_signal.get('is_close', False)

        if is_close or quantity == 'all':
            position = self.get_position(symbol)
            if not position or position.get('_no_position'):
                all_pos = self.get_positions() or []
                position = next((p for p in all_pos if p.get('symbol') == symbol), None)

            if not position or position.get('_no_position'):
                return {'success': False, 'error': f'No open position in Alpaca for {symbol}, skipping close order'}

            pos_qty = float(position.get('qty', 0))
            abs_pos_qty = abs(pos_qty)
            pos_is_long = pos_qty > 0

            try:
                cancel_result = self.cancel_orders_for_symbol(symbol)
                if cancel_result.get('cancelled_count', 0) > 0:
                    logger.info(f"[{symbol}] Pre-close: cancelled {cancel_result['cancelled_count']} existing orders to release shares")
            except Exception as cancel_err:
                logger.warning(f"[{symbol}] Pre-close cancel failed: {cancel_err}")

            if quantity == 'all' or (is_close and (not quantity or float(quantity) == 0)):
                quantity = abs_pos_qty
                logger.info(f"[{symbol}] Close order: using full position qty {abs_pos_qty}")
            elif is_close:
                close_side_is_sell = (side == 'sell')
                if close_side_is_sell and not pos_is_long:
                    return {'success': False, 'error': f'{symbol}: trying to sell-to-close but position is SHORT ({pos_qty}), skipping'}
                if not close_side_is_sell and pos_is_long:
                    return {'success': False, 'error': f'{symbol}: trying to buy-to-close but position is LONG ({pos_qty}), skipping'}
                if float(quantity) > abs_pos_qty:
                    logger.warning(f"[{symbol}] Close qty {quantity} > position qty {abs_pos_qty}, capping to position size")
                    quantity = abs_pos_qty

        order_data = {
            'symbol': symbol,
            'qty': str(int(quantity)) if float(quantity) == int(float(quantity)) else str(quantity),
            'side': side,
            'type': order_type,
            'time_in_force': time_in_force,
        }

        def _round_price(p):
            if p is not None:
                return round(float(p), 2)
            return None

        price = _round_price(price)
        stop_loss = _round_price(stop_loss)
        take_profit = _round_price(take_profit)

        if order_type == 'limit' and price:
            order_data['limit_price'] = str(price)
        elif order_type == 'stop' and price:
            order_data['stop_price'] = str(price)
        elif order_type == 'stop_limit':
            sp = _round_price(parsed_signal.get('stop_price'))
            if sp:
                order_data['stop_price'] = str(sp)
            if price:
                order_data['limit_price'] = str(price)

        if extended_hours and order_type == 'limit':
            order_data['extended_hours'] = True
            order_data['time_in_force'] = 'day'

        if not extended_hours and not is_close:
            base_price = price
            if not base_price:
                try:
                    trade_data = self.get_latest_trade(symbol)
                    base_price = float(trade_data.get('trade', trade_data).get('p', 0) or trade_data.get('price', 0))
                except Exception:
                    base_price = None

            if take_profit and base_price:
                is_buy = (side == 'buy')
                if is_buy and take_profit < base_price + 0.01:
                    logger.warning(f"[{symbol}] TP {take_profit} invalid for BUY (base_price={base_price}), need >= {base_price + 0.01}. Skipping TP.")
                    take_profit = None
                elif not is_buy and take_profit > base_price - 0.01:
                    logger.warning(f"[{symbol}] TP {take_profit} invalid for SELL (base_price={base_price}), need <= {base_price - 0.01}. Skipping TP.")
                    take_profit = None

            if stop_loss and base_price:
                is_buy = (side == 'buy')
                if is_buy and stop_loss > base_price - 0.01:
                    logger.warning(f"[{symbol}] SL {stop_loss} invalid for BUY (base_price={base_price}), need <= {base_price - 0.01}. Skipping SL.")
                    stop_loss = None
                elif not is_buy and stop_loss < base_price + 0.01:
                    logger.warning(f"[{symbol}] SL {stop_loss} invalid for SELL (base_price={base_price}), need >= {base_price + 0.01}. Skipping SL.")
                    stop_loss = None

            if stop_loss and take_profit:
                order_data['order_class'] = 'bracket'
                order_data['stop_loss'] = {'stop_price': str(stop_loss)}
                order_data['take_profit'] = {'limit_price': str(take_profit)}
                if time_in_force != 'gtc':
                    order_data['time_in_force'] = 'gtc'
                logger.info(f"Creating bracket order (GTC): type={order_type}, SL={stop_loss}, TP={take_profit}")
            elif stop_loss and not take_profit:
                order_data['order_class'] = 'oto'
                order_data['stop_loss'] = {'stop_price': str(stop_loss)}
                if time_in_force != 'gtc':
                    order_data['time_in_force'] = 'gtc'
                logger.info(f"Creating OTO order (GTC) with SL={stop_loss}")
            elif not stop_loss and take_profit:
                order_data['order_class'] = 'oto'
                order_data['take_profit'] = {'limit_price': str(take_profit)}
                if time_in_force != 'gtc':
                    order_data['time_in_force'] = 'gtc'
                logger.info(f"Creating OTO order (GTC) with TP={take_profit}")

        try:
            logger.info(f"Placing Alpaca order: {json.dumps(order_data)}")
            result = self._request('POST', '/v2/orders', data=order_data)

            return {
                'success': True,
                'order_id': result.get('id'),
                'client_order_id': result.get('client_order_id'),
                'status': result.get('status'),
                'order_data': result,
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
            }

    def cancel_order(self, order_id: str) -> Dict:
        try:
            self._request('DELETE', f'/v2/orders/{order_id}')
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def cancel_orders_for_symbol(self, symbol: str, wait_for_release: bool = True) -> Dict:
        import time
        try:
            orders = self._request('GET', f'/v2/orders?status=open&symbols={symbol}&limit=50')
            if not orders:
                return {'success': True, 'cancelled_count': 0}

            cancelled = 0
            for order in orders:
                order_id = order.get('id', '')
                try:
                    self._request('DELETE', f'/v2/orders/{order_id}')
                    cancelled += 1
                except Exception as ce:
                    logger.warning(f"Failed to cancel order {order_id[:12]}... for {symbol}: {ce}")

            if cancelled > 0 and wait_for_release:
                for attempt in range(3):
                    time.sleep(0.3)
                    remaining = self._request('GET', f'/v2/orders?status=open&symbols={symbol}&limit=10')
                    if not remaining:
                        break
                    still_open = [o for o in remaining if o.get('status') not in ('canceled', 'cancelled', 'filled', 'expired', 'rejected')]
                    if not still_open:
                        break
                    logger.info(f"[{symbol}] Waiting for {len(still_open)} orders to clear (attempt {attempt+1}/3)")
                    if attempt == 2:
                        for so in still_open:
                            try:
                                self._request('DELETE', f'/v2/orders/{so.get("id", "")}')
                            except Exception:
                                pass

            return {'success': True, 'cancelled_count': cancelled}
        except Exception as e:
            return {'success': False, 'error': str(e), 'cancelled_count': 0}

    def cancel_all_orders(self) -> Dict:
        try:
            result = self._request('DELETE', '/v2/orders')
            return {'success': True, 'cancelled': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def replace_order(self, order_id: str, updates: Dict) -> Dict:
        try:
            result = self._request('PATCH', f'/v2/orders/{order_id}', data=updates)
            return {'success': True, 'order': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def close_position(self, symbol: str, qty: Optional[float] = None) -> Dict:
        try:
            params = {}
            if qty:
                params['qty'] = str(qty)
            result = self._request('DELETE', f'/v2/positions/{symbol}', params=params)
            return {'success': True, 'order': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def close_all_positions(self) -> Dict:
        try:
            result = self._request('DELETE', '/v2/positions')
            return {'success': True, 'result': result}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_activities(self, activity_type: str = 'FILL', 
                       after: Optional[str] = None,
                       until: Optional[str] = None,
                       page_size: int = 100) -> List[Dict]:
        params = {'activity_type': activity_type, 'page_size': page_size, 'direction': 'desc'}
        if after:
            params['after'] = after
        if until:
            params['until'] = until
        return self._request('GET', '/v2/account/activities/' + activity_type, params=params)

    def get_bars(self, symbol: str, timeframe: str = '1Day',
                 start: Optional[str] = None, end: Optional[str] = None,
                 limit: int = 100) -> Dict:
        params = {
            'timeframe': timeframe,
            'limit': limit,
        }
        if start:
            params['start'] = start
        if end:
            params['end'] = end
        return self._request('GET', f'/v2/stocks/{symbol}/bars',
                             params=params, base_url=self.data_url)

    def get_latest_quote(self, symbol: str) -> Dict:
        return self._request('GET', f'/v2/stocks/{symbol}/quotes/latest',
                             base_url=self.data_url)

    def get_latest_trade(self, symbol: str) -> Dict:
        return self._request('GET', f'/v2/stocks/{symbol}/trades/latest',
                             base_url=self.data_url)

    def get_latest_trades_batch(self, symbols: List[str]) -> Dict:
        if not symbols:
            return {}
        symbols_str = ','.join(symbols)
        return self._request('GET', '/v2/stocks/trades/latest',
                             params={'symbols': symbols_str},
                             base_url=self.data_url)

    def is_connected(self) -> bool:
        try:
            account = self.get_account()
            return account.get('status') == 'ACTIVE'
        except Exception:
            return False
