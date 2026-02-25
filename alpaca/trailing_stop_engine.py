import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from app import db
from alpaca.models import (
    AlpacaTrailingStopPosition, AlpacaTrailingStopConfig,
    AlpacaTrailingStopLog, AlpacaOCOGroup, AlpacaOCOStatus
)

logger = logging.getLogger(__name__)


def _is_regular_trading_hours() -> bool:
    try:
        import pytz
        from datetime import time as dt_time
        et_tz = pytz.timezone('America/New_York')
        now_et = datetime.now(et_tz)
        if now_et.weekday() > 4:
            return False
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        current_time = now_et.time()
        is_regular = market_open <= current_time <= market_close
        logger.debug(f"Alpaca market hours check: {now_et.strftime('%H:%M %Z')}, regular={is_regular}")
        return is_regular
    except Exception as e:
        logger.warning(f"Error checking trading hours: {e}, defaulting to extended hours (limit order)")
        return False


PROGRESSIVE_TIERS = [
    {'profit_pct': 1.0, 'stop_pct': 0.0, 'label': 'Tier 1: Breakeven'},
    {'profit_pct': 2.0, 'stop_pct': 0.5, 'label': 'Tier 2: Lock 0.5%'},
    {'profit_pct': 3.0, 'stop_pct': 1.5, 'label': 'Tier 3: Lock 1.5%'},
    {'profit_pct': 4.0, 'stop_pct': 2.5, 'label': 'Tier 4: Lock 2.5%'},
    {'profit_pct': 5.0, 'stop_pct': 3.5, 'label': 'Tier 5: Lock 3.5%'},
    {'profit_pct': 6.0, 'stop_pct': 4.5, 'label': 'Tier 6: Lock 4.5%'},
    {'profit_pct': 7.0, 'stop_pct': 5.5, 'label': 'Tier 7: Lock 5.5%'},
    {'profit_pct': 8.0, 'stop_pct': 6.5, 'label': 'Tier 8: Lock 6.5%'},
]


class BarsCache:
    def __init__(self, cache_duration_seconds: int = 300):
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._cache_duration = timedelta(seconds=cache_duration_seconds)

    def get(self, symbol: str, timeframe: str) -> Optional[List[Dict]]:
        key = f"{symbol}_{timeframe}"
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if datetime.now() - entry['timestamp'] < self._cache_duration:
                    return entry['bars']
                del self._cache[key]
        return None

    def set(self, symbol: str, timeframe: str, bars: List[Dict]):
        key = f"{symbol}_{timeframe}"
        with self._lock:
            self._cache[key] = {'bars': bars, 'timestamp': datetime.now()}

    def invalidate(self, symbol: str = None):
        with self._lock:
            if symbol:
                keys = [k for k in self._cache if k.startswith(f"{symbol}_")]
                for k in keys:
                    del self._cache[k]
            else:
                self._cache.clear()


_bars_cache = BarsCache(cache_duration_seconds=300)


def get_trailing_stop_config() -> AlpacaTrailingStopConfig:
    config = AlpacaTrailingStopConfig.query.first()
    if not config:
        config = AlpacaTrailingStopConfig()
        db.session.add(config)
        db.session.commit()
    return config


def was_manually_deactivated_alpaca(symbol: str) -> bool:
    """Check if an Alpaca trailing stop for this symbol was manually deactivated."""
    manual_ts = AlpacaTrailingStopPosition.query.filter(
        AlpacaTrailingStopPosition.symbol == symbol,
        AlpacaTrailingStopPosition.is_active == False,
        db.or_(
            AlpacaTrailingStopPosition.trigger_reason.ilike('%手动停用%'),
            AlpacaTrailingStopPosition.trigger_reason.ilike('%manually deactivated%'),
            AlpacaTrailingStopPosition.trigger_reason.ilike('%manual_deactivat%'),
        ),
    ).order_by(AlpacaTrailingStopPosition.updated_at.desc()).first()
    
    if manual_ts:
        newer_active = AlpacaTrailingStopPosition.query.filter(
            AlpacaTrailingStopPosition.symbol == symbol,
            AlpacaTrailingStopPosition.created_at > manual_ts.updated_at,
            AlpacaTrailingStopPosition.is_active == True,
        ).first()
        if newer_active:
            return False
        logger.info(f"⛔ [{symbol}] Was manually deactivated (Alpaca TS #{manual_ts.id}), blocking auto-recreation")
        return True
    return False


def create_trailing_stop_for_entry(
    symbol: str,
    side: str,
    entry_price: float,
    quantity: float,
    stop_loss_price: float = None,
    take_profit_price: float = None,
    trade_id: int = None,
    timeframe: str = None,
    from_reconciliation: bool = False,
) -> Optional[AlpacaTrailingStopPosition]:
    config = get_trailing_stop_config()
    if not config.is_enabled:
        logger.info(f"Trailing stop disabled, skipping for {symbol}")
        return None

    if from_reconciliation and was_manually_deactivated_alpaca(symbol):
        logger.warning(f"⛔ [{symbol}] Skipping auto TS creation (reconciliation): manually deactivated")
        return None

    existing = AlpacaTrailingStopPosition.query.filter_by(
        symbol=symbol,
        is_active=True
    ).first()
    if existing:
        if from_reconciliation and existing.side != side:
            logger.info(f"[{symbol}] Reconciliation: active TS #{existing.id} has wrong side ({existing.side} vs {side}), deactivating for re-creation")
            existing.is_active = False
            existing.is_triggered = True
            existing.triggered_at = datetime.utcnow()
            existing.trigger_reason = f"Deactivated by reconciliation: side mismatch ({existing.side} → {side})"
            db.session.flush()
        else:
            from alpaca.position_service import find_open_position, link_trailing_stop_to_position
            open_pos = find_open_position(symbol)
            if open_pos and not open_pos.trailing_stop_id:
                link_trailing_stop_to_position(open_pos, existing.id)
                logger.info(f"Active trailing stop #{existing.id} exists for {symbol}, linked to position #{open_pos.id}")
            else:
                logger.info(f"Active trailing stop already exists for {symbol}: #{existing.id}")
            return existing

    if trade_id:
        any_ts_for_trade = AlpacaTrailingStopPosition.query.filter_by(
            trade_id=trade_id
        ).first()
        if any_ts_for_trade:
            logger.info(f"Trailing stop already exists for trade #{trade_id} ({symbol}): TS #{any_ts_for_trade.id} (active={any_ts_for_trade.is_active}), skipping duplicate creation")
            return any_ts_for_trade if any_ts_for_trade.is_active else None

    if not from_reconciliation:
        any_triggered = AlpacaTrailingStopPosition.query.filter(
            AlpacaTrailingStopPosition.symbol == symbol,
            AlpacaTrailingStopPosition.is_active == False,
            AlpacaTrailingStopPosition.is_triggered == True,
        ).order_by(AlpacaTrailingStopPosition.triggered_at.desc()).first()

        if any_triggered:
            if any_triggered.triggered_at and any_triggered.triggered_at >= (datetime.utcnow() - timedelta(minutes=10)):
                logger.warning(f"⚠️ Skipping trailing stop creation for {symbol}: "
                              f"recently triggered {(datetime.utcnow() - any_triggered.triggered_at).seconds}s ago")
                return None

            is_ghost_closed = any_triggered.trigger_reason and 'ghost' in any_triggered.trigger_reason.lower()
            if is_ghost_closed and any_triggered.triggered_at and any_triggered.triggered_at >= (datetime.utcnow() - timedelta(minutes=30)):
                logger.warning(f"⚠️ Skipping trailing stop creation for {symbol}: "
                              f"ghost-closed {(datetime.utcnow() - any_triggered.triggered_at).seconds}s ago, "
                              f"waiting 30min cooldown")
                return None

            from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole
            recent_cutoff = datetime.utcnow() - timedelta(minutes=30)
            pending_close = AlpacaOrderTracker.query.filter(
                AlpacaOrderTracker.symbol == symbol,
                AlpacaOrderTracker.role.in_([AlpacaOrderRole.EXIT_TRAILING, AlpacaOrderRole.EXIT_SIGNAL]),
                AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD', 'PARTIALLY_FILLED']),
                AlpacaOrderTracker.created_at >= recent_cutoff,
            ).first()

            if pending_close:
                logger.warning(f"⚠️ Skipping trailing stop creation for {symbol}: "
                              f"recent pending exit order exists ({pending_close.alpaca_order_id}, "
                              f"created {pending_close.created_at})")
                return None

            from alpaca.position_service import find_open_position
            open_pos = find_open_position(symbol)
            if not open_pos:
                logger.warning(f"⚠️ Skipping trailing stop creation for {symbol}: "
                              f"previously triggered and no OPEN position found "
                              f"(triggered: {any_triggered.trigger_reason})")
                return None
    else:
        logger.info(f"[{symbol}] Reconciliation-driven TS creation: bypassing cooldown checks (order+position dual confirmed)")

    if stop_loss_price is not None and entry_price and entry_price > 0:
        is_long = side == 'long'
        if (is_long and stop_loss_price >= entry_price) or (not is_long and stop_loss_price <= entry_price):
            logger.warning(f"⚠️ [{symbol}] Invalid SL for {'LONG' if is_long else 'SHORT'}: "
                          f"SL ${stop_loss_price:.2f} vs entry ${entry_price:.2f}, discarding SL")
            stop_loss_price = None

    if take_profit_price is not None and entry_price and entry_price > 0:
        is_long = side == 'long'
        if (is_long and take_profit_price <= entry_price) or (not is_long and take_profit_price >= entry_price):
            logger.warning(f"⚠️ [{symbol}] Invalid TP for {'LONG' if is_long else 'SHORT'}: "
                          f"TP ${take_profit_price:.2f} vs entry ${entry_price:.2f}, discarding TP")
            take_profit_price = None

    initial_stop = stop_loss_price
    if not initial_stop:
        if side == 'long':
            initial_stop = round(entry_price * (1 - config.initial_stop_pct), 2)
        else:
            initial_stop = round(entry_price * (1 + config.initial_stop_pct), 2)

    live_price = _fetch_current_price(symbol)
    init_price = live_price if live_price else entry_price

    if side == 'long':
        init_highest = max(entry_price, init_price)
    else:
        init_highest = entry_price

    ts_position = AlpacaTrailingStopPosition(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        first_entry_price=entry_price,
        quantity=quantity,
        current_price=init_price,
        highest_price=init_highest,
        lowest_price=min(entry_price, init_price) if side == 'short' else entry_price,
        stop_loss_price=initial_stop,
        signal_stop_loss=stop_loss_price,
        take_profit_price=take_profit_price,
        trailing_stop_price=initial_stop,
        is_dynamic=False,
        phase='progressive',
        is_active=True,
        trade_id=trade_id,
        timeframe=timeframe,
    )
    db.session.add(ts_position)
    db.session.commit()

    _log_event(ts_position.id, 'created', init_price, init_highest, initial_stop,
               details=f"Entry=${entry_price}, SL=${initial_stop}, TP={take_profit_price}")

    logger.info(f"Created trailing stop #{ts_position.id} for {symbol}: "
                f"entry=${entry_price}, stop=${initial_stop}")

    try:
        from watchlist_service import on_signal_received
        on_signal_received(symbol, source_broker='alpaca')
    except Exception as e:
        logger.debug(f"Could not auto-add {symbol} to watchlist: {e}")

    from alpaca.position_service import find_open_position, link_trailing_stop_to_position
    position = find_open_position(symbol)
    if position:
        link_trailing_stop_to_position(position, ts_position.id)

    return ts_position


def update_trailing_stop_on_scaling(
    symbol: str,
    new_quantity: float,
    new_entry_price: float,
    new_stop_loss: float = None,
    new_take_profit: float = None,
):
    ts_pos = AlpacaTrailingStopPosition.query.filter_by(
        symbol=symbol, is_active=True
    ).first()
    if not ts_pos:
        return

    ts_pos.quantity = new_quantity
    ts_pos.entry_price = new_entry_price
    if new_stop_loss:
        ts_pos.stop_loss_price = new_stop_loss
        ts_pos.trailing_stop_price = new_stop_loss
    if new_take_profit:
        ts_pos.take_profit_price = new_take_profit

    _log_event(ts_pos.id, 'scaling_update', ts_pos.current_price,
               ts_pos.highest_price, ts_pos.trailing_stop_price,
               details=f"Qty={new_quantity}, AvgEntry=${new_entry_price}")
    db.session.flush()


def process_trailing_stop(ts_position: AlpacaTrailingStopPosition, prefetched_price: float = None) -> Dict:
    if not ts_position.is_active:
        return {'action': 'skip', 'reason': 'inactive'}

    from alpaca.position_service import find_open_position
    open_pos = find_open_position(ts_position.symbol)
    if not open_pos:
        from alpaca.models import AlpacaPosition, AlpacaPositionStatus
        any_pos = AlpacaPosition.query.filter_by(
            symbol=ts_position.symbol
        ).order_by(AlpacaPosition.id.desc()).first()
        if any_pos and any_pos.status == AlpacaPositionStatus.CLOSED:
            ts_position.is_active = False
            ts_position.is_triggered = True
            ts_position.triggered_at = datetime.utcnow()
            ts_position.trigger_reason = f"Position #{any_pos.id} already CLOSED"
            db.session.commit()
            logger.warning(f"⚠️ [{ts_position.symbol}] Deactivated TS #{ts_position.id}: position #{any_pos.id} already CLOSED")
            return {'action': 'deactivated', 'reason': 'position_closed'}
        if not any_pos:
            ts_position.is_active = False
            ts_position.is_triggered = True
            ts_position.triggered_at = datetime.utcnow()
            ts_position.trigger_reason = "No position record found in DB"
            db.session.commit()
            logger.warning(f"⚠️ [{ts_position.symbol}] Deactivated TS #{ts_position.id}: no position record exists")
            return {'action': 'deactivated', 'reason': 'no_position_record'}

    current_price = prefetched_price if prefetched_price is not None else _fetch_current_price(ts_position.symbol)
    if current_price is None or current_price <= 0:
        logger.warning(f"⚠️ No price available for {ts_position.symbol}, all sources failed")
        return {'action': 'skip', 'reason': 'no_price'}

    ts_position.current_price = current_price

    side = ts_position.side
    if side == 'long':
        if current_price > (ts_position.highest_price or 0):
            ts_position.highest_price = current_price
    else:
        if ts_position.lowest_price is None or current_price < ts_position.lowest_price:
            ts_position.lowest_price = current_price

    profit_pct = _calc_profit_pct(ts_position)

    config = get_trailing_stop_config()
    signal_tf = ts_position.timeframe
    atr = _fetch_atr(ts_position.symbol, config.atr_period, timeframe=signal_tf)
    bars = _fetch_bars_for_trend(ts_position.symbol, config, timeframe=signal_tf)

    trend_data = {'trend_strength': 0.0}
    if bars and atr and atr > 0:
        trend_data = calculate_trend_strength(
            bars, current_price, ts_position.entry_price,
            ts_position.side, atr, config
        )

    trend_strength = trend_data['trend_strength']
    ts_position.trend_score = trend_strength

    if _should_trigger_stop(ts_position, current_price):
        return _trigger_trailing_stop(ts_position, current_price, profit_pct)

    if ts_position.trigger_reason and ts_position.trigger_reason.startswith('pending_exit:'):
        _cancel_pending_exit_on_recovery(ts_position, current_price)

    if not ts_position.is_dynamic:
        inverse_enabled = getattr(config, 'inverse_protection_enabled', True)
        if inverse_enabled and profit_pct < 0:
            inverse_stop, inverse_details = calculate_inverse_protection_stop(
                ts_position, current_price, trend_strength, config
            )
            if inverse_stop is not None and inverse_details.get('action') == 'tighten':
                old_stop = ts_position.trailing_stop_price
                ts_position.trailing_stop_price = inverse_stop
                ts_position.stop_loss_price = inverse_stop

                _log_event(ts_position.id, 'inverse_protection', current_price,
                           ts_position.highest_price, inverse_stop,
                           profit_pct=profit_pct,
                           details=f"反向保护: 止损收紧至${inverse_stop:.2f} "
                                   f"(趋势{trend_strength:.0f}, "
                                   f"收紧系数{inverse_details['tightening_factor']})")

                _update_oco_stop(ts_position, inverse_stop)
                db.session.flush()

                logger.info(f"[{ts_position.symbol}] 反向保护触发: ${old_stop} -> ${inverse_stop} "
                           f"趋势强度{trend_strength:.0f}")

                try:
                    from alpaca.db_logger import log_info as _db_log_info
                    _db_log_info('trailing_stop', f'{ts_position.symbol} inverse protection: trend={trend_strength:.0f}', category='inverse', symbol=ts_position.symbol, extra_data={'old_stop': old_stop, 'new_stop': inverse_stop, 'tightening_factor': inverse_details['tightening_factor']})
                except Exception:
                    pass

                return {'action': 'inverse_protection', 'old_stop': old_stop, 'new_stop': inverse_stop,
                        'trend_strength': trend_strength, 'tightening_factor': inverse_details['tightening_factor']}

    if ts_position.is_dynamic:
        return _process_dynamic_trailing(ts_position, current_price, profit_pct)
    else:
        return _process_progressive_trailing(ts_position, current_price, profit_pct, trend_strength)


def _calc_profit_pct(ts: AlpacaTrailingStopPosition) -> float:
    if not ts.entry_price or ts.entry_price <= 0:
        return 0
    if ts.side == 'long':
        return ((ts.current_price - ts.entry_price) / ts.entry_price) * 100
    else:
        return ((ts.entry_price - ts.current_price) / ts.entry_price) * 100


def _should_trigger_stop(ts: AlpacaTrailingStopPosition, price: float) -> bool:
    if not ts.trailing_stop_price:
        return False
    if ts.side == 'long':
        return price <= ts.trailing_stop_price
    else:
        return price >= ts.trailing_stop_price


def _trigger_trailing_stop(ts: AlpacaTrailingStopPosition, price: float, profit_pct: float) -> Dict:
    """Trigger trailing stop exit. TS stays ACTIVE until exit order FILLS.

    Key design: TS is NOT deactivated when placing exit order. Instead:
    - TS remains is_active=True with trigger_reason='pending_exit:{order_id}'
    - Each cycle: if stop still breached, cancel stale exit order and re-place at current price
    - Only deactivated when EXIT_TRAILING order fills (via trade stream / order tracker)
    """
    logger.info(f"TRAILING STOP TRIGGERED for {ts.symbol}: "
                f"price=${price}, stop=${ts.trailing_stop_price}, profit={profit_pct:.2f}%")

    MAX_TRIGGER_RETRIES = 5

    ts.triggered_price = price
    if not ts.trigger_retry_count or ts.trigger_retry_count <= 0:
        ts.trigger_retry_count = 0
    db.session.flush()

    from alpaca.client import AlpacaClient
    client = AlpacaClient()

    position = client.get_position(ts.symbol)
    if position is None:
        retry_count = (ts.trigger_retry_count or 0) + 1
        if retry_count >= MAX_TRIGGER_RETRIES:
            ts.is_active = False
            ts.is_triggered = True
            ts.triggered_at = datetime.utcnow()
            ts.trigger_reason = f"Deactivated: API error after {retry_count} retries"
            ts.trigger_retry_count = retry_count
            db.session.commit()
            logger.error(f"⚠️ [{ts.symbol}] Max retries ({MAX_TRIGGER_RETRIES}) reached for API error, permanently deactivating trailing stop")
            return {'action': 'deactivated', 'reason': 'max_retries_api_error'}
        ts.trigger_retry_count = retry_count
        db.session.commit()
        logger.warning(f"⚠️ [{ts.symbol}] Alpaca API error checking position, will retry next cycle (attempt {retry_count}/{MAX_TRIGGER_RETRIES})")
        return {'action': 'trigger_failed', 'reason': 'api_error'}

    if position.get('_no_position') or position.get('symbol') is None:
        fallback_position = None
        try:
            all_positions = client.get_positions()
            for p in (all_positions or []):
                if p.get('symbol') == ts.symbol:
                    fallback_position = p
                    break
        except Exception as fb_err:
            logger.warning(f"[{ts.symbol}] get_positions fallback also failed: {fb_err}")

        if fallback_position:
            logger.warning(f"⚠️ [{ts.symbol}] get_position returned 404 but get_positions found the position! Using fallback.")
            position = fallback_position
        else:
            ghost_retry_count = (ts.trigger_retry_count or 0) + 1
            MAX_GHOST_RETRIES = 2
            if ghost_retry_count < MAX_GHOST_RETRIES:
                ts.trigger_retry_count = ghost_retry_count
                db.session.commit()
                logger.warning(f"⚠️ [{ts.symbol}] No position found via both APIs, will retry next cycle (attempt {ghost_retry_count}/{MAX_GHOST_RETRIES})")
                return {'action': 'trigger_failed', 'reason': 'no_position_retry'}

            ts.is_active = False
            ts.is_triggered = True
            ts.triggered_at = datetime.utcnow()
            ts.trigger_reason = f"No position found in Alpaca after {ghost_retry_count} checks (ghost prevention)"
            logger.warning(f"⚠️ [{ts.symbol}] No open position confirmed after {ghost_retry_count} retries, deactivating trailing stop")
            _log_event(ts.id, 'trigger_skipped', price, ts.highest_price, ts.trailing_stop_price,
                       profit_pct=profit_pct,
                       details=f"No position found after {ghost_retry_count} retries (ghost prevention)")

            try:
                from alpaca.position_service import find_open_position, add_exit_leg
                from alpaca.models import AlpacaExitMethod, AlpacaOrderRole
                ghost_position = find_open_position(ts.symbol)
                if ghost_position:
                    remaining = ghost_position.total_entry_quantity - (ghost_position.total_exit_quantity or 0)
                    if remaining > 0.001:
                        ghost_exit_id = f"ghost_prevention_{ghost_position.id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                        add_exit_leg(
                            position=ghost_position,
                            alpaca_order_id=ghost_exit_id,
                            price=price,
                            quantity=remaining,
                            filled_at=datetime.utcnow(),
                            exit_method=AlpacaExitMethod.EXTERNAL,
                        )
                        try:
                            from alpaca.order_tracker import ensure_tracker_for_fill
                            exit_side = 'sell' if ghost_position.side == 'long' else 'buy'
                            ensure_tracker_for_fill(
                                alpaca_order_id=ghost_exit_id,
                                symbol=ts.symbol,
                                role=AlpacaOrderRole.EXIT_SIGNAL,
                                side=exit_side,
                                quantity=remaining,
                                fill_price=price,
                                source='ghost_prevention',
                            )
                        except Exception:
                            pass
                        logger.info(f"✅ [{ts.symbol}] Ghost position #{ghost_position.id} closed: {remaining} shares @ ${price:.2f} (ghost prevention)")
                    else:
                        logger.info(f"[{ts.symbol}] Ghost position #{ghost_position.id} already fully exited")
            except Exception as gpe:
                logger.error(f"[{ts.symbol}] Error closing ghost position: {gpe}")

            db.session.commit()
            try:
                from alpaca.db_logger import log_warning
                log_warning('trailing_stop', f'{ts.symbol} deactivated: no position after {ghost_retry_count} retries (ghost prevention)', category='trigger', symbol=ts.symbol)
            except Exception:
                pass
            return {'action': 'deactivated_no_position', 'reason': 'no_position'}

    position_qty = abs(float(position.get('qty', 0)))
    position_side_long = float(position.get('qty', 0)) > 0
    ts_side_long = ts.side == 'long'

    if position_side_long != ts_side_long:
        logger.warning(f"⚠️ [{ts.symbol}] Position side mismatch: Alpaca={'long' if position_side_long else 'short'}, TS={ts.side}, skipping exit")
        _log_event(ts.id, 'trigger_skipped', price, ts.highest_price, ts.trailing_stop_price,
                   profit_pct=profit_pct,
                   details=f"Position side mismatch: Alpaca={'long' if position_side_long else 'short'}, TS={ts.side}")
        return {'action': 'trigger_skipped', 'reason': 'side_mismatch'}

    from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole
    pending_exit = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.symbol == ts.symbol,
        AlpacaOrderTracker.role == AlpacaOrderRole.EXIT_TRAILING,
        AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD', 'PARTIALLY_FILLED']),
    ).first()

    if pending_exit:
        old_limit = pending_exit.limit_price
        is_regular = _is_regular_trading_hours()
        if is_regular:
            new_limit_price = None
        else:
            exit_side = 'sell' if ts.side == 'long' else 'buy'
            new_limit_price = round(price * (0.998 if exit_side == 'sell' else 1.002), 2)

        needs_replace = False
        if is_regular and pending_exit.order_type == 'limit':
            needs_replace = True
            logger.info(f"[{ts.symbol}] 交易时段已变为盘中，需要将限价单转为市价单")
        elif not is_regular and old_limit and new_limit_price:
            price_diff_pct = abs(new_limit_price - old_limit) / old_limit * 100 if old_limit > 0 else 100
            if price_diff_pct > 0.3:
                needs_replace = True
                logger.info(f"[{ts.symbol}] 限价偏离 {price_diff_pct:.1f}%: old=${old_limit} → new=${new_limit_price}")

        if needs_replace:
            try:
                cancel_result = client._request('DELETE', f'/v2/orders/{pending_exit.alpaca_order_id}')
                logger.info(f"[{ts.symbol}] 取消旧退出订单 {pending_exit.alpaca_order_id[:12]}...")
                from alpaca.order_tracker import update_order_status
                update_order_status(alpaca_order_id=pending_exit.alpaca_order_id, status='CANCELLED')
                db.session.flush()
                import time
                time.sleep(0.5)
            except Exception as cancel_err:
                logger.warning(f"[{ts.symbol}] 取消旧退出订单失败: {cancel_err}, 保持现有订单")
                return {'action': 'trigger_skipped', 'reason': 'cancel_failed_keep_existing'}
        else:
            logger.debug(f"[{ts.symbol}] 现有退出订单价格仍然合理，保持不变: {pending_exit.alpaca_order_id[:12]}...")
            return {'action': 'trigger_skipped', 'reason': 'pending_exit_price_ok'}

    try:
        from alpaca.db_logger import log_warning
        is_retrigger = pending_exit is not None
        msg = f'{ts.symbol} stop {"re-triggered" if is_retrigger else "triggered"} @ ${price}'
        log_warning('trailing_stop', msg, category='trigger', symbol=ts.symbol, extra_data={'stop_price': ts.trailing_stop_price, 'profit_pct': round(profit_pct, 2), 'phase': ts.phase, 'entry_price': ts.entry_price, 'is_retrigger': is_retrigger})
    except Exception:
        pass

    _log_event(ts.id, 'triggered', price, ts.highest_price, ts.trailing_stop_price,
               profit_pct=profit_pct,
               details=f"Stop triggered at ${price}, phase={ts.phase}")

    exit_side = 'sell' if ts.side == 'long' else 'buy'
    if position_qty > ts.quantity + 0.01:
        logger.warning(f"[{ts.symbol}] TS quantity ({ts.quantity}) < Alpaca position ({position_qty}), using full Alpaca qty for exit")
        exit_qty = position_qty
    else:
        exit_qty = min(ts.quantity, position_qty)

    try:
        if not pending_exit:
            _cancel_active_oco(ts.symbol)

        is_regular = _is_regular_trading_hours()
        qty_str = str(int(exit_qty)) if float(exit_qty) == int(float(exit_qty)) else str(exit_qty)

        if is_regular:
            order_data = {
                'symbol': ts.symbol,
                'qty': qty_str,
                'side': exit_side,
                'type': 'market',
                'time_in_force': 'day',
            }
            logger.info(f"[{ts.symbol}] 盘中时段 → 使用市价单: side={exit_side}, qty={qty_str}")
        else:
            limit_price = str(round(price * (0.998 if exit_side == 'sell' else 1.002), 2))
            order_data = {
                'symbol': ts.symbol,
                'qty': qty_str,
                'side': exit_side,
                'type': 'limit',
                'time_in_force': 'gtc',
                'limit_price': limit_price,
                'extended_hours': True,
            }
            logger.info(f"[{ts.symbol}] 盘前盘后 → 使用限价单: side={exit_side}, qty={qty_str}, limit=${limit_price}")

        result = client._request('POST', '/v2/orders', data=order_data)
        order_id = result.get('id')

        if order_id:
            from alpaca.order_tracker import register_order
            order_type = 'market' if is_regular else 'limit'
            register_order(
                alpaca_order_id=order_id,
                symbol=ts.symbol,
                role=AlpacaOrderRole.EXIT_TRAILING,
                side=exit_side,
                quantity=exit_qty,
                order_type=order_type,
                limit_price=float(order_data.get('limit_price', 0)) if order_data.get('limit_price') else None,
                trailing_stop_id=ts.id,
            )

        ts.trigger_reason = f"pending_exit:{order_id or 'unknown'}"
        ts.triggered_at = datetime.utcnow()
        db.session.commit()
        logger.info(f"[{ts.symbol}] TS stays ACTIVE with pending exit order {order_id}, will re-check each cycle")

        try:
            from alpaca.discord_notifier import alpaca_discord
            alpaca_discord.send_trailing_stop_notification(
                ts.symbol, 'trigger', price, ts.entry_price, profit_pct / 100,
                f"Stop=${ts.trailing_stop_price:.2f}, Phase={ts.phase}"
            )
        except Exception as de:
            logger.debug(f"Discord notification error: {de}")

        return {'action': 'triggered', 'order_id': order_id, 'price': price}

    except Exception as e:
        retry_count = (ts.trigger_retry_count or 0) + 1
        if retry_count >= MAX_TRIGGER_RETRIES:
            ts.is_active = False
            ts.is_triggered = True
            ts.triggered_at = datetime.utcnow()
            ts.trigger_reason = f"Deactivated: order placement failed after {retry_count} retries ({str(e)[:100]})"
            ts.trigger_retry_count = retry_count
            logger.error(f"⚠️ [{ts.symbol}] Max retries ({MAX_TRIGGER_RETRIES}) reached for order error, permanently deactivating: {e}")

            try:
                from alpaca.position_service import find_open_position, add_exit_leg
                from alpaca.models import AlpacaExitMethod
                failed_pos = find_open_position(ts.symbol)
                if failed_pos:
                    try:
                        alpaca_position = client.get_position(ts.symbol)
                    except Exception:
                        alpaca_position = None

                    if not alpaca_position:
                        remaining = failed_pos.total_entry_quantity - (failed_pos.total_exit_quantity or 0)
                        if remaining > 0.001:
                            add_exit_leg(
                                position=failed_pos,
                                price=price,
                                quantity=remaining,
                                filled_at=datetime.utcnow(),
                                exit_method=AlpacaExitMethod.EXTERNAL,
                            )
                            logger.info(f"✅ [{ts.symbol}] Position #{failed_pos.id} closed after max retries (no Alpaca position)")
            except Exception as close_err:
                logger.error(f"[{ts.symbol}] Error closing position after max retries: {close_err}")

            db.session.commit()
            try:
                from alpaca.db_logger import log_error
                log_error('trailing_stop', f'{ts.symbol} permanently deactivated after {retry_count} retries: {str(e)[:200]}', category='error', symbol=ts.symbol)
            except Exception:
                pass
            return {'action': 'deactivated', 'reason': 'max_retries_order_error', 'error': str(e)}
        try:
            logger.info(f"[{ts.symbol}] Retry {retry_count}: re-cancelling all open orders before next attempt")
            _cancel_active_oco(ts.symbol)
            import time
            time.sleep(1)
        except Exception as cancel_err:
            logger.warning(f"[{ts.symbol}] Error re-cancelling orders during retry: {cancel_err}")
        ts.trigger_retry_count = retry_count
        db.session.commit()
        logger.error(f"Error triggering trailing stop for {ts.symbol}: {e}, will retry next cycle (attempt {retry_count}/{MAX_TRIGGER_RETRIES})")
        try:
            from alpaca.db_logger import log_error
            log_error('trailing_stop', f'{ts.symbol} trigger error (retry {retry_count}/{MAX_TRIGGER_RETRIES}): {str(e)}', category='error', symbol=ts.symbol)
        except Exception:
            pass
        return {'action': 'trigger_failed', 'error': str(e)}


def _cancel_pending_exit_on_recovery(ts: AlpacaTrailingStopPosition, price: float):
    """Price recovered above stop level while exit order is pending. Cancel exit and resume trailing."""
    pending_order_id = ts.trigger_reason.replace('pending_exit:', '')
    logger.info(f"[{ts.symbol}] Price ${price} recovered above stop ${ts.trailing_stop_price}, "
                f"cancelling pending exit order {pending_order_id[:12]}... and resuming trailing")

    from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole
    pending_exit = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.symbol == ts.symbol,
        AlpacaOrderTracker.role == AlpacaOrderRole.EXIT_TRAILING,
        AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD', 'PARTIALLY_FILLED']),
    ).first()

    if pending_exit:
        try:
            from alpaca.client import AlpacaClient
            client = AlpacaClient()
            client._request('DELETE', f'/v2/orders/{pending_exit.alpaca_order_id}')
            logger.info(f"[{ts.symbol}] 已取消退出订单 {pending_exit.alpaca_order_id[:12]}...")
            from alpaca.order_tracker import update_order_status
            update_order_status(alpaca_order_id=pending_exit.alpaca_order_id, status='CANCELLED')
        except Exception as e:
            logger.warning(f"[{ts.symbol}] 取消退出订单失败: {e}")

    ts.trigger_reason = None
    ts.triggered_at = None
    ts.triggered_price = None
    ts.trigger_retry_count = 0
    db.session.commit()

    _log_event(ts.id, 'exit_cancelled_recovery', price, ts.highest_price, ts.trailing_stop_price,
               details=f"Price recovered to ${price:.2f}, cancelled exit order, resumed trailing")

    try:
        from alpaca.db_logger import log_info
        log_info('trailing_stop', f'{ts.symbol} price recovered to ${price:.2f}, exit cancelled, trailing resumed',
                 category='recovery', symbol=ts.symbol)
    except Exception:
        pass


def _process_progressive_trailing(ts: AlpacaTrailingStopPosition, price: float, profit_pct: float, trend_strength: float = 0.0) -> Dict:
    should_switch, switch_reason = _should_switch_to_dynamic(ts, profit_pct, trend_strength)
    if should_switch:
        ts.is_dynamic = True
        ts.phase = 'dynamic'
        _log_event(ts.id, 'switch_to_dynamic', price, ts.highest_price,
                   ts.trailing_stop_price, profit_pct=profit_pct,
                   details=f"{switch_reason} | trend={trend_strength:.0f}")

        try:
            from alpaca.db_logger import log_info as _db_log_info
            _db_log_info('trailing_stop', f'{ts.symbol} switched to dynamic: {switch_reason}', category='switch', symbol=ts.symbol)
        except Exception:
            pass

        try:
            from alpaca.discord_notifier import alpaca_discord
            alpaca_discord.send_trailing_stop_notification(
                ts.symbol, 'switch', price, ts.entry_price, profit_pct / 100,
                f"{switch_reason}"
            )
        except Exception as de:
            logger.debug(f"Discord notification error: {de}")

        return _process_dynamic_trailing(ts, price, profit_pct)

    new_stop = _calc_progressive_stop(ts, profit_pct)

    if new_stop and new_stop > (ts.trailing_stop_price or 0) if ts.side == 'long' else \
       new_stop and new_stop < (ts.trailing_stop_price or float('inf')):
        old_stop = ts.trailing_stop_price
        ts.trailing_stop_price = new_stop
        ts.stop_loss_price = new_stop

        _log_event(ts.id, 'tier_upgrade', price, ts.highest_price, new_stop,
                   profit_pct=profit_pct,
                   details=f"Progressive stop: ${old_stop} -> ${new_stop}")

        try:
            from alpaca.db_logger import log_info as _db_log_info
            _db_log_info('trailing_stop', f'{ts.symbol} tier upgrade: ${old_stop} -> ${new_stop}', category='tier', symbol=ts.symbol, extra_data={'old_stop': old_stop, 'new_stop': new_stop, 'profit_pct': round(profit_pct, 2)})
        except Exception:
            pass

        _update_oco_stop(ts, new_stop)
        db.session.flush()

        return {'action': 'tier_upgrade', 'old_stop': old_stop, 'new_stop': new_stop}

    db.session.flush()
    return {'action': 'hold', 'profit_pct': profit_pct}


def _calc_progressive_stop(ts: AlpacaTrailingStopPosition, profit_pct: float) -> Optional[float]:
    applicable_tier = None
    for tier in PROGRESSIVE_TIERS:
        if profit_pct >= tier['profit_pct']:
            applicable_tier = tier

    if not applicable_tier:
        return None

    if ts.side == 'long':
        new_stop = round(ts.entry_price * (1 + applicable_tier['stop_pct'] / 100), 2)
    else:
        new_stop = round(ts.entry_price * (1 - applicable_tier['stop_pct'] / 100), 2)

    current_stop = ts.trailing_stop_price or 0
    if ts.side == 'long':
        return new_stop if new_stop > current_stop else None
    else:
        return new_stop if (current_stop == 0 or new_stop < current_stop) else None


def _should_switch_to_dynamic(ts: AlpacaTrailingStopPosition, profit_pct: float, trend_strength: float = None) -> Tuple[bool, str]:
    config = get_trailing_stop_config()

    if trend_strength is None:
        trend_strength = ts.trend_score or 0.0

    profit_ratio = profit_pct / 100.0

    switch_force_profit = getattr(config, 'switch_force_profit', 0.10)
    if profit_ratio >= switch_force_profit:
        return True, f"[条件C] 利润{profit_pct:.1f}% >= {switch_force_profit*100:.0f}% 强制切换"

    switch_profit_threshold = getattr(config, 'switch_profit_threshold', 0.05)
    ts_threshold = getattr(config, 'trend_strength_threshold', 60.0)
    if profit_ratio >= switch_profit_threshold and trend_strength >= ts_threshold:
        return True, f"[条件B] 利润{profit_pct:.1f}% + 趋势强度{trend_strength:.0f} 触发切换"

    if ts.take_profit_price and ts.entry_price:
        if ts.side == 'long':
            planned_profit = ts.take_profit_price - ts.entry_price
            current_profit = ts.current_price - ts.entry_price
        else:
            planned_profit = ts.entry_price - ts.take_profit_price
            current_profit = ts.entry_price - ts.current_price

        if planned_profit > 0:
            profit_achieved_ratio = current_profit / planned_profit

            if trend_strength >= ts_threshold:
                switch_ratio = getattr(config, 'switch_profit_ratio_strong', 0.85)
                label = "强势趋势"
            else:
                switch_ratio = getattr(config, 'switch_profit_ratio', 0.90)
                label = "普通趋势"

            if profit_achieved_ratio >= switch_ratio:
                return True, f"[条件A-{label}] 达到 {profit_achieved_ratio*100:.1f}% 计划利润"

            return False, f"[{label}] 利润{profit_pct:.1f}%, 计划利润进度{profit_achieved_ratio*100:.1f}%"

    return False, f"利润{profit_pct:.1f}%, 趋势{trend_strength:.0f}, 未达切换条件"


def _process_dynamic_trailing(ts: AlpacaTrailingStopPosition, price: float, profit_pct: float) -> Dict:
    config = get_trailing_stop_config()
    is_long = ts.side == 'long'

    if is_long:
        cost_distance_ratio = (price - ts.entry_price) / price if price > 0 else 0
    else:
        cost_distance_ratio = (ts.entry_price - price) / price if price > 0 else 0

    cost_threshold = getattr(config, 'cost_distance_threshold', 0.02)
    cost_tighten = cost_distance_ratio < cost_threshold and cost_distance_ratio > 0

    if cost_tighten:
        effective_atr_mult = getattr(config, 'cost_tighten_atr_multiplier', 0.6)
        effective_trail_pct = getattr(config, 'cost_tighten_trail_pct', 0.005)
        logger.debug(f"🔧 {ts.symbol} 收紧trailing: 成本距离{cost_distance_ratio*100:.2f}% < {cost_threshold*100}%")

        atr = _fetch_atr(ts.symbol, config.atr_period, timeframe=ts.timeframe)
        if atr:
            ts.atr_value = atr
            if is_long:
                atr_stop = ts.highest_price - atr * effective_atr_mult
            else:
                ref = ts.lowest_price or price
                atr_stop = ref + atr * effective_atr_mult
        else:
            atr_stop = None

        if is_long:
            pct_stop = ts.highest_price * (1 - effective_trail_pct)
        else:
            ref = ts.lowest_price or price
            pct_stop = ref * (1 + effective_trail_pct)

        if atr_stop and pct_stop:
            new_stop = min(atr_stop, pct_stop) if is_long else max(atr_stop, pct_stop)
        else:
            new_stop = atr_stop or pct_stop
    else:
        atr_stop = _calc_atr_trailing_stop(ts, config)
        pct_stop = _calc_percent_trailing_stop(ts, config)

        if atr_stop and pct_stop:
            new_stop = min(atr_stop, pct_stop) if is_long else max(atr_stop, pct_stop)
        else:
            new_stop = atr_stop or pct_stop

    if not new_stop:
        db.session.flush()
        return {'action': 'hold', 'reason': 'no_dynamic_stop'}

    new_stop = round(new_stop, 2)

    improved = False
    if is_long:
        improved = new_stop > (ts.trailing_stop_price or 0)
    else:
        improved = (ts.trailing_stop_price is None) or new_stop < ts.trailing_stop_price

    if improved:
        old_stop = ts.trailing_stop_price
        ts.trailing_stop_price = new_stop
        ts.stop_loss_price = new_stop

        tighten_tag = " [cost-tighten]" if cost_tighten else ""
        _log_event(ts.id, 'dynamic_update', price, ts.highest_price, new_stop,
                   atr_value=ts.atr_value, profit_pct=profit_pct,
                   details=f"Dynamic: ${old_stop} -> ${new_stop}{tighten_tag}")

        _update_oco_stop(ts, new_stop)
        db.session.flush()

        return {'action': 'dynamic_update', 'old_stop': old_stop, 'new_stop': new_stop,
                'cost_tighten': cost_tighten, 'cost_distance': cost_distance_ratio}

    db.session.flush()
    return {'action': 'hold', 'profit_pct': profit_pct}


def calculate_dynamic_percent(profit_pct: float) -> float:
    if profit_pct < 0.03:
        percent = 0.005 + (profit_pct / 0.03) * 0.001
    elif profit_pct < 0.05:
        percent = 0.006 + ((profit_pct - 0.03) / 0.02) * 0.002
    elif profit_pct < 0.08:
        percent = 0.008 + ((profit_pct - 0.05) / 0.03) * 0.002
    elif profit_pct < 0.10:
        percent = 0.010 + ((profit_pct - 0.08) / 0.02) * 0.002
    elif profit_pct < 0.15:
        percent = 0.012 + ((profit_pct - 0.10) / 0.05) * 0.004
    else:
        percent = 0.020
    return percent


def _calc_atr_trailing_stop(ts: AlpacaTrailingStopPosition, config: AlpacaTrailingStopConfig) -> Optional[float]:
    atr = _fetch_atr(ts.symbol, config.atr_period, timeframe=ts.timeframe)
    if not atr:
        return None

    ts.atr_value = atr

    if ts.side == 'long':
        return ts.highest_price - atr * config.atr_multiplier
    else:
        return ts.lowest_price + atr * config.atr_multiplier


def _calc_percent_trailing_stop(ts: AlpacaTrailingStopPosition, config: AlpacaTrailingStopConfig) -> Optional[float]:
    profit_pct = 0
    if ts.side == 'long' and ts.entry_price and ts.entry_price > 0:
        price = ts.current_price or ts.highest_price
        profit_pct = (price - ts.entry_price) / ts.entry_price if price else 0
    elif ts.entry_price and ts.entry_price > 0:
        price = ts.current_price or ts.lowest_price
        profit_pct = (ts.entry_price - price) / ts.entry_price if price else 0
    dynamic_pct = calculate_dynamic_percent(max(0, profit_pct))
    if ts.side == 'long':
        return ts.highest_price * (1 - dynamic_pct)
    else:
        return (ts.lowest_price or ts.current_price) * (1 + dynamic_pct)


def _fetch_current_price(symbol: str) -> Optional[float]:
    try:
        from tiger_push_client import get_push_manager
        manager = get_push_manager()
        if manager.is_connected:
            cached = manager.get_cached_quote(symbol)
            if cached:
                price = cached.get('latest_price')
                ts = cached.get('timestamp')
                if price and float(price) > 0 and ts:
                    from datetime import datetime
                    age = (datetime.utcnow() - ts).total_seconds()
                    max_age = manager.get_adaptive_cache_max_age()
                    if age <= max_age:
                        logger.debug(f"📊 {symbol} price from Tiger WebSocket cache: ${price:.2f} (age: {age:.0f}s)")
                        return float(price)
    except Exception as e:
        logger.debug(f"Tiger WebSocket cache not available for {symbol}: {e}")

    try:
        from tiger_client import get_tiger_quote_client
        tiger = get_tiger_quote_client()
        if tiger:
            smart = tiger.get_smart_price(symbol)
            if smart and smart.get('price', 0) > 0:
                price = float(smart['price'])
                try:
                    from tiger_push_client import get_push_manager as _gpm
                    mgr = _gpm()
                    if mgr.is_connected:
                        mgr.update_cache_from_api(symbol, price, smart.get('session', 'regular'))
                except Exception:
                    pass
                return price
    except Exception as e:
        logger.debug(f"Tiger API price error for {symbol}: {e}")

    try:
        from eodhd_price_service import get_eodhd_smart_price
        eodhd_result = get_eodhd_smart_price(symbol)
        if eodhd_result and eodhd_result.get('price', 0) > 0:
            logger.debug(f"📊 {symbol} price from EODHD: ${eodhd_result['price']:.2f}")
            return float(eodhd_result['price'])
    except Exception as e:
        logger.debug(f"EODHD price fallback error for {symbol}: {e}")

    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        pos = client.get_position(symbol)
        if pos and not pos.get('_no_position') and pos.get('current_price'):
            price = float(pos['current_price'])
            if price > 0:
                logger.debug(f"📊 {symbol} price from Alpaca position API: ${price:.2f}")
                return price
    except Exception as e:
        logger.debug(f"Alpaca position price error for {symbol}: {e}")

    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        quote = client.get_latest_trade(symbol)
        if quote and quote.get('trade'):
            p = quote['trade'].get('p')
            if p is not None and float(p) > 0:
                return float(p)
    except Exception as e:
        logger.debug(f"Alpaca API price error for {symbol}: {e}")

    try:
        from alpaca.models import AlpacaHolding
        holding = AlpacaHolding.query.filter_by(symbol=symbol).first()
        if holding and holding.current_price and holding.current_price > 0:
            return float(holding.current_price)
    except Exception:
        pass

    return None


def _map_timeframe_to_alpaca(timeframe: str) -> str:
    tf_mapping = {
        '1': '1Min',
        '5': '5Min',
        '15': '15Min',
        '30': '30Min',
        '60': '1Hour',
        '240': '1Day',
        'D': '1Day',
        'W': '1Week',
    }
    return tf_mapping.get(timeframe or '15', '15Min')


def _fetch_atr(symbol: str, period: int = 14, timeframe: str = None) -> Optional[float]:
    try:
        from atr_cache_service import get_atr_and_bars
        atr, bars = get_atr_and_bars(symbol, timeframe, atr_period=period)
        if atr and atr > 0:
            logger.debug(f"ATR from Tiger cache: {symbol}/{timeframe} = {atr:.4f}")
            return round(atr, 4)
    except Exception as e:
        logger.debug(f"Tiger ATR cache unavailable for {symbol}: {e}")

    alpaca_tf = _map_timeframe_to_alpaca(timeframe)
    try:
        cached = _bars_cache.get(symbol, alpaca_tf)
        if cached:
            bars = cached
        else:
            from alpaca.client import AlpacaClient
            client = AlpacaClient()
            result = client.get_bars(symbol, timeframe=alpaca_tf, limit=period + 5)
            bars_data = result.get('bars', [])
            if not bars_data:
                return None
            bars = bars_data
            _bars_cache.set(symbol, alpaca_tf, bars)

        if len(bars) < period:
            return None

        trs = []
        for i in range(1, len(bars)):
            high = float(bars[i].get('h', 0))
            low = float(bars[i].get('l', 0))
            prev_close = float(bars[i - 1].get('c', 0))

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            trs.append(tr)

        if len(trs) < period:
            return None

        atr = sum(trs[-period:]) / period
        logger.debug(f"ATR from Alpaca API fallback: {symbol}/{alpaca_tf} = {atr:.4f}")
        return round(atr, 4)

    except Exception as e:
        logger.debug(f"Error fetching ATR for {symbol}: {e}")
        return None


def calculate_trend_strength(
    bars: List[Dict],
    current_price: float,
    entry_price: float,
    side: str,
    atr: float,
    config: AlpacaTrailingStopConfig
) -> Dict:
    result = {
        'trend_strength': 0.0,
        'atr_convergence': 1.0,
        'momentum_score': 0.0,
        'consecutive_highs': 0,
        'atr_convergence_score': 0.0,
        'momentum_normalized': 0.0,
        'consecutive_score': 0.0
    }

    momentum_lookback = getattr(config, 'momentum_lookback', 10)
    if len(bars) < momentum_lookback + 1 or atr <= 0:
        return result

    is_long = side == 'long'

    recent_bars = bars[-(momentum_lookback + 5):]
    atrs = []
    for i in range(1, len(recent_bars)):
        high = float(recent_bars[i].get('h', 0))
        low = float(recent_bars[i].get('l', 0))
        prev_close = float(recent_bars[i - 1].get('c', 0))
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        atrs.append(tr)

    if len(atrs) >= 2:
        avg_atr = sum(atrs) / len(atrs)
        if avg_atr > 0:
            atr_convergence = atr / avg_atr
            result['atr_convergence'] = atr_convergence
            if atr_convergence < 0.7:
                result['atr_convergence_score'] = 100
            elif atr_convergence < 0.85:
                result['atr_convergence_score'] = 70
            elif atr_convergence < 1.0:
                result['atr_convergence_score'] = 50
            elif atr_convergence < 1.2:
                result['atr_convergence_score'] = 30
            else:
                result['atr_convergence_score'] = 0

    lookback_bars = bars[-momentum_lookback:]
    if len(lookback_bars) >= 2:
        start_price = float(lookback_bars[0].get('c', 0))
        price_move = current_price - start_price
        if not is_long:
            price_move = -price_move

        momentum = price_move / atr if atr > 0 else 0
        result['momentum_score'] = momentum

        if momentum >= 3.0:
            result['momentum_normalized'] = 100
        elif momentum >= 2.0:
            result['momentum_normalized'] = 80
        elif momentum >= 1.0:
            result['momentum_normalized'] = 60
        elif momentum >= 0.5:
            result['momentum_normalized'] = 40
        elif momentum > 0:
            result['momentum_normalized'] = 20
        else:
            result['momentum_normalized'] = 0

    consecutive = 0
    recent_check = bars[-min(10, len(bars)):]

    if is_long:
        prev_high = None
        for bar in recent_check:
            h = float(bar.get('h', 0))
            if prev_high is not None and h > prev_high:
                consecutive += 1
            else:
                consecutive = 0
            prev_high = h
    else:
        prev_low = None
        for bar in recent_check:
            l_val = float(bar.get('l', 0))
            if prev_low is not None and l_val < prev_low:
                consecutive += 1
            else:
                consecutive = 0
            prev_low = l_val

    result['consecutive_highs'] = consecutive

    if consecutive >= 5:
        result['consecutive_score'] = 100
    elif consecutive >= 4:
        result['consecutive_score'] = 80
    elif consecutive >= 3:
        result['consecutive_score'] = 60
    elif consecutive >= 2:
        result['consecutive_score'] = 40
    elif consecutive >= 1:
        result['consecutive_score'] = 20
    else:
        result['consecutive_score'] = 0

    w_atr = getattr(config, 'atr_convergence_weight', 0.3)
    w_mom = getattr(config, 'momentum_weight', 0.4)
    w_con = getattr(config, 'consecutive_weight', 0.3)

    trend_strength = (
        result['atr_convergence_score'] * w_atr +
        result['momentum_normalized'] * w_mom +
        result['consecutive_score'] * w_con
    )
    result['trend_strength'] = min(100, max(0, trend_strength))

    return result


def _convert_tiger_bars_to_alpaca_format(tiger_bars: List[Dict]) -> List[Dict]:
    result = []
    for bar in tiger_bars:
        result.append({
            'h': bar.get('high', 0),
            'l': bar.get('low', 0),
            'c': bar.get('close', 0),
            'o': bar.get('open', 0),
            'v': bar.get('volume', 0),
            't': bar.get('timestamp', bar.get('time', '')),
        })
    return result


def _fetch_bars_for_trend(symbol: str, config: AlpacaTrailingStopConfig, timeframe: str = None) -> List[Dict]:
    lookback = getattr(config, 'momentum_lookback', 10)
    limit = lookback + 10

    try:
        from atr_cache_service import resolve_timeframe
        from kline_service import get_cached_bars
        tiger_tf = resolve_timeframe(timeframe)
        tiger_bars = get_cached_bars(symbol, tiger_tf, limit=limit)
        if tiger_bars and len(tiger_bars) >= limit:
            converted = _convert_tiger_bars_to_alpaca_format(tiger_bars)
            logger.debug(f"Trend bars from Tiger cache: {symbol}/{tiger_tf} ({len(converted)} bars)")
            return converted
    except Exception as e:
        logger.debug(f"Tiger bars cache unavailable for trend: {symbol}: {e}")

    alpaca_tf = _map_timeframe_to_alpaca(timeframe)

    cached = _bars_cache.get(symbol, alpaca_tf)
    if cached and len(cached) >= limit:
        return cached

    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        result = client.get_bars(symbol, timeframe=alpaca_tf, limit=limit)
        bars = result.get('bars', [])
        if bars:
            _bars_cache.set(symbol, alpaca_tf, bars)
        return bars
    except Exception as e:
        logger.debug(f"Error fetching bars for trend: {symbol}: {e}")
        return cached or []


def calculate_inverse_protection_stop(
    ts: AlpacaTrailingStopPosition,
    current_price: float,
    trend_strength: float,
    config: AlpacaTrailingStopConfig
) -> Tuple[Optional[float], Dict]:
    is_long = ts.side == 'long'
    original_stop = ts.signal_stop_loss or ts.stop_loss_price

    if original_stop is None:
        return None, {'reason': 'No original stop loss set'}

    if is_long:
        stop_distance = ts.entry_price - original_stop
        current_loss = ts.entry_price - current_price
    else:
        stop_distance = original_stop - ts.entry_price
        current_loss = current_price - ts.entry_price

    if stop_distance <= 0:
        return None, {'reason': 'Invalid stop distance'}

    loss_ratio = current_loss / stop_distance

    trigger_ratio = getattr(config, 'inverse_trigger_ratio', 0.50)

    if loss_ratio < trigger_ratio:
        return None, {
            'reason': 'Loss ratio below threshold',
            'loss_ratio': loss_ratio,
            'threshold': trigger_ratio
        }

    threshold = getattr(config, 'trend_strength_threshold', 60.0)

    if trend_strength >= threshold:
        return None, {
            'reason': 'Strong trend - keep original stop',
            'loss_ratio': loss_ratio,
            'trend_strength': trend_strength,
            'tightening_factor': 1.0
        }
    elif trend_strength >= 30:
        tightening_factor = 0.70
    else:
        tightening_factor = 0.60

    if is_long:
        new_stop = ts.entry_price - (stop_distance * tightening_factor)
    else:
        new_stop = ts.entry_price + (stop_distance * tightening_factor)

    current_stop = ts.trailing_stop_price or original_stop

    should_update = False
    if is_long:
        if new_stop > current_stop:
            should_update = True
    else:
        if new_stop < current_stop:
            should_update = True

    if not should_update:
        return None, {
            'reason': 'New stop not tighter than current',
            'loss_ratio': loss_ratio,
            'trend_strength': trend_strength,
            'new_stop': new_stop,
            'current_stop': current_stop
        }

    return round(new_stop, 2), {
        'action': 'tighten',
        'loss_ratio': loss_ratio,
        'trend_strength': trend_strength,
        'tightening_factor': tightening_factor,
        'original_stop': original_stop,
        'new_stop': round(new_stop, 2),
        'stop_distance': stop_distance,
        'current_loss': current_loss
    }


def _calc_atr_from_bars(bars: list, period: int) -> Optional[float]:
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        high = float(bars[i].get('h', 0))
        low = float(bars[i].get('l', 0))
        prev_close = float(bars[i - 1].get('c', 0))
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def _update_oco_stop(ts: AlpacaTrailingStopPosition, new_stop: float):
    oco_group = AlpacaOCOGroup.query.filter_by(
        symbol=ts.symbol,
        status=AlpacaOCOStatus.ACTIVE
    ).first()

    if oco_group:
        try:
            from alpaca.oco_service import update_oco_stop_price
            success, msg = update_oco_stop_price(oco_group.id, new_stop, ts.side)
            if success:
                logger.info(f"OCO stop updated for {ts.symbol}: ${new_stop} ({msg})")
            else:
                logger.warning(f"OCO stop update failed for {ts.symbol}: {msg}")
        except Exception as e:
            logger.error(f"Error updating OCO stop for {ts.symbol}: {e}")
        return

    _update_bracket_leg_stop(ts, new_stop)


def _update_bracket_leg_stop(ts: AlpacaTrailingStopPosition, new_stop: float):
    from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole

    sl_tracker = AlpacaOrderTracker.query.filter_by(
        trailing_stop_id=ts.id,
        role=AlpacaOrderRole.STOP_LOSS,
    ).filter(
        AlpacaOrderTracker.status.in_(['NEW', 'HELD', 'ACCEPTED', 'PENDING'])
    ).first()

    if not sl_tracker:
        sl_tracker = AlpacaOrderTracker.query.filter_by(
            symbol=ts.symbol,
            role=AlpacaOrderRole.STOP_LOSS,
        ).filter(
            AlpacaOrderTracker.status.in_(['NEW', 'HELD', 'ACCEPTED', 'PENDING']),
            AlpacaOrderTracker.parent_order_id.isnot(None),
        ).first()

    if not sl_tracker:
        logger.debug(f"No active bracket SL leg found for {ts.symbol}")
        return

    new_stop = round(new_stop, 2)

    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()

        replace_data = {'stop_price': str(round(new_stop, 2))}

        result = client.replace_order(sl_tracker.alpaca_order_id, replace_data)

        if result.get('success'):
            new_order = result.get('order', {})
            new_order_id = new_order.get('id', sl_tracker.alpaca_order_id)

            if new_order_id != sl_tracker.alpaca_order_id:
                sl_tracker.alpaca_order_id = new_order_id

            sl_tracker.stop_price = new_stop
            sl_tracker.updated_at = datetime.utcnow()
            db.session.flush()

            logger.info(f"Bracket SL leg updated for {ts.symbol}: ${new_stop} (order={new_order_id[:12]}...)")
        else:
            error = result.get('error', 'Unknown')
            logger.warning(f"Bracket SL leg modify failed for {ts.symbol}: {error}")
    except Exception as e:
        logger.error(f"Error updating bracket SL leg for {ts.symbol}: {e}")


def _cancel_active_oco(symbol: str):
    try:
        from alpaca.oco_service import cancel_oco_for_close
        count, msg = cancel_oco_for_close(symbol)
        if count > 0:
            logger.info(f"Cancelled {count} OCO groups for {symbol} before trailing stop exit")
    except Exception as e:
        logger.warning(f"Error cancelling OCO for {symbol}: {e}")

    _cancel_bracket_legs(symbol)

    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        cancel_result = client.cancel_orders_for_symbol(symbol)
        if cancel_result.get('cancelled_count', 0) > 0:
            logger.info(f"[{symbol}] Pre-exit: cancelled {cancel_result['cancelled_count']} remaining broker orders")
    except Exception as e:
        logger.warning(f"[{symbol}] Error cancelling remaining orders: {e}")


def _cancel_bracket_legs(symbol: str):
    from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole

    bracket_legs = AlpacaOrderTracker.query.filter_by(
        symbol=symbol,
    ).filter(
        AlpacaOrderTracker.role.in_([AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT]),
        AlpacaOrderTracker.status.in_(['NEW', 'HELD', 'ACCEPTED', 'PENDING']),
        AlpacaOrderTracker.parent_order_id.isnot(None),
    ).all()

    if not bracket_legs:
        return

    from alpaca.client import AlpacaClient
    client = AlpacaClient()

    for leg in bracket_legs:
        try:
            result = client.cancel_order(leg.alpaca_order_id)
            if result.get('success'):
                leg.status = 'CANCELED'
                leg.updated_at = datetime.utcnow()
                logger.info(f"Cancelled bracket {leg.role.value} leg for {symbol}: {leg.alpaca_order_id[:12]}...")
            else:
                logger.warning(f"Failed to cancel bracket leg {leg.alpaca_order_id[:12]}...: {result.get('error')}")
        except Exception as e:
            logger.warning(f"Error cancelling bracket leg for {symbol}: {e}")

    db.session.flush()


def _log_event(
    trailing_stop_id: int,
    event_type: str,
    current_price: float = None,
    highest_price: float = None,
    trailing_stop_price: float = None,
    atr_value: float = None,
    profit_pct: float = None,
    details: str = None,
):
    log = AlpacaTrailingStopLog(
        trailing_stop_id=trailing_stop_id,
        event_type=event_type,
        current_price=current_price,
        highest_price=highest_price,
        trailing_stop_price=trailing_stop_price,
        atr_value=atr_value,
        profit_pct=profit_pct,
        details=details,
    )
    db.session.add(log)


def _batch_fetch_prices(symbols: list) -> Dict[str, float]:
    price_map = {}

    try:
        from tiger_push_client import get_push_manager
        manager = get_push_manager()
        if manager.is_connected:
            from datetime import datetime as _dt
            max_age = manager.get_adaptive_cache_max_age()
            for sym in symbols:
                cached = manager.get_cached_quote(sym)
                if cached:
                    price = cached.get('latest_price')
                    ts = cached.get('timestamp')
                    if price and float(price) > 0 and ts:
                        age = (_dt.utcnow() - ts).total_seconds()
                        if age <= max_age:
                            price_map[sym] = float(price)
    except Exception as e:
        logger.debug(f"Tiger WebSocket batch cache error: {e}")

    missing = [s for s in symbols if s not in price_map]

    if missing:
        try:
            from tiger_client import get_tiger_quote_client
            tiger = get_tiger_quote_client()
            if tiger:
                smart_prices = tiger.get_batch_smart_prices(missing)
                for sym, data in smart_prices.items():
                    p = data.get('price', 0)
                    if p and float(p) > 0:
                        price_map[sym] = float(p)
                        try:
                            from tiger_push_client import get_push_manager as _gpm
                            mgr = _gpm()
                            if mgr.is_connected:
                                mgr.update_cache_from_api(sym, float(p), data.get('session', 'regular'))
                        except Exception:
                            pass
                fetched_count = len([s for s in missing if s in price_map])
                if fetched_count > 0:
                    logger.debug(f"📊 Tiger API batch: {fetched_count}/{len(missing)} prices (source: {data.get('source', '?')})")
        except Exception as e:
            logger.warning(f"Tiger API batch price fetch error: {e}")

    still_missing = [s for s in symbols if s not in price_map]

    if still_missing:
        try:
            from eodhd_price_service import get_eodhd_batch_prices
            eodhd_results = get_eodhd_batch_prices(still_missing)
            for sym, data in eodhd_results.items():
                p = data.get('price', 0)
                if p and float(p) > 0:
                    price_map[sym] = float(p)
            eodhd_count = len([s for s in still_missing if s in price_map])
            if eodhd_count > 0:
                logger.debug(f"📊 EODHD batch: {eodhd_count}/{len(still_missing)} prices")
        except Exception as e:
            logger.debug(f"EODHD batch price fallback error: {e}")

    still_missing = [s for s in symbols if s not in price_map]

    if still_missing:
        try:
            from alpaca.client import AlpacaClient
            client = AlpacaClient()
            batch_result = client.get_latest_trades_batch(still_missing)
            trades = batch_result.get('trades', {})
            for sym, trade_data in trades.items():
                p = trade_data.get('p')
                if p and float(p) > 0:
                    price_map[sym] = float(p)
        except Exception as e:
            logger.debug(f"Alpaca batch price fallback error: {e}")

    still_missing2 = [s for s in symbols if s not in price_map]
    if still_missing2:
        try:
            from alpaca.models import AlpacaHolding
            holdings = AlpacaHolding.query.filter(
                AlpacaHolding.symbol.in_(still_missing2)
            ).all()
            for h in holdings:
                if h.current_price and h.current_price > 0:
                    price_map[h.symbol] = float(h.current_price)
        except Exception as e:
            logger.debug(f"Holdings fallback error: {e}")

    final_missing = [s for s in symbols if s not in price_map]
    if final_missing:
        logger.warning(f"⚠️ No price for {len(final_missing)} symbols: {final_missing}")

    return price_map


def process_all_active_positions() -> Dict:
    active = AlpacaTrailingStopPosition.query.filter_by(is_active=True).all()
    results = {
        'total': len(active),
        'triggered': 0,
        'upgraded': 0,
        'dynamic_updated': 0,
        'held': 0,
        'skipped_no_price': 0,
        'errors': 0,
    }

    if not active:
        return results

    all_symbols = list(set(ts.symbol for ts in active))
    price_map = _batch_fetch_prices(all_symbols)

    for ts in active:
        try:
            prefetched = price_map.get(ts.symbol)
            result = process_trailing_stop(ts, prefetched_price=prefetched)
            action = result.get('action', '')

            if action == 'triggered':
                results['triggered'] += 1
            elif action == 'tier_upgrade':
                results['upgraded'] += 1
            elif action == 'dynamic_update':
                results['dynamic_updated'] += 1
            elif action == 'skip' and result.get('reason') == 'no_price':
                results['skipped_no_price'] += 1
            elif action == 'hold':
                results['held'] += 1
            else:
                results['held'] += 1
        except Exception as e:
            results['errors'] += 1
            logger.error(f"Error processing trailing stop #{ts.id} ({ts.symbol}): {e}")
            try:
                from alpaca.db_logger import log_error as _db_log_error
                _db_log_error('trailing_stop', f'Error processing {ts.symbol}: {str(e)}', category='error', symbol=ts.symbol)
            except Exception:
                pass

    if results['total'] > 0:
        db.session.commit()

    return results


def deactivate_trailing_stop(symbol: str, reason: str = 'manual'):
    ts = AlpacaTrailingStopPosition.query.filter_by(
        symbol=symbol, is_active=True
    ).first()
    if ts:
        ts.is_active = False
        ts.is_triggered = True
        ts.triggered_at = datetime.utcnow()
        ts.trigger_reason = reason
        _log_event(ts.id, 'deactivated', ts.current_price,
                   ts.highest_price, ts.trailing_stop_price,
                   details=f"Deactivated: {reason}")
        db.session.commit()
        logger.info(f"Deactivated trailing stop #{ts.id} for {symbol}: {reason}")


def reactivate_trailing_stop(symbol: str, reason: str = 'exit_order_failed',
                            trailing_stop_id: int = None,
                            partial_filled_qty: float = 0) -> Optional[AlpacaTrailingStopPosition]:
    """Reactivate/clear pending exit state on a trailing stop.

    With the new design, TS stays active during pending exit (trigger_reason='pending_exit:...').
    This function handles both:
    1. Old-style: TS was deactivated (is_active=False) → reactivate
    2. New-style: TS is still active but has pending_exit state → clear the pending state
    """
    ts = None
    if trailing_stop_id:
        ts = AlpacaTrailingStopPosition.query.get(trailing_stop_id)
        if ts and ts.symbol != symbol:
            ts = None

    if ts and ts.is_active and ts.trigger_reason and ts.trigger_reason.startswith('pending_exit:'):
        if partial_filled_qty and partial_filled_qty > 0:
            old_qty = ts.quantity
            remaining = old_qty - partial_filled_qty
            if remaining <= 0:
                logger.info(f"[{symbol}] Partial fill {partial_filled_qty} >= TS qty {old_qty}, "
                           f"exit effectively complete")
                return None
            ts.quantity = remaining
            logger.info(f"[{symbol}] TS #{ts.id} quantity adjusted: {old_qty} -> {remaining} "
                       f"(partial fill {partial_filled_qty})")

        ts.trigger_reason = None
        ts.triggered_at = None
        ts.triggered_price = None
        ts.trigger_retry_count = 0
        _log_event(ts.id, 'exit_cancelled_cleared', ts.current_price, ts.highest_price, ts.trailing_stop_price,
                   details=f"Pending exit cleared: {reason}")
        db.session.commit()
        logger.info(f"✅ [{symbol}] Cleared pending exit state on TS #{ts.id}: {reason}")
        return ts

    if ts and ts.is_active:
        logger.info(f"[{symbol}] TS #{ts.id} is already active, no reactivation needed")
        return ts

    if not ts:
        ts = AlpacaTrailingStopPosition.query.filter(
            AlpacaTrailingStopPosition.symbol == symbol,
            AlpacaTrailingStopPosition.is_active == False,
            AlpacaTrailingStopPosition.is_triggered == True,
        ).order_by(AlpacaTrailingStopPosition.triggered_at.desc()).first()

    if not ts:
        logger.warning(f"[{symbol}] No triggered trailing stop found to reactivate")
        return None

    retry_count = (ts.trigger_retry_count or 0) + 1
    MAX_REACTIVATE_RETRIES = 5

    if retry_count >= MAX_REACTIVATE_RETRIES:
        logger.error(f"⚠️ [{symbol}] Max reactivation retries ({MAX_REACTIVATE_RETRIES}) reached, "
                     f"TS #{ts.id} stays inactive. Manual intervention needed.")
        ts.trigger_reason = f"Permanently deactivated: exit order failed {retry_count} times"
        db.session.commit()
        return None

    from alpaca.position_service import find_open_position
    open_pos = find_open_position(symbol)
    if not open_pos:
        logger.warning(f"[{symbol}] No open position found, skipping TS reactivation")
        return None

    if partial_filled_qty and partial_filled_qty > 0:
        old_qty = ts.quantity
        remaining = old_qty - partial_filled_qty
        if remaining <= 0:
            logger.info(f"[{symbol}] Partial fill {partial_filled_qty} >= TS qty {old_qty}, "
                       f"exit effectively complete, not reactivating")
            return None
        ts.quantity = remaining
        logger.info(f"[{symbol}] TS #{ts.id} quantity adjusted: {old_qty} -> {remaining} "
                   f"(partial fill {partial_filled_qty})")

    ts.is_active = True
    ts.is_triggered = False
    ts.triggered_at = None
    ts.trigger_retry_count = retry_count
    ts.trigger_reason = None
    _log_event(ts.id, 'reactivated', ts.current_price, ts.highest_price, ts.trailing_stop_price,
               details=f"Reactivated: {reason} (retry {retry_count}/{MAX_REACTIVATE_RETRIES})"
                       f"{f', qty adjusted to {ts.quantity}' if partial_filled_qty else ''}")
    db.session.commit()
    logger.info(f"✅ [{symbol}] Reactivated trailing stop #{ts.id}: {reason} "
                f"(retry {retry_count}/{MAX_REACTIVATE_RETRIES})")
    return ts


def get_active_trailing_stops() -> List[AlpacaTrailingStopPosition]:
    return AlpacaTrailingStopPosition.query.filter_by(
        is_active=True
    ).order_by(AlpacaTrailingStopPosition.created_at.desc()).all()


def get_trailing_stop_logs(trailing_stop_id: int, limit: int = 50) -> List[AlpacaTrailingStopLog]:
    return AlpacaTrailingStopLog.query.filter_by(
        trailing_stop_id=trailing_stop_id
    ).order_by(AlpacaTrailingStopLog.created_at.desc()).limit(limit).all()
