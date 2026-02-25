import logging
import json
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

from app import db
from alpaca.models import (
    AlpacaOCOGroup, AlpacaOCOStatus, AlpacaTrailingStopPosition,
    AlpacaOrderTracker, AlpacaOrderRole, AlpacaExitMethod
)

logger = logging.getLogger(__name__)


def create_oco_for_entry(
    symbol: str,
    quantity: float,
    entry_price: float,
    stop_loss_price: float,
    take_profit_price: float,
    trade_id: int = None,
    trailing_stop_id: int = None,
    side: str = 'long',
) -> Tuple[Optional[AlpacaOCOGroup], str]:
    from alpaca.client import AlpacaClient

    try:
        existing = AlpacaOCOGroup.query.filter_by(
            symbol=symbol,
            status=AlpacaOCOStatus.ACTIVE
        ).first()
        if existing:
            logger.info(f"OCO already exists for {symbol}: #{existing.id}")
            return existing, "already_exists"

        client = AlpacaClient()

        alpaca_position = client.get_position(symbol)
        if not alpaca_position or alpaca_position.get('_no_position'):
            logger.warning(f"[{symbol}] No Alpaca position found, skipping OCO creation")
            return None, "no_position"

        actual_qty = abs(float(alpaca_position.get('qty', 0)))
        if actual_qty <= 0:
            logger.warning(f"[{symbol}] Alpaca position qty is 0, skipping OCO creation")
            return None, "zero_position"

        if abs(actual_qty - quantity) > 0.01:
            logger.warning(f"[{symbol}] OCO qty mismatch: requested={quantity}, Alpaca actual={actual_qty}, using actual qty")
            quantity = actual_qty

        cancel_result = client.cancel_orders_for_symbol(symbol)
        if cancel_result.get('cancelled_count', 0) > 0:
            logger.info(f"[{symbol}] Pre-OCO: cancelled {cancel_result['cancelled_count']} existing orders")

        exit_side = 'sell' if side == 'long' else 'buy'
        stop_limit_price = round(stop_loss_price * (0.995 if side == 'long' else 1.005), 2)

        order_data = {
            'symbol': symbol,
            'qty': str(int(quantity)) if float(quantity) == int(float(quantity)) else str(quantity),
            'side': exit_side,
            'type': 'limit',
            'time_in_force': 'gtc',
            'order_class': 'oco',
            'stop_loss': {
                'stop_price': str(stop_loss_price),
                'limit_price': str(stop_limit_price),
            },
            'take_profit': {
                'limit_price': str(take_profit_price),
            },
        }

        logger.info(f"Creating OCO for {symbol}: qty={quantity}, SL=${stop_loss_price}, TP=${take_profit_price}")
        result = client._request('POST', '/v2/orders', data=order_data)

        oco_order_id = result.get('id', '')
        legs = result.get('legs', [])

        stop_order_id = None
        tp_order_id = None
        for leg in legs:
            leg_type = leg.get('order_type', '')
            if leg_type in ('stop_limit', 'stop'):
                stop_order_id = leg.get('id')
            elif leg_type == 'limit':
                tp_order_id = leg.get('id')

        if not stop_order_id and not tp_order_id and legs:
            if len(legs) >= 2:
                stop_order_id = legs[0].get('id')
                tp_order_id = legs[1].get('id')

        oco_group = AlpacaOCOGroup(
            oco_order_id=oco_order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            entry_price=entry_price,
            stop_order_id=stop_order_id,
            take_profit_order_id=tp_order_id,
            stop_price=stop_loss_price,
            stop_limit_price=stop_limit_price,
            take_profit_price=take_profit_price,
            time_in_force='gtc',
            status=AlpacaOCOStatus.ACTIVE,
            trade_id=trade_id,
            trailing_stop_id=trailing_stop_id,
        )
        db.session.add(oco_group)
        db.session.flush()

        from alpaca.order_tracker import register_order

        if stop_order_id:
            register_order(
                alpaca_order_id=stop_order_id,
                symbol=symbol,
                role=AlpacaOrderRole.STOP_LOSS,
                side=exit_side,
                quantity=quantity,
                order_type='stop_limit',
                stop_price=stop_loss_price,
                limit_price=stop_limit_price,
                parent_order_id=oco_order_id,
                trade_id=trade_id,
                oco_group_id=oco_group.id,
                leg_role='stop_loss',
                trailing_stop_id=trailing_stop_id,
            )

        if tp_order_id:
            register_order(
                alpaca_order_id=tp_order_id,
                symbol=symbol,
                role=AlpacaOrderRole.TAKE_PROFIT,
                side=exit_side,
                quantity=quantity,
                order_type='limit',
                limit_price=take_profit_price,
                parent_order_id=oco_order_id,
                trade_id=trade_id,
                oco_group_id=oco_group.id,
                leg_role='take_profit',
                trailing_stop_id=trailing_stop_id,
            )

        from alpaca.position_service import find_open_position, link_oco_to_position
        position = find_open_position(symbol)
        if position:
            link_oco_to_position(position, oco_group)

        db.session.commit()
        logger.info(f"OCO created for {symbol}: #{oco_group.id}, "
                     f"stop={stop_order_id}, tp={tp_order_id}")

        try:
            from alpaca.db_logger import log_info as _db_log_info
            _db_log_info('oco', f'OCO created: {symbol}', category='create', symbol=symbol, extra_data={'oco_id': oco_group.id, 'stop_price': stop_loss_price, 'take_profit_price': take_profit_price})
        except Exception:
            pass

        return oco_group, "created"

    except Exception as e:
        logger.error(f"Error creating OCO for {symbol}: {e}")
        try:
            from alpaca.db_logger import log_error as _db_log_error
            _db_log_error('oco', f'OCO creation error: {symbol}: {str(e)}', category='error', symbol=symbol)
        except Exception:
            pass
        db.session.rollback()
        return None, f"error: {str(e)}"


def handle_oco_leg_fill(
    alpaca_order_id: str,
    filled_price: float,
    filled_quantity: float,
) -> Tuple[Optional[AlpacaOCOGroup], str]:
    try:
        oco_group = AlpacaOCOGroup.query.filter(
            db.or_(
                AlpacaOCOGroup.stop_order_id == alpaca_order_id,
                AlpacaOCOGroup.take_profit_order_id == alpaca_order_id
            ),
            AlpacaOCOGroup.status == AlpacaOCOStatus.ACTIVE
        ).first()

        if not oco_group:
            return None, "not_found"

        if alpaca_order_id == oco_group.stop_order_id:
            oco_group.status = AlpacaOCOStatus.TRIGGERED_STOP
            exit_method = AlpacaExitMethod.OCO_STOP
            leg_type = "stop_loss"
            logger.info(f"OCO stop triggered for {oco_group.symbol}: ${filled_price}")
        else:
            oco_group.status = AlpacaOCOStatus.TRIGGERED_TP
            exit_method = AlpacaExitMethod.OCO_TAKE_PROFIT
            leg_type = "take_profit"
            logger.info(f"OCO take profit triggered for {oco_group.symbol}: ${filled_price}")

        oco_group.triggered_order_id = alpaca_order_id
        oco_group.triggered_price = filled_price
        oco_group.triggered_at = datetime.utcnow()

        from alpaca.position_service import find_open_position, add_exit_leg
        position = find_open_position(oco_group.symbol)
        if position:
            add_exit_leg(
                position=position,
                alpaca_order_id=alpaca_order_id,
                price=filled_price,
                quantity=filled_quantity,
                filled_at=datetime.utcnow(),
                exit_method=exit_method,
            )

        if oco_group.trailing_stop_id:
            ts_pos = AlpacaTrailingStopPosition.query.get(oco_group.trailing_stop_id)
            if ts_pos and ts_pos.is_active:
                from alpaca.models import AlpacaPositionStatus
                position_fully_closed = (position and position.status == AlpacaPositionStatus.CLOSED)
                if position_fully_closed:
                    ts_pos.is_active = False
                    ts_pos.is_triggered = True
                    ts_pos.triggered_at = datetime.utcnow()
                    ts_pos.trigger_reason = f"OCO {leg_type} filled at ${filled_price}"
                    logger.info(f"Deactivated TrailingStop #{ts_pos.id} (OCO {leg_type}, position fully closed)")
                else:
                    remaining = position.total_entry_quantity - (position.total_exit_quantity or 0) if position else 0
                    remaining = max(0, remaining)
                    if remaining > 0:
                        old_qty = ts_pos.quantity
                        ts_pos.quantity = remaining
                        logger.warning(f"⚠️ [{oco_group.symbol}] Partial OCO {leg_type} exit: "
                                      f"{filled_quantity} filled, remaining {remaining} shares. "
                                      f"TS #{ts_pos.id} stays active (qty: {old_qty} → {remaining})")
                    elif not position:
                        logger.warning(f"⚠️ [{oco_group.symbol}] OCO {leg_type} filled but no position found. "
                                      f"TS #{ts_pos.id} stays active for safety (verify_exit_position_closure will handle)")
                    else:
                        ts_pos.is_active = False
                        ts_pos.is_triggered = True
                        ts_pos.triggered_at = datetime.utcnow()
                        ts_pos.trigger_reason = f"OCO {leg_type} filled at ${filled_price}"
                        logger.info(f"Deactivated TrailingStop #{ts_pos.id} (OCO {leg_type}, remaining qty=0)")

        db.session.commit()

        try:
            from alpaca.discord_notifier import alpaca_discord
            details = f"Price: ${filled_price:.2f}, Qty: {filled_quantity}"
            if leg_type == 'stop_loss':
                alpaca_discord.send_oco_notification(oco_group.symbol, 'triggered_stop', details)
            else:
                alpaca_discord.send_oco_notification(oco_group.symbol, 'triggered_tp', details)
        except Exception as de:
            logger.debug(f"Discord notification error: {de}")

        try:
            from alpaca.db_logger import log_info as _db_log_info
            _db_log_info('oco', f'OCO triggered: {oco_group.symbol} {leg_type}', category='trigger', symbol=oco_group.symbol, extra_data={'filled_price': filled_price, 'filled_quantity': filled_quantity, 'leg_type': leg_type})
        except Exception:
            pass

        return oco_group, f"{leg_type}_triggered"

    except Exception as e:
        logger.error(f"Error handling OCO leg fill {alpaca_order_id}: {e}")
        try:
            from alpaca.db_logger import log_error as _db_log_error
            _db_log_error('oco', f'OCO leg fill error: {str(e)}', category='error')
        except Exception:
            pass
        db.session.rollback()
        return None, f"error: {str(e)}"


def cancel_oco_for_close(symbol: str) -> Tuple[int, str]:
    from alpaca.client import AlpacaClient

    try:
        active_groups = AlpacaOCOGroup.query.filter_by(
            symbol=symbol,
            status=AlpacaOCOStatus.ACTIVE
        ).all()

        if not active_groups:
            return 0, "no_active_groups"

        client = AlpacaClient()
        cancelled_count = 0

        for group in active_groups:
            order_ids = [group.stop_order_id, group.take_profit_order_id]
            for order_id in order_ids:
                if order_id:
                    try:
                        client.cancel_order(order_id)
                        logger.info(f"Cancelled OCO order {order_id}")
                    except Exception as e:
                        logger.warning(f"Failed to cancel order {order_id}: {e}")

            group.status = AlpacaOCOStatus.CANCELLED
            cancelled_count += 1

        db.session.commit()
        return cancelled_count, f"cancelled_{cancelled_count}"

    except Exception as e:
        logger.error(f"Error cancelling OCO for {symbol}: {e}")
        db.session.rollback()
        return 0, f"error: {str(e)}"


def update_oco_stop_price(
    oco_group_id: int,
    new_stop_price: float,
    side: str = 'long',
) -> Tuple[bool, str]:
    from alpaca.client import AlpacaClient

    try:
        group = AlpacaOCOGroup.query.get(oco_group_id)
        if not group:
            return False, "group_not_found"
        if group.status != AlpacaOCOStatus.ACTIVE:
            return False, f"group_not_active: {group.status.value}"

        new_stop_price = round(new_stop_price, 2)
        new_stop_limit = round(new_stop_price * (0.995 if side == 'long' else 1.005), 2)

        if not group.stop_order_id:
            return False, "no_stop_order_id"

        client = AlpacaClient()

        try:
            result = client.replace_order(group.stop_order_id, {
                'stop_price': str(new_stop_price),
                'limit_price': str(new_stop_limit),
            })

            if result.get('success'):
                new_order = result.get('order', {})
                new_order_id = new_order.get('id', group.stop_order_id)

                group.stop_price = new_stop_price
                group.stop_limit_price = new_stop_limit
                if new_order_id != group.stop_order_id:
                    group.stop_order_id = new_order_id
                group.modify_count = (group.modify_count or 0) + 1
                group.last_modified_at = datetime.utcnow()

                db.session.commit()
                logger.info(f"OCO stop updated for {group.symbol}: ${new_stop_price}")
                return True, "modified"
            else:
                error = result.get('error', 'Unknown')
                logger.warning(f"OCO stop modify failed: {error}, trying cancel+recreate")
        except Exception as e:
            logger.warning(f"OCO stop modify exception: {e}, trying cancel+recreate")

        try:
            client.cancel_order(group.stop_order_id)
        except Exception:
            pass

        group.status = AlpacaOCOStatus.CANCELLED
        db.session.commit()

        new_group, status = create_oco_for_entry(
            symbol=group.symbol,
            quantity=group.quantity,
            entry_price=group.entry_price or 0,
            stop_loss_price=new_stop_price,
            take_profit_price=group.take_profit_price,
            trade_id=group.trade_id,
            trailing_stop_id=group.trailing_stop_id,
            side=side,
        )

        if new_group:
            return True, f"recreated: {status}"
        return False, f"recreate_failed: {status}"

    except Exception as e:
        logger.error(f"Error updating OCO stop: {e}")
        db.session.rollback()
        return False, f"error: {str(e)}"


def modify_oco_for_scaling(
    symbol: str,
    new_quantity: float,
    new_stop_price: float,
    new_take_profit_price: float,
    side: str = 'long',
) -> Tuple[bool, str]:
    from alpaca.client import AlpacaClient

    try:
        group = AlpacaOCOGroup.query.filter_by(
            symbol=symbol,
            status=AlpacaOCOStatus.ACTIVE
        ).first()

        if not group:
            return False, "no_active_oco"

        client = AlpacaClient()
        new_stop_price = round(new_stop_price, 2)
        new_take_profit_price = round(new_take_profit_price, 2)
        new_stop_limit = round(new_stop_price * (0.995 if side == 'long' else 1.005), 2)
        qty_str = str(int(new_quantity)) if float(new_quantity) == int(float(new_quantity)) else str(new_quantity)

        sl_modified = False
        tp_modified = False

        if group.stop_order_id:
            result = client.replace_order(group.stop_order_id, {
                'qty': qty_str,
                'stop_price': str(new_stop_price),
                'limit_price': str(new_stop_limit),
            })
            if result.get('success'):
                new_order = result.get('order', {})
                new_order_id = new_order.get('id', group.stop_order_id)
                if new_order_id != group.stop_order_id:
                    from alpaca.models import AlpacaOrderTracker
                    sl_tracker = AlpacaOrderTracker.query.filter_by(
                        alpaca_order_id=group.stop_order_id
                    ).first()
                    if sl_tracker:
                        sl_tracker.alpaca_order_id = new_order_id
                        sl_tracker.quantity = new_quantity
                        sl_tracker.stop_price = new_stop_price
                        sl_tracker.limit_price = new_stop_limit
                    group.stop_order_id = new_order_id
                else:
                    from alpaca.models import AlpacaOrderTracker
                    sl_tracker = AlpacaOrderTracker.query.filter_by(
                        alpaca_order_id=group.stop_order_id
                    ).first()
                    if sl_tracker:
                        sl_tracker.quantity = new_quantity
                        sl_tracker.stop_price = new_stop_price
                        sl_tracker.limit_price = new_stop_limit

                group.stop_price = new_stop_price
                group.stop_limit_price = new_stop_limit
                sl_modified = True
                logger.info(f"OCO SL modified for {symbol}: qty={qty_str}, stop=${new_stop_price}")
            else:
                logger.warning(f"OCO SL modify failed for {symbol}: {result.get('error')}")

        if group.take_profit_order_id:
            result = client.replace_order(group.take_profit_order_id, {
                'qty': qty_str,
                'limit_price': str(new_take_profit_price),
            })
            if result.get('success'):
                new_order = result.get('order', {})
                new_order_id = new_order.get('id', group.take_profit_order_id)
                if new_order_id != group.take_profit_order_id:
                    from alpaca.models import AlpacaOrderTracker
                    tp_tracker = AlpacaOrderTracker.query.filter_by(
                        alpaca_order_id=group.take_profit_order_id
                    ).first()
                    if tp_tracker:
                        tp_tracker.alpaca_order_id = new_order_id
                        tp_tracker.quantity = new_quantity
                        tp_tracker.limit_price = new_take_profit_price
                    group.take_profit_order_id = new_order_id
                else:
                    from alpaca.models import AlpacaOrderTracker
                    tp_tracker = AlpacaOrderTracker.query.filter_by(
                        alpaca_order_id=group.take_profit_order_id
                    ).first()
                    if tp_tracker:
                        tp_tracker.quantity = new_quantity
                        tp_tracker.limit_price = new_take_profit_price

                group.take_profit_price = new_take_profit_price
                tp_modified = True
                logger.info(f"OCO TP modified for {symbol}: qty={qty_str}, tp=${new_take_profit_price}")
            else:
                logger.warning(f"OCO TP modify failed for {symbol}: {result.get('error')}")

        if sl_modified or tp_modified:
            group.quantity = new_quantity
            group.modify_count = (group.modify_count or 0) + 1
            group.last_modified_at = datetime.utcnow()
            db.session.commit()

            try:
                from alpaca.db_logger import log_info as _db_log_info
                _db_log_info('oco', f'OCO modified for scaling: {symbol}', category='scaling', symbol=symbol,
                             extra_data={'quantity': new_quantity, 'stop_price': new_stop_price, 'take_profit': new_take_profit_price,
                                         'sl_modified': sl_modified, 'tp_modified': tp_modified})
            except Exception:
                pass

            return True, f"modified (SL={'ok' if sl_modified else 'fail'}, TP={'ok' if tp_modified else 'fail'})"

        return False, "both_modify_failed"

    except Exception as e:
        logger.error(f"Error modifying OCO for scaling {symbol}: {e}")
        db.session.rollback()
        return False, f"error: {str(e)}"


def recreate_oco_for_scaling(
    symbol: str,
    new_quantity: float,
    entry_price: float,
    stop_loss_price: float,
    take_profit_price: float,
    trade_id: int = None,
    trailing_stop_id: int = None,
    side: str = 'long',
) -> Tuple[Optional[AlpacaOCOGroup], str]:
    try:
        cancelled_count, cancel_status = cancel_oco_for_close(symbol)
        logger.info(f"Scaling: cancelled {cancelled_count} OCO groups for {symbol}: {cancel_status}")

        new_group, status = create_oco_for_entry(
            symbol=symbol,
            quantity=new_quantity,
            entry_price=entry_price,
            stop_loss_price=round(stop_loss_price, 2),
            take_profit_price=round(take_profit_price, 2),
            trade_id=trade_id,
            trailing_stop_id=trailing_stop_id,
            side=side,
        )

        if new_group:
            logger.info(f"Scaling OCO recreated for {symbol}: qty={new_quantity}, SL=${stop_loss_price}, TP=${take_profit_price}")
            try:
                from alpaca.db_logger import log_info as _db_log_info
                _db_log_info('oco', f'OCO recreated for scaling: {symbol}', category='scaling', symbol=symbol,
                             extra_data={'quantity': new_quantity, 'stop_loss': stop_loss_price, 'take_profit': take_profit_price})
            except Exception:
                pass
        return new_group, status

    except Exception as e:
        logger.error(f"Error recreating OCO for scaling {symbol}: {e}")
        try:
            from alpaca.db_logger import log_error as _db_log_error
            _db_log_error('oco', f'OCO scaling recreate error: {symbol}: {str(e)}', category='error', symbol=symbol)
        except Exception:
            pass
        db.session.rollback()
        return None, f"error: {str(e)}"


def get_active_oco_for_symbol(symbol: str) -> Optional[AlpacaOCOGroup]:
    return AlpacaOCOGroup.query.filter_by(
        symbol=symbol,
        status=AlpacaOCOStatus.ACTIVE
    ).first()


def poll_oco_order_status():
    from alpaca.client import AlpacaClient

    active_groups = AlpacaOCOGroup.query.filter_by(
        status=AlpacaOCOStatus.ACTIVE
    ).all()

    if not active_groups:
        return {'checked': 0, 'triggered': 0}

    client = AlpacaClient()
    results = {'checked': 0, 'triggered': 0}

    for group in active_groups:
        for order_id in [group.stop_order_id, group.take_profit_order_id]:
            if not order_id:
                continue
            try:
                order = client.get_order(order_id)
                status = order.get('status', '').lower()
                results['checked'] += 1

                if status == 'filled':
                    filled_qty = float(order.get('filled_qty', 0) or 0)
                    avg_price = float(order.get('filled_avg_price', 0) or 0)

                    if filled_qty > 0 and avg_price > 0:
                        handle_oco_leg_fill(order_id, avg_price, filled_qty)
                        results['triggered'] += 1
                elif status in ('cancelled', 'expired', 'rejected'):
                    logger.info(f"OCO order {order_id} status: {status}")
            except Exception as e:
                logger.debug(f"Error polling OCO order {order_id}: {e}")

    return results
