import logging
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

from app import db
from alpaca.models import (
    AlpacaOrderTracker, AlpacaOrderRole, AlpacaTrade, AlpacaOrderStatus
)

logger = logging.getLogger(__name__)


def ensure_tracker_for_fill(
    alpaca_order_id: str,
    symbol: str,
    role: AlpacaOrderRole,
    side: str = None,
    quantity: float = None,
    fill_price: float = None,
    fill_time: datetime = None,
    trade_id: int = None,
    source: str = 'reconciliation',
) -> AlpacaOrderTracker:
    if not alpaca_order_id:
        return None
    existing = AlpacaOrderTracker.query.filter_by(alpaca_order_id=alpaca_order_id).first()
    if existing:
        if existing.status not in ('FILLED', 'PARTIALLY_FILLED') and fill_price:
            existing.status = 'FILLED'
            existing.avg_fill_price = fill_price
            existing.filled_quantity = quantity
            existing.fill_time = fill_time or datetime.utcnow()
            db.session.flush()
            logger.info(f"Updated existing tracker {alpaca_order_id} to FILLED (from {source})")
        return existing

    tracker = AlpacaOrderTracker(
        alpaca_order_id=alpaca_order_id,
        symbol=symbol,
        role=role,
        side=side,
        quantity=quantity,
        status='FILLED',
        avg_fill_price=fill_price,
        filled_quantity=quantity,
        fill_time=fill_time or datetime.utcnow(),
        trade_id=trade_id,
    )
    db.session.add(tracker)
    db.session.flush()
    logger.info(f"Created tracker for {alpaca_order_id} ({role.value}) from {source}: "
               f"{symbol} {side} {quantity}@${fill_price}")
    return tracker


def register_order(
    alpaca_order_id: str,
    symbol: str,
    role: AlpacaOrderRole,
    side: str = None,
    quantity: float = None,
    order_type: str = None,
    limit_price: float = None,
    stop_price: float = None,
    client_order_id: str = None,
    parent_order_id: str = None,
    trade_id: int = None,
    oco_group_id: int = None,
    leg_role: str = None,
    trailing_stop_id: int = None,
    signal_content: str = None,
) -> AlpacaOrderTracker:
    existing = AlpacaOrderTracker.query.filter_by(alpaca_order_id=alpaca_order_id).first()
    if existing:
        logger.debug(f"Order {alpaca_order_id} already tracked, updating")
        existing.role = role
        if side:
            existing.side = side
        if quantity:
            existing.quantity = quantity
        if trade_id:
            existing.trade_id = trade_id
        if oco_group_id:
            existing.oco_group_id = oco_group_id
        if leg_role:
            existing.leg_role = leg_role
        db.session.flush()
        return existing

    tracker = AlpacaOrderTracker(
        alpaca_order_id=alpaca_order_id,
        client_order_id=client_order_id,
        parent_order_id=parent_order_id,
        symbol=symbol,
        role=role,
        side=side,
        quantity=quantity,
        order_type=order_type,
        limit_price=limit_price,
        stop_price=stop_price,
        status='NEW',
        trade_id=trade_id,
        oco_group_id=oco_group_id,
        leg_role=leg_role,
        trailing_stop_id=trailing_stop_id,
        signal_content=signal_content,
    )
    db.session.add(tracker)
    db.session.flush()
    logger.info(f"Registered order {alpaca_order_id} as {role.value} for {symbol}")
    return tracker


def update_order_status(
    alpaca_order_id: str,
    status: str,
    filled_quantity: float = None,
    avg_fill_price: float = None,
    fill_time: datetime = None,
) -> Optional[AlpacaOrderTracker]:
    tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=alpaca_order_id).first()
    if not tracker:
        logger.warning(f"Order {alpaca_order_id} not found in tracker")
        return None

    tracker.status = status
    if filled_quantity is not None:
        tracker.filled_quantity = filled_quantity
    if avg_fill_price is not None:
        tracker.avg_fill_price = avg_fill_price
    if fill_time:
        tracker.fill_time = fill_time
    tracker.updated_at = datetime.utcnow()

    db.session.flush()
    logger.info(f"Updated order {alpaca_order_id} status to {status}")
    return tracker


def handle_order_fill(
    alpaca_order_id: str,
    filled_qty: float,
    avg_fill_price: float,
    fill_time: datetime = None,
    fill_source: str = None,
) -> Optional[AlpacaOrderTracker]:
    tracker = update_order_status(
        alpaca_order_id=alpaca_order_id,
        status='FILLED',
        filled_quantity=filled_qty,
        avg_fill_price=avg_fill_price,
        fill_time=fill_time or datetime.utcnow(),
    )

    if not tracker:
        return None

    if fill_source:
        tracker.fill_source = fill_source

    if tracker.role == AlpacaOrderRole.ENTRY:
        _handle_entry_fill(tracker)
    elif tracker.role in (AlpacaOrderRole.EXIT_SIGNAL, AlpacaOrderRole.EXIT_TRAILING,
                          AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT):
        _handle_exit_fill(tracker)

    return tracker


def _handle_entry_fill(tracker: AlpacaOrderTracker):
    from alpaca.position_service import get_or_create_position, add_entry_leg

    position, is_new = get_or_create_position(
        symbol=tracker.symbol,
        side='long' if tracker.side == 'buy' else 'short',
        entry_price=tracker.avg_fill_price,
        entry_quantity=tracker.filled_quantity,
        filled_at=tracker.fill_time,
    )

    tracker.position_id = position.id

    trade = AlpacaTrade.query.get(tracker.trade_id) if tracker.trade_id else None
    stop_price = None
    take_profit_price = None

    from alpaca.signal_utils import parse_signal_fields
    parsed = parse_signal_fields(trade.signal_data if trade else None)

    if trade:
        stop_price = trade.stop_loss_price
        take_profit_price = trade.take_profit_price

    add_entry_leg(
        position=position,
        alpaca_order_id=tracker.alpaca_order_id,
        price=tracker.avg_fill_price,
        quantity=tracker.filled_quantity,
        filled_at=tracker.fill_time,
        trade_id=tracker.trade_id,
        signal_content=parsed['signal_content'],
        signal_grade=parsed['signal_grade'],
        signal_score=parsed['signal_score'],
        signal_timeframe=parsed['signal_timeframe'],
        signal_indicator=parsed['signal_indicator'],
        stop_price=stop_price,
        take_profit_price=take_profit_price,
    )

    if trade:
        trade.filled_price = tracker.avg_fill_price
        trade.filled_quantity = tracker.filled_quantity
        trade.status = AlpacaOrderStatus.FILLED

    db.session.commit()
    logger.info(f"Entry fill processed: {tracker.symbol} {tracker.filled_quantity}@{tracker.avg_fill_price}")

    try:
        from alpaca.db_logger import log_info
        log_info('order_tracker', f'Entry filled: {tracker.symbol} @ ${tracker.avg_fill_price}', category='fill', symbol=tracker.symbol, extra_data={'order_id': tracker.alpaca_order_id, 'quantity': tracker.filled_quantity, 'price': tracker.avg_fill_price})
    except Exception:
        pass

    try:
        from alpaca.discord_notifier import AlpacaDiscordNotifier
        notifier = AlpacaDiscordNotifier()
        notifier.send_order_notification(trade, 'filled', is_close=False)
    except Exception as e:
        logger.error(f"Discord notification failed for entry fill: {e}")

    _create_protection_for_entry(tracker, trade, position, is_new)


def _cancel_bracket_legs(entry_order_id: str, symbol: str):
    from alpaca.client import AlpacaClient
    client = AlpacaClient()

    bracket_trackers = AlpacaOrderTracker.query.filter_by(
        parent_order_id=entry_order_id
    ).filter(
        AlpacaOrderTracker.role.in_([AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT])
    ).all()

    terminal_statuses = {'filled', 'cancelled', 'expired', 'rejected',
                         'FILLED', 'CANCELLED', 'EXPIRED', 'REJECTED'}

    cancelled = 0
    for bt in bracket_trackers:
        if bt.status not in terminal_statuses:
            try:
                client.cancel_order(bt.alpaca_order_id)
                bt.status = 'CANCELLED'
                cancelled += 1
                logger.info(f"Cancelled bracket leg {bt.role.value} for {symbol}: {bt.alpaca_order_id}")
            except Exception as e:
                logger.warning(f"Failed to cancel bracket leg {bt.alpaca_order_id}: {e}")

    if cancelled > 0:
        db.session.flush()
    return cancelled


def _modify_bracket_legs_for_scaling(symbol: str, new_qty: float, new_sl: float, new_tp: float, side: str, trailing_stop_id: int = None) -> Tuple[bool, str]:
    from alpaca.client import AlpacaClient
    from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole

    active_statuses = {'NEW', 'HELD', 'ACCEPTED', 'PENDING',
                       'new', 'held', 'accepted', 'pending'}

    base_query_sl = AlpacaOrderTracker.query.filter_by(
        symbol=symbol,
        role=AlpacaOrderRole.STOP_LOSS,
    ).filter(
        AlpacaOrderTracker.status.in_(list(active_statuses)),
        AlpacaOrderTracker.parent_order_id.isnot(None),
    )
    if trailing_stop_id:
        base_query_sl = base_query_sl.filter_by(trailing_stop_id=trailing_stop_id)
    sl_tracker = base_query_sl.first()

    base_query_tp = AlpacaOrderTracker.query.filter_by(
        symbol=symbol,
        role=AlpacaOrderRole.TAKE_PROFIT,
    ).filter(
        AlpacaOrderTracker.status.in_(list(active_statuses)),
        AlpacaOrderTracker.parent_order_id.isnot(None),
    )
    if trailing_stop_id:
        base_query_tp = base_query_tp.filter_by(trailing_stop_id=trailing_stop_id)
    tp_tracker = base_query_tp.first()

    if not sl_tracker and not tp_tracker:
        return False, "no_active_bracket_legs"

    client = AlpacaClient()
    qty_str = str(int(new_qty)) if float(new_qty) == int(float(new_qty)) else str(new_qty)
    sl_modified = False
    tp_modified = False

    if sl_tracker and new_sl:
        new_sl = round(new_sl, 2)
        stop_limit = round(new_sl * (0.995 if side == 'long' else 1.005), 2)
        replace_data = {
            'qty': qty_str,
            'stop_price': str(new_sl),
        }
        if sl_tracker.limit_price is not None:
            replace_data['limit_price'] = str(stop_limit)

        result = client.replace_order(sl_tracker.alpaca_order_id, replace_data)
        if result.get('success'):
            new_order = result.get('order', {})
            new_order_id = new_order.get('id', sl_tracker.alpaca_order_id)
            if new_order_id != sl_tracker.alpaca_order_id:
                sl_tracker.alpaca_order_id = new_order_id
            sl_tracker.quantity = new_qty
            sl_tracker.stop_price = new_sl
            if sl_tracker.limit_price is not None:
                sl_tracker.limit_price = stop_limit
            sl_modified = True
            logger.info(f"Bracket SL modified for {symbol}: qty={qty_str}, stop=${new_sl}")
        else:
            logger.warning(f"Bracket SL modify failed for {symbol}: {result.get('error')}")

    if tp_tracker and new_tp:
        new_tp = round(new_tp, 2)
        result = client.replace_order(tp_tracker.alpaca_order_id, {
            'qty': qty_str,
            'limit_price': str(new_tp),
        })
        if result.get('success'):
            new_order = result.get('order', {})
            new_order_id = new_order.get('id', tp_tracker.alpaca_order_id)
            if new_order_id != tp_tracker.alpaca_order_id:
                tp_tracker.alpaca_order_id = new_order_id
            tp_tracker.quantity = new_qty
            tp_tracker.limit_price = new_tp
            tp_modified = True
            logger.info(f"Bracket TP modified for {symbol}: qty={qty_str}, tp=${new_tp}")
        else:
            logger.warning(f"Bracket TP modify failed for {symbol}: {result.get('error')}")

    if sl_modified or tp_modified:
        db.session.flush()
        return True, f"bracket_modified (SL={'ok' if sl_modified else 'skip'}, TP={'ok' if tp_modified else 'skip'})"

    return False, "bracket_modify_failed"


def _determine_scaling_sl_tp(position, trade, existing_ts):
    side = position.side
    new_sl = round(float(trade.stop_loss_price), 2) if trade and trade.stop_loss_price else None
    new_tp = round(float(trade.take_profit_price), 2) if trade and trade.take_profit_price else None

    old_sl = existing_ts.trailing_stop_price if existing_ts else None
    old_tp = existing_ts.take_profit_price if existing_ts else None

    if new_sl and old_sl:
        if side == 'long':
            chosen_sl = max(new_sl, old_sl)
        else:
            chosen_sl = min(new_sl, old_sl)
    else:
        chosen_sl = new_sl or old_sl

    chosen_tp = new_tp or old_tp

    return chosen_sl, chosen_tp


def _create_protection_for_entry(tracker: AlpacaOrderTracker, trade, position, is_new: bool):
    try:
        side = 'long' if tracker.side == 'buy' else 'short'
        symbol = tracker.symbol
        stop_loss = round(float(trade.stop_loss_price), 2) if trade and trade.stop_loss_price else None
        take_profit = round(float(trade.take_profit_price), 2) if trade and trade.take_profit_price else None
        timeframe = trade.signal_timeframe if trade else None

        if is_new:
            if not stop_loss and not take_profit:
                logger.info(f"⏭️ [{symbol}] Skipping TrailingStop creation: no SL/TP in entry signal")
                ts_pos = None
            else:
                from alpaca.trailing_stop_engine import create_trailing_stop_for_entry
                ts_pos = create_trailing_stop_for_entry(
                    symbol=symbol,
                    side=side,
                    entry_price=tracker.avg_fill_price,
                    quantity=tracker.filled_quantity,
                    stop_loss_price=stop_loss,
                    take_profit_price=take_profit,
                    trade_id=tracker.trade_id,
                    timeframe=timeframe,
                )

            has_bracket_legs = not (trade.needs_auto_protection if trade else True)
            if has_bracket_legs:
                logger.info(f"Bracket order already has SL/TP legs for {symbol}, skipping OCO creation")
                if ts_pos:
                    existing_sl_tracker = AlpacaOrderTracker.query.filter_by(
                        parent_order_id=tracker.alpaca_order_id,
                        role=AlpacaOrderRole.STOP_LOSS
                    ).first()
                    existing_tp_tracker = AlpacaOrderTracker.query.filter_by(
                        parent_order_id=tracker.alpaca_order_id,
                        role=AlpacaOrderRole.TAKE_PROFIT
                    ).first()
                    if existing_sl_tracker:
                        existing_sl_tracker.trailing_stop_id = ts_pos.id
                    if existing_tp_tracker:
                        existing_tp_tracker.trailing_stop_id = ts_pos.id
                    db.session.commit()
                    logger.info(f"Linked bracket legs to trailing stop #{ts_pos.id} for {symbol}")
            elif stop_loss and take_profit:
                from alpaca.oco_service import create_oco_for_entry
                trailing_stop_id = ts_pos.id if ts_pos else None
                oco_group, oco_status = create_oco_for_entry(
                    symbol=symbol,
                    quantity=tracker.filled_quantity,
                    entry_price=tracker.avg_fill_price,
                    stop_loss_price=stop_loss,
                    take_profit_price=take_profit,
                    trade_id=tracker.trade_id,
                    trailing_stop_id=trailing_stop_id,
                    side=side,
                )
                if oco_group:
                    logger.info(f"OCO protection created for {symbol}: {oco_status}")
                else:
                    logger.warning(f"OCO creation failed for {symbol}: {oco_status}")
        else:
            logger.info(f"Scaling detected for {symbol}: updating protection for full position")

            from alpaca.trailing_stop_engine import update_trailing_stop_on_scaling
            from alpaca.models import AlpacaTrailingStopPosition

            existing_ts = AlpacaTrailingStopPosition.query.filter_by(
                symbol=symbol, is_active=True
            ).first()

            new_qty = position.total_entry_quantity
            new_avg = position.avg_entry_price
            chosen_sl, chosen_tp = _determine_scaling_sl_tp(position, trade, existing_ts)

            if not chosen_sl:
                from alpaca.trailing_stop_engine import get_trailing_stop_config
                config = get_trailing_stop_config()
                if side == 'long':
                    chosen_sl = round(new_avg * (1 - config.initial_stop_pct), 2)
                else:
                    chosen_sl = round(new_avg * (1 + config.initial_stop_pct), 2)
                logger.info(f"No SL for scaling {symbol}, using default: ${chosen_sl}")

            update_trailing_stop_on_scaling(
                symbol=symbol,
                new_quantity=new_qty,
                new_entry_price=new_avg,
                new_stop_loss=chosen_sl,
                new_take_profit=chosen_tp,
            )
            logger.info(f"Trailing stop updated for scaling {symbol}: qty={new_qty}, avg=${new_avg:.2f}, SL=${chosen_sl}, TP=${chosen_tp}")

            from alpaca.oco_service import modify_oco_for_scaling, get_active_oco_for_symbol

            modified = False
            modify_method = None

            active_oco = get_active_oco_for_symbol(symbol)
            if active_oco and chosen_sl:
                oco_success, oco_msg = modify_oco_for_scaling(
                    symbol=symbol,
                    new_quantity=new_qty,
                    new_stop_price=chosen_sl,
                    new_take_profit_price=chosen_tp or active_oco.take_profit_price,
                    side=side,
                )
                if oco_success:
                    modified = True
                    modify_method = f"oco_replace: {oco_msg}"
                    logger.info(f"Scaling: OCO modified via replace_order for {symbol}: {oco_msg}")
                else:
                    logger.warning(f"Scaling: OCO modify failed for {symbol}: {oco_msg}, trying cancel+recreate")

            ts_id = existing_ts.id if existing_ts else None

            if not modified and chosen_sl:
                bracket_success, bracket_msg = _modify_bracket_legs_for_scaling(
                    symbol=symbol, new_qty=new_qty, new_sl=chosen_sl, new_tp=chosen_tp,
                    side=side, trailing_stop_id=ts_id,
                )
                if bracket_success:
                    modified = True
                    modify_method = f"bracket_replace: {bracket_msg}"
                    logger.info(f"Scaling: bracket legs modified via replace_order for {symbol}: {bracket_msg}")
                else:
                    logger.info(f"Scaling: no bracket legs to modify for {symbol}: {bracket_msg}")

            if not modified and chosen_sl and chosen_tp:
                logger.info(f"Scaling: modify failed, falling back to cancel+recreate for {symbol}")

                all_entry_trackers = AlpacaOrderTracker.query.filter_by(
                    symbol=symbol,
                    role=AlpacaOrderRole.ENTRY,
                ).filter(
                    AlpacaOrderTracker.status.in_(['FILLED', 'filled'])
                ).all()
                for et in all_entry_trackers:
                    _cancel_bracket_legs(et.alpaca_order_id, symbol)

                from alpaca.oco_service import recreate_oco_for_scaling
                oco_group, oco_status = recreate_oco_for_scaling(
                    symbol=symbol,
                    new_quantity=new_qty,
                    entry_price=new_avg,
                    stop_loss_price=chosen_sl,
                    take_profit_price=chosen_tp,
                    trade_id=tracker.trade_id,
                    trailing_stop_id=ts_id,
                    side=side,
                )
                if oco_group:
                    modify_method = f"cancel_recreate: {oco_status}"
                    logger.info(f"Scaling: OCO recreated for {symbol}: {oco_status}")
                else:
                    logger.warning(f"Scaling: all protection methods failed for {symbol}")
                    modify_method = f"all_failed: {oco_status}"

            try:
                from alpaca.db_logger import log_info
                log_info('order_tracker', f'Scaling protection updated: {symbol}', category='scaling', symbol=symbol,
                         extra_data={'new_qty': new_qty, 'new_avg': round(new_avg, 2), 'sl': chosen_sl, 'tp': chosen_tp, 'method': modify_method})
            except Exception:
                pass

            db.session.commit()

    except Exception as e:
        logger.error(f"Error creating protection for {tracker.symbol}: {e}")
        try:
            from alpaca.db_logger import log_error
            log_error('order_tracker', f'Error creating protection: {str(e)}', category='error', symbol=tracker.symbol)
        except Exception:
            pass


def _handle_exit_fill(tracker: AlpacaOrderTracker):
    from alpaca.models import AlpacaExitMethod, AlpacaPositionLeg, AlpacaLegType
    from alpaca.position_service import find_open_position, add_exit_leg

    exit_method_map = {
        AlpacaOrderRole.EXIT_SIGNAL: AlpacaExitMethod.WEBHOOK_SIGNAL,
        AlpacaOrderRole.EXIT_TRAILING: AlpacaExitMethod.TRAILING_STOP,
        AlpacaOrderRole.STOP_LOSS: AlpacaExitMethod.OCO_STOP,
        AlpacaOrderRole.TAKE_PROFIT: AlpacaExitMethod.OCO_TAKE_PROFIT,
    }
    exit_method = exit_method_map.get(tracker.role, AlpacaExitMethod.EXTERNAL)

    exit_leg_created = False

    if tracker.role in (AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT):
        try:
            from alpaca.oco_service import handle_oco_leg_fill
            oco_group, status = handle_oco_leg_fill(
                alpaca_order_id=tracker.alpaca_order_id,
                filled_price=tracker.avg_fill_price,
                filled_quantity=tracker.filled_quantity,
            )
            if oco_group:
                logger.info(f"OCO exit processed via oco_service: {tracker.symbol} {status}")
                existing_leg = AlpacaPositionLeg.query.filter_by(
                    alpaca_order_id=tracker.alpaca_order_id,
                    leg_type=AlpacaLegType.EXIT,
                ).first()
                if existing_leg:
                    exit_leg_created = True
                    logger.debug(f"OCO service created EXIT leg for {tracker.symbol}")
                else:
                    logger.warning(f"OCO service returned group but no EXIT leg for {tracker.symbol}, creating fallback")
        except Exception as e:
            logger.warning(f"OCO service failed for {tracker.symbol}, falling back: {e}")

    if not exit_leg_created:
        expected_side = None
        if tracker.side:
            expected_side = 'long' if tracker.side == 'sell' else 'short'

        position = find_open_position(tracker.symbol, side=expected_side)
        if not position and expected_side:
            position = find_open_position(tracker.symbol)
            if position and position.side != expected_side:
                logger.warning(
                    f"⚠️ [{tracker.symbol}] Exit fill side mismatch: "
                    f"exit order side={tracker.side} expects position side={expected_side}, "
                    f"but found position #{position.id} side={position.side}. "
                    f"Skipping to prevent cross-position matching (COIN-style bug prevention). "
                    f"Order: {tracker.alpaca_order_id}"
                )
                try:
                    from alpaca.db_logger import log_warning
                    log_warning('order_tracker',
                        f'{tracker.symbol} exit fill rejected: side mismatch '
                        f'(exit={tracker.side}, position={position.side})',
                        category='side_mismatch', symbol=tracker.symbol,
                        extra_data={'order_id': tracker.alpaca_order_id,
                                    'exit_side': tracker.side,
                                    'position_side': position.side,
                                    'position_id': position.id})
                except Exception:
                    pass
                return
        if not position:
            logger.warning(f"No open position found for exit fill: {tracker.symbol}")
            return

        tracker.position_id = position.id

        add_exit_leg(
            position=position,
            alpaca_order_id=tracker.alpaca_order_id,
            price=tracker.avg_fill_price,
            quantity=tracker.filled_quantity,
            filled_at=tracker.fill_time,
            exit_method=exit_method,
        )

        from alpaca.models import AlpacaPositionStatus
        position_fully_closed = (position.status == AlpacaPositionStatus.CLOSED)

        if tracker.role == AlpacaOrderRole.EXIT_TRAILING:
            if position_fully_closed:
                from alpaca.trailing_stop_engine import deactivate_trailing_stop
                deactivate_trailing_stop(tracker.symbol, reason='trailing_stop_filled')
            else:
                remaining = max(0, (position.total_entry_quantity or 0) - (position.total_exit_quantity or 0))
                from alpaca.models import AlpacaTrailingStopPosition
                ts = AlpacaTrailingStopPosition.query.filter_by(
                    symbol=tracker.symbol, is_active=True
                ).first()
                if ts:
                    if remaining > 0:
                        old_qty = ts.quantity
                        ts.quantity = remaining
                        logger.warning(f"⚠️ [{tracker.symbol}] Partial EXIT_TRAILING: "
                                      f"{tracker.filled_quantity} filled, remaining {remaining}. "
                                      f"TS #{ts.id} stays active (qty: {old_qty} → {remaining})")
                    else:
                        from alpaca.trailing_stop_engine import deactivate_trailing_stop
                        deactivate_trailing_stop(tracker.symbol, reason='trailing_stop_filled')
                        logger.info(f"🎯 [{tracker.symbol}] EXIT_TRAILING filled, remaining qty=0, "
                                   f"deactivating TS (position status not yet CLOSED)")

        db.session.commit()
        logger.info(f"Exit fill processed: {tracker.symbol} {tracker.filled_quantity}@{tracker.avg_fill_price} "
                    f"method={exit_method.value} position={'CLOSED' if position_fully_closed else 'OPEN'}")

    try:
        from alpaca.db_logger import log_info
        log_info('order_tracker', f'Exit filled: {tracker.symbol} @ ${tracker.avg_fill_price}', category='fill', symbol=tracker.symbol, extra_data={'order_id': tracker.alpaca_order_id, 'quantity': tracker.filled_quantity, 'price': tracker.avg_fill_price, 'exit_method': exit_method.value})
    except Exception:
        pass

    try:
        from alpaca.discord_notifier import AlpacaDiscordNotifier
        from alpaca.models import AlpacaTrade
        trade = AlpacaTrade.query.filter_by(alpaca_order_id=tracker.alpaca_order_id).first()
        if not trade:
            class _FakeTrade:
                pass
            trade = _FakeTrade()
            trade.symbol = tracker.symbol
            trade.quantity = tracker.filled_quantity
            trade.filled_price = tracker.avg_fill_price
            trade.filled_quantity = tracker.filled_quantity
            trade.side = tracker.side
        notifier = AlpacaDiscordNotifier()
        notifier.send_order_notification(trade, 'filled', is_close=True)
    except Exception as e:
        logger.error(f"Discord notification failed for exit fill: {e}")


def get_tracked_order(alpaca_order_id: str) -> Optional[AlpacaOrderTracker]:
    return AlpacaOrderTracker.query.filter_by(alpaca_order_id=alpaca_order_id).first()


def get_pending_orders(symbol: str = None) -> list:
    query = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.status.in_(['NEW', 'PENDING', 'ACCEPTED', 'PARTIALLY_FILLED', 'HELD'])
    )
    if symbol:
        query = query.filter_by(symbol=symbol)
    return query.order_by(AlpacaOrderTracker.created_at.desc()).all()


def poll_order_status(alpaca_order_id: str) -> Optional[Dict[str, Any]]:
    from alpaca.client import AlpacaClient

    try:
        tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=alpaca_order_id).first()
        if tracker and tracker.status == 'FILLED':
            logger.debug(f"Poll skipped: order {alpaca_order_id[:8]}... already FILLED")
            return {'order_id': alpaca_order_id, 'status': 'FILLED',
                    'filled_qty': tracker.filled_quantity or 0, 'avg_price': tracker.avg_fill_price or 0}

        client = AlpacaClient()
        order = client.get_order(alpaca_order_id)

        status = order.get('status', '').upper()
        filled_qty = float(order.get('filled_qty', 0) or 0)
        avg_price = float(order.get('filled_avg_price', 0) or 0)
        filled_at = order.get('filled_at')

        fill_time = None
        if filled_at:
            try:
                fill_time = datetime.fromisoformat(filled_at.replace('Z', '+00:00'))
            except Exception:
                fill_time = datetime.utcnow()

        if status == 'FILLED' and filled_qty > 0:
            tracker_recheck = AlpacaOrderTracker.query.filter_by(alpaca_order_id=alpaca_order_id).first()
            if tracker_recheck and tracker_recheck.status == 'FILLED':
                logger.debug(f"Poll skipped (recheck): order {alpaca_order_id[:8]}... already FILLED")
                return {'order_id': alpaca_order_id, 'status': 'FILLED',
                        'filled_qty': filled_qty, 'avg_price': avg_price}

            handle_order_fill(
                alpaca_order_id=alpaca_order_id,
                filled_qty=filled_qty,
                avg_fill_price=avg_price,
                fill_time=fill_time,
                fill_source='polling',
            )
        else:
            update_order_status(
                alpaca_order_id=alpaca_order_id,
                status=status,
                filled_quantity=filled_qty if filled_qty > 0 else None,
                avg_fill_price=avg_price if avg_price > 0 else None,
            )

        return {
            'order_id': alpaca_order_id,
            'status': status,
            'filled_qty': filled_qty,
            'avg_price': avg_price,
        }
    except Exception as e:
        logger.error(f"Error polling order {alpaca_order_id}: {str(e)}")
        try:
            from alpaca.db_logger import log_error
            log_error('order_tracker', f'Error polling order: {str(e)}', category='error')
        except Exception:
            pass
        return None


def poll_all_pending_orders() -> Dict[str, Any]:
    pending = get_pending_orders()
    results = {'polled': 0, 'filled': 0, 'errors': 0}

    for tracker in pending:
        result = poll_order_status(tracker.alpaca_order_id)
        results['polled'] += 1
        if result:
            if result['status'] == 'FILLED':
                results['filled'] += 1
        else:
            results['errors'] += 1

    if results['polled'] > 0:
        logger.info(f"Polled {results['polled']} pending orders: {results['filled']} filled, {results['errors']} errors")
    return results


def apply_trade_update(event: Dict[str, Any]) -> Dict[str, Any]:
    event_type = event.get('event', '')
    order_id = event.get('order_id', '')
    symbol = event.get('symbol', '')

    result = {
        'event': event_type,
        'order_id': order_id,
        'symbol': symbol,
        'action': 'none',
        'skipped': False,
    }

    if not order_id:
        logger.warning(f"WS event missing order_id: {event}")
        result['action'] = 'error_no_order_id'
        return result

    if event_type == 'fill':
        result = _apply_fill_event(event, result)
    elif event_type == 'partial_fill':
        result = _apply_partial_fill_event(event, result)
    elif event_type in ('canceled', 'expired', 'rejected'):
        result = _apply_terminal_event(event, result)
    elif event_type == 'replaced':
        result = _apply_replaced_event(event, result)
    elif event_type in ('new', 'accepted', 'pending_new', 'pending_cancel',
                         'pending_replace', 'done_for_day', 'stopped',
                         'suspended', 'calculated'):
        result = _apply_status_update_event(event, result)
    else:
        logger.debug(f"WS unhandled event type: {event_type} for {symbol}")
        result['action'] = 'unhandled'

    return result


def _apply_fill_event(event: Dict, result: Dict) -> Dict:
    order_id = event['order_id']
    symbol = event.get('symbol', '')

    tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=order_id).first()
    if tracker and tracker.status == 'FILLED':
        logger.debug(f"WS fill already processed for {order_id[:8]}... ({symbol}), skipping")
        result['action'] = 'fill_already_processed'
        result['skipped'] = True
        return result

    filled_qty = _safe_float(event.get('filled_qty', 0))
    avg_price = _safe_float(event.get('filled_avg_price', 0))
    timestamp_str = event.get('timestamp', '')

    fill_time = None
    if timestamp_str:
        try:
            fill_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except Exception:
            fill_time = datetime.utcnow()

    if filled_qty <= 0 or avg_price <= 0:
        logger.warning(f"WS fill event with invalid data: qty={filled_qty}, price={avg_price}, order={order_id[:8]}...")
        result['action'] = 'fill_invalid_data'
        return result

    if tracker and tracker.oco_group_id:
        from alpaca.oco_service import handle_oco_leg_fill
        try:
            oco_group, status = handle_oco_leg_fill(
                alpaca_order_id=order_id,
                filled_price=avg_price,
                filled_quantity=filled_qty,
            )
            if oco_group:
                if tracker.status != 'FILLED':
                    update_order_status(
                        alpaca_order_id=order_id,
                        status='FILLED',
                        filled_quantity=filled_qty,
                        avg_fill_price=avg_price,
                        fill_time=fill_time,
                    )
                result['action'] = f'fill_oco_{status}'
                logger.info(f"WS fill via OCO: {symbol} {filled_qty}@{avg_price} ({status})")
                db.session.commit()
                return result
        except Exception as e:
            logger.warning(f"WS OCO fill handling failed for {symbol}, falling through: {e}")

    handle_order_fill(
        alpaca_order_id=order_id,
        filled_qty=filled_qty,
        avg_fill_price=avg_price,
        fill_time=fill_time,
        fill_source='websocket',
    )
    db.session.commit()

    result['action'] = 'fill_processed'
    logger.info(f"WS fill processed: {symbol} {filled_qty}@{avg_price}")
    return result


def _apply_partial_fill_event(event: Dict, result: Dict) -> Dict:
    order_id = event['order_id']
    filled_qty = _safe_float(event.get('filled_qty', 0))
    avg_price = _safe_float(event.get('filled_avg_price', 0))

    tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=order_id).first()
    if tracker and tracker.status in ('FILLED', 'CANCELLED', 'EXPIRED', 'REJECTED', 'REPLACED'):
        logger.debug(f"WS partial_fill skipped, order {order_id[:8]}... already terminal ({tracker.status})")
        result['action'] = 'partial_fill_already_terminal'
        result['skipped'] = True
        return result

    update_order_status(
        alpaca_order_id=order_id,
        status='PARTIALLY_FILLED',
        filled_quantity=filled_qty if filled_qty > 0 else None,
        avg_fill_price=avg_price if avg_price > 0 else None,
    )
    db.session.commit()

    result['action'] = 'partial_fill_updated'
    logger.info(f"WS partial fill: {event.get('symbol', '')} {filled_qty}@{avg_price}")
    return result


def _apply_terminal_event(event: Dict, result: Dict) -> Dict:
    order_id = event['order_id']
    event_type = event['event']
    symbol = event.get('symbol', '')
    status_map = {
        'canceled': 'CANCELLED',
        'expired': 'EXPIRED',
        'rejected': 'REJECTED',
    }
    status = status_map.get(event_type, event_type.upper())

    tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=order_id).first()
    if tracker and tracker.status in ('FILLED', 'CANCELLED', 'EXPIRED', 'REJECTED'):
        logger.debug(f"WS {event_type} already terminal for {order_id[:8]}..., skipping")
        result['action'] = f'{event_type}_already_terminal'
        result['skipped'] = True
        return result

    update_order_status(alpaca_order_id=order_id, status=status)

    if tracker and tracker.oco_group_id and status == 'CANCELLED':
        try:
            from alpaca.models import AlpacaOCOGroup, AlpacaOCOStatus
            oco_group = AlpacaOCOGroup.query.get(tracker.oco_group_id)
            if oco_group and oco_group.status == AlpacaOCOStatus.ACTIVE:
                is_stop = order_id == oco_group.stop_order_id
                is_tp = order_id == oco_group.take_profit_order_id
                other_id = oco_group.take_profit_order_id if is_stop else oco_group.stop_order_id

                if other_id:
                    other_tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=other_id).first()
                    if other_tracker and other_tracker.status == 'FILLED':
                        leg = 'stop' if is_stop else 'take_profit'
                        logger.info(f"WS: OCO {leg} cancelled for {symbol}, other leg already filled")
        except Exception as e:
            logger.debug(f"WS OCO cancel check error: {e}")

    if tracker and tracker.role == AlpacaOrderRole.EXIT_TRAILING and status in ('CANCELLED', 'EXPIRED', 'REJECTED'):
        try:
            from alpaca.trailing_stop_engine import reactivate_trailing_stop
            partial_filled = tracker.filled_quantity or 0
            reactivated_ts = reactivate_trailing_stop(
                symbol,
                reason=f"exit_order_{status.lower()} (order {order_id[:12]})"
                       f"{f', partial fill {partial_filled}' if partial_filled > 0 else ''}",
                trailing_stop_id=tracker.trailing_stop_id,
                partial_filled_qty=partial_filled,
            )
            if reactivated_ts:
                logger.info(f"✅ [{symbol}] Reactivated TS #{reactivated_ts.id} after EXIT_TRAILING order {status}"
                           f"{f' (partial fill {partial_filled})' if partial_filled > 0 else ''}")
            else:
                logger.warning(f"⚠️ [{symbol}] Could not reactivate TS after EXIT_TRAILING order {status}")
        except Exception as e:
            logger.error(f"[{symbol}] Error reactivating trailing stop after exit order {status}: {e}")

    db.session.commit()

    result['action'] = f'{event_type}_processed'
    logger.info(f"WS {event_type}: {symbol} order={order_id[:8]}...")
    return result


def _apply_replaced_event(event: Dict, result: Dict) -> Dict:
    order_id = event['order_id']
    symbol = event.get('symbol', '')
    replaced_by = event.get('replaced_by')

    if not replaced_by:
        raw_order = event.get('raw_order', {})
        replaced_by = raw_order.get('replaced_by')

    if not replaced_by:
        logger.warning(f"WS replaced event without replaced_by for {order_id[:8]}...")
        update_order_status(alpaca_order_id=order_id, status='REPLACED')
        db.session.commit()
        result['action'] = 'replaced_no_new_id'
        return result

    tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=order_id).first()
    if not tracker:
        logger.warning(f"WS replaced: original order {order_id[:8]}... not found in tracker")
        result['action'] = 'replaced_not_tracked'
        return result

    old_status = tracker.status
    tracker.status = 'REPLACED'
    tracker.updated_at = datetime.utcnow()

    new_tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=replaced_by).first()
    if not new_tracker:
        new_tracker = AlpacaOrderTracker(
            alpaca_order_id=replaced_by,
            client_order_id=tracker.client_order_id,
            parent_order_id=tracker.parent_order_id,
            symbol=tracker.symbol,
            role=tracker.role,
            side=tracker.side,
            quantity=tracker.quantity,
            order_type=tracker.order_type,
            limit_price=tracker.limit_price,
            stop_price=tracker.stop_price,
            status='NEW',
            trade_id=tracker.trade_id,
            oco_group_id=tracker.oco_group_id,
            leg_role=tracker.leg_role,
            trailing_stop_id=tracker.trailing_stop_id,
        )
        db.session.add(new_tracker)
        db.session.flush()
        logger.info(f"WS replaced: created new tracker for {replaced_by[:8]}... (was {order_id[:8]}...)")

    if tracker.oco_group_id:
        try:
            from alpaca.models import AlpacaOCOGroup
            oco_group = AlpacaOCOGroup.query.get(tracker.oco_group_id)
            if oco_group:
                if oco_group.stop_order_id == order_id:
                    oco_group.stop_order_id = replaced_by
                    logger.info(f"WS replaced: updated OCO stop_order_id for {symbol}")
                elif oco_group.take_profit_order_id == order_id:
                    oco_group.take_profit_order_id = replaced_by
                    logger.info(f"WS replaced: updated OCO take_profit_order_id for {symbol}")
        except Exception as e:
            logger.error(f"WS replaced: OCO update error for {symbol}: {e}")

    db.session.commit()

    result['action'] = 'replaced_processed'
    result['new_order_id'] = replaced_by
    logger.info(f"WS replaced: {symbol} {order_id[:8]}... -> {replaced_by[:8]}...")
    return result


def _apply_status_update_event(event: Dict, result: Dict) -> Dict:
    order_id = event['order_id']
    event_type = event['event']
    status = event.get('status', event_type).upper()

    tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=order_id).first()
    if tracker and tracker.status in ('FILLED', 'CANCELLED', 'EXPIRED', 'REJECTED', 'REPLACED'):
        result['action'] = f'{event_type}_already_terminal'
        result['skipped'] = True
        return result

    update_order_status(alpaca_order_id=order_id, status=status)
    db.session.commit()

    result['action'] = f'{event_type}_updated'
    logger.debug(f"WS status update: {event.get('symbol', '')} -> {status}")
    return result


def _safe_float(val) -> float:
    try:
        if val is None or val == '':
            return 0.0
        return float(val)
    except (ValueError, TypeError):
        return 0.0
