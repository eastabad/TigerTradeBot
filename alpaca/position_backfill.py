import logging
import json
from datetime import datetime, date
from typing import Optional, Dict, List
from collections import defaultdict

from app import db
from alpaca.models import (
    AlpacaFilledOrder, AlpacaPosition, AlpacaPositionLeg,
    AlpacaPositionStatus, AlpacaLegType, AlpacaExitMethod,
    AlpacaEntrySignalRecord, AlpacaTrade, AlpacaOrderTracker
)

logger = logging.getLogger(__name__)


def rebuild_positions_from_fills(start_date: str = None, end_date: str = None,
                                  clear_existing: bool = False) -> Dict:
    result = {
        'status': 'running',
        'positions_created': 0,
        'entry_legs_created': 0,
        'exit_legs_created': 0,
        'positions_closed': 0,
        'fills_processed': 0,
        'errors': 0,
        'details': [],
    }

    try:
        query = AlpacaFilledOrder.query.order_by(AlpacaFilledOrder.filled_at.asc())

        if start_date:
            query = query.filter(AlpacaFilledOrder.filled_at >= start_date)
        if end_date:
            query = query.filter(AlpacaFilledOrder.filled_at <= end_date)

        fills = query.all()

        if not fills:
            result['status'] = 'completed'
            result['details'].append('No fills found to process')
            return result

        result['details'].append(f"Found {len(fills)} fills to process")

        if clear_existing:
            cleared = _clear_positions(start_date, end_date)
            result['details'].append(f"Cleared {cleared['positions']} positions, "
                                    f"{cleared['legs']} legs")

        symbols = set(f.symbol for f in fills)
        result['details'].append(f"Processing {len(symbols)} symbols: {', '.join(sorted(symbols))}")

        for symbol in sorted(symbols):
            symbol_fills = [f for f in fills if f.symbol == symbol]
            try:
                r = _rebuild_symbol(symbol, symbol_fills)
                result['positions_created'] += r['positions_created']
                result['entry_legs_created'] += r['entry_legs_created']
                result['exit_legs_created'] += r['exit_legs_created']
                result['positions_closed'] += r['positions_closed']
                result['fills_processed'] += r['fills_processed']
                if r.get('detail'):
                    result['details'].append(r['detail'])
            except Exception as e:
                result['errors'] += 1
                result['details'].append(f"Error processing {symbol}: {str(e)}")
                logger.error(f"Backfill error for {symbol}: {str(e)}")
                db.session.rollback()

        db.session.commit()
        result['status'] = 'completed'
        result['details'].append(
            f"Summary: {result['positions_created']} positions created, "
            f"{result['entry_legs_created']} entries, {result['exit_legs_created']} exits, "
            f"{result['positions_closed']} closed, {result['errors']} errors"
        )

        logger.info(f"Position backfill completed: {result['fills_processed']} fills processed, "
                    f"{result['positions_created']} positions created")
        return result

    except Exception as e:
        logger.error(f"Position backfill failed: {str(e)}")
        import traceback
        traceback.print_exc()
        db.session.rollback()
        result['status'] = 'failed'
        result['details'].append(f"Fatal error: {str(e)}")
        return result


def _clear_positions(start_date: str = None, end_date: str = None) -> Dict:
    query = AlpacaPosition.query
    if start_date:
        query = query.filter(AlpacaPosition.trade_date >= start_date)
    if end_date:
        query = query.filter(AlpacaPosition.trade_date <= end_date)

    positions = query.all()
    position_ids = [p.id for p in positions]

    legs_deleted = 0
    if position_ids:
        legs_deleted = AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.position_id.in_(position_ids)
        ).delete(synchronize_session='fetch')

        for p in positions:
            db.session.delete(p)

    db.session.flush()
    return {'positions': len(positions), 'legs': legs_deleted}


def _rebuild_symbol(symbol: str, fills: List[AlpacaFilledOrder]) -> Dict:
    result = {
        'positions_created': 0,
        'entry_legs_created': 0,
        'exit_legs_created': 0,
        'positions_closed': 0,
        'fills_processed': 0,
        'detail': '',
    }

    current_position = None
    remaining_position_qty = 0.0

    for fill in fills:
        fill_time = _parse_fill_time(fill.filled_at)
        qty = fill.filled_qty or fill.quantity or 0
        price = fill.filled_avg_price or 0
        side = (fill.side or '').lower()

        if qty <= 0 or price <= 0:
            continue

        existing_leg = AlpacaPositionLeg.query.filter_by(
            alpaca_order_id=fill.alpaca_order_id
        ).first()
        if existing_leg:
            fill.reconciled = True
            fill.reconciled_at = datetime.utcnow()
            result['fills_processed'] += 1
            continue

        is_entry = _classify_fill(side, current_position, remaining_position_qty)

        if is_entry:
            position_side = 'long' if side == 'buy' else 'short'

            if current_position is None or current_position.status == AlpacaPositionStatus.CLOSED:
                trade_date = (fill_time or datetime.utcnow()).date() if fill_time else date.today()
                seq = _next_seq(symbol, trade_date)
                position_key = f"{symbol}_{trade_date.isoformat()}_{seq}"

                current_position = AlpacaPosition(
                    position_key=position_key,
                    symbol=symbol,
                    trade_date=trade_date,
                    sequence_number=seq,
                    side=position_side,
                    status=AlpacaPositionStatus.OPEN,
                    total_entry_quantity=0,
                    total_exit_quantity=0,
                    opened_at=fill_time or datetime.utcnow(),
                )
                db.session.add(current_position)
                db.session.flush()
                result['positions_created'] += 1
                remaining_position_qty = 0.0

            is_first = current_position.total_entry_quantity == 0
            leg = AlpacaPositionLeg(
                position_id=current_position.id,
                leg_type=AlpacaLegType.ENTRY if is_first else AlpacaLegType.ADD,
                alpaca_order_id=fill.alpaca_order_id,
                price=price,
                quantity=qty,
                filled_at=fill_time,
            )

            _try_link_signal(leg, fill)

            db.session.add(leg)

            old_cost = (current_position.avg_entry_price or 0) * current_position.total_entry_quantity
            new_cost = old_cost + price * qty
            new_total = current_position.total_entry_quantity + qty
            current_position.total_entry_quantity = new_total
            current_position.avg_entry_price = new_cost / new_total if new_total > 0 else 0
            remaining_position_qty += qty

            try:
                from alpaca.order_tracker import ensure_tracker_for_fill
                from alpaca.models import AlpacaOrderRole
                ensure_tracker_for_fill(
                    alpaca_order_id=fill.alpaca_order_id,
                    symbol=symbol,
                    role=AlpacaOrderRole.ENTRY if is_first else AlpacaOrderRole.ADD,
                    side=side,
                    quantity=qty,
                    fill_price=price,
                    fill_time=fill_time,
                    source='backfill',
                )
            except Exception as tracker_err:
                logger.warning(f"[{symbol}] Backfill: failed to ensure entry tracker: {tracker_err}")

            result['entry_legs_created'] += 1

        else:
            if current_position is None or current_position.status == AlpacaPositionStatus.CLOSED:
                logger.warning(f"Exit fill without open position: {symbol} {fill.alpaca_order_id}")
                result['fills_processed'] += 1
                fill.reconciled = True
                fill.reconciled_at = datetime.utcnow()
                continue

            exit_qty = min(qty, remaining_position_qty)
            if exit_qty <= 0.001:
                logger.warning(f"Exit fill but no remaining qty: {symbol} {fill.alpaca_order_id}")
                fill.reconciled = True
                fill.reconciled_at = datetime.utcnow()
                result['fills_processed'] += 1
                continue

            exit_method = _determine_exit_method(fill)

            leg = AlpacaPositionLeg(
                position_id=current_position.id,
                leg_type=AlpacaLegType.EXIT,
                alpaca_order_id=fill.alpaca_order_id,
                price=price,
                quantity=exit_qty,
                filled_at=fill_time,
                exit_method=exit_method,
            )
            db.session.add(leg)

            try:
                from alpaca.order_tracker import ensure_tracker_for_fill
                from alpaca.models import AlpacaOrderRole
                exit_side = 'sell' if current_position.side == 'long' else 'buy'
                from alpaca.reconciliation import _exit_method_to_role
                exit_role = _exit_method_to_role(exit_method)
                ensure_tracker_for_fill(
                    alpaca_order_id=fill.alpaca_order_id,
                    symbol=symbol,
                    role=exit_role,
                    side=exit_side,
                    quantity=exit_qty,
                    fill_price=price,
                    fill_time=fill_time,
                    source='backfill',
                )
            except Exception as tracker_err:
                logger.warning(f"[{symbol}] Backfill: failed to ensure exit tracker: {tracker_err}")

            current_position.total_exit_quantity = (current_position.total_exit_quantity or 0) + exit_qty

            old_exit_cost = (current_position.avg_exit_price or 0) * ((current_position.total_exit_quantity or 0) - exit_qty)
            new_exit_cost = old_exit_cost + price * exit_qty
            current_position.avg_exit_price = new_exit_cost / current_position.total_exit_quantity if current_position.total_exit_quantity > 0 else 0

            if current_position.avg_entry_price and exit_qty:
                if current_position.side == 'long':
                    pnl = (price - current_position.avg_entry_price) * exit_qty
                else:
                    pnl = (current_position.avg_entry_price - price) * exit_qty
                current_position.realized_pnl = (current_position.realized_pnl or 0) + pnl

            remaining_position_qty -= exit_qty

            if remaining_position_qty <= 0.001:
                current_position.status = AlpacaPositionStatus.CLOSED
                current_position.closed_at = fill_time or datetime.utcnow()
                result['positions_closed'] += 1
                remaining_position_qty = 0.0
                logger.info(f"Backfill closed position {current_position.position_key}: "
                           f"P&L=${current_position.realized_pnl:.2f}")
                try:
                    from alpaca.position_service import _deactivate_trailing_stop_for_position
                    _deactivate_trailing_stop_for_position(current_position)
                except Exception as ts_err:
                    logger.error(f"Backfill: error deactivating TS for {current_position.symbol}: {ts_err}")

            result['exit_legs_created'] += 1

            overflow_qty = qty - exit_qty
            if overflow_qty > 0.001:
                reverse_side = 'short' if side == 'sell' else 'long'
                trade_date = (fill_time or datetime.utcnow()).date() if fill_time else date.today()
                seq = _next_seq(symbol, trade_date)
                position_key = f"{symbol}_{trade_date.isoformat()}_{seq}"

                current_position = AlpacaPosition(
                    position_key=position_key,
                    symbol=symbol,
                    trade_date=trade_date,
                    sequence_number=seq,
                    side=reverse_side,
                    status=AlpacaPositionStatus.OPEN,
                    total_entry_quantity=overflow_qty,
                    total_exit_quantity=0,
                    avg_entry_price=price,
                    opened_at=fill_time or datetime.utcnow(),
                )
                db.session.add(current_position)
                db.session.flush()
                result['positions_created'] += 1
                remaining_position_qty = overflow_qty

                overflow_leg = AlpacaPositionLeg(
                    position_id=current_position.id,
                    leg_type=AlpacaLegType.ENTRY,
                    alpaca_order_id=fill.alpaca_order_id + '_overflow',
                    price=price,
                    quantity=overflow_qty,
                    filled_at=fill_time,
                )
                db.session.add(overflow_leg)
                result['entry_legs_created'] += 1

                logger.info(f"Reversal detected: overflow {overflow_qty} created new {reverse_side} "
                           f"position {position_key}")

        fill.reconciled = True
        fill.reconciled_at = datetime.utcnow()
        result['fills_processed'] += 1

    db.session.flush()

    total_entries = result['entry_legs_created']
    total_exits = result['exit_legs_created']
    result['detail'] = (f"{symbol}: {result['fills_processed']} fills -> "
                       f"{result['positions_created']} positions, "
                       f"{total_entries} entries, {total_exits} exits, "
                       f"{result['positions_closed']} closed")

    return result


def _classify_fill(side: str, current_position, remaining_qty: float) -> bool:
    if current_position is None or current_position.status == AlpacaPositionStatus.CLOSED:
        return True

    if remaining_qty <= 0.001:
        return True

    if current_position.side == 'long':
        return side == 'buy'
    elif current_position.side == 'short':
        return side == 'sell'

    return True


def _try_link_signal(leg: AlpacaPositionLeg, fill: AlpacaFilledOrder):
    tracker = AlpacaOrderTracker.query.filter_by(
        alpaca_order_id=fill.alpaca_order_id
    ).first()

    if tracker and tracker.trade_id:
        trade = AlpacaTrade.query.get(tracker.trade_id)
        if trade and trade.signal_data:
            try:
                sig = json.loads(trade.signal_data)
                extras = sig.get('extras', {})
                leg.signal_grade = extras.get('grade')
                leg.signal_score = extras.get('score')
                leg.signal_timeframe = extras.get('timeframe')
                leg.signal_content = trade.signal_data
                leg.trade_id = trade.id
            except Exception:
                leg.signal_content = trade.signal_data
                leg.trade_id = trade.id

    if not leg.signal_content:
        from alpaca.models import AlpacaEntrySignalRecord
        fill_time = _parse_fill_time(fill.filled_at)
        if fill_time:
            from datetime import timedelta
            time_window_start = fill_time - timedelta(minutes=10)
            time_window_end = fill_time + timedelta(minutes=10)

            signal = AlpacaEntrySignalRecord.query.filter(
                AlpacaEntrySignalRecord.symbol == fill.symbol,
                AlpacaEntrySignalRecord.entry_time >= time_window_start,
                AlpacaEntrySignalRecord.entry_time <= time_window_end,
            ).first()

            if signal:
                leg.signal_grade = signal.signal_grade
                leg.signal_score = signal.signal_score
                leg.signal_timeframe = signal.timeframe
                leg.signal_content = signal.raw_json


def _determine_exit_method(fill: AlpacaFilledOrder) -> AlpacaExitMethod:
    tracker = AlpacaOrderTracker.query.filter_by(
        alpaca_order_id=fill.alpaca_order_id
    ).first()

    if tracker:
        from alpaca.models import AlpacaOrderRole
        role_map = {
            AlpacaOrderRole.EXIT_SIGNAL: AlpacaExitMethod.WEBHOOK_SIGNAL,
            AlpacaOrderRole.EXIT_TRAILING: AlpacaExitMethod.TRAILING_STOP,
            AlpacaOrderRole.STOP_LOSS: AlpacaExitMethod.OCO_STOP,
            AlpacaOrderRole.TAKE_PROFIT: AlpacaExitMethod.OCO_TAKE_PROFIT,
        }
        return role_map.get(tracker.role, AlpacaExitMethod.EXTERNAL)

    order_type = (fill.order_type or '').lower()
    if 'stop' in order_type:
        return AlpacaExitMethod.STOP_LOSS

    return AlpacaExitMethod.EXTERNAL


def _parse_fill_time(ts_str: str) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        ts_str = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(ts_str)
    except Exception:
        try:
            return datetime.strptime(ts_str[:19], '%Y-%m-%dT%H:%M:%S')
        except Exception:
            return None


def _next_seq(symbol: str, trade_date) -> int:
    max_seq = db.session.query(db.func.max(AlpacaPosition.sequence_number)).filter_by(
        symbol=symbol,
        trade_date=trade_date,
    ).scalar()
    return (max_seq or 0) + 1
