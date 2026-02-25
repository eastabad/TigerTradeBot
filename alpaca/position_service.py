import logging
from datetime import datetime, date
from typing import Optional, Tuple, List

from app import db
from alpaca.models import (
    AlpacaPosition, AlpacaPositionLeg, AlpacaPositionStatus,
    AlpacaLegType, AlpacaExitMethod
)

logger = logging.getLogger(__name__)


def get_or_create_position(
    symbol: str,
    side: str,
    entry_price: float,
    entry_quantity: float,
    filled_at: datetime = None,
    trade_date: date = None,
) -> Tuple[AlpacaPosition, bool]:
    if trade_date is None:
        trade_date = (filled_at or datetime.utcnow()).date()

    existing = AlpacaPosition.query.filter_by(
        symbol=symbol,
        side=side,
        status=AlpacaPositionStatus.OPEN
    ).first()

    if existing:
        return existing, False

    seq = _next_sequence_number(symbol, trade_date)
    position_key = f"{symbol}_{trade_date.isoformat()}_{seq}"

    position = AlpacaPosition(
        position_key=position_key,
        symbol=symbol,
        trade_date=trade_date,
        sequence_number=seq,
        side=side,
        status=AlpacaPositionStatus.OPEN,
        total_entry_quantity=0,
        total_exit_quantity=0,
        opened_at=filled_at or datetime.utcnow(),
    )
    db.session.add(position)
    db.session.flush()
    logger.info(f"Created new Position {position_key} ({side} {symbol})")
    return position, True


def _next_sequence_number(symbol: str, trade_date: date) -> int:
    max_seq = db.session.query(db.func.max(AlpacaPosition.sequence_number)).filter_by(
        symbol=symbol,
        trade_date=trade_date,
    ).scalar()
    return (max_seq or 0) + 1


def add_entry_leg(
    position: AlpacaPosition,
    alpaca_order_id: str = None,
    price: float = None,
    quantity: float = None,
    filled_at: datetime = None,
    trade_id: int = None,
    signal_content: str = None,
    signal_grade: str = None,
    signal_score: int = None,
    signal_indicator: str = None,
    signal_timeframe: str = None,
    oco_group_id: int = None,
    stop_order_id: str = None,
    take_profit_order_id: str = None,
    stop_price: float = None,
    take_profit_price: float = None,
) -> AlpacaPositionLeg:
    if alpaca_order_id:
        existing_leg = AlpacaPositionLeg.query.filter_by(
            position_id=position.id,
            alpaca_order_id=alpaca_order_id,
        ).first()
        if existing_leg:
            logger.warning(f"Duplicate entry leg blocked: {position.position_key} already has leg for order {alpaca_order_id[:8]}...")
            return existing_leg

    is_first = position.total_entry_quantity == 0
    leg_type = AlpacaLegType.ENTRY if is_first else AlpacaLegType.ADD

    leg = AlpacaPositionLeg(
        position_id=position.id,
        leg_type=leg_type,
        alpaca_order_id=alpaca_order_id,
        price=price,
        quantity=quantity,
        filled_at=filled_at or datetime.utcnow(),
        trade_id=trade_id,
        signal_content=signal_content,
        signal_grade=signal_grade,
        signal_score=signal_score,
        signal_indicator=signal_indicator,
        signal_timeframe=signal_timeframe,
        oco_group_id=oco_group_id,
        stop_order_id=stop_order_id,
        take_profit_order_id=take_profit_order_id,
        stop_price=stop_price,
        take_profit_price=take_profit_price,
    )
    db.session.add(leg)

    if price and quantity:
        old_cost = (position.avg_entry_price or 0) * position.total_entry_quantity
        new_cost = old_cost + price * quantity
        new_qty = position.total_entry_quantity + quantity
        position.total_entry_quantity = new_qty
        position.avg_entry_price = new_cost / new_qty if new_qty > 0 else 0

    db.session.flush()
    label = "Entry" if leg_type == AlpacaLegType.ENTRY else "Add"
    logger.info(f"{label} leg added to {position.position_key}: "
                f"{quantity}@${price}, avg=${position.avg_entry_price:.2f}")
    return leg


def add_exit_leg(
    position: AlpacaPosition,
    alpaca_order_id: str = None,
    price: float = None,
    quantity: float = None,
    filled_at: datetime = None,
    exit_method: AlpacaExitMethod = None,
    realized_pnl: float = None,
    commission: float = None,
) -> AlpacaPositionLeg:
    leg = AlpacaPositionLeg(
        position_id=position.id,
        leg_type=AlpacaLegType.EXIT,
        alpaca_order_id=alpaca_order_id,
        price=price,
        quantity=quantity,
        filled_at=filled_at or datetime.utcnow(),
        exit_method=exit_method,
        realized_pnl=realized_pnl,
        commission=commission,
    )
    db.session.add(leg)

    exit_qty = quantity or 0
    position.total_exit_quantity = (position.total_exit_quantity or 0) + exit_qty

    if price and exit_qty:
        old_exit_cost = (position.avg_exit_price or 0) * ((position.total_exit_quantity or 0) - exit_qty)
        new_exit_cost = old_exit_cost + price * exit_qty
        position.avg_exit_price = new_exit_cost / position.total_exit_quantity if position.total_exit_quantity > 0 else 0

    if realized_pnl is not None:
        position.realized_pnl = (position.realized_pnl or 0) + realized_pnl
    elif price and position.avg_entry_price and exit_qty:
        if position.side == 'long':
            calc_pnl = (price - position.avg_entry_price) * exit_qty
        else:
            calc_pnl = (position.avg_entry_price - price) * exit_qty
        position.realized_pnl = (position.realized_pnl or 0) + calc_pnl

    if commission is not None:
        position.commission = (position.commission or 0) + commission

    remaining = position.total_entry_quantity - position.total_exit_quantity
    position_closed = False
    if remaining <= 0.001:
        position.status = AlpacaPositionStatus.CLOSED
        position.closed_at = filled_at or datetime.utcnow()
        logger.info(f"Position {position.position_key} CLOSED. P&L=${position.realized_pnl:.2f}")

        _deactivate_trailing_stop_for_position(position, exit_method)
        position_closed = True
    else:
        logger.info(f"Exit leg added to {position.position_key}: {exit_qty}@${price}, "
                    f"remaining={remaining}")

    db.session.flush()

    if position_closed:
        try:
            from alpaca.discord_notifier import alpaca_discord
            alpaca_discord.send_position_close_notification(position)
        except Exception as de:
            logger.debug(f"Discord notification error: {de}")

    return leg


def _deactivate_trailing_stop_for_position(position: AlpacaPosition, exit_method=None):
    from alpaca.models import AlpacaTrailingStopPosition
    try:
        ts_to_deactivate = []

        if position.trailing_stop_id:
            linked_ts = AlpacaTrailingStopPosition.query.get(position.trailing_stop_id)
            if linked_ts and linked_ts.is_active:
                ts_to_deactivate.append(linked_ts)

        active_ts_by_symbol = AlpacaTrailingStopPosition.query.filter_by(
            symbol=position.symbol,
            is_active=True
        ).all()
        for ts in active_ts_by_symbol:
            if ts not in ts_to_deactivate:
                ts_to_deactivate.append(ts)

        for ts in ts_to_deactivate:
            ts.is_active = False
            ts.is_triggered = True
            ts.triggered_at = datetime.utcnow()
            reason = f"Position #{position.id} closed"
            if exit_method:
                reason += f" via {exit_method.value if hasattr(exit_method, 'value') else exit_method}"
            ts.trigger_reason = reason
            logger.info(f"Deactivated trailing stop #{ts.id} for {position.symbol}: {reason}")

        if not ts_to_deactivate:
            logger.debug(f"No active trailing stop to deactivate for {position.symbol}")
    except Exception as e:
        logger.error(f"Error deactivating trailing stop for {position.symbol}: {e}")


def find_open_position(symbol: str, side: str = None) -> Optional[AlpacaPosition]:
    query = AlpacaPosition.query.filter_by(
        symbol=symbol,
        status=AlpacaPositionStatus.OPEN
    )
    if side:
        query = query.filter_by(side=side)
    return query.first()


def find_all_open_positions() -> List[AlpacaPosition]:
    return AlpacaPosition.query.filter_by(
        status=AlpacaPositionStatus.OPEN
    ).order_by(AlpacaPosition.opened_at.desc()).all()


def get_position_summary() -> dict:
    open_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.OPEN).all()
    closed_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.CLOSED).all()

    total_realized = sum(p.realized_pnl or 0 for p in closed_positions)
    win_count = sum(1 for p in closed_positions if (p.realized_pnl or 0) > 0)
    loss_count = sum(1 for p in closed_positions if (p.realized_pnl or 0) < 0)
    win_rate = (win_count / len(closed_positions) * 100) if closed_positions else 0

    avg_win = 0
    avg_loss = 0
    if win_count > 0:
        avg_win = sum(p.realized_pnl for p in closed_positions if (p.realized_pnl or 0) > 0) / win_count
    if loss_count > 0:
        avg_loss = sum(p.realized_pnl for p in closed_positions if (p.realized_pnl or 0) < 0) / loss_count

    return {
        'open_count': len(open_positions),
        'closed_count': len(closed_positions),
        'total_realized_pnl': total_realized,
        'win_count': win_count,
        'loss_count': loss_count,
        'win_rate': win_rate,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
    }


def link_oco_to_position(position: AlpacaPosition, oco_group):
    from alpaca.models import AlpacaPositionLeg
    latest_entry = AlpacaPositionLeg.query.filter(
        AlpacaPositionLeg.position_id == position.id,
        AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD])
    ).order_by(AlpacaPositionLeg.created_at.desc()).first()

    if latest_entry:
        latest_entry.oco_group_id = oco_group.id
        latest_entry.stop_order_id = oco_group.stop_order_id
        latest_entry.take_profit_order_id = oco_group.take_profit_order_id
        latest_entry.stop_price = oco_group.stop_price
        latest_entry.take_profit_price = oco_group.take_profit_price
        db.session.flush()
        logger.info(f"Linked OCO #{oco_group.id} to {position.position_key} entry leg #{latest_entry.id}")


def link_trailing_stop_to_position(position: AlpacaPosition, trailing_stop_id: int):
    position.trailing_stop_id = trailing_stop_id
    db.session.flush()
