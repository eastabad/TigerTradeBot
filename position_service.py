"""Position Service - Core logic for position lifecycle management.

Handles creating positions, adding entry/exit legs, and closing positions.
Position is identified by: symbol + account_type + trade_date + sequence_number
"""
import logging
from datetime import datetime, date
from typing import Optional, Tuple

from app import db
from models import (
    Position, PositionLeg, PositionStatus, LegType, ExitMethod,
    OCAGroup, Trade, TrailingStopPosition
)

logger = logging.getLogger(__name__)


def get_or_create_position(
    symbol: str,
    account_type: str,
    side: str,
    entry_price: float,
    entry_quantity: float,
    filled_at: datetime = None,
    trade_date: date = None,
) -> Tuple[Position, bool]:
    """Find an existing OPEN position or create a new one.
    
    Returns (position, is_new)
    """
    if trade_date is None:
        trade_date = (filled_at or datetime.utcnow()).date()
    
    existing = Position.query.filter_by(
        symbol=symbol,
        account_type=account_type,
        side=side,
        status=PositionStatus.OPEN
    ).first()
    
    if existing:
        return existing, False
    
    seq = _next_sequence_number(symbol, account_type, trade_date)
    position_key = f"{symbol}_{trade_date.isoformat()}_{seq}"
    
    position = Position(
        position_key=position_key,
        symbol=symbol,
        account_type=account_type,
        trade_date=trade_date,
        sequence_number=seq,
        side=side,
        status=PositionStatus.OPEN,
        total_entry_quantity=0,
        total_exit_quantity=0,
        opened_at=filled_at or datetime.utcnow(),
    )
    db.session.add(position)
    db.session.flush()
    logger.info(f"📦 Created new Position {position_key} ({side} {symbol})")
    return position, True


def _next_sequence_number(symbol: str, account_type: str, trade_date: date) -> int:
    max_seq = db.session.query(db.func.max(Position.sequence_number)).filter_by(
        symbol=symbol,
        account_type=account_type,
        trade_date=trade_date,
    ).scalar()
    return (max_seq or 0) + 1


def add_entry_leg(
    position: Position,
    tiger_order_id: str = None,
    price: float = None,
    quantity: float = None,
    filled_at: datetime = None,
    trade_id: int = None,
    signal_content: str = None,
    signal_grade: str = None,
    signal_score: int = None,
    signal_indicator: str = None,
    signal_timeframe: str = None,
    oca_group_id: int = None,
    stop_order_id: str = None,
    take_profit_order_id: str = None,
    stop_price: float = None,
    take_profit_price: float = None,
) -> PositionLeg:
    """Add an entry or add-on leg to a position."""
    if tiger_order_id:
        existing = PositionLeg.query.filter_by(
            position_id=position.id,
            tiger_order_id=str(tiger_order_id),
        ).filter(PositionLeg.leg_type.in_([LegType.ENTRY, LegType.ADD])).first()
        if existing:
            logger.debug(f"Entry leg already exists for order {tiger_order_id}, skipping")
            return existing
    
    is_first = position.total_entry_quantity == 0
    leg_type = LegType.ENTRY if is_first else LegType.ADD
    
    leg = PositionLeg(
        position_id=position.id,
        leg_type=leg_type,
        tiger_order_id=tiger_order_id,
        price=price,
        quantity=quantity,
        filled_at=filled_at or datetime.utcnow(),
        trade_id=trade_id,
        signal_content=signal_content,
        signal_grade=signal_grade,
        signal_score=signal_score,
        signal_indicator=signal_indicator,
        signal_timeframe=signal_timeframe,
        oca_group_id=oca_group_id,
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
    label = "Entry" if leg_type == LegType.ENTRY else "Add"
    logger.info(f"📦 {label} leg added to {position.position_key}: "
                f"{quantity}@${price}, avg=${position.avg_entry_price:.2f}")
    return leg


def add_exit_leg(
    position: Position,
    tiger_order_id: str = None,
    price: float = None,
    quantity: float = None,
    filled_at: datetime = None,
    exit_method: ExitMethod = None,
    realized_pnl: float = None,
    commission: float = None,
    close_source: str = None,
) -> PositionLeg:
    """Add an exit leg and potentially close the position."""
    if tiger_order_id:
        existing = PositionLeg.query.filter_by(
            position_id=position.id,
            tiger_order_id=str(tiger_order_id),
            leg_type=LegType.EXIT,
        ).first()
        if existing:
            logger.debug(f"Exit leg already exists for order {tiger_order_id}, skipping")
            return existing
    
    exit_qty = abs(quantity) if quantity else 0
    if exit_qty <= 0:
        logger.warning(f"⚠️ add_exit_leg called with zero/null quantity for {position.position_key}, skipping")
        return None

    remaining_before = position.total_entry_quantity - (position.total_exit_quantity or 0)
    if exit_qty > remaining_before + 0.01:
        logger.warning(f"⚠️ [{position.position_key}] exit_qty {exit_qty} > remaining {remaining_before}, capping to remaining")
        exit_qty = remaining_before
        if exit_qty <= 0:
            logger.warning(f"⚠️ [{position.position_key}] no remaining quantity to exit, skipping")
            return None

    leg = PositionLeg(
        position_id=position.id,
        leg_type=LegType.EXIT,
        tiger_order_id=tiger_order_id,
        price=price,
        quantity=exit_qty,
        filled_at=filled_at or datetime.utcnow(),
        exit_method=exit_method,
        realized_pnl=realized_pnl,
        commission=commission,
    )
    db.session.add(leg)
    
    position.total_exit_quantity = (position.total_exit_quantity or 0) + exit_qty
    
    if price and exit_qty:
        old_exit_cost = (position.avg_exit_price or 0) * ((position.total_exit_quantity or 0) - exit_qty)
        new_exit_cost = old_exit_cost + price * exit_qty
        position.avg_exit_price = new_exit_cost / position.total_exit_quantity if position.total_exit_quantity > 0 else 0
    
    if realized_pnl is not None and realized_pnl != 0:
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
    if remaining <= 0.001:
        position.status = PositionStatus.CLOSED
        position.closed_at = filled_at or datetime.utcnow()
        if close_source:
            position.close_source = close_source
        logger.info(f"📦 Position {position.position_key} CLOSED. P&L=${position.realized_pnl:.2f} (close_source={close_source})")
    else:
        logger.info(f"📦 Exit leg added to {position.position_key}: {exit_qty}@${price}, "
                    f"remaining={remaining}")
    
    db.session.flush()
    return leg


def find_open_position(symbol: str, account_type: str, side: str = None) -> Optional[Position]:
    """Find an open position for a symbol/account, optionally filtered by side."""
    query = Position.query.filter_by(
        symbol=symbol,
        account_type=account_type,
        status=PositionStatus.OPEN
    )
    if side:
        query = query.filter_by(side=side)
    return query.first()


def link_oca_to_position(position: Position, oca_group: OCAGroup):
    """Link OCA group info to the most recent entry leg of a position."""
    latest_entry = PositionLeg.query.filter(
        PositionLeg.position_id == position.id,
        PositionLeg.leg_type.in_([LegType.ENTRY, LegType.ADD])
    ).order_by(PositionLeg.created_at.desc()).first()
    
    if latest_entry:
        latest_entry.oca_group_id = oca_group.id
        latest_entry.stop_order_id = oca_group.stop_order_id
        latest_entry.take_profit_order_id = oca_group.take_profit_order_id
        latest_entry.stop_price = oca_group.stop_price
        latest_entry.take_profit_price = oca_group.take_profit_price
        db.session.flush()
        logger.info(f"📦 Linked OCA #{oca_group.id} to {position.position_key} entry leg #{latest_entry.id}")


def link_trailing_stop_to_position(position: Position, trailing_stop_id: int):
    """Link trailing stop to position."""
    position.trailing_stop_id = trailing_stop_id
    db.session.flush()
