"""Position Backfill - Build Position records from historical TigerFilledOrder data.

Processes TigerFilledOrder records chronologically, creating Position and PositionLeg
records that match the actual trading history.
"""
import logging
from datetime import datetime, date, timedelta
from collections import defaultdict
from typing import Dict, List, Optional

from app import db
from models import (
    Position, PositionLeg, PositionStatus, LegType, ExitMethod,
    TigerFilledOrder, EntrySignalRecord, OCAGroup, Trade
)

logger = logging.getLogger(__name__)


def backfill_positions(account_type: str = 'real') -> Dict:
    """Build Position records from historical TigerFilledOrder data.
    
    Processes all filled orders chronologically, matching opens and closes
    to create position records with proper entry/exit legs.
    
    Returns summary dict with counts.
    """
    result = {
        'positions_created': 0,
        'entry_legs_created': 0,
        'exit_legs_created': 0,
        'orders_processed': 0,
        'errors': [],
    }
    
    position_ids = [p.id for p in Position.query.filter_by(account_type=account_type).all()]
    if position_ids:
        PositionLeg.query.filter(PositionLeg.position_id.in_(position_ids)).delete(synchronize_session='fetch')
        Position.query.filter(Position.id.in_(position_ids)).delete(synchronize_session='fetch')
    
    db.session.flush()
    logger.info(f"🔄 Cleared existing Position data for {account_type} ({len(position_ids)} positions)")
    
    orders = TigerFilledOrder.query.filter_by(
        account_type=account_type
    ).order_by(TigerFilledOrder.trade_time.asc()).all()
    
    if not orders:
        logger.info(f"No TigerFilledOrder records found for {account_type}")
        return result
    
    logger.info(f"📊 Processing {len(orders)} historical orders for {account_type}")
    
    open_positions: Dict[str, Position] = {}
    
    for order in orders:
        try:
            symbol = order.symbol
            action = (order.action or '').upper()
            is_open = order.is_open
            
            trade_dt = None
            if order.trade_time:
                trade_dt = datetime.utcfromtimestamp(order.trade_time / 1000)
            elif order.order_time:
                trade_dt = datetime.utcfromtimestamp(order.order_time / 1000)
            else:
                trade_dt = order.created_at or datetime.utcnow()
            
            trade_date = trade_dt.date()
            price = order.avg_fill_price or order.limit_price or 0
            quantity = order.filled or order.quantity or 0
            
            if is_open:
                side = 'long' if action in ('BUY', 'BUY_OPEN') else 'short'
                pos_key = f"{symbol}_{side}"
                
                if pos_key in open_positions:
                    position = open_positions[pos_key]
                    leg_type = LegType.ADD
                else:
                    seq = _next_seq(symbol, account_type, trade_date)
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
                        opened_at=trade_dt,
                    )
                    db.session.add(position)
                    db.session.flush()
                    open_positions[pos_key] = position
                    result['positions_created'] += 1
                    leg_type = LegType.ENTRY
                
                from signal_utils import parse_signal_fields
                signal_content = None
                signal_grade = None
                signal_score = None
                signal_indicator = None
                signal_timeframe = None
                trade_id = None
                oca_group_id = None
                stop_order_id = None
                take_profit_order_id = None
                stop_price = None
                take_profit_price = None
                
                entry_signal = EntrySignalRecord.query.filter_by(
                    entry_order_id=order.order_id,
                    account_type=account_type
                ).first()
                if not entry_signal:
                    entry_signal = EntrySignalRecord.query.filter(
                        EntrySignalRecord.account_type == account_type,
                        EntrySignalRecord.symbol == symbol,
                        EntrySignalRecord.entry_time >= trade_dt - timedelta(minutes=10),
                        EntrySignalRecord.entry_time <= trade_dt + timedelta(minutes=10)
                    ).first()
                if entry_signal:
                    raw_json = entry_signal.raw_json
                    parsed = parse_signal_fields(raw_json)
                    signal_content = parsed['signal_content']
                    signal_grade = parsed['signal_grade'] or entry_signal.signal_grade
                    signal_score = parsed['signal_score'] if parsed['signal_score'] is not None else entry_signal.signal_score
                    signal_indicator = parsed['signal_indicator'] or entry_signal.indicator_trigger
                    signal_timeframe = parsed['signal_timeframe'] or entry_signal.timeframe
                
                trade = Trade.query.filter_by(tiger_order_id=order.order_id).first()
                if not trade:
                    trade = Trade.query.filter(
                        Trade.account_type == account_type,
                        Trade.symbol == symbol,
                        Trade.created_at >= trade_dt - timedelta(minutes=10),
                        Trade.created_at <= trade_dt + timedelta(minutes=10)
                    ).first()
                if trade:
                    trade_id = trade.id
                    if not signal_content:
                        parsed = parse_signal_fields(trade.signal_data)
                        signal_content = parsed['signal_content']
                        if not signal_grade:
                            signal_grade = parsed['signal_grade']
                        if signal_score is None:
                            signal_score = parsed['signal_score']
                        if not signal_indicator:
                            signal_indicator = parsed['signal_indicator']
                        if not signal_timeframe:
                            signal_timeframe = parsed['signal_timeframe']
                
                oca = OCAGroup.query.filter_by(
                    trade_id=trade_id,
                    account_type=account_type
                ).first() if trade_id else None
                if not oca:
                    oca = OCAGroup.query.filter_by(
                        symbol=symbol,
                        account_type=account_type
                    ).filter(
                        OCAGroup.created_at >= trade_dt - timedelta(minutes=5),
                        OCAGroup.created_at <= trade_dt + timedelta(minutes=30)
                    ).first()
                
                if oca:
                    oca_group_id = oca.id
                    stop_order_id = oca.stop_order_id
                    take_profit_order_id = oca.take_profit_order_id
                    stop_price = oca.stop_price
                    take_profit_price = oca.take_profit_price
                
                leg = PositionLeg(
                    position_id=position.id,
                    leg_type=leg_type,
                    tiger_order_id=order.order_id,
                    price=price,
                    quantity=quantity,
                    filled_at=trade_dt,
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
                
                old_cost = (position.avg_entry_price or 0) * position.total_entry_quantity
                new_cost = old_cost + price * quantity
                new_qty = position.total_entry_quantity + quantity
                position.total_entry_quantity = new_qty
                position.avg_entry_price = new_cost / new_qty if new_qty > 0 else 0
                
                result['entry_legs_created'] += 1
                
            else:
                if action in ('SELL', 'SELL_CLOSE'):
                    position_side = 'long'
                elif action in ('BUY', 'BUY_CLOSE'):
                    position_side = 'short'
                else:
                    logger.warning(f"⚠️ Unknown close action: {action} for {symbol}")
                    result['orders_processed'] += 1
                    continue
                
                pos_key = f"{symbol}_{position_side}"
                position = open_positions.get(pos_key)
                
                if not position:
                    logger.warning(f"⚠️ No open position for close order {order.order_id} "
                                 f"({symbol} {position_side})")
                    result['orders_processed'] += 1
                    continue
                
                exit_method = _determine_exit_method_from_order(order, position)
                
                leg = PositionLeg(
                    position_id=position.id,
                    leg_type=LegType.EXIT,
                    tiger_order_id=order.order_id,
                    price=price,
                    quantity=quantity,
                    filled_at=trade_dt,
                    exit_method=exit_method,
                    realized_pnl=order.realized_pnl,
                    commission=order.commission,
                )
                db.session.add(leg)
                
                position.total_exit_quantity = (position.total_exit_quantity or 0) + quantity
                
                if price and quantity:
                    old_exit_cost = (position.avg_exit_price or 0) * ((position.total_exit_quantity or 0) - quantity)
                    new_exit_cost = old_exit_cost + price * quantity
                    position.avg_exit_price = new_exit_cost / position.total_exit_quantity if position.total_exit_quantity > 0 else 0
                
                if order.realized_pnl is not None:
                    position.realized_pnl = (position.realized_pnl or 0) + order.realized_pnl
                elif price and position.avg_entry_price:
                    if position_side == 'long':
                        calc_pnl = (price - position.avg_entry_price) * quantity
                    else:
                        calc_pnl = (position.avg_entry_price - price) * quantity
                    position.realized_pnl = (position.realized_pnl or 0) + calc_pnl
                
                if order.commission is not None:
                    position.commission = (position.commission or 0) + order.commission
                
                remaining = position.total_entry_quantity - position.total_exit_quantity
                if remaining <= 0.001:
                    position.status = PositionStatus.CLOSED
                    position.closed_at = trade_dt
                    del open_positions[pos_key]
                
                result['exit_legs_created'] += 1
            
            result['orders_processed'] += 1
            
        except Exception as e:
            error_msg = f"Error processing order {order.order_id}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            result['errors'].append(error_msg)
    
    db.session.commit()
    
    logger.info(f"📊 Backfill complete for {account_type}: "
               f"{result['positions_created']} positions, "
               f"{result['entry_legs_created']} entry legs, "
               f"{result['exit_legs_created']} exit legs, "
               f"{len(result['errors'])} errors")
    
    return result


def _next_seq(symbol: str, account_type: str, trade_date: date) -> int:
    max_seq = db.session.query(db.func.max(Position.sequence_number)).filter_by(
        symbol=symbol,
        account_type=account_type,
        trade_date=trade_date,
    ).scalar()
    return (max_seq or 0) + 1


def _determine_exit_method_from_order(order: TigerFilledOrder, position: Position) -> ExitMethod:
    """Determine exit method from order characteristics."""
    oca = OCAGroup.query.filter(
        (OCAGroup.stop_order_id == order.order_id) |
        (OCAGroup.take_profit_order_id == order.order_id)
    ).first()
    
    if oca:
        if order.order_id == oca.stop_order_id:
            return ExitMethod.STOP_LOSS
        else:
            return ExitMethod.TAKE_PROFIT
    
    if order.order_type and 'STP' in order.order_type.upper():
        return ExitMethod.STOP_LOSS
    
    trade = Trade.query.filter_by(tiger_order_id=order.order_id).first()
    if trade and trade.is_close_position:
        return ExitMethod.WEBHOOK_SIGNAL
    
    return ExitMethod.EXTERNAL
