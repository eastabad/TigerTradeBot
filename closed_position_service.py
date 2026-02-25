"""
Unified ClosedPosition creation service with idempotent handling and FIFO entry matching.

This module provides a single entry point for creating ClosedPosition records,
ensuring consistent behavior across all exit scenarios:
1. WEBHOOK_SIGNAL - TradingView close/flat signal
2. STOP_LOSS - Tiger stop loss order triggered
3. TAKE_PROFIT - Tiger take profit order triggered  
4. TRAILING_STOP - Dynamic trailing stop triggered

Key features:
- Idempotent: Uses exit_order_id to prevent duplicate records
- Entry matching: Links exits to entries via parent_order_id or FIFO
- Contribution P&L: Calculates per-entry profit contribution
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from app import db
from models import ClosedPosition, EntrySignalRecord, ExitMethod, Trade

logger = logging.getLogger(__name__)


def create_closed_position(
    symbol: str,
    account_type: str,
    side: str,
    exit_price: float,
    exit_quantity: float,
    exit_time: datetime,
    exit_method: ExitMethod,
    exit_order_id: str = None,
    parent_order_id: str = None,
    realized_pnl: float = None,
    commission: float = None,
    avg_entry_price: float = None,
    trailing_stop_id: int = None,
    exit_signal_content: str = None,
    exit_indicator: str = None
) -> Tuple[Optional[ClosedPosition], str]:
    """
    Unified service for creating ClosedPosition with entry matching.
    
    Args:
        symbol: Stock symbol (e.g., 'AAPL')
        account_type: 'real' or 'paper'
        side: Original position side - 'long' or 'short'
        exit_price: Exit fill price
        exit_quantity: Exit quantity
        exit_time: Exit timestamp
        exit_method: ExitMethod enum value
        exit_order_id: Tiger order ID for deduplication (required for idempotency)
        parent_order_id: Parent order ID for precise entry matching
        realized_pnl: Tiger API realized P&L (preferred)
        commission: Tiger API commission/fees
        avg_entry_price: Average entry price (if known)
        trailing_stop_id: Related TrailingStopPosition ID
        exit_signal_content: Exit signal JSON (only for WEBHOOK_SIGNAL)
        exit_indicator: Signal indicator name
        
    Returns:
        Tuple of (ClosedPosition or None, message string)
    """
    try:
        logger.info(f"📋 Creating ClosedPosition: {symbol}/{account_type} {side} "
                   f"exit_price={exit_price}, qty={exit_quantity}, method={exit_method.value}, "
                   f"order_id={exit_order_id}, pnl={realized_pnl}")
        
        if exit_quantity is not None:
            exit_quantity = abs(exit_quantity)
        
        if exit_order_id:
            existing = ClosedPosition.query.filter_by(exit_order_id=exit_order_id).first()
            if existing:
                logger.info(f"⏭️ ClosedPosition already exists for order {exit_order_id}")
                return existing, "already_exists"
        
        if trailing_stop_id and not exit_order_id:
            existing = ClosedPosition.query.filter_by(trailing_stop_id=trailing_stop_id).first()
            if existing:
                logger.info(f"⏭️ ClosedPosition already exists for trailing_stop_id {trailing_stop_id}")
                return existing, "already_exists"
        
        if not exit_order_id:
            from datetime import timedelta
            clean_symbol_check = symbol.replace('[PAPER]', '').strip() if symbol else symbol
            time_window = datetime.utcnow() - timedelta(seconds=60)
            recent_close = ClosedPosition.query.filter(
                ClosedPosition.symbol == clean_symbol_check,
                ClosedPosition.account_type == account_type,
                ClosedPosition.created_at >= time_window
            ).first()
            if recent_close:
                logger.info(f"⏭️ ClosedPosition already exists for {clean_symbol_check}/{account_type} "
                           f"within 60s (id={recent_close.id}, created={recent_close.created_at})")
                return recent_close, "already_exists"
        
        clean_symbol_for_lookup = symbol.replace('[PAPER]', '').strip() if symbol else symbol
        if not avg_entry_price:
            avg_entry_price = _lookup_entry_price(clean_symbol_for_lookup, account_type, side, trailing_stop_id)
            if avg_entry_price:
                logger.info(f"📋 Looked up avg_entry_price=${avg_entry_price:.4f} for {symbol}")

        total_pnl_pct = None
        if realized_pnl is not None and realized_pnl != 0 and avg_entry_price and avg_entry_price > 0 and exit_quantity:
            total_pnl_pct = realized_pnl / (avg_entry_price * exit_quantity) * 100
            logger.info(f"💰 P&L from Tiger API: ${realized_pnl:.2f} ({total_pnl_pct:.2f}%)")
        elif (realized_pnl is None or realized_pnl == 0) and exit_price and avg_entry_price and exit_quantity:
            if side == 'long':
                realized_pnl = (exit_price - avg_entry_price) * exit_quantity
            else:
                realized_pnl = (avg_entry_price - exit_price) * exit_quantity
            if avg_entry_price > 0:
                total_pnl_pct = realized_pnl / (avg_entry_price * exit_quantity) * 100
            logger.info(f"💰 P&L calculated manually: ${realized_pnl:.2f} ({total_pnl_pct or 0:.2f}%) "
                       f"[entry={avg_entry_price}, exit={exit_price}, qty={exit_quantity}]")
        else:
            logger.warning(f"⚠️ Cannot calculate P&L: realized_pnl={realized_pnl}, "
                          f"avg_entry={avg_entry_price}, exit_price={exit_price}, qty={exit_quantity}")
        
        # Clean symbol for matching (remove [PAPER] prefix if present)
        clean_symbol = symbol.replace('[PAPER]', '').strip() if symbol else symbol
        
        # Create ClosedPosition record (use clean symbol for consistency)
        closed_pos = ClosedPosition(
            symbol=clean_symbol,  # Store clean symbol for consistency
            account_type=account_type,
            side=side,
            exit_order_id=exit_order_id,
            exit_time=exit_time,
            exit_price=exit_price,
            exit_quantity=exit_quantity,
            exit_method=exit_method,
            exit_signal_content=exit_signal_content,
            exit_indicator=exit_indicator,
            total_pnl=realized_pnl,
            total_pnl_pct=total_pnl_pct,
            avg_entry_price=avg_entry_price,
            trailing_stop_id=trailing_stop_id,
            commission=commission
        )
        db.session.add(closed_pos)
        db.session.flush()  # Get the ID
        
        # Match entry signals using clean symbol
        matched_entries = _match_entry_signals(
            closed_pos=closed_pos,
            symbol=clean_symbol,
            account_type=account_type,
            side=side,
            exit_quantity=exit_quantity,
            exit_price=exit_price,
            parent_order_id=parent_order_id
        )
        
        _sync_position_exit_data(
            symbol=clean_symbol,
            account_type=account_type,
            side=side,
            exit_price=exit_price,
            exit_quantity=exit_quantity,
            exit_time=exit_time,
            exit_method=exit_method,
            exit_order_id=exit_order_id,
            realized_pnl=realized_pnl,
            commission=commission,
            trailing_stop_id=trailing_stop_id,
        )
        
        logger.info(f"✅ Created ClosedPosition #{closed_pos.id} for {symbol} "
                   f"({exit_method.value}), matched {len(matched_entries)} entries")
        
        return closed_pos, "created"
        
    except Exception as e:
        # NOTE: Do NOT rollback here - let caller manage transaction
        logger.error(f"❌ Failed to create ClosedPosition: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, f"error: {str(e)}"


def _sync_position_exit_data(
    symbol: str,
    account_type: str,
    side: str,
    exit_price: float,
    exit_quantity: float,
    exit_time: 'datetime',
    exit_method: 'ExitMethod',
    exit_order_id: str = None,
    realized_pnl: float = None,
    commission: float = None,
    trailing_stop_id: int = None,
):
    """Sync exit data to Position + PositionLeg tables.
    
    Called from create_closed_position to ensure both old (ClosedPosition) and new (Position+PositionLeg)
    tables stay in sync. Uses position_service.add_exit_leg for proper accounting.
    """
    try:
        from position_service import find_open_position, add_exit_leg
        
        position = None
        clean_symbol = symbol.replace('[PAPER]', '').strip() if symbol else symbol
        
        if trailing_stop_id:
            from models import Position as PositionModel, PositionStatus
            position = PositionModel.query.filter_by(
                trailing_stop_id=trailing_stop_id,
                status=PositionStatus.OPEN
            ).first()
        
        if not position:
            position = find_open_position(clean_symbol, account_type, side)
        
        if not position and clean_symbol != symbol:
            position = find_open_position(symbol, account_type, side)
        
        if not position:
            paper_symbol = f"[PAPER]{clean_symbol}"
            position = find_open_position(paper_symbol, account_type, side)
        
        if not position:
            logger.debug(f"📋 _sync_position_exit_data: no OPEN Position for {symbol}/{account_type}/{side}, "
                        f"EXIT PositionLeg will be created by other paths if needed")
            return
        
        from models import PositionLeg, LegType
        if exit_order_id:
            existing = PositionLeg.query.filter_by(
                position_id=position.id,
                tiger_order_id=str(exit_order_id),
                leg_type=LegType.EXIT,
            ).first()
            if existing:
                logger.debug(f"📋 EXIT PositionLeg already exists for order {exit_order_id}")
                return
        
        exit_leg = add_exit_leg(
            position=position,
            tiger_order_id=str(exit_order_id) if exit_order_id else None,
            price=exit_price,
            quantity=exit_quantity,
            filled_at=exit_time,
            exit_method=exit_method,
            realized_pnl=realized_pnl,
            commission=commission,
        )
        
        if exit_leg:
            logger.info(f"📋 _sync_position_exit_data: created EXIT PositionLeg for Position #{position.id} "
                       f"({position.position_key}), exit@${exit_price}, method={exit_method.value}")
    except Exception as e:
        logger.error(f"📋 _sync_position_exit_data error (non-fatal): {e}")


def _match_entry_signals(
    closed_pos: ClosedPosition,
    symbol: str,
    account_type: str,
    side: str,
    exit_quantity: float,
    exit_price: float,
    parent_order_id: str = None
) -> List[EntrySignalRecord]:
    """
    Match exit to entry signals using precise OrderTracker-based matching with FIFO fallback.
    
    Matching strategy (ordered by precision):
    0. OrderTracker bridge: Use exit order's trade_id/trailing_stop_id to find
       sibling ENTRY orders, then match their order IDs to EntrySignalRecord
    1. Direct parent_order_id match to EntrySignalRecord.entry_order_id
    2. Exact quantity match (single unlinked entry with matching qty)
    3. FIFO fallback (last resort, scoped by trailing_stop_id when available)
    
    Args:
        closed_pos: The ClosedPosition record to link entries to
        symbol: Stock symbol
        account_type: 'real' or 'paper'
        side: Position side
        exit_quantity: Quantity exited
        exit_price: Exit price for P&L calculation
        parent_order_id: Parent order ID for precise matching
        
    Returns:
        List of matched EntrySignalRecord objects
    """
    matched_entries = []
    
    clean_symbol = symbol.replace('[PAPER]', '').strip() if symbol.startswith('[PAPER]') else symbol
    
    from sqlalchemy import or_
    symbol_variants = [clean_symbol]
    if account_type == 'paper':
        symbol_variants.append(f'[PAPER]{clean_symbol}')
    
    # Strategy 0: OrderTracker bridge (highest confidence)
    # Use the exit order ID to find the exit OrderTracker record,
    # then use its trade_id or trailing_stop_id to find sibling ENTRY orders
    exit_order_id = closed_pos.exit_order_id
    if exit_order_id:
        try:
            from models import OrderTracker, OrderRole
            
            exit_tracker = OrderTracker.query.filter_by(tiger_order_id=str(exit_order_id)).first()
            
            if exit_tracker:
                entry_order_ids = []
                
                if exit_tracker.trade_id:
                    sibling_entries = OrderTracker.query.filter_by(
                        trade_id=exit_tracker.trade_id,
                        role=OrderRole.ENTRY,
                        account_type=account_type
                    ).all()
                    entry_order_ids = [e.tiger_order_id for e in sibling_entries if e.tiger_order_id]
                    if entry_order_ids:
                        logger.info(f"🔗 Strategy 0a (trade_id={exit_tracker.trade_id}): "
                                   f"found {len(entry_order_ids)} entry orders: {entry_order_ids}")
                
                if not entry_order_ids and exit_tracker.trailing_stop_id:
                    sibling_entries = OrderTracker.query.filter_by(
                        trailing_stop_id=exit_tracker.trailing_stop_id,
                        role=OrderRole.ENTRY,
                        account_type=account_type
                    ).all()
                    entry_order_ids = [e.tiger_order_id for e in sibling_entries if e.tiger_order_id]
                    if entry_order_ids:
                        logger.info(f"🔗 Strategy 0b (trailing_stop_id={exit_tracker.trailing_stop_id}): "
                                   f"found {len(entry_order_ids)} entry orders: {entry_order_ids}")
                
                if not entry_order_ids and exit_tracker.parent_order_id:
                    parent_tracker = OrderTracker.query.filter_by(
                        tiger_order_id=exit_tracker.parent_order_id
                    ).first()
                    if parent_tracker and parent_tracker.role == OrderRole.ENTRY:
                        entry_order_ids = [parent_tracker.tiger_order_id]
                        logger.info(f"🔗 Strategy 0c (parent_order_id={exit_tracker.parent_order_id}): "
                                   f"found entry order via parent link")
                    elif parent_tracker:
                        search_id = parent_tracker.trade_id or parent_tracker.trailing_stop_id
                        if parent_tracker.trade_id:
                            sibling_entries = OrderTracker.query.filter_by(
                                trade_id=parent_tracker.trade_id,
                                role=OrderRole.ENTRY,
                                account_type=account_type
                            ).all()
                            entry_order_ids = [e.tiger_order_id for e in sibling_entries if e.tiger_order_id]
                        elif parent_tracker.trailing_stop_id:
                            sibling_entries = OrderTracker.query.filter_by(
                                trailing_stop_id=parent_tracker.trailing_stop_id,
                                role=OrderRole.ENTRY,
                                account_type=account_type
                            ).all()
                            entry_order_ids = [e.tiger_order_id for e in sibling_entries if e.tiger_order_id]
                        if entry_order_ids:
                            logger.info(f"🔗 Strategy 0c (via parent's trade/ts): "
                                       f"found {len(entry_order_ids)} entry orders")
                
                if entry_order_ids:
                    precise_entries = EntrySignalRecord.query.filter(
                        EntrySignalRecord.entry_order_id.in_(entry_order_ids),
                        or_(*[EntrySignalRecord.symbol == s for s in symbol_variants]),
                        EntrySignalRecord.account_type == account_type,
                        EntrySignalRecord.side == side,
                        EntrySignalRecord.closed_position_id == None
                    ).order_by(EntrySignalRecord.entry_time.asc()).all()
                    
                    if precise_entries:
                        remaining_qty = exit_quantity
                        for entry in precise_entries:
                            if remaining_qty <= 0:
                                break
                            entry_qty = entry.quantity or 0
                            matched_qty = min(entry_qty, remaining_qty)
                            if matched_qty > 0:
                                if len(precise_entries) == 1 and abs(entry_qty - exit_quantity) < 0.01:
                                    _link_entry_to_exit(entry, closed_pos, exit_price)
                                else:
                                    _link_entry_to_exit(entry, closed_pos, exit_price, matched_qty)
                                matched_entries.append(entry)
                                remaining_qty -= matched_qty
                        
                        logger.info(f"🎯 Strategy 0 (OrderTracker): matched {len(matched_entries)} entries "
                                   f"via order IDs {entry_order_ids} -> Exit #{closed_pos.id}")
                        if remaining_qty > 0.01:
                            logger.warning(f"⚠️ Strategy 0: unmatched qty {remaining_qty} for {clean_symbol}")
                        return matched_entries
                    else:
                        logger.warning(f"⚠️ Strategy 0: found entry order IDs {entry_order_ids} "
                                      f"but no matching unlinked EntrySignalRecords")
                else:
                    logger.debug(f"Strategy 0: no entry order IDs found for exit {exit_order_id}")
            else:
                logger.debug(f"Strategy 0: exit order {exit_order_id} not in OrderTracker")
        except Exception as e:
            logger.warning(f"⚠️ Strategy 0 error: {str(e)}")
    
    # Strategy 1: Precise match via parent_order_id -> EntrySignalRecord.entry_order_id
    if parent_order_id:
        precise_entry = EntrySignalRecord.query.filter(
            EntrySignalRecord.entry_order_id == str(parent_order_id),
            or_(*[EntrySignalRecord.symbol == s for s in symbol_variants]),
            EntrySignalRecord.account_type == account_type,
            EntrySignalRecord.side == side,
            EntrySignalRecord.closed_position_id == None
        ).first()
        
        if precise_entry:
            _link_entry_to_exit(precise_entry, closed_pos, exit_price)
            matched_entries.append(precise_entry)
            logger.info(f"🎯 Strategy 1 (parent order ID): Entry #{precise_entry.id} order={parent_order_id} -> Exit #{closed_pos.id}")
            return matched_entries
        else:
            logger.warning(f"⚠️ Strategy 1 failed: no unlinked entry with order_id={parent_order_id}")
    
    # Strategy 2+3: Scoped FIFO - narrow to trailing_stop_id when available
    trailing_stop_id = closed_pos.trailing_stop_id
    unlinked_entries = None
    
    if trailing_stop_id:
        ts_entry_ids = set()
        try:
            from models import OrderTracker, OrderRole
            ts_entries = OrderTracker.query.filter_by(
                trailing_stop_id=trailing_stop_id,
                role=OrderRole.ENTRY,
                account_type=account_type
            ).all()
            ts_entry_ids = {e.tiger_order_id for e in ts_entries if e.tiger_order_id}
        except Exception:
            pass
        
        if ts_entry_ids:
            ts_scoped = EntrySignalRecord.query.filter(
                EntrySignalRecord.entry_order_id.in_(list(ts_entry_ids)),
                or_(*[EntrySignalRecord.symbol == s for s in symbol_variants]),
                EntrySignalRecord.account_type == account_type,
                EntrySignalRecord.side == side,
                EntrySignalRecord.closed_position_id == None
            ).order_by(EntrySignalRecord.entry_time.asc()).all()
            if ts_scoped:
                unlinked_entries = ts_scoped
                logger.info(f"📊 Scoped to {len(ts_scoped)} entries via trailing_stop_id={trailing_stop_id}")
    
    if unlinked_entries is None:
        unlinked_entries = EntrySignalRecord.query.filter(
            or_(*[EntrySignalRecord.symbol == s for s in symbol_variants]),
            EntrySignalRecord.account_type == account_type,
            EntrySignalRecord.side == side,
            EntrySignalRecord.closed_position_id == None
        ).order_by(EntrySignalRecord.entry_time.asc()).all()
    
    if not unlinked_entries:
        logger.warning(f"⚠️ No unlinked entries found for {clean_symbol}/{account_type}/{side}")
        return matched_entries
    
    # Strategy 2: Exact quantity match (single entry with matching qty)
    if len(unlinked_entries) == 1 and abs((unlinked_entries[0].quantity or 0) - exit_quantity) < 0.01:
        entry = unlinked_entries[0]
        _link_entry_to_exit(entry, closed_pos, exit_price)
        matched_entries.append(entry)
        logger.info(f"🎯 Strategy 2 (exact qty): Entry #{entry.id} qty={entry.quantity} -> Exit #{closed_pos.id}")
        return matched_entries
    
    # Strategy 3: FIFO matching (last resort)
    remaining_qty = exit_quantity
    
    for entry in unlinked_entries:
        if remaining_qty <= 0:
            break
            
        entry_qty = entry.quantity or 0
        matched_qty = min(entry_qty, remaining_qty)
        
        if matched_qty > 0:
            _link_entry_to_exit(entry, closed_pos, exit_price, matched_qty)
            matched_entries.append(entry)
            remaining_qty -= matched_qty
            
            logger.info(f"📊 Strategy 3 (FIFO): Entry #{entry.id} (qty={entry_qty}, matched={matched_qty}) -> Exit #{closed_pos.id}")
    
    if remaining_qty > 0.01:
        logger.warning(f"⚠️ Unmatched exit quantity: {remaining_qty} for {clean_symbol}")
    
    return matched_entries


def _link_entry_to_exit(
    entry: EntrySignalRecord,
    closed_pos: ClosedPosition,
    exit_price: float,
    matched_qty: float = None
) -> None:
    """
    Link an EntrySignalRecord to a ClosedPosition and set contribution P&L.
    
    P&L source priority:
    1. Tiger API returned realized_pnl (from closed_pos.total_pnl) - most accurate
       - Precise match (1:1): use total_pnl directly
       - FIFO multi-entry: proportionally allocate total_pnl by quantity
    2. Fallback: calculate from price difference (only when Tiger returns 0 or None)
    
    Args:
        entry: EntrySignalRecord to link
        closed_pos: ClosedPosition to link to
        exit_price: Exit price for fallback P&L calculation
        matched_qty: Quantity matched (None = full entry quantity, means precise 1:1 match)
    """
    entry.closed_position_id = closed_pos.id
    
    entry_price = entry.entry_price or 0
    qty = matched_qty if matched_qty else (entry.quantity or 0)
    tiger_pnl = closed_pos.total_pnl
    tiger_pnl_pct = closed_pos.total_pnl_pct
    
    if tiger_pnl is not None and abs(tiger_pnl) > 0.01:
        if matched_qty is None:
            entry.contribution_pnl = tiger_pnl
            entry.contribution_pct = tiger_pnl_pct
            logger.info(f"💰 Entry #{entry.id} P&L from Tiger (precise match): ${tiger_pnl:.2f}")
        else:
            exit_qty = closed_pos.exit_quantity or 1
            ratio = qty / exit_qty if exit_qty > 0 else 1
            entry.contribution_pnl = round(tiger_pnl * ratio, 2)
            if tiger_pnl_pct is not None:
                entry.contribution_pct = round(tiger_pnl_pct, 2)
            elif entry_price > 0 and qty > 0:
                entry.contribution_pct = (entry.contribution_pnl / (entry_price * qty)) * 100
            logger.info(f"💰 Entry #{entry.id} P&L from Tiger (FIFO ratio={ratio:.2f}): ${entry.contribution_pnl:.2f}")
    elif entry_price > 0 and qty > 0 and exit_price > 0:
        if entry.side == 'long':
            contribution_pnl = (exit_price - entry_price) * qty
        else:
            contribution_pnl = (entry_price - exit_price) * qty
        entry.contribution_pnl = contribution_pnl
        if entry_price > 0:
            entry.contribution_pct = (contribution_pnl / (entry_price * qty)) * 100
        logger.info(f"💰 Entry #{entry.id} P&L fallback (price diff): ${contribution_pnl:.2f} "
                   f"[Tiger total_pnl={tiger_pnl}]")
    
    logger.debug(f"Linked entry #{entry.id} to exit #{closed_pos.id}, "
                f"contribution_pnl={entry.contribution_pnl}")


def get_entry_signal_for_order(order_id: str) -> Optional[EntrySignalRecord]:
    """Get EntrySignalRecord by Tiger order ID"""
    return EntrySignalRecord.query.filter_by(entry_order_id=str(order_id)).first()


def get_trade_signal_data(order_id: str) -> Optional[str]:
    """
    Get original signal JSON from Trade table by order ID.
    Used when WebSocket creates EntrySignalRecord but needs raw_json.
    """
    trade = Trade.query.filter_by(tiger_order_id=str(order_id)).first()
    if trade and trade.signal_data:
        return trade.signal_data
    return None


def determine_exit_method(
    realized_pnl: float = None,
    order_type: str = None,
    is_trailing_stop: bool = False,
    is_webhook_signal: bool = False,
    stop_order_id: str = None,
    take_profit_order_id: str = None,
    exit_order_id: str = None
) -> ExitMethod:
    """
    Determine ExitMethod based on available information.
    
    Priority:
    1. Explicit flags (is_webhook_signal, is_trailing_stop)
    2. Order ID matching (stop_order_id, take_profit_order_id)
    3. realized_pnl sign (positive = TAKE_PROFIT, negative = STOP_LOSS)
    4. Fallback to STOP_LOSS
    
    Args:
        realized_pnl: Tiger API realized P&L
        order_type: Order type string
        is_trailing_stop: Flag if this is a trailing stop exit
        is_webhook_signal: Flag if this is a webhook close signal
        stop_order_id: Stop loss order ID
        take_profit_order_id: Take profit order ID
        exit_order_id: The exit order ID
        
    Returns:
        ExitMethod enum value
    """
    if is_webhook_signal:
        logger.debug(f"ExitMethod determined: WEBHOOK_SIGNAL (explicit flag)")
        return ExitMethod.WEBHOOK_SIGNAL
    
    if is_trailing_stop:
        logger.debug(f"ExitMethod determined: TRAILING_STOP (explicit flag)")
        return ExitMethod.TRAILING_STOP
    
    if exit_order_id:
        if stop_order_id and str(exit_order_id) == str(stop_order_id):
            logger.debug(f"ExitMethod determined: STOP_LOSS (order ID match: {exit_order_id})")
            return ExitMethod.STOP_LOSS
        if take_profit_order_id and str(exit_order_id) == str(take_profit_order_id):
            logger.debug(f"ExitMethod determined: TAKE_PROFIT (order ID match: {exit_order_id})")
            return ExitMethod.TAKE_PROFIT
    
    if realized_pnl is not None:
        method = ExitMethod.TAKE_PROFIT if realized_pnl >= 0 else ExitMethod.STOP_LOSS
        logger.debug(f"ExitMethod determined: {method.value} (pnl sign: ${realized_pnl})")
        return method
    
    logger.debug(f"ExitMethod determined: STOP_LOSS (fallback)")
    return ExitMethod.STOP_LOSS


def _lookup_entry_price(symbol: str, account_type: str, side: str, trailing_stop_id: int = None) -> Optional[float]:
    """
    Look up avg entry price from TrailingStopPosition or EntrySignalRecord
    when not provided by the caller.
    """
    try:
        from models import TrailingStopPosition
        
        if trailing_stop_id:
            ts_pos = TrailingStopPosition.query.get(trailing_stop_id)
            if ts_pos:
                price = getattr(ts_pos, 'first_entry_price', None) or ts_pos.entry_price
                if price:
                    logger.debug(f"Found entry price from TrailingStopPosition #{trailing_stop_id}: ${price} (first_entry_price preferred)")
                    return price
        
        symbol_variants = [symbol]
        if account_type == 'paper' and not symbol.startswith('[PAPER]'):
            symbol_variants.append(f'[PAPER]{symbol}')
        elif symbol.startswith('[PAPER]'):
            symbol_variants.append(symbol.replace('[PAPER]', '').strip())
        
        from sqlalchemy import or_
        ts_pos = TrailingStopPosition.query.filter(
            or_(*[TrailingStopPosition.symbol == s for s in symbol_variants]),
            TrailingStopPosition.account_type == account_type,
            TrailingStopPosition.side == side
        ).order_by(TrailingStopPosition.created_at.desc()).first()
        
        if ts_pos:
            price = getattr(ts_pos, 'first_entry_price', None) or ts_pos.entry_price
            if price:
                logger.debug(f"Found entry price from TrailingStopPosition #{ts_pos.id}: ${price} (first_entry_price preferred)")
                return price
        
        entry = EntrySignalRecord.query.filter(
            or_(*[EntrySignalRecord.symbol == s for s in symbol_variants]),
            EntrySignalRecord.account_type == account_type,
            EntrySignalRecord.side == side,
            EntrySignalRecord.entry_price != None
        ).order_by(EntrySignalRecord.created_at.desc()).first()
        
        if entry and entry.entry_price:
            logger.debug(f"Found entry price from EntrySignalRecord #{entry.id}: ${entry.entry_price}")
            return entry.entry_price
        
        from models import TigerFilledOrder
        entry_action = 'BUY' if side == 'long' else 'SELL'
        tiger_entry = TigerFilledOrder.query.filter(
            TigerFilledOrder.symbol == symbol,
            TigerFilledOrder.account_type == account_type,
            TigerFilledOrder.action == entry_action,
            TigerFilledOrder.is_open == True
        ).order_by(TigerFilledOrder.trade_time.desc()).first()
        
        if tiger_entry and tiger_entry.avg_fill_price:
            logger.debug(f"Found entry price from TigerFilledOrder #{tiger_entry.id}: ${tiger_entry.avg_fill_price}")
            return tiger_entry.avg_fill_price
        
        logger.debug(f"No entry price found for {symbol}/{account_type}/{side}")
        return None
        
    except Exception as e:
        logger.error(f"Error looking up entry price: {str(e)}")
        return None
