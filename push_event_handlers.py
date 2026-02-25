"""
WebSocket Push Event Handlers
Handles order fills, position changes, and quote updates from Tiger WebSocket
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

from app import db
from models import (
    Trade, TrailingStopPosition,
    OrderStatus, ExitMethod, OCAGroup, OCAStatus
)

logger = logging.getLogger(__name__)

_position_cache = {}

def _detect_account_type(account: str) -> str:
    """Detect account type from account string.
    Paper accounts have long numeric IDs (>15 digits), real accounts are shorter."""
    if not account:
        return 'real'
    account_str = str(account).strip()
    from tiger_client import get_config
    paper_account = get_config('TIGER_PAPER_ACCOUNT', '21994480083284213')
    if account_str == paper_account:
        return 'paper'
    if len(account_str) > 15:
        return 'paper'
    return 'real'

def update_position_cache(symbol: str, account_type: str, quantity: int, avg_cost: float = 0):
    """Update the WebSocket position cache.
    Keeps zero-quantity entries (marked as closed) so we can detect closed positions.
    """
    key = f"{symbol}:{account_type}"
    _position_cache[key] = {
        'symbol': symbol,
        'quantity': quantity,
        'average_cost': avg_cost,
        'updated_at': datetime.utcnow()
    }

def get_all_cached_positions(account_type: str, max_age_seconds: int = 30) -> Dict[str, Dict]:
    """Get all WebSocket-cached positions for an account type.
    Only returns entries with fresh timestamps (within max_age_seconds).
    
    Returns:
        Dict of symbol -> position data (including zero-quantity closed positions)
    """
    result = {}
    suffix = f":{account_type}"
    now = datetime.utcnow()
    for key, cached in _position_cache.items():
        if key.endswith(suffix):
            age = (now - cached['updated_at']).total_seconds()
            if age <= max_age_seconds:
                result[cached['symbol']] = cached.copy()
                result[cached['symbol']]['age_seconds'] = age
                result[cached['symbol']]['is_fresh'] = True
    return result

def get_cached_position(symbol: str, account_type: str, max_age_seconds: int = 30) -> Optional[Dict]:
    """Get cached position data from WebSocket updates.
    
    Args:
        symbol: Stock symbol
        account_type: 'real' or 'paper'
        max_age_seconds: Maximum age in seconds to consider cache fresh.
                        During regular hours WebSocket pushes every ~1s, so 30s is very safe.
                        During extended hours there are no pushes, so cache will be stale.
    
    Returns:
        Copy of position dict with 'is_fresh' flag, or None if not cached.
    """
    key = f"{symbol}:{account_type}"
    cached = _position_cache.get(key)
    if cached:
        result = cached.copy()
        age = (datetime.utcnow() - result['updated_at']).total_seconds()
        result['age_seconds'] = age
        result['is_fresh'] = age <= max_age_seconds
        return result
    return None


def handle_order_fill(order_data: Any) -> None:
    """Handle order fill event from WebSocket.

    Simplified: routes ALL fills through handle_fill_event() in order_tracker_service.
    This ensures a single code path for fill processing regardless of source (WebSocket or polling).

    Flow:
    1. Parse WebSocket data
    2. If order is in OrderTracker → call handle_fill_event() (handles everything)
    3. If order has a Trade but no OrderTracker → register it first, then handle_fill_event()
    4. If external order (no Trade, no OrderTracker) → register as appropriate role, then handle_fill_event()
    """
    from app import app
    try:
        symbol = getattr(order_data, 'symbol', None) or getattr(order_data, 'contract', {}).get('symbol')
        order_id = str(getattr(order_data, 'id', None) or getattr(order_data, 'order_id', ''))
        status = getattr(order_data, 'status', None)
        action = getattr(order_data, 'action', None)
        parent_id = getattr(order_data, 'parent_id', None) or getattr(order_data, 'parentId', None)
        if parent_id:
            parent_id = str(parent_id)
        filled_quantity = (
            getattr(order_data, 'filledQuantity', 0) or
            getattr(order_data, 'filled_quantity', 0) or
            getattr(order_data, 'filled', 0) or
            getattr(order_data, 'totalQuantity', 0)
        )
        avg_fill_price = (
            getattr(order_data, 'avgFillPrice', 0) or
            getattr(order_data, 'avg_fill_price', 0)
        )
        realized_pnl = getattr(order_data, 'realized_pnl', 0) or getattr(order_data, 'realizedPnl', 0)
        commission = getattr(order_data, 'commission', 0) or getattr(order_data, 'commissionAndFee', 0)
        account = str(getattr(order_data, 'account', ''))
        order_type_str = getattr(order_data, 'order_type', None) or getattr(order_data, 'orderType', None)
        if hasattr(order_type_str, 'value'):
            order_type_str = order_type_str.value
        order_type_str = str(order_type_str).upper() if order_type_str else None

        if hasattr(status, 'value'):
            status = status.value
        if hasattr(action, 'value'):
            action = action.value

        status_str = str(status).upper() if status else ''

        logger.info(f"📋 WebSocket order event: {symbol} {action} status={status} "
                    f"filled={filled_quantity} @ ${avg_fill_price}")

        if status_str in ('CANCELLED', 'EXPIRED', 'REJECTED', 'INACTIVE'):
            _handle_exit_order_terminal_failure(order_id, symbol, status_str, account, filled_quantity)
            return

        if status_str != 'FILLED':
            logger.debug(f"Order {order_id} not filled yet, status={status}")
            return

        account_type = _detect_account_type(account)

        with app.app_context():
            from order_tracker_service import handle_fill_event, register_order
            from models import OrderTracker

            tracker = OrderTracker.query.filter_by(tiger_order_id=order_id).first()

            if tracker:
                result, fill_status = handle_fill_event(
                    tiger_order_id=order_id,
                    filled_quantity=filled_quantity,
                    avg_fill_price=avg_fill_price,
                    realized_pnl=realized_pnl,
                    commission=commission,
                    fill_time=datetime.utcnow(),
                    source='websocket',
                )
                logger.info(f"✅ WebSocket fill routed via OrderTracker: {order_id} → {fill_status}")

                if tracker.role.value == 'entry':
                    _handle_post_entry_fill_websocket(order_id, account_type)

            else:
                trade = Trade.query.filter_by(tiger_order_id=order_id).first()

                if trade:
                    is_close = hasattr(trade, 'is_close_position') and trade.is_close_position
                    role = 'exit_signal' if is_close else 'entry'
                    side_val = trade.side.value if hasattr(trade.side, 'value') else str(trade.side)

                    entry_signal = None
                    if is_close:
                        entry_trade = Trade.query.filter_by(
                            symbol=trade.symbol,
                            is_close_position=False,
                        ).order_by(Trade.created_at.desc()).first()
                        if entry_trade:
                            entry_signal = entry_trade.raw_signal
                    else:
                        entry_signal = trade.raw_signal if hasattr(trade, 'raw_signal') else None

                    register_order(
                        tiger_order_id=order_id,
                        symbol=trade.symbol,
                        account_type=account_type,
                        role=role,
                        side=side_val.upper() if side_val else None,
                        quantity=trade.quantity,
                        trade_id=trade.id,
                        signal_content=entry_signal,
                    )

                    result, fill_status = handle_fill_event(
                        tiger_order_id=order_id,
                        filled_quantity=filled_quantity,
                        avg_fill_price=avg_fill_price,
                        realized_pnl=realized_pnl,
                        commission=commission,
                        fill_time=datetime.utcnow(),
                        source='websocket',
                    )
                    logger.info(f"✅ WebSocket fill (Trade found, registered): {order_id} → {fill_status}")

                    if not is_close:
                        _handle_post_entry_fill_websocket(order_id, account_type)

                else:
                    _handle_external_fill_via_tracker(
                        symbol=symbol,
                        order_id=order_id,
                        account_type=account_type,
                        action=action,
                        parent_id=parent_id,
                        filled_quantity=filled_quantity,
                        avg_fill_price=avg_fill_price,
                        realized_pnl=realized_pnl,
                        commission=commission,
                        order_type=order_type_str,
                    )

    except Exception as e:
        logger.error(f"❌ Error handling order fill: {str(e)}")
        import traceback
        traceback.print_exc()
        try:
            with app.app_context():
                db.session.rollback()
        except Exception:
            pass


def _handle_exit_order_terminal_failure(order_id: str, symbol: str, status: str, account: str,
                                        filled_quantity: float = 0) -> None:
    """Handle EXIT_TRAILING orders that were cancelled/expired/rejected by broker.

    When Tiger accepts an order but later cancels/expires/rejects it, the position
    loses trailing stop protection. This function detects that scenario and reactivates
    the trailing stop for retry, mirroring the Alpaca-side logic.

    If the order was partially filled before cancellation, the TS quantity is reduced
    by the filled amount so only the remaining position is protected on retry.
    """
    from app import app
    try:
        with app.app_context():
            from models import OrderTracker, OrderRole, TrailingStopPosition

            tracker = OrderTracker.query.filter_by(tiger_order_id=order_id).first()
            if not tracker:
                return
            if tracker.role != OrderRole.EXIT_TRAILING:
                logger.debug(f"Order {order_id} is {tracker.role.value}, not EXIT_TRAILING, skipping reactivation")
                return

            tracker.status = status
            account_type = _detect_account_type(account)

            partial_filled = filled_quantity or tracker.filled_quantity or 0

            MAX_EXIT_RETRIES = 5
            ts = None
            if tracker.trailing_stop_id:
                ts = TrailingStopPosition.query.get(tracker.trailing_stop_id)
            if not ts:
                ts = TrailingStopPosition.query.filter(
                    TrailingStopPosition.symbol == (symbol or tracker.symbol),
                    TrailingStopPosition.account_type == account_type,
                    TrailingStopPosition.is_active == False,
                    TrailingStopPosition.is_triggered == True,
                ).order_by(TrailingStopPosition.triggered_at.desc()).first()

            if not ts:
                logger.warning(f"⚠️ [{symbol}] EXIT_TRAILING order {order_id} {status}, "
                              f"but no triggered trailing stop found to reactivate")
                db.session.commit()
                return

            if partial_filled and partial_filled > 0:
                old_qty = ts.quantity
                remaining = old_qty - partial_filled
                if remaining <= 0:
                    logger.info(f"[{symbol}] Partial fill {partial_filled} >= TS qty {old_qty}, "
                               f"exit effectively complete, not reactivating")
                    db.session.commit()
                    return
                ts.quantity = remaining
                logger.info(f"[{symbol}] TS #{ts.id} quantity adjusted: {old_qty} -> {remaining} "
                           f"(partial fill {partial_filled})")

            retry_count = (ts.trigger_retry_count or 0) + 1
            ts.trigger_retry_count = retry_count

            if retry_count < MAX_EXIT_RETRIES:
                ts.is_active = True
                ts.is_triggered = False
                ts.triggered_at = datetime.utcnow()
                ts.triggered_price = None
                ts.trigger_reason = None

                partial_info = f", partial fill {partial_filled}" if partial_filled > 0 else ""
                logger.info(f"🔄 [{symbol}] Reactivated TS #{ts.id} after EXIT_TRAILING order {status} "
                           f"(retry {retry_count}/{MAX_EXIT_RETRIES}{partial_info})")

                try:
                    from discord_notifier import discord_notifier
                    discord_notifier.send_notification(
                        f"🚨 **出场订单被取消/拒绝 (重试 {retry_count}/{MAX_EXIT_RETRIES})**\n"
                        f"股票: {symbol}\n"
                        f"账户: {account_type}\n"
                        f"订单状态: {status}\n"
                        f"订单ID: {order_id}\n"
                        f"Trailing Stop 已重新激活，将自动重试",
                        title="出场订单失败(WS)"
                    )
                except Exception:
                    pass
            else:
                ts.is_active = False
                ts.is_triggered = True
                ts.triggered_at = datetime.utcnow()
                ts.trigger_reason = f"Exit order {status} {MAX_EXIT_RETRIES} times (WS detected)"

                logger.error(f"🚨🚨 [{symbol}] EXIT_TRAILING order failed {MAX_EXIT_RETRIES} times via WS! "
                            f"TS #{ts.id} permanently deactivated.")

                try:
                    from discord_notifier import discord_notifier
                    discord_notifier.send_notification(
                        f"🚨🚨🚨 **出场订单彻底失败 (WebSocket检测)**\n"
                        f"股票: {symbol}\n"
                        f"账户: {account_type}\n"
                        f"已重试 {MAX_EXIT_RETRIES} 次均失败\n"
                        f"最后状态: {status}\n"
                        f"⚠️ 仓位可能仍在券商，需要手动处理！",
                        title="紧急: 出场订单彻底失败"
                    )
                except Exception:
                    pass

            db.session.commit()

    except Exception as e:
        logger.error(f"❌ Error handling EXIT_TRAILING terminal failure for {order_id}: {e}")
        import traceback
        traceback.print_exc()
        try:
            with app.app_context():
                db.session.rollback()
        except Exception:
            pass


def _handle_post_entry_fill_websocket(order_id: str, account_type: str) -> None:
    """Handle auto-protection after entry fill detected via WebSocket.

    This covers the case where the entry fill is detected by WebSocket before polling.
    If the Trade has needs_auto_protection set, creates OCA protection.
    """
    try:
        trade = Trade.query.filter_by(tiger_order_id=order_id).first()
        if trade and hasattr(trade, 'needs_auto_protection') and trade.needs_auto_protection:
            _handle_auto_protection_from_websocket(
                trade=trade,
                account_type=account_type,
                filled_price=trade.filled_price or 0,
                filled_quantity=trade.filled_quantity or 0,
            )
    except Exception as e:
        logger.error(f"❌ Error in post-entry fill WebSocket handling: {e}")


def _handle_external_fill_via_tracker(
    symbol: str,
    order_id: str,
    account_type: str,
    action: str,
    parent_id: str,
    filled_quantity: float,
    avg_fill_price: float,
    realized_pnl: float,
    commission: float,
    order_type: str = None,
) -> None:
    """Handle fills for orders not in Trade table (external, SL/TP from Tiger).

    Strategy:
    1. Check if this is an OCA leg → handle via oca_service
    2. Check if this is a bracket sub-order → detect SL/TP via order_type (STP/LMT)
    3. Determine role from parent_order_id or action
    4. Register to OrderTracker
    5. Call handle_fill_event()
    """
    from order_tracker_service import handle_fill_event, register_order

    action_upper = (action or '').upper()
    is_sell = action_upper in ('SELL', 'SELL_OPEN', 'SELL_CLOSE')
    is_buy = action_upper in ('BUY', 'BUY_OPEN', 'BUY_CLOSE')
    is_explicit_open = action_upper in ('BUY_OPEN', 'SELL_OPEN')

    if is_explicit_open and (not realized_pnl or realized_pnl == 0):
        logger.info(f"📊 Order {order_id} is explicitly OPEN ({action_upper}), skipping")
        return

    oca_group = OCAGroup.query.filter(
        (OCAGroup.stop_order_id == order_id) |
        (OCAGroup.take_profit_order_id == order_id)
    ).filter(OCAGroup.status == OCAStatus.ACTIVE).first()

    if oca_group:
        is_stop = order_id == oca_group.stop_order_id
        role = 'stop_loss' if is_stop else 'take_profit'
        side = 'SELL' if is_sell else 'BUY'

        register_order(
            tiger_order_id=order_id,
            symbol=symbol,
            account_type=account_type,
            role=role,
            side=side,
            quantity=filled_quantity,
            parent_order_id=parent_id,
            trailing_stop_id=oca_group.trailing_stop_id,
        )

        from oca_service import on_oca_leg_filled
        oca_result, oca_status = on_oca_leg_filled(
            tiger_order_id=order_id,
            filled_price=avg_fill_price,
            filled_quantity=filled_quantity,
            realized_pnl=realized_pnl,
            commission=commission,
        )

        if oca_result:
            logger.info(f"📊 OCA leg handled: {oca_status}")

        handle_fill_event(
            tiger_order_id=order_id,
            filled_quantity=filled_quantity,
            avg_fill_price=avg_fill_price,
            realized_pnl=realized_pnl,
            commission=commission,
            fill_time=datetime.utcnow(),
            source='websocket_oca',
        )
        return

    if parent_id:
        from models import OrderTracker as OT
        parent_tracker = OT.query.filter_by(tiger_order_id=parent_id).first()
        trailing_stop_id = parent_tracker.trailing_stop_id if parent_tracker else None
        
        parent_side = None
        if parent_tracker and parent_tracker.side:
            parent_side = parent_tracker.side.upper()
        
        if order_type in ('STP', 'STOP', 'STP_LMT'):
            role = 'stop_loss'
        elif order_type in ('LMT', 'LIMIT') and parent_tracker and parent_tracker.role.value == 'entry':
            role = 'take_profit'
        elif parent_side:
            is_closing_long = parent_side in ('BUY', 'BUY_OPEN') and is_sell
            is_closing_short = parent_side in ('SELL', 'SELL_OPEN', 'SELL_SHORT') and is_buy
            if is_closing_long or is_closing_short:
                role = 'stop_loss' if order_type in ('STP', 'STOP', 'STP_LMT', None) else 'take_profit'
            else:
                role = 'stop_loss'
        else:
            role = 'stop_loss' if order_type in ('STP', 'STOP', 'STP_LMT', None) else 'take_profit'
        
        logger.info(f"📋 Bracket sub-order role inferred: {role} (order_type={order_type}, "
                    f"parent_side={parent_side}, is_sell={is_sell}, is_buy={is_buy})")

        register_order(
            tiger_order_id=order_id,
            symbol=symbol,
            account_type=account_type,
            role=role,
            side='SELL' if is_sell else 'BUY',
            quantity=filled_quantity,
            parent_order_id=parent_id,
            trailing_stop_id=trailing_stop_id,
        )

        handle_fill_event(
            tiger_order_id=order_id,
            filled_quantity=filled_quantity,
            avg_fill_price=avg_fill_price,
            realized_pnl=realized_pnl,
            commission=commission,
            fill_time=datetime.utcnow(),
            source='websocket_attached',
        )
        return

    if (not realized_pnl or realized_pnl == 0):
        ts_side = 'long' if is_sell else 'short'
        any_ts = TrailingStopPosition.query.filter_by(
            symbol=symbol, account_type=account_type, side=ts_side
        ).first()
        if not any_ts:
            logger.info(f"📊 External order {order_id}: no position history, likely opening order, skipping")
            return

    role = 'exit_signal'
    register_order(
        tiger_order_id=order_id,
        symbol=symbol,
        account_type=account_type,
        role=role,
        side='SELL' if is_sell else 'BUY',
        quantity=filled_quantity,
    )

    handle_fill_event(
        tiger_order_id=order_id,
        filled_quantity=filled_quantity,
        avg_fill_price=avg_fill_price,
        realized_pnl=realized_pnl,
        commission=commission,
        fill_time=datetime.utcnow(),
        source='websocket_external',
    )
    logger.info(f"📊 External fill registered and processed: {symbol} {order_id}")


def _handle_auto_protection_from_websocket(
    trade: Trade,
    account_type: str,
    filled_price: float,
    filled_quantity: float
) -> None:
    """Handle needs_auto_protection when entry order fills via WebSocket.
    This covers the case where routes.py 1-second poll missed the fill.
    Creates/updates OCA protection for the position (scaling/position increase).
    Routes through oca_service for centralized dedup and OCAGroup record creation.
    
    Paper accounts use bracket-only architecture (no OCA), so auto-protection
    is skipped — bracket sub-orders are already created with the entry order.
    """
    try:
        if account_type == 'paper':
            logger.info(f"📎 Skipping OCA auto-protection for Paper {trade.symbol}: "
                       f"Paper uses bracket-only architecture (sub-orders created with entry)")
            trade.needs_auto_protection = False
            trade.protection_info = None
            return
        
        import json
        protection_info = {}
        if hasattr(trade, 'protection_info') and trade.protection_info:
            protection_info = json.loads(trade.protection_info)
        
        stop_loss_price = protection_info.get('stop_loss_price') or trade.stop_loss_price
        take_profit_price = protection_info.get('take_profit_price') or trade.take_profit_price
        
        if not stop_loss_price and not take_profit_price:
            logger.warning(f"⚠️ needs_auto_protection set but no SL/TP prices for {trade.symbol}")
            trade.needs_auto_protection = False
            return
        
        from models import TrailingStopPosition
        existing_ts = TrailingStopPosition.query.filter_by(
            symbol=trade.symbol, account_type=account_type, is_active=True
        ).first()
        has_switched = existing_ts.has_switched_to_trailing if existing_ts else False
        
        take_profit_for_oca = None if has_switched else take_profit_price
        if has_switched:
            logger.info(f"🔄 {trade.symbol} already switched to dynamic trailing, only creating stop loss for scaling")
        
        ts_side = 'long' if trade.side and trade.side.value == 'buy' else 'short'
        trailing_stop_id = existing_ts.id if existing_ts else None
        
        from oca_service import create_oca_protection
        oca_result, oca_status = create_oca_protection(
            trailing_stop_id=trailing_stop_id,
            symbol=trade.symbol,
            side=ts_side,
            quantity=filled_quantity,
            stop_price=stop_loss_price,
            take_profit_price=take_profit_for_oca,
            account_type=account_type,
            trade_id=trade.id,
            entry_price=filled_price,
            force_replace=True,
            creation_source='websocket_auto_protection'
        )
        
        if oca_result:
            logger.info(f"✅ Auto-protection applied via WebSocket for {trade.symbol}: {oca_status}")
            if hasattr(oca_result, 'stop_order_id'):
                trade.stop_loss_order_id = oca_result.stop_order_id
            if hasattr(oca_result, 'take_profit_order_id'):
                trade.take_profit_order_id = oca_result.take_profit_order_id
            
            if existing_ts:
                try:
                    from trailing_stop_engine import update_trailing_stop_on_position_increase
                    
                    if account_type == 'paper':
                        from tiger_client import TigerPaperClient
                        tiger_client = TigerPaperClient()
                    else:
                        from tiger_client import TigerClient
                        tiger_client = TigerClient()
                    
                    position_result = tiger_client.get_positions(symbol=trade.symbol)
                    if position_result.get('success') and position_result.get('positions'):
                        avg_cost = position_result['positions'][0].get('average_cost', 0)
                        current_quantity = abs(position_result['positions'][0]['quantity'])
                        
                        ts_update = update_trailing_stop_on_position_increase(
                            symbol=trade.symbol,
                            account_type=account_type,
                            new_quantity=current_quantity,
                            new_entry_price=avg_cost,
                            new_stop_loss_price=stop_loss_price,
                            new_take_profit_price=take_profit_price,
                            new_stop_loss_order_id=getattr(oca_result, 'stop_order_id', None),
                            new_take_profit_order_id=getattr(oca_result, 'take_profit_order_id', None)
                        )
                        if ts_update.get('success'):
                            logger.info(f"✅ TrailingStop updated after scaling via WebSocket: {ts_update['message']}")
                        else:
                            logger.warning(f"⚠️ TrailingStop update after scaling: {ts_update.get('message')}")
                except Exception as ts_err:
                    logger.error(f"❌ TrailingStop update error after scaling: {ts_err}")
        else:
            logger.error(f"❌ Auto-protection failed via WebSocket: {oca_status}")
        
        trade.needs_auto_protection = False
        trade.protection_info = None
        
    except Exception as e:
        logger.error(f"❌ Error handling auto-protection from WebSocket: {str(e)}")










def handle_position_change(position_data: Any) -> None:
    """
    Handle position change event from WebSocket
    Detects when positions are closed externally - serves as FALLBACK
    Creates ClosedPosition if Order Fill event didn't catch it
    """
    from app import app
    try:
        symbol = getattr(position_data, 'symbol', None) or getattr(position_data, 'contract', {}).get('symbol')
        
        # CRITICAL FIX: Safely extract quantity - don't default to 0 if attribute is missing
        # This prevents false deactivation when WebSocket pushes incomplete data
        raw_quantity = getattr(position_data, 'quantity', None)
        raw_position = getattr(position_data, 'position', None)
        raw_position_qty = getattr(position_data, 'positionQty', None)
        
        # Use the first non-None value, or None if all are missing
        quantity = raw_quantity if raw_quantity is not None else (raw_position if raw_position is not None else raw_position_qty)
        
        market_value = getattr(position_data, 'market_value', 0) or getattr(position_data, 'marketValue', 0)
        avg_cost = getattr(position_data, 'average_cost', 0) or getattr(position_data, 'averageCost', 0)
        realized_pnl = getattr(position_data, 'realized_pnl', 0) or getattr(position_data, 'realizedPnl', 0)
        account = str(getattr(position_data, 'account', ''))
        
        unrealized_pnl = getattr(position_data, 'unrealized_pnl', None) or getattr(position_data, 'unrealizedPnl', None)
        latest_price = getattr(position_data, 'latest_price', None) or getattr(position_data, 'latestPrice', None)
        salable_qty = getattr(position_data, 'salable_qty', None) or getattr(position_data, 'salableQty', None)
        sec_type = getattr(position_data, 'sec_type', None) or getattr(position_data, 'secType', None)
        currency = getattr(position_data, 'currency', None)
        
        logger.info(f"📋 Position change: {symbol} qty={quantity} value=${market_value} realized_pnl=${realized_pnl}")
        
        account_type = _detect_account_type(account)
        
        if symbol and quantity is not None:
            update_position_cache(symbol, account_type, quantity, avg_cost)
        
        if symbol:
            try:
                with app.app_context():
                    from models import TigerHolding
                    from datetime import datetime
                    
                    if quantity is not None and quantity == 0:
                        holding = TigerHolding.query.filter_by(
                            account_type=account_type, symbol=symbol
                        ).first()
                        if holding:
                            db.session.delete(holding)
                            db.session.commit()
                    elif quantity is not None and quantity != 0:
                        now = datetime.utcnow()
                        pnl_pct = None
                        if avg_cost and avg_cost > 0 and unrealized_pnl is not None and quantity:
                            pnl_pct = (unrealized_pnl / (avg_cost * abs(quantity))) * 100
                        
                        from sqlalchemy.dialects.postgresql import insert as pg_insert
                        values = {
                            'account_type': account_type,
                            'symbol': symbol,
                            'quantity': quantity or 0,
                            'average_cost': avg_cost,
                            'market_value': market_value,
                            'unrealized_pnl': unrealized_pnl,
                            'unrealized_pnl_pct': pnl_pct,
                            'sec_type': sec_type,
                            'currency': currency,
                            'latest_price': latest_price,
                            'salable_qty': salable_qty,
                            'synced_at': now,
                        }
                        update_fields = {k: v for k, v in values.items() 
                                        if k not in ('account_type', 'symbol') and v is not None}
                        update_fields['synced_at'] = now
                        
                        stmt = pg_insert(TigerHolding).values(**values)
                        stmt = stmt.on_conflict_do_update(
                            constraint='uq_tiger_holding_account_symbol',
                            set_=update_fields
                        )
                        db.session.execute(stmt)
                        db.session.commit()
            except Exception as e:
                logger.error(f"Error updating TigerHolding for {symbol}: {e}")
                try:
                    db.session.rollback()
                except:
                    pass
        
        if quantity is not None and quantity == 0:
            with app.app_context():
                active_trailing = TrailingStopPosition.query.filter_by(
                    symbol=symbol,
                    account_type=account_type,
                    is_active=True
                ).first()
                
                from position_service import find_open_position, add_exit_leg
                from models import PositionStatus as PS, Position as PositionModel

                if active_trailing:
                    try:
                        open_pos = find_open_position(symbol, account_type, active_trailing.side)
                        if open_pos and open_pos.status == PS.OPEN:
                            exit_order_id = None
                            exit_price = None
                            exit_pnl = realized_pnl if realized_pnl else None
                            try:
                                from models import OrderTracker as OT, OrderRole
                                recent_cutoff = datetime.utcnow() - timedelta(minutes=5)
                                exit_roles = [OrderRole.EXIT_SIGNAL, OrderRole.EXIT_TRAILING,
                                             OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT]
                                recent_exit = OT.query.filter(
                                    OT.symbol == symbol,
                                    OT.account_type == account_type,
                                    OT.status == 'FILLED',
                                    OT.role.in_(exit_roles),
                                    OT.fill_time >= recent_cutoff,
                                ).order_by(OT.fill_time.desc()).first()
                                if recent_exit:
                                    exit_order_id = recent_exit.tiger_order_id
                                    exit_price = recent_exit.avg_fill_price
                                    if recent_exit.realized_pnl is not None:
                                        exit_pnl = recent_exit.realized_pnl
                                    logger.info(f"📊 Position-change fallback matched exit order {exit_order_id} for {symbol}")
                            except Exception as match_err:
                                logger.debug(f"Could not match exit order for {symbol}: {match_err}")

                            add_exit_leg(
                                position=open_pos,
                                tiger_order_id=exit_order_id,
                                price=exit_price,
                                quantity=active_trailing.quantity,
                                filled_at=datetime.utcnow(),
                                exit_method=ExitMethod.TRAILING_STOP,
                                realized_pnl=exit_pnl,
                            )
                            logger.info(f"📊 Added exit leg to Position via position-change fallback for {symbol}")
                        elif open_pos:
                            logger.info(f"📊 Position already CLOSED for {symbol}, skipping fallback exit leg")
                    except Exception as pos_err:
                        logger.error(f"❌ Failed to add exit leg to Position via fallback: {pos_err}")
                    
                    active_trailing.is_active = False
                    active_trailing.is_triggered = True
                    active_trailing.triggered_at = datetime.utcnow()
                    if not active_trailing.trigger_reason:
                        active_trailing.trigger_reason = "Position closed (WebSocket position change)"
                    
                    db.session.commit()
                else:
                    try:
                        from models import OrderTracker as OT, OrderRole
                        recent_cutoff = datetime.utcnow() - timedelta(minutes=5)
                        exit_roles = [OrderRole.EXIT_SIGNAL, OrderRole.EXIT_TRAILING, 
                                     OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT]
                        recent_exit_fill = OT.query.filter(
                            OT.symbol == symbol,
                            OT.account_type == account_type,
                            OT.status == 'FILLED',
                            OT.role.in_(exit_roles),
                            OT.fill_time >= recent_cutoff
                        ).first()

                        if not recent_exit_fill:
                            logger.debug(f"📊 Holdings=0 for {symbol} but no recent exit fill found and no active TS, "
                                        f"skipping aggressive closure (scheduler fallback will handle)")
                        else:
                            open_positions = PositionModel.query.filter_by(
                                symbol=symbol,
                                account_type=account_type,
                                status=PS.OPEN
                            ).all()
                            for open_pos in open_positions:
                                remaining = open_pos.total_entry_quantity - (open_pos.total_exit_quantity or 0)
                                if remaining > 0.001:
                                    add_exit_leg(
                                        position=open_pos,
                                        price=None,
                                        quantity=remaining,
                                        filled_at=datetime.utcnow(),
                                        exit_method=ExitMethod.EXTERNAL,
                                        realized_pnl=realized_pnl if realized_pnl else None,
                                    )
                                    logger.info(f"📊 Closed Position #{open_pos.id} ({symbol}/{open_pos.side}) "
                                               f"via position-change fallback (confirmed by exit fill #{recent_exit_fill.id})")
                                else:
                                    open_pos.status = PS.CLOSED
                                    open_pos.closed_at = datetime.utcnow()
                                    logger.info(f"📊 Marked Position #{open_pos.id} ({symbol}) CLOSED "
                                               f"(no remaining qty, holdings=0)")
                            if open_positions:
                                db.session.commit()
                    except Exception as pos_err:
                        logger.error(f"❌ Failed to close Position via no-TS fallback: {pos_err}")
                        try:
                            db.session.rollback()
                        except:
                            pass
        
    except Exception as e:
        logger.error(f"❌ Error handling position change: {str(e)}")
        try:
            with app.app_context():
                db.session.rollback()
        except Exception:
            pass


def handle_quote_update(symbol: str, quote_data: Dict) -> None:
    """
    Handle quote update for trailing stop monitoring
    This is called for each quote pushed via WebSocket
    """
    pass
