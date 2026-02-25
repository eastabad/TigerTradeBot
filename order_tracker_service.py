"""
Order Tracker Service - Unified order monitoring and fill handling

Architecture (mirrors Alpaca's clean design):
- register_order(): Register any order for tracking
- handle_fill_event(): THE SINGLE ENTRY POINT for all fill processing
  - _handle_entry_fill(): Entry fill → Position + PositionLeg(entry) + TrailingStop
  - _handle_exit_fill(): Exit fill → PositionLeg(exit) → Position auto-close
- poll_pending_orders(): Background polling calls handle_fill_event()

Single source of truth: Position + PositionLeg (no legacy tables needed).

Four exit scenarios tracked via OrderRole:
1. WEBHOOK_SIGNAL - TradingView sends close signal
2. TRAILING_STOP - System trailing stop triggers close
3. STOP_LOSS - Tiger attached stop loss order fills
4. TAKE_PROFIT - Tiger attached take profit order fills

Order linkage chain:
  Entry: Trade → register_order(ENTRY) → fill → Position + PositionLeg(entry)
  Exit:  Trade/TrailingStop → register_order(EXIT_*) → fill → PositionLeg(exit) → Position closes
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def register_order(
    tiger_order_id: str,
    symbol: str,
    account_type: str,
    role: str,
    side: Optional[str] = None,
    quantity: Optional[float] = None,
    order_type: Optional[str] = None,
    limit_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    parent_order_id: Optional[str] = None,
    trade_id: Optional[int] = None,
    trailing_stop_id: Optional[int] = None,
    signal_content: Optional[str] = None
) -> Tuple[Optional[object], str]:
    """Register an order for tracking. Idempotent by tiger_order_id."""
    from app import db
    from models import OrderTracker, OrderRole

    try:
        existing = OrderTracker.query.filter_by(tiger_order_id=tiger_order_id).first()
        if existing:
            logger.debug(f"OrderTracker already exists for {tiger_order_id}")
            return existing, "already_exists"

        role_enum = OrderRole(role)

        tracker = OrderTracker(
            tiger_order_id=tiger_order_id,
            symbol=symbol,
            account_type=account_type,
            role=role_enum,
            side=side,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
            stop_price=stop_price,
            parent_order_id=parent_order_id,
            trade_id=trade_id,
            trailing_stop_id=trailing_stop_id,
            signal_content=signal_content,
            status='PENDING'
        )

        db.session.add(tracker)
        db.session.commit()

        logger.info(f"📋 Registered order {tiger_order_id}: {symbol} {role} ({account_type})")
        return tracker, "created"

    except Exception as e:
        logger.error(f"Error registering order {tiger_order_id}: {e}")
        db.session.rollback()
        return None, f"error: {str(e)}"


def handle_fill_event(
    tiger_order_id: str,
    filled_quantity: float,
    avg_fill_price: float,
    realized_pnl: Optional[float] = None,
    commission: Optional[float] = None,
    fill_time: Optional[datetime] = None,
    source: str = 'polling'
) -> Tuple[Optional[object], str]:
    """THE SINGLE ENTRY POINT for all fill processing.

    Called by:
    - poll_pending_orders() when polling detects a fill
    - push_event_handlers when WebSocket pushes a fill

    Idempotent: if OrderTracker already FILLED, skips processing.

    Flow:
    1. Find OrderTracker by tiger_order_id
    2. Update status to FILLED (idempotent check)
    3. Route to _handle_entry_fill() or _handle_exit_fill() based on role
    """
    from app import db
    from models import OrderTracker, OrderRole

    try:
        tracker = OrderTracker.query.filter_by(tiger_order_id=tiger_order_id).first()
        if not tracker:
            logger.warning(f"📋 No OrderTracker found for {tiger_order_id} (source={source})")
            return None, "not_found"

        if tracker.status == 'FILLED':
            logger.debug(f"📋 Order {tiger_order_id} already FILLED, skipping (source={source})")
            return tracker, "already_filled"

        tracker.status = 'FILLED'
        tracker.filled_quantity = filled_quantity
        tracker.avg_fill_price = avg_fill_price
        tracker.realized_pnl = realized_pnl
        tracker.commission = commission
        tracker.fill_time = fill_time or datetime.utcnow()
        tracker.fill_source = source

        db.session.flush()

        logger.info(f"📋 Order {tiger_order_id} FILLED (source={source}): "
                    f"{tracker.symbol} {tracker.role.value} "
                    f"qty={filled_quantity} price=${avg_fill_price:.2f} "
                    f"pnl=${realized_pnl or 0:.2f}")

        if tracker.role == OrderRole.ENTRY:
            _handle_entry_fill(tracker)
        elif tracker.role in (OrderRole.EXIT_SIGNAL, OrderRole.EXIT_TRAILING,
                              OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT):
            _handle_exit_fill(tracker)
        else:
            logger.warning(f"📋 Unknown role {tracker.role} for order {tiger_order_id}")

        db.session.commit()
        return tracker, "filled"

    except Exception as e:
        logger.error(f"❌ Error in handle_fill_event for {tiger_order_id}: {e}")
        import traceback
        traceback.print_exc()
        db.session.rollback()
        return None, f"error: {str(e)}"


def _handle_entry_fill(tracker):
    """Process entry fill: create/update Position + add entry PositionLeg.

    Mirrors Alpaca's _handle_entry_fill() in alpaca/order_tracker.py.
    """
    from models import Trade, TrailingStopPosition
    from position_service import get_or_create_position, add_entry_leg, link_trailing_stop_to_position

    side = 'long' if tracker.side and tracker.side.upper() in ('BUY', 'BUY_OPEN') else 'short'

    position, is_new = get_or_create_position(
        symbol=tracker.symbol,
        account_type=tracker.account_type,
        side=side,
        entry_price=tracker.avg_fill_price,
        entry_quantity=tracker.filled_quantity,
        filled_at=tracker.fill_time,
    )

    stop_price = None
    take_profit_price = None
    stop_order_id = None
    take_profit_order_id = None

    from signal_utils import parse_signal_fields
    trade = None
    if tracker.trade_id:
        trade = Trade.query.get(tracker.trade_id)
        if trade:
            if trade.signal_data:
                try:
                    sig = json.loads(trade.signal_data)
                    extras = sig.get('extras', {})
                    sl_raw = sig.get('stopLoss') or extras.get('stopLoss')
                    tp_raw = sig.get('takeProfit') or extras.get('takeProfit')
                    if isinstance(sl_raw, dict):
                        stop_price = sl_raw.get('stopPrice') or sl_raw.get('stop_price') or sl_raw.get('price')
                    else:
                        stop_price = sl_raw
                    if isinstance(tp_raw, dict):
                        take_profit_price = tp_raw.get('limitPrice') or tp_raw.get('limit_price') or tp_raw.get('price')
                    else:
                        take_profit_price = tp_raw
                except (json.JSONDecodeError, AttributeError):
                    pass

            stop_order_id = getattr(trade, 'stop_loss_order_id', None)
            take_profit_order_id = getattr(trade, 'take_profit_order_id', None)
            if not stop_price:
                stop_price = getattr(trade, 'stop_loss_price', None)
            if not take_profit_price:
                take_profit_price = getattr(trade, 'take_profit_price', None)

            from models import OrderStatus as TradeOrderStatus
            trade.status = TradeOrderStatus.FILLED
            trade.filled_price = tracker.avg_fill_price
            trade.filled_quantity = tracker.filled_quantity

    signal_data_raw = trade.signal_data if trade else None
    parsed = parse_signal_fields(signal_data_raw)
    signal_content = parsed['signal_content'] or signal_data_raw

    add_entry_leg(
        position=position,
        tiger_order_id=tracker.tiger_order_id,
        price=tracker.avg_fill_price,
        quantity=tracker.filled_quantity,
        filled_at=tracker.fill_time,
        trade_id=tracker.trade_id,
        signal_content=signal_content,
        signal_grade=parsed['signal_grade'],
        signal_score=parsed['signal_score'],
        signal_indicator=parsed['signal_indicator'],
        signal_timeframe=parsed['signal_timeframe'],
        stop_order_id=stop_order_id,
        take_profit_order_id=take_profit_order_id,
        stop_price=stop_price,
        take_profit_price=take_profit_price,
    )

    if tracker.trailing_stop_id:
        link_trailing_stop_to_position(position, tracker.trailing_stop_id)

    _create_trailing_stop_for_entry(tracker, position, side,
                                     parsed['signal_timeframe'], stop_price, take_profit_price,
                                     stop_order_id, take_profit_order_id)

    logger.info(f"📦 Entry fill → Position {'created' if is_new else 'updated'}: "
                f"{tracker.symbol} {tracker.filled_quantity}@${tracker.avg_fill_price:.2f}")

    try:
        from discord_notifier import DiscordNotifier
        notifier = DiscordNotifier()
        notifier.send_notification(
            f"📦 Entry filled: {tracker.symbol} {side.upper()} "
            f"{tracker.filled_quantity}@${tracker.avg_fill_price:.2f} "
            f"({tracker.account_type})",
            title="Entry Fill"
        )
    except Exception:
        pass


def _create_trailing_stop_for_entry(tracker, position, side,
                                      signal_timeframe, stop_price, take_profit_price,
                                      stop_order_id, take_profit_order_id):
    """Create TrailingStop on entry fill if enabled and not already present."""
    from models import TrailingStopPosition
    from position_service import link_trailing_stop_to_position

    try:
        existing_ts = TrailingStopPosition.query.filter_by(
            trade_id=tracker.trade_id,
            is_active=True,
        ).first() if tracker.trade_id else None

        if not existing_ts:
            existing_ts = TrailingStopPosition.query.filter_by(
                symbol=tracker.symbol,
                account_type=tracker.account_type,
                is_active=True,
            ).first()

        if not existing_ts:
            if not stop_price and not take_profit_price:
                logger.info(f"⏭️ [{tracker.symbol}] Skipping TrailingStop creation: no SL/TP in entry signal")
            else:
                from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
                from models import TrailingStopMode
                ts_config = get_trailing_stop_config()
                if ts_config.is_enabled:
                    timeframe = signal_timeframe or '15'
                    ts = create_trailing_stop_for_trade(
                        trade_id=tracker.trade_id,
                        symbol=tracker.symbol,
                        side=side,
                        entry_price=tracker.avg_fill_price,
                        quantity=tracker.filled_quantity,
                        account_type=tracker.account_type,
                        fixed_stop_loss=stop_price,
                        fixed_take_profit=take_profit_price,
                        stop_loss_order_id=stop_order_id,
                        take_profit_order_id=take_profit_order_id,
                        mode=TrailingStopMode.BALANCED,
                        timeframe=str(timeframe),
                    )
                    if ts:
                        link_trailing_stop_to_position(position, ts.id)
                        logger.info(f"🎯 Created TrailingStop for {tracker.symbol}")
    except Exception as e:
        logger.error(f"❌ Failed to create TrailingStop: {e}")


def _handle_exit_fill(tracker):
    """Process exit fill: add exit PositionLeg → auto-close Position.

    Mirrors Alpaca's _handle_exit_fill() in alpaca/order_tracker.py.
    Position + PositionLeg is the single source of truth - no legacy tables needed.

    IMPORTANT: TS deactivation happens AFTER Position update to preserve fallback paths.
    If Position can't be found, TS stays active so scheduler fallbacks can catch it.

    Position lookup priority:
    1. trailing_stop_id → Position.trailing_stop_id (most precise)
    2. symbol + account_type + side
    3. symbol + account_type (no side filter)
    4. [PAPER] prefix variants
    """
    from models import OrderRole, ExitMethod, Trade, TrailingStopPosition, PositionStatus
    from models import Position as PositionModel

    exit_method_map = {
        OrderRole.EXIT_SIGNAL: ExitMethod.WEBHOOK_SIGNAL,
        OrderRole.EXIT_TRAILING: ExitMethod.TRAILING_STOP,
        OrderRole.STOP_LOSS: ExitMethod.STOP_LOSS,
        OrderRole.TAKE_PROFIT: ExitMethod.TAKE_PROFIT,
    }
    exit_method = exit_method_map.get(tracker.role, ExitMethod.EXTERNAL)

    if tracker.role in (OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT):
        _cancel_sibling_oca_orders(tracker)

    from position_service import find_open_position, add_exit_leg

    side = _determine_position_side(tracker)
    position = None

    if tracker.trailing_stop_id:
        position = PositionModel.query.filter_by(
            trailing_stop_id=tracker.trailing_stop_id,
            status=PositionStatus.OPEN
        ).first()
        if position:
            logger.debug(f"📋 Found Position #{position.id} via trailing_stop_id={tracker.trailing_stop_id}")

    if not position:
        position = find_open_position(tracker.symbol, tracker.account_type, side)

    if not position:
        clean_symbol = tracker.symbol.replace('[PAPER]', '').strip()
        if clean_symbol != tracker.symbol:
            position = find_open_position(clean_symbol, tracker.account_type, side)
        if not position:
            paper_symbol = f"[PAPER]{clean_symbol}"
            if paper_symbol != tracker.symbol:
                position = find_open_position(paper_symbol, tracker.account_type, side)

    if not position:
        from models import PositionLeg, LegType
        
        if tracker.trailing_stop_id:
            closed_pos = PositionModel.query.filter_by(
                trailing_stop_id=tracker.trailing_stop_id,
                status=PositionStatus.CLOSED
            ).order_by(PositionModel.closed_at.desc()).first()
            if closed_pos:
                has_exit = PositionLeg.query.filter_by(
                    position_id=closed_pos.id, leg_type=LegType.EXIT
                ).first()
                if not has_exit:
                    position = closed_pos
                    logger.info(f"📋 Found CLOSED Position #{closed_pos.id} missing EXIT leg "
                              f"(ts_id={tracker.trailing_stop_id}), adding exit data")
                else:
                    existing_oid = has_exit.tiger_order_id
                    if existing_oid and existing_oid != tracker.tiger_order_id:
                        logger.debug(f"📋 Position #{closed_pos.id} already has EXIT leg "
                                    f"(order={existing_oid}), skip duplicate for {tracker.tiger_order_id}")
                    return
        
        if not position:
            clean_sym = tracker.symbol.replace('[PAPER]', '').strip()
            recent_cutoff = datetime.utcnow() - timedelta(hours=24)
            for sym in set([tracker.symbol, clean_sym]):
                candidates = PositionModel.query.filter_by(
                    symbol=sym,
                    account_type=tracker.account_type,
                    status=PositionStatus.CLOSED,
                ).filter(
                    PositionModel.closed_at >= recent_cutoff
                ).order_by(PositionModel.closed_at.desc()).limit(3).all()
                for cand in candidates:
                    if side and cand.side != side:
                        continue
                    has_exit = PositionLeg.query.filter_by(
                        position_id=cand.id, leg_type=LegType.EXIT
                    ).first()
                    if not has_exit:
                        has_entry = PositionLeg.query.filter(
                            PositionLeg.position_id == cand.id,
                            PositionLeg.leg_type.in_([LegType.ENTRY, LegType.ADD])
                        ).first()
                        if tracker.parent_order_id and has_entry:
                            if has_entry.tiger_order_id != tracker.parent_order_id:
                                logger.debug(f"📋 Skipping CLOSED Position #{cand.id}: "
                                           f"entry order {has_entry.tiger_order_id} != parent {tracker.parent_order_id}")
                                continue
                        elif not tracker.parent_order_id and has_entry:
                            entry_qty = cand.total_entry_quantity or 0
                            exit_qty = tracker.filled_quantity or 0
                            if entry_qty > 0 and exit_qty > 0 and abs(exit_qty - entry_qty) / entry_qty > 0.5:
                                logger.debug(f"📋 Skipping CLOSED Position #{cand.id}: "
                                           f"qty mismatch (entry={entry_qty}, exit={exit_qty})")
                                continue
                        position = cand
                        logger.info(f"📋 Found CLOSED Position #{cand.id} ({sym}) missing EXIT leg "
                                  f"(closed within 24h), adding exit data from {tracker.role.value}")
                        break
                if position:
                    break
        
        if not position:
            logger.warning(f"⚠️ No Position (open or recently-closed) found for exit fill: "
                          f"{tracker.symbol}/{tracker.account_type} "
                          f"(side={side}, role={tracker.role.value}, ts_id={tracker.trailing_stop_id})")
            return

    if tracker.fill_source:
        if tracker.fill_source in ('reconciliation', 'ghost_detection', 'soft_stop', 'manual'):
            close_source_val = tracker.fill_source
        else:
            close_source_val = f"{tracker.fill_source}_fill"
    else:
        close_source_val = None
    exit_leg = add_exit_leg(
        position=position,
        tiger_order_id=tracker.tiger_order_id,
        price=tracker.avg_fill_price,
        quantity=tracker.filled_quantity,
        filled_at=tracker.fill_time,
        exit_method=exit_method,
        realized_pnl=tracker.realized_pnl,
        commission=tracker.commission,
        close_source=close_source_val,
    )

    if exit_leg and tracker.role in (OrderRole.EXIT_TRAILING, OrderRole.STOP_LOSS,
                                      OrderRole.TAKE_PROFIT, OrderRole.EXIT_SIGNAL):
        try:
            ts = None
            if tracker.trailing_stop_id:
                ts = TrailingStopPosition.query.get(tracker.trailing_stop_id)
            if not ts:
                ts = TrailingStopPosition.query.filter_by(
                    symbol=tracker.symbol,
                    account_type=tracker.account_type,
                    is_active=True
                ).first()
            if not ts and position.trailing_stop_id:
                ts = TrailingStopPosition.query.get(position.trailing_stop_id)

            if ts and ts.is_active:
                position_fully_closed = (position.status == PositionStatus.CLOSED)
                if position_fully_closed:
                    ts.is_active = False
                    ts.is_triggered = True
                    ts.triggered_at = datetime.utcnow()
                    ts.trigger_reason = f"{tracker.role.value}_filled"
                    logger.info(f"🎯 Deactivated TrailingStopPosition #{ts.id} for {tracker.symbol} "
                               f"via {tracker.role.value} fill (position fully closed)")
                else:
                    remaining = max(0, (position.total_entry_quantity or 0) - (position.total_exit_quantity or 0))
                    if remaining <= 0:
                        ts.is_active = False
                        ts.is_triggered = True
                        ts.triggered_at = datetime.utcnow()
                        ts.trigger_reason = f"{tracker.role.value}_filled"
                        logger.info(f"🎯 Deactivated TS #{ts.id} for {tracker.symbol}: "
                                   f"remaining qty <= 0 after partial exit (position status not yet CLOSED)")
                    else:
                        old_qty = ts.quantity
                        ts.quantity = remaining
                        logger.warning(f"⚠️ [{tracker.symbol}] Partial exit via {tracker.role.value}: "
                                      f"{tracker.filled_quantity} filled, remaining {remaining} shares. "
                                      f"TS #{ts.id} stays active (qty: {old_qty} → {remaining})")
                        try:
                            from discord_notifier import DiscordNotifier
                            DiscordNotifier().send_notification(
                                f"⚠️ Partial {tracker.role.value} fill: {tracker.symbol} "
                                f"{tracker.filled_quantity} filled, {remaining} remaining. "
                                f"TS #{ts.id} stays active for continued protection ({tracker.account_type})",
                                title="Partial Exit"
                            )
                        except Exception:
                            pass
        except Exception as e:
            logger.error(f"Error deactivating trailing stop: {e}")

    logger.info(f"📦 Exit fill → {tracker.symbol} {exit_method.value}: "
                f"{tracker.filled_quantity}@${tracker.avg_fill_price:.2f}, "
                f"position={'CLOSED' if position.status == PositionStatus.CLOSED else 'OPEN'}")

    try:
        from discord_notifier import DiscordNotifier
        notifier = DiscordNotifier()
        pnl_str = f"${tracker.realized_pnl:.2f}" if tracker.realized_pnl else "pending"
        notifier.send_notification(
            f"📦 Exit filled: {tracker.symbol} {exit_method.value} "
            f"{tracker.filled_quantity}@${tracker.avg_fill_price:.2f} "
            f"P&L={pnl_str} ({tracker.account_type})",
            title="Exit Fill"
        )
    except Exception:
        pass


def _determine_position_side(tracker) -> str:
    """Determine position side from tracker context."""
    from models import TrailingStopPosition, Trade

    if tracker.trailing_stop_id:
        ts = TrailingStopPosition.query.get(tracker.trailing_stop_id)
        if ts:
            return ts.side

    if tracker.trade_id:
        trade = Trade.query.get(tracker.trade_id)
        if trade:
            if hasattr(trade, 'is_close_position') and trade.is_close_position:
                action = trade.side.value if hasattr(trade.side, 'value') else str(trade.side)
                return 'long' if action.upper() in ('SELL', 'SELL_CLOSE') else 'short'
            else:
                action = trade.side.value if hasattr(trade.side, 'value') else str(trade.side)
                return 'long' if action.upper() in ('BUY', 'BUY_OPEN') else 'short'

    if tracker.parent_order_id:
        from models import OrderTracker
        parent = OrderTracker.query.filter_by(tiger_order_id=tracker.parent_order_id).first()
        if parent:
            parent_side = parent.side or ''
            return 'long' if parent_side.upper() in ('BUY', 'BUY_OPEN') else 'short'

    if tracker.side:
        return 'long' if tracker.side.upper() in ('SELL', 'SELL_CLOSE') else 'short'

    logger.warning(f"⚠️ _determine_position_side: no side info for tracker {tracker.tiger_order_id} "
                  f"(role={tracker.role.value if tracker.role else 'none'}, "
                  f"trade_id={tracker.trade_id}, ts_id={tracker.trailing_stop_id}), defaulting to 'long'")
    return 'long'


def _cancel_sibling_oca_orders(tracker):
    """When SL fills, cancel TP (and vice versa) — both in DB and at Tiger broker.
    
    For OCA groups, Tiger auto-cancels the sibling, but for bracket sub-orders
    or cross-day rebuilt orders, we must explicitly cancel at the broker.
    """
    from models import OrderTracker, OrderRole

    if not tracker.parent_order_id:
        return

    sibling_roles = {
        OrderRole.STOP_LOSS: OrderRole.TAKE_PROFIT,
        OrderRole.TAKE_PROFIT: OrderRole.STOP_LOSS,
    }
    sibling_role = sibling_roles.get(tracker.role)
    if not sibling_role:
        return

    siblings = OrderTracker.query.filter_by(
        parent_order_id=tracker.parent_order_id,
        role=sibling_role,
        status='PENDING'
    ).all()

    for sib in siblings:
        sib.status = 'CANCELLED'
        logger.info(f"📋 Cancelled sibling {sib.role.value} order {sib.tiger_order_id} "
                    f"(parent={tracker.parent_order_id})")

        try:
            from tiger_client import TigerClient, TigerPaperClient
            client = TigerPaperClient() if tracker.account_type == 'paper' else TigerClient()
            cancel_result = client.cancel_order(sib.tiger_order_id)
            if cancel_result.get('success'):
                logger.info(f"✅ Broker cancel confirmed for sibling {sib.tiger_order_id}")
            else:
                error = cancel_result.get('error', '')
                if 'already' in str(error).lower() or 'cancel' in str(error).lower() or 'filled' in str(error).lower():
                    logger.debug(f"Sibling {sib.tiger_order_id} already cancelled/filled at broker: {error}")
                else:
                    logger.warning(f"⚠️ Failed to cancel sibling {sib.tiger_order_id} at broker: {error}")
        except Exception as e:
            logger.warning(f"⚠️ Error cancelling sibling {sib.tiger_order_id} at broker: {e}")




def get_role_to_exit_method(role: str) -> str:
    """Map OrderRole value to ExitMethod value."""
    mapping = {
        'exit_signal': 'webhook_signal',
        'exit_trailing': 'trailing_stop',
        'stop_loss': 'stop_loss',
        'take_profit': 'take_profit'
    }
    return mapping.get(role, 'external')


def poll_pending_orders(app, max_per_cycle=20):
    """Poll Tiger API for pending order status updates.

    Simplified: when a fill is detected, calls handle_fill_event() instead of
    processing entry/exit separately.
    Also cleans up stale OCA protection orders (STOP_LOSS/TAKE_PROFIT) from
    previous days whose trailing stop is no longer active or position is closed.
    """
    import time

    try:
        with app.app_context():
            from app import db
            from models import OrderTracker, OrderRole, TrailingStopPosition
            from tiger_client import TigerClient, TigerPaperClient

            stale_cutoff = datetime.utcnow() - timedelta(days=3)
            stale_orders = OrderTracker.query.filter(
                OrderTracker.status == 'PENDING',
                OrderTracker.created_at < stale_cutoff
            ).all()

            if stale_orders:
                for stale in stale_orders:
                    stale.status = 'CANCELLED'
                db.session.commit()
                logger.info(f"📋 Auto-expired {len(stale_orders)} stale PENDING orders (older than 3 days)")

            overnight_cutoff = datetime.utcnow() - timedelta(hours=12)
            overnight_oca_orders = OrderTracker.query.filter(
                OrderTracker.status.in_(['PENDING', 'NEW', 'SUBMITTED', 'HELD']),
                OrderTracker.role.in_([OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT]),
                OrderTracker.created_at < overnight_cutoff,
            ).all()

            if overnight_oca_orders:
                cleaned = 0
                oca_real = [o for o in overnight_oca_orders if o.account_type == 'real']
                oca_paper = [o for o in overnight_oca_orders if o.account_type == 'paper']

                for acct_type, oca_list, client_cls in [
                    ('real', oca_real, TigerClient),
                    ('paper', oca_paper, TigerPaperClient)
                ]:
                    if not oca_list:
                        continue

                    client = None
                    try:
                        client = client_cls()
                    except Exception as e:
                        logger.warning(f"🧹 Cannot init {acct_type} client for OCA cleanup: {e}")

                    for oca_order in oca_list:
                        should_clean = False
                        reason = ''

                        locally_stale = False
                        if oca_order.trailing_stop_id:
                            ts = TrailingStopPosition.query.get(oca_order.trailing_stop_id)
                            if ts and not ts.is_active:
                                locally_stale = True
                                reason = f'linked TS#{ts.id} inactive (triggered={ts.is_triggered})'
                            elif not ts:
                                locally_stale = True
                                reason = f'linked TS#{oca_order.trailing_stop_id} not found'
                        else:
                            from models import Position as PositionModel
                            pos = PositionModel.query.filter(
                                PositionModel.symbol == oca_order.symbol,
                                PositionModel.account_type == oca_order.account_type,
                                PositionModel.status == 'OPEN',
                            ).first()
                            if not pos:
                                locally_stale = True
                                reason = 'no OPEN position found for symbol'

                        if not locally_stale:
                            continue

                        if client and oca_order.tiger_order_id:
                            try:
                                result = client.get_order_status(oca_order.tiger_order_id)
                                if result.get('success'):
                                    broker_status = result.get('status', '').lower()
                                    if broker_status in ['invalid', 'cancelled', 'expired', 'filled', '']:
                                        should_clean = True
                                        reason += f', broker_status={broker_status}'
                                    elif broker_status in ['pending', 'new', 'submitted', 'held']:
                                        logger.info(f"🧹 OCA order {oca_order.tiger_order_id} ({oca_order.symbol} "
                                                    f"{oca_order.role.value}) locally stale ({reason}) but "
                                                    f"still {broker_status} at broker - skipping cleanup")
                                        continue
                                    else:
                                        should_clean = True
                                        reason += f', unknown broker_status={broker_status}'
                                else:
                                    error_msg = str(result.get('error', ''))
                                    if 'not found' in error_msg.lower() or 'order not exist' in error_msg.lower():
                                        should_clean = True
                                        reason += ', order not found at broker'
                                    elif '1200' in error_msg or 'forbidden' in error_msg.lower():
                                        logger.debug(f"🧹 Rate limited checking OCA order {oca_order.tiger_order_id}, skipping")
                                        break
                                    else:
                                        logger.warning(f"🧹 Cannot verify OCA order {oca_order.tiger_order_id} "
                                                       f"at broker ({error_msg}), skipping cleanup")
                                        continue
                                time.sleep(0.3)
                            except Exception as e:
                                logger.warning(f"🧹 Error checking OCA order {oca_order.tiger_order_id}: {e}, skipping")
                                continue
                        else:
                            should_clean = True
                            reason += ', no tiger_order_id or client unavailable'

                        if should_clean:
                            oca_order.status = 'CANCELLED'
                            cleaned += 1
                            logger.info(f"🧹 Auto-cancelled stale OCA order: {oca_order.symbol} "
                                        f"{oca_order.role.value} (tiger_order={oca_order.tiger_order_id}, "
                                        f"created={oca_order.created_at}, reason={reason})")

                if cleaned > 0:
                    db.session.commit()
                    logger.info(f"🧹 Cleaned up {cleaned} stale overnight OCA protection orders")

            pending_orders = OrderTracker.query.filter_by(
                status='PENDING'
            ).order_by(OrderTracker.created_at.desc()).limit(max_per_cycle * 2).all()

            if not pending_orders:
                return

            logger.debug(f"📋 Polling up to {max_per_cycle} of {len(pending_orders)} pending orders")

            real_orders = [o for o in pending_orders if o.account_type == 'real']
            paper_orders = [o for o in pending_orders if o.account_type == 'paper']

            checked = 0
            for account_type, orders, client_class in [
                ('real', real_orders, TigerClient),
                ('paper', paper_orders, TigerPaperClient)
            ]:
                if not orders or checked >= max_per_cycle:
                    continue

                try:
                    client = client_class()

                    for tracker in orders:
                        if checked >= max_per_cycle:
                            break
                        checked += 1

                        try:
                            result = client.get_order_status(tracker.tiger_order_id)

                            if not result.get('success'):
                                error_msg = result.get('error', '')
                                if '1200' in str(error_msg) or 'forbidden' in str(error_msg).lower():
                                    logger.debug(f"Rate limited, stopping poll cycle")
                                    return
                                logger.debug(f"Failed to get status for order {tracker.tiger_order_id}: {error_msg}")
                                continue

                            status = result.get('status', '').lower()

                            if status == 'filled':
                                handle_fill_event(
                                    tiger_order_id=tracker.tiger_order_id,
                                    filled_quantity=result.get('filled_quantity', 0),
                                    avg_fill_price=result.get('filled_price', 0),
                                    realized_pnl=result.get('realized_pnl', 0),
                                    commission=result.get('commission', 0),
                                    fill_time=datetime.utcnow(),
                                    source='polling',
                                )

                            elif status in ['invalid', 'cancelled', 'expired']:
                                tracker.status = 'CANCELLED'
                                reason = result.get('reason', '')
                                logger.warning(f"📋 Order {tracker.tiger_order_id} {status.upper()}: "
                                             f"{tracker.symbol} {tracker.role.value} - {reason}")
                                db.session.commit()

                            elif status == 'pending':
                                pass

                            time.sleep(0.3)

                        except Exception as e:
                            logger.error(f"Error checking order {tracker.tiger_order_id}: {e}")

                except Exception as e:
                    logger.error(f"Error polling {account_type} orders: {e}")

    except Exception as e:
        logger.error(f"Error in poll_pending_orders: {e}")
