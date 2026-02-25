"""
TBUS Protection Service - Lifecycle management for TBUS protection orders.

This is the TBUS equivalent of oca_service.py, handling:
1. Creating protection after entry fills (GTC STP + GTC LMT)
2. Updating stop prices when TrailingStop adjusts
3. Triggering soft stop in extended hours (when GTC STP doesn't execute)
4. Managing sibling cancellation (when one leg fills, cancel the other)

Key differences from TBSG (oca_service.py):
- No OCA grouping — orders are independent GTC STP + GTC LMT
- STP instead of STP_LMT for stop loss
- Soft stop needed for extended hours (GTC STP outside_rth=False on TBUS)
- No daily rebuild needed (GTC doesn't expire)
"""

import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)


def create_tbus_protection(
    trailing_stop_id: Optional[int],
    symbol: str,
    side: str,
    quantity: float,
    stop_price: float,
    take_profit_price: float,
    account_type: str,
    trade_id: Optional[int] = None,
    entry_price: Optional[float] = None,
    force_replace: bool = False,
    creation_source: str = None
) -> Tuple[Optional[object], str]:
    """Create TBUS protection orders with duplicate check.
    
    This is the TBUS entry point, replacing create_oca_protection() for TBUS accounts.
    
    Args:
        trailing_stop_id: Related TrailingStopPosition ID (can be None if TS disabled)
        symbol: Stock symbol
        side: 'long' or 'short'
        quantity: Number of shares
        stop_price: Stop loss price
        take_profit_price: Take profit price
        account_type: Should always be 'real' for TBUS
        trade_id: Related Trade record ID
        entry_price: Entry price for reference
        force_replace: Force replacement of existing protection
        creation_source: Source description for logging
        
    Returns:
        Tuple of (OCAGroup record or None, status message)
    """
    from tiger_client import TigerClient
    from models import OCAGroup, OCAStatus
    from tbus.tbus_client import create_tbus_protection_orders

    try:
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        quantity = abs(quantity) if quantity else 0

        if not quantity or quantity <= 0:
            logger.error(f"[TBUS] create_tbus_protection: invalid quantity={quantity} for {clean_symbol}")
            return None, f"invalid_quantity:{quantity}"

        if not force_replace:
            existing_oca = None
            if trailing_stop_id:
                existing_oca = OCAGroup.query.filter_by(
                    trailing_stop_id=trailing_stop_id,
                    status=OCAStatus.ACTIVE
                ).first()
            if not existing_oca:
                existing_oca = OCAGroup.query.filter_by(
                    symbol=clean_symbol,
                    account_type=account_type,
                    status=OCAStatus.ACTIVE
                ).first()
            if existing_oca:
                has_live_orders = existing_oca.stop_order_id or existing_oca.take_profit_order_id
                if has_live_orders:
                    logger.info(f"[TBUS] Skipping duplicate — active OCAGroup #{existing_oca.id} "
                               f"for {clean_symbol}/{account_type} "
                               f"(SL={existing_oca.stop_order_id}, TP={existing_oca.take_profit_order_id})")
                    return existing_oca, "already_exists"
                else:
                    logger.warning(f"[TBUS] Stale OCAGroup #{existing_oca.id} with no live orders — "
                                  f"marking CANCELLED and proceeding")
                    from app import db as dedup_db
                    existing_oca.status = OCAStatus.CANCELLED
                    dedup_db.session.commit()

        client = TigerClient()

        result = create_tbus_protection_orders(
            tiger_client=client,
            symbol=clean_symbol,
            quantity=int(quantity),
            stop_loss_price=stop_price,
            take_profit_price=take_profit_price,
            entry_price=entry_price,
            trade_id=trade_id,
            trailing_stop_id=trailing_stop_id,
            cancel_existing=True,
            skip_position_check=True,
            position_side=side
        )

        if result.get('success'):
            oca_group_id = result.get('oca_group_record_id')
            if oca_group_id:
                oca_group = OCAGroup.query.get(oca_group_id)
                if oca_group and creation_source:
                    from app import db as oca_db
                    oca_group.creation_source = creation_source
                    oca_db.session.commit()
                try:
                    from tbus.tbus_quote_ws import get_eodhd_quote_manager
                    eodhd = get_eodhd_quote_manager()
                    if eodhd.is_running:
                        eodhd.subscribe([clean_symbol])
                except Exception:
                    pass
                logger.info(f"[TBUS] Protection created for {clean_symbol}: "
                           f"stop=${stop_price}, tp=${take_profit_price}, "
                           f"OCAGroup ID={oca_group_id} (source={creation_source})")
                return oca_group, "created"
            return None, "created_no_record"
        else:
            error_msg = result.get('error', 'Unknown')
            is_rate_limited = result.get('rate_limited', False)
            logger.error(f"[TBUS] Failed to create protection: {error_msg}")
            status = f"failed: {error_msg}"
            if is_rate_limited:
                status = f"rate limited: {error_msg}"
            return None, status

    except Exception as e:
        logger.error(f"[TBUS] Error in create_tbus_protection: {e}")
        return None, f"error: {str(e)}"


def update_tbus_stop_price(
    oca_group_id: int,
    new_stop_price: float
) -> Tuple[bool, str]:
    """Update the stop price in an active TBUS protection group.
    
    Called when TrailingStop adjusts the stop price (tier progression).
    TBUS uses software-only stop loss (no broker STP order), so this
    only updates the DB record. The trailing stop engine monitors price
    and triggers close when breached.
    
    Args:
        oca_group_id: OCAGroup record ID
        new_stop_price: New stop loss price
        
    Returns:
        Tuple of (success, status message)
    """
    from app import db
    from models import OCAGroup, OCAStatus

    try:
        group = OCAGroup.query.get(oca_group_id)
        if not group:
            return False, "group_not_found"

        if group.status != OCAStatus.ACTIVE:
            return False, f"group_not_active: {group.status.value}"

        new_stop_price = round(new_stop_price, 2)
        old_stop_price = group.stop_price

        group.stop_price = new_stop_price
        group.stop_limit_price = None
        db.session.commit()

        logger.info(f"[TBUS] Software stop price updated: ${old_stop_price} → ${new_stop_price} "
                   f"(group {group.id}, {group.symbol}, no broker order)")
        return True, "software_stop_updated"

    except Exception as e:
        logger.error(f"[TBUS] Error updating stop price: {e}")
        db.session.rollback()
        return False, f"error: {str(e)}"


def trigger_tbus_soft_stop(
    oca_group_id: int,
    current_price: float
) -> Tuple[bool, str]:
    """Trigger soft stop protection for TBUS account in extended hours.
    
    When price hits stop level during pre/post market (when TBUS GTC STP
    doesn't execute due to outside_rth=False), we:
    1. Cancel existing SL and TP orders
    2. Place a limit close order
    3. Mark the OCAGroup as SOFT_STOP
    4. Deactivate the trailing stop
    
    Args:
        oca_group_id: OCAGroup record ID
        current_price: Current market price that triggered the stop
        
    Returns:
        Tuple of (success, status message)
    """
    from app import db
    from models import OCAGroup, OCAStatus, TrailingStopPosition
    from tiger_client import TigerClient
    from order_tracker_service import register_order

    try:
        group = OCAGroup.query.get(oca_group_id)
        if not group:
            return False, "group_not_found"

        if group.status != OCAStatus.ACTIVE:
            return False, f"group_not_active: {group.status.value}"

        logger.info(f"[TBUS] Triggering soft stop for {group.symbol} at ${current_price}")

        client = TigerClient()

        order_ids_to_cancel = []
        if group.stop_order_id:
            order_ids_to_cancel.append(group.stop_order_id)
        if group.take_profit_order_id:
            order_ids_to_cancel.append(group.take_profit_order_id)

        for order_id in order_ids_to_cancel:
            try:
                client.cancel_order(order_id)
                logger.info(f"[TBUS] Cancelled order {order_id} for soft stop")
            except Exception as e:
                logger.warning(f"[TBUS] Error cancelling {order_id}: {e}")

        action = 'SELL' if group.side == 'long' else 'BUY'
        close_price = round(current_price * (0.998 if action == 'SELL' else 1.002), 2)

        close_result = client.place_limit_order(
            symbol=group.symbol,
            action=action,
            quantity=int(group.quantity),
            limit_price=close_price,
            outside_rth=False,
            time_in_force='GTC'
        )

        if close_result.get('success'):
            close_order_id = close_result.get('order_id')
            logger.info(f"[TBUS] Soft stop close order placed: {close_order_id} at ${close_price}")

            register_order(
                tiger_order_id=str(close_order_id),
                symbol=group.symbol,
                account_type='real',
                role='exit_trailing',
                side=action,
                quantity=group.quantity,
                order_type='LMT',
                limit_price=close_price,
                trade_id=group.trade_id,
                trailing_stop_id=group.trailing_stop_id
            )

            group.status = OCAStatus.SOFT_STOP
            group.triggered_price = current_price
            group.triggered_at = datetime.utcnow()

            if group.trailing_stop_id:
                ts_pos = TrailingStopPosition.query.get(group.trailing_stop_id)
                if ts_pos and ts_pos.is_active:
                    ts_pos.is_active = False
                    ts_pos.is_triggered = True
                    ts_pos.triggered_at = datetime.utcnow()
                    ts_pos.triggered_price = current_price
                    ts_pos.trigger_reason = "Soft stop (TBUS extended hours)"

            db.session.commit()

            try:
                from discord_notify import send_discord_notification
                send_discord_notification(
                    f"🛑 [TBUS] Soft Stop Triggered\n"
                    f"Symbol: {group.symbol}\n"
                    f"Price: ${current_price:.2f}\n"
                    f"Close Order: {close_order_id} at ${close_price:.2f}\n"
                    f"Reason: GTC STP inactive in extended hours"
                )
            except Exception:
                pass

            return True, f"soft_stop_triggered_order_{close_order_id}"
        else:
            logger.error(f"[TBUS] Failed to place soft stop close order: {close_result.get('error')}")
            return False, f"close_order_failed: {close_result.get('error')}"

    except Exception as e:
        logger.error(f"[TBUS] Error triggering soft stop: {e}")
        db.session.rollback()
        return False, f"error: {str(e)}"
