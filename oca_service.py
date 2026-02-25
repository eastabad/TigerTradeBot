"""
OCA Service - Lifecycle management for OCA (One-Cancels-All) order groups

This service handles:
1. Creating OCA protection after entry fills
2. Handling OCA leg fills (stop/take profit triggers)
3. Cancelling OCA groups for manual closes
4. Paper account daily rebuild of DAY orders
5. Modifying stop prices when TrailingStop adjusts
6. Soft stop protection for Paper accounts in extended hours
"""

import logging
import time
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

_oca_rebuild_failure_tracker: Dict[str, float] = {}
_OCA_REBUILD_COOLDOWN_SECONDS = 300
_last_oca_rebuild_time: float = 0
_OCA_REBUILD_MIN_GAP_SECONDS = 120
_oca_rate_limited_until: float = 0
_OCA_RATE_LIMIT_BACKOFF_SECONDS = 600


def create_oca_protection_for_entry(
    trade_id: int,
    symbol: str,
    account_type: str,
    quantity: float,
    entry_price: float,
    stop_loss_price: float,
    take_profit_price: float,
    trailing_stop_id: Optional[int] = None,
    position_side: str = 'long',
    cancel_existing: bool = True,
    skip_dedup: bool = False,
    creation_source: str = None
) -> Tuple[Optional[object], str]:
    """Create OCA protection after an entry order fills.
    
    Args:
        trade_id: Related Trade record ID
        symbol: Stock symbol
        account_type: 'real' or 'paper'
        quantity: Number of shares
        entry_price: Filled entry price
        stop_loss_price: Stop loss price from TradingView signal
        take_profit_price: Take profit price from TradingView signal
        trailing_stop_id: Related TrailingStopPosition ID (optional)
        position_side: 'long' or 'short' (default 'long')
        cancel_existing: Whether to cancel existing orders first (default True, 
                         set False for Paper rebuild since DAY orders already expired)
        skip_dedup: Skip duplicate check (only when caller already verified, default False)
        
    Returns:
        Tuple of (OCAGroup record or None, status message)
    """
    from tiger_client import TigerClient, TigerPaperClient
    from models import OCAGroup, OCAStatus
    
    try:
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        quantity = abs(quantity) if quantity else 0
        
        if not quantity or quantity <= 0:
            logger.error(f"❌ create_oca_protection_for_entry: invalid quantity={quantity} for {clean_symbol}/{account_type}, skipping")
            return None, f"invalid_quantity:{quantity}"
        
        if not skip_dedup:
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
                    logger.info(f"⏭️ create_oca_protection_for_entry: skipping duplicate - "
                               f"active OCAGroup #{existing_oca.id} already exists for {clean_symbol}/{account_type} "
                               f"(SL={existing_oca.stop_order_id}, TP={existing_oca.take_profit_order_id})")
                    return existing_oca, "already_exists"
                else:
                    logger.warning(f"⚠️ Found stale OCAGroup #{existing_oca.id} for {clean_symbol}/{account_type} "
                                  f"with no live orders - marking CANCELLED and proceeding with new creation")
                    from app import db as dedup_db
                    existing_oca.status = OCAStatus.CANCELLED
                    dedup_db.session.commit()
        
        if account_type == 'paper':
            client = TigerPaperClient()
        else:
            client = TigerClient()
        
        result = client.create_oca_orders_for_position(
            symbol=clean_symbol,
            quantity=quantity,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            entry_price=entry_price,
            trade_id=trade_id,
            trailing_stop_id=trailing_stop_id,
            cancel_existing=cancel_existing,
            skip_position_check=True,
            position_side=position_side
        )
        
        if result.get('success'):
            oca_group_id = result.get('oca_group_record_id')
            if oca_group_id:
                oca_group = OCAGroup.query.get(oca_group_id)
                if oca_group and creation_source:
                    from app import db as oca_db
                    oca_group.creation_source = creation_source
                    oca_db.session.commit()
                logger.info(f"OCA protection created for {clean_symbol}: "
                           f"stop={stop_loss_price}, tp={take_profit_price}, "
                           f"OCAGroup ID={oca_group_id} (source={creation_source})")
                return oca_group, "created"
            return None, "created_no_record"
        else:
            error_msg = result.get('error', 'Unknown')
            is_rate_limited = result.get('rate_limited', False)
            logger.error(f"Failed to create OCA protection: {error_msg}")
            status = f"failed: {error_msg}"
            if is_rate_limited:
                status = f"rate limited: {error_msg}"
            return None, status
            
    except Exception as e:
        logger.error(f"Error in create_oca_protection_for_entry: {e}")
        return None, f"error: {str(e)}"


def create_oca_protection(
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
    """Create OCA protection for a position with duplicate check.
    
    This is the main entry point for OCA creation from various paths.
    Includes check to prevent duplicate OCA groups for same position.
    Supports trailing_stop_id=None when trailing stop is disabled.
    
    Args:
        trailing_stop_id: Related TrailingStopPosition ID (can be None if TS disabled)
        symbol: Stock symbol
        side: 'long' or 'short'
        quantity: Number of shares
        stop_price: Stop loss price
        take_profit_price: Take profit price
        account_type: 'real' or 'paper'
        trade_id: Related Trade ID (used when trailing_stop_id is None)
        entry_price: Entry price (used when trailing_stop_id is None)
        force_replace: If True, cancel existing ACTIVE OCA and create new one (for scaling/加仓)
        
    Returns:
        Tuple of (OCAGroup record or None, status message)
    """
    from app import db
    from models import OCAGroup, OCAStatus, TrailingStopPosition
    
    try:
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        
        quantity = abs(quantity) if quantity else 0
        logger.info(f"🔍 [TRACE] create_oca_protection called: symbol={clean_symbol}, quantity={quantity}, side={side}, stop={stop_price}, tp={take_profit_price}, account={account_type}, ts_id={trailing_stop_id}, trade_id={trade_id}")
        
        if not quantity or quantity <= 0:
            logger.error(f"❌ create_oca_protection: invalid quantity={quantity} for {clean_symbol}/{account_type}, skipping")
            return None, f"invalid_quantity:{quantity}"
        
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
            if force_replace:
                logger.info(f"🔄 force_replace=True: cancelling existing OCAGroup #{existing_oca.id} for {clean_symbol}/{account_type} "
                           f"(old qty={existing_oca.quantity}, new qty={quantity}, old SL={existing_oca.stop_price}, new SL={stop_price})")
                existing_oca.status = OCAStatus.CANCELLED
                try:
                    from models import OrderTracker
                    old_order_ids = [oid for oid in [existing_oca.stop_order_id, existing_oca.take_profit_order_id] if oid]
                    if old_order_ids:
                        stale_trackers = OrderTracker.query.filter(
                            OrderTracker.tiger_order_id.in_(old_order_ids),
                            OrderTracker.status == 'PENDING'
                        ).all()
                        for st in stale_trackers:
                            st.status = 'CANCELLED'
                            logger.info(f"📋 Cancelled stale OrderTracker {st.tiger_order_id} ({st.role.value}) for OCA replace")
                except Exception as ot_err:
                    logger.warning(f"Failed to cancel old OrderTracker records: {ot_err}")
                db.session.commit()
            else:
                logger.info(f"OCA protection already exists for {clean_symbol}/{account_type}: OCAGroup #{existing_oca.id}")
                return existing_oca, "already_exists"
        
        resolved_trade_id = trade_id
        resolved_entry_price = entry_price or 0
        
        if trailing_stop_id:
            ts_position = TrailingStopPosition.query.get(trailing_stop_id)
            if ts_position:
                resolved_trade_id = resolved_trade_id or ts_position.trade_id
                resolved_entry_price = resolved_entry_price or ts_position.entry_price
            else:
                logger.warning(f"TrailingStopPosition {trailing_stop_id} not found, proceeding without it")
                trailing_stop_id = None
        
        return create_oca_protection_for_entry(
            trade_id=resolved_trade_id if resolved_trade_id else None,
            symbol=clean_symbol,
            account_type=account_type,
            quantity=quantity,
            entry_price=resolved_entry_price,
            stop_loss_price=stop_price,
            take_profit_price=take_profit_price,
            trailing_stop_id=trailing_stop_id,
            position_side=side,
            skip_dedup=True,
            creation_source=creation_source
        )
        
    except Exception as e:
        logger.error(f"Error in create_oca_protection: {e}")
        return None, f"error: {str(e)}"


def on_oca_leg_filled(
    tiger_order_id: str,
    filled_price: float,
    filled_quantity: float,
    realized_pnl: Optional[float] = None,
    commission: Optional[float] = None
) -> Tuple[Optional[object], str]:
    """Handle when an OCA leg (stop or take profit) is filled.
    
    This function ONLY handles OCAGroup-specific bookkeeping:
    1. Identifies which OCAGroup this order belongs to
    2. Updates OCAGroup status (triggered_stop or triggered_tp)
    3. Deactivates TrailingStopPosition
    4. Explicitly cancels sibling OCA order via Tiger API
    
    Position/PositionLeg updates are handled by handle_fill_event() which is
    called separately (single source of truth via order_tracker_service).
    
    Args:
        tiger_order_id: The filled order's Tiger ID
        filled_price: Fill price
        filled_quantity: Fill quantity
        realized_pnl: Realized P&L from Tiger API
        commission: Commission from Tiger API
        
    Returns:
        Tuple of (OCAGroup or None, status message)
    """
    from app import db
    from models import OCAGroup, OCAStatus, TrailingStopPosition
    
    try:
        oca_group = OCAGroup.query.filter(
            (OCAGroup.stop_order_id == tiger_order_id) | 
            (OCAGroup.take_profit_order_id == tiger_order_id)
        ).filter(OCAGroup.status == OCAStatus.ACTIVE).first()
        
        if not oca_group:
            logger.debug(f"No active OCAGroup found for order {tiger_order_id}")
            return None, "not_found"
        
        if tiger_order_id == oca_group.stop_order_id:
            oca_group.status = OCAStatus.TRIGGERED_STOP
            leg_type = "stop_loss"
            sibling_order_id = oca_group.take_profit_order_id
            logger.info(f"OCA stop loss triggered for {oca_group.symbol}: ${filled_price}")
        else:
            oca_group.status = OCAStatus.TRIGGERED_TP
            leg_type = "take_profit"
            sibling_order_id = oca_group.stop_order_id
            logger.info(f"OCA take profit triggered for {oca_group.symbol}: ${filled_price}")
        
        oca_group.triggered_order_id = tiger_order_id
        oca_group.triggered_price = filled_price
        oca_group.triggered_at = datetime.utcnow()
        
        if oca_group.trailing_stop_id:
            ts_pos = TrailingStopPosition.query.get(oca_group.trailing_stop_id)
            if ts_pos and ts_pos.is_active:
                ts_pos.is_active = False
                ts_pos.is_triggered = True
                ts_pos.triggered_at = datetime.utcnow()
                ts_pos.triggered_price = filled_price
                ts_pos.trigger_reason = f"OCA {leg_type} triggered"
                logger.info(f"Deactivated TrailingStopPosition {ts_pos.id} (set triggered_at={ts_pos.triggered_at}, trigger_reason={ts_pos.trigger_reason})")
        
        if sibling_order_id:
            _cancel_sibling_order_at_tiger(sibling_order_id, oca_group.account_type, oca_group.symbol, leg_type)
        
        db.session.commit()
        
        return oca_group, f"{leg_type}_triggered"
        
    except Exception as e:
        logger.error(f"Error handling OCA leg fill for {tiger_order_id}: {e}")
        db.session.rollback()
        return None, f"error: {str(e)}"


def _cancel_sibling_order_at_tiger(
    sibling_order_id: str,
    account_type: str,
    symbol: str,
    triggered_leg: str
) -> None:
    """Explicitly cancel the sibling OCA order via Tiger API.
    
    Tiger's OCA mechanism should auto-cancel, but we send an explicit cancel
    as a safety net to avoid stale orders remaining in the market.
    """
    from tiger_client import TigerClient, TigerPaperClient
    
    try:
        client = TigerPaperClient() if account_type == 'paper' else TigerClient()
        cancel_result = client.cancel_order(sibling_order_id)
        if cancel_result.get('success'):
            logger.info(f"Cancelled sibling OCA order {sibling_order_id} for {symbol} "
                       f"({triggered_leg} triggered)")
        else:
            error = cancel_result.get('error', '')
            if 'filled' in str(error).lower() or 'cancelled' in str(error).lower():
                logger.debug(f"Sibling order {sibling_order_id} already filled/cancelled: {error}")
            else:
                logger.warning(f"Failed to cancel sibling order {sibling_order_id}: {error}")
    except Exception as e:
        logger.warning(f"Error cancelling sibling OCA order {sibling_order_id}: {e}")


def cancel_oca_for_manual_close(
    symbol: str,
    account_type: str
) -> Tuple[int, str]:
    """Cancel active OCA groups when manually closing a position.
    
    Args:
        symbol: Stock symbol
        account_type: 'real' or 'paper'
        
    Returns:
        Tuple of (number of groups cancelled, status message)
    """
    from app import db
    from models import OCAGroup, OCAStatus
    from tiger_client import TigerClient, TigerPaperClient
    
    try:
        active_groups = OCAGroup.query.filter_by(
            symbol=symbol,
            account_type=account_type,
            status=OCAStatus.ACTIVE
        ).all()
        
        if not active_groups:
            logger.debug(f"No active OCA groups found for {symbol} ({account_type})")
            return 0, "no_active_groups"
        
        if account_type == 'paper':
            client = TigerPaperClient()
        else:
            client = TigerClient()
        
        cancelled_count = 0
        for group in active_groups:
            order_ids_to_cancel = []
            if group.stop_order_id:
                order_ids_to_cancel.append(group.stop_order_id)
            if group.take_profit_order_id:
                order_ids_to_cancel.append(group.take_profit_order_id)
            
            for order_id in order_ids_to_cancel:
                try:
                    cancel_result = client.cancel_order(order_id)
                    if cancel_result.get('success'):
                        logger.info(f"Cancelled OCA order {order_id}")
                    else:
                        logger.warning(f"Failed to cancel order {order_id}: {cancel_result.get('error')}")
                except Exception as e:
                    logger.warning(f"Error cancelling order {order_id}: {e}")
            
            group.status = OCAStatus.CANCELLED
            cancelled_count += 1
            logger.info(f"Marked OCAGroup {group.id} as cancelled")
        
        db.session.commit()
        return cancelled_count, f"cancelled_{cancelled_count}_groups"
        
    except Exception as e:
        logger.error(f"Error cancelling OCA groups for {symbol}: {e}")
        db.session.rollback()
        return 0, f"error: {str(e)}"


def update_oca_stop_price(
    oca_group_id: int,
    new_stop_price: float
) -> Tuple[bool, str]:
    """Update the stop price in an active OCA group.
    
    This is called when TrailingStop adjusts the stop price (tier progression).
    We try to modify the existing order; if that fails, cancel and recreate.
    
    Args:
        oca_group_id: OCAGroup record ID
        new_stop_price: New stop loss price
        
    Returns:
        Tuple of (success, status message)
    """
    from app import db
    from models import OCAGroup, OCAStatus
    from tiger_client import TigerClient, TigerPaperClient
    
    try:
        group = OCAGroup.query.get(oca_group_id)
        if not group:
            return False, "group_not_found"
        
        if group.status != OCAStatus.ACTIVE:
            return False, f"group_not_active: {group.status.value}"
        
        new_stop_price = round(new_stop_price, 2)
        new_stop_limit_price = round(new_stop_price * 0.995, 2)
        
        if group.account_type == 'paper':
            client = TigerPaperClient()
        else:
            client = TigerClient()
        
        if group.stop_order_id:
            try:
                modify_result = client.modify_order(
                    order_id=group.stop_order_id,
                    limit_price=new_stop_limit_price,
                    aux_price=new_stop_price
                )
                
                if modify_result.get('success'):
                    group.stop_price = new_stop_price
                    group.stop_limit_price = new_stop_limit_price
                    db.session.commit()
                    logger.info(f"Modified OCA stop order {group.stop_order_id} to ${new_stop_price}")
                    return True, "modified"
                else:
                    logger.warning(f"Modify failed: {modify_result.get('error')}, will recreate")
            except Exception as e:
                logger.warning(f"Modify exception: {e}, will recreate")
        
        logger.info(f"Recreating OCA group with new stop price ${new_stop_price}")
        
        old_stop_id = group.stop_order_id
        old_tp_id = group.take_profit_order_id
        
        try:
            result = client.create_oca_orders_for_position(
                symbol=group.symbol,
                quantity=group.quantity,
                stop_loss_price=new_stop_price,
                take_profit_price=group.take_profit_price,
                entry_price=group.entry_price,
                trade_id=group.trade_id,
                trailing_stop_id=group.trailing_stop_id,
                cancel_existing=True
            )
            
            if result.get('success'):
                group.previous_stop_order_id = old_stop_id
                group.previous_tp_order_id = old_tp_id
                group.stop_order_id = result.get('stop_loss_order_id')
                group.take_profit_order_id = result.get('take_profit_order_id')
                group.stop_price = new_stop_price
                group.stop_limit_price = new_stop_limit_price
                group.rebuild_count += 1
                group.last_rebuild_at = datetime.utcnow()
                
                db.session.commit()
                logger.info(f"Recreated OCA group {group.id} with new stop ${new_stop_price}")
                return True, "recreated"
            else:
                logger.error(f"Failed to recreate OCA: {result.get('error')}")
                return False, f"recreate_failed: {result.get('error')}"
                
        except Exception as e:
            logger.error(f"Error recreating OCA: {e}")
            return False, f"recreate_error: {str(e)}"
        
    except Exception as e:
        logger.error(f"Error updating OCA stop price: {e}")
        db.session.rollback()
        return False, f"error: {str(e)}"


def expire_paper_oca_groups(app) -> int:
    """Mark Paper account OCA groups as EXPIRED when their DAY orders have expired.
    Also cleans up orphaned groups whose TrailingStopPosition is no longer active.
    
    This is a lightweight operation (no API calls) that should run periodically.
    
    Returns:
        Number of groups expired
    """
    from models import OCAGroup, OCAStatus, TrailingStopPosition
    
    expired_count = 0
    try:
        with app.app_context():
            from app import db
            
            active_paper_groups = OCAGroup.query.filter_by(
                account_type='paper',
                status=OCAStatus.ACTIVE
            ).all()
            
            if not active_paper_groups:
                return 0
            
            for group in active_paper_groups:
                should_expire = False
                reason = ""
                
                if group.trailing_stop_id:
                    ts_pos = TrailingStopPosition.query.get(group.trailing_stop_id)
                    if not ts_pos or not ts_pos.is_active:
                        should_expire = True
                        reason = "trailing stop no longer active"
                
                if should_expire:
                    group.status = OCAStatus.EXPIRED
                    expired_count += 1
                    logger.info(f"Expired OCA group #{group.id} for {group.symbol}: {reason}")
            
            if expired_count > 0:
                db.session.commit()
                logger.info(f"Expired {expired_count} Paper OCA groups")
            
            return expired_count
    except Exception as e:
        logger.error(f"Error in expire_paper_oca_groups: {e}")
        return expired_count


def mark_paper_oca_expired_after_day_expiry(app) -> int:
    """Mark all Paper ACTIVE OCA groups as NEEDS_REBUILD after DAY orders expire at 20:00 ET.
    Called once per day after 20:00 ET.
    
    Since OCAStatus doesn't have NEEDS_REBUILD, we mark them as EXPIRED.
    The gradual rebuild will detect positions without OCA and recreate them.
    
    Returns:
        Number of groups marked
    """
    from models import OCAGroup, OCAStatus
    
    marked_count = 0
    try:
        with app.app_context():
            from app import db
            
            active_paper_groups = OCAGroup.query.filter_by(
                account_type='paper',
                status=OCAStatus.ACTIVE
            ).all()
            
            for group in active_paper_groups:
                group.status = OCAStatus.EXPIRED
                marked_count += 1
            
            if marked_count > 0:
                db.session.commit()
                logger.info(f"Marked {marked_count} Paper OCA groups as EXPIRED (DAY orders expired at 20:00 ET)")
            
            return marked_count
    except Exception as e:
        logger.error(f"Error marking Paper OCA groups expired: {e}")
        return marked_count


def _get_last_oca_rebuild_timestamp(app) -> float:
    """Get the last OCA rebuild timestamp from database (survives restarts)."""
    try:
        with app.app_context():
            from models import TradingConfig
            config = TradingConfig.query.filter_by(key='last_oca_rebuild_timestamp').first()
            if config and config.value:
                return float(config.value)
    except Exception as e:
        logger.debug(f"Could not get last OCA rebuild timestamp: {e}")
    return 0


def _set_last_oca_rebuild_timestamp(app, timestamp: float):
    """Persist the last OCA rebuild timestamp to database."""
    try:
        with app.app_context():
            from models import TradingConfig
            from app import db
            config = TradingConfig.query.filter_by(key='last_oca_rebuild_timestamp').first()
            if config:
                config.value = str(timestamp)
            else:
                config = TradingConfig(key='last_oca_rebuild_timestamp', value=str(timestamp), description='DB-persisted throttle for gradual OCA rebuild')
                db.session.add(config)
            db.session.commit()
    except Exception as e:
        logger.debug(f"Could not save last OCA rebuild timestamp: {e}")


def rebuild_one_paper_oca(app) -> Dict[str, Any]:
    """Gradual OCA rebuild: processes at most ONE position per call.
    
    This replaces the old bulk rebuild_paper_oca_daily function.
    Called from the scheduler's slow loop. Each call:
    1. Finds ONE Paper position that needs OCA protection (no active OCA group)
    2. Creates OCA orders for that single position
    3. Returns immediately (next position handled in the next cycle)
    
    Key safety features to avoid API rate limiting:
    - Minimum gap between rebuilds: _OCA_REBUILD_MIN_GAP_SECONDS (120s)
    - cancel_existing=False: DAY orders already expired, no need to cancel
    - Global rate-limit backoff: if any rebuild gets rate-limited, pause ALL
      rebuilds for _OCA_RATE_LIMIT_BACKOFF_SECONDS (600s = 10 minutes)
    - Per-symbol cooldown on failure: _OCA_REBUILD_COOLDOWN_SECONDS (300s)
    
    Priority: positions closest to their stop loss price are rebuilt first.
    
    Args:
        app: Flask app context
        
    Returns:
        Dict with rebuild result for this single cycle
    """
    global _last_oca_rebuild_time, _oca_rate_limited_until
    from models import OCAGroup, OCAStatus, TrailingStopPosition, Trade as TradeModel
    from tiger_client import TigerPaperClient
    
    result = {
        'action': None,
        'symbol': None,
        'status': 'no_action',
        'pending_count': 0,
        'error': None
    }
    
    try:
        now = time.time()
        
        if now < _oca_rate_limited_until:
            remaining = int(_oca_rate_limited_until - now)
            result['status'] = 'rate_limit_backoff'
            result['error'] = f"Global rate-limit backoff active, {remaining}s remaining"
            return result
        
        if _last_oca_rebuild_time == 0:
            persisted_time = _get_last_oca_rebuild_timestamp(app)
            if persisted_time > 0:
                _last_oca_rebuild_time = persisted_time
        
        time_since_last = now - _last_oca_rebuild_time
        if time_since_last < _OCA_REBUILD_MIN_GAP_SECONDS:
            result['status'] = 'throttled'
            result['error'] = f"Too soon since last rebuild ({int(time_since_last)}s < {_OCA_REBUILD_MIN_GAP_SECONDS}s)"
            return result
        
        with app.app_context():
            from app import db
            
            expire_paper_oca_groups(app)
            
            active_paper_positions = TrailingStopPosition.query.filter_by(
                account_type='paper',
                is_active=True
            ).all()
            
            if not active_paper_positions:
                return result
            
            active_oca_ts_ids = set(
                g.trailing_stop_id for g in OCAGroup.query.filter_by(
                    account_type='paper',
                    status=OCAStatus.ACTIVE
                ).all() if g.trailing_stop_id
            )
            
            candidates = []
            for ts_pos in active_paper_positions:
                if ts_pos.id in active_oca_ts_ids:
                    continue
                
                symbol = ts_pos.symbol.replace('[PAPER]', '').strip()
                
                stop_price = ts_pos.fixed_stop_loss or ts_pos.current_trailing_stop
                take_profit_price = ts_pos.fixed_take_profit
                if not stop_price or not take_profit_price:
                    continue
                
                last_fail_time = _oca_rebuild_failure_tracker.get(symbol, 0)
                if now - last_fail_time < _OCA_REBUILD_COOLDOWN_SECONDS:
                    continue
                
                candidates.append(ts_pos)
            
            result['pending_count'] = len(candidates)
            
            if not candidates:
                return result
            
            try:
                from tiger_push_client import get_latest_price
                def get_risk_score(ts_pos):
                    symbol = ts_pos.symbol.replace('[PAPER]', '').strip()
                    stop_price = ts_pos.fixed_stop_loss or ts_pos.current_trailing_stop
                    latest = get_latest_price(symbol)
                    if latest and stop_price and latest > 0:
                        distance_pct = abs(latest - stop_price) / latest * 100
                        return distance_pct
                    return 999
                candidates.sort(key=get_risk_score)
            except Exception:
                pass
            
            target = candidates[0]
            symbol = target.symbol.replace('[PAPER]', '').strip()
            stop_price = target.fixed_stop_loss or target.current_trailing_stop
            take_profit_price = target.fixed_take_profit
            position_side = 'long' if target.quantity > 0 else 'short'
            
            entry_order_id = None
            try:
                if target.trade_id:
                    entry_trade = TradeModel.query.get(target.trade_id)
                    if entry_trade and entry_trade.tiger_order_id:
                        entry_order_id = entry_trade.tiger_order_id
            except Exception:
                pass
            
            result['symbol'] = symbol
            result['action'] = 'create_oca'
            
            logger.info(f"🔧 Gradual OCA rebuild: creating protection for {symbol} "
                       f"(1 of {len(candidates)} pending, stop=${stop_price}, tp=${take_profit_price}, "
                       f"trade_id={target.trade_id}, trailing_stop_id={target.id}, "
                       f"entry_order_id={entry_order_id}, cancel_existing=False)")
            
            oca_group, status = create_oca_protection_for_entry(
                trade_id=target.trade_id,
                symbol=symbol,
                account_type='paper',
                quantity=abs(target.quantity),
                entry_price=target.entry_price,
                stop_loss_price=stop_price,
                take_profit_price=take_profit_price,
                trailing_stop_id=target.id,
                position_side=position_side,
                cancel_existing=False,
                creation_source='oca_rebuild'
            )
            
            _last_oca_rebuild_time = time.time()
            _set_last_oca_rebuild_timestamp(app, _last_oca_rebuild_time)
            
            if oca_group and "created" in status:
                result['status'] = 'created'
                _oca_rebuild_failure_tracker.pop(symbol, None)
                logger.info(f"✅ Gradual OCA rebuild: created protection for {symbol} "
                           f"(OCAGroup #{oca_group.id}, trade_id={oca_group.trade_id}, "
                           f"trailing_stop_id={oca_group.trailing_stop_id}, "
                           f"entry_order_id={entry_order_id}, {len(candidates)-1} remaining)")
            else:
                if 'already_exists' in status:
                    result['status'] = 'already_exists'
                    logger.info(f"✅ Gradual OCA rebuild: {symbol} already has active OCA protection, skipping")
                else:
                    result['status'] = 'failed'
                    result['error'] = status
                    if 'rate limit' in status.lower():
                        _oca_rate_limited_until = time.time() + _OCA_RATE_LIMIT_BACKOFF_SECONDS
                        logger.warning(f"🚨 Rate limited! All OCA rebuilds paused for {_OCA_RATE_LIMIT_BACKOFF_SECONDS}s")
                    _oca_rebuild_failure_tracker[symbol] = time.time()
                    logger.error(f"❌ Gradual OCA rebuild: failed for {symbol}: {status}")
            
            return result
            
    except Exception as e:
        logger.error(f"Error in rebuild_one_paper_oca: {e}")
        result['status'] = 'error'
        result['error'] = str(e)
        return result


def get_paper_oca_rebuild_status(app) -> Dict[str, Any]:
    """Get current status of Paper OCA rebuild progress.
    
    Returns summary of how many positions need OCA, how many have it, etc.
    Useful for dashboard display.
    """
    from models import OCAGroup, OCAStatus, TrailingStopPosition
    
    try:
        with app.app_context():
            active_positions = TrailingStopPosition.query.filter_by(
                account_type='paper',
                is_active=True
            ).count()
            
            active_oca_count = OCAGroup.query.filter_by(
                account_type='paper',
                status=OCAStatus.ACTIVE
            ).count()
            
            cooldown_symbols = []
            now = time.time()
            for symbol, fail_time in _oca_rebuild_failure_tracker.items():
                remaining = _OCA_REBUILD_COOLDOWN_SECONDS - (now - fail_time)
                if remaining > 0:
                    cooldown_symbols.append({'symbol': symbol, 'remaining_seconds': int(remaining)})
            
            rate_limit_remaining = max(0, int(_oca_rate_limited_until - now)) if _oca_rate_limited_until > now else 0
            
            return {
                'total_positions': active_positions,
                'protected_count': active_oca_count,
                'unprotected_count': max(0, active_positions - active_oca_count),
                'cooldown_symbols': cooldown_symbols,
                'min_gap_seconds': _OCA_REBUILD_MIN_GAP_SECONDS,
                'cooldown_seconds': _OCA_REBUILD_COOLDOWN_SECONDS,
                'last_rebuild_time': _last_oca_rebuild_time,
                'rate_limit_backoff_remaining': rate_limit_remaining,
                'rate_limit_backoff_seconds': _OCA_RATE_LIMIT_BACKOFF_SECONDS
            }
    except Exception as e:
        logger.error(f"Error getting OCA rebuild status: {e}")
        return {'error': str(e)}


def trigger_soft_stop(
    oca_group_id: int,
    current_price: float
) -> Tuple[bool, str]:
    """Trigger soft stop protection for Paper account in extended hours.
    
    When price hits stop level during pre/post market (when Tiger stop orders
    don't execute), we manually cancel OCA and send a limit close order.
    
    Args:
        oca_group_id: OCAGroup record ID
        current_price: Current market price that triggered the stop
        
    Returns:
        Tuple of (success, status message)
    """
    from app import db
    from models import OCAGroup, OCAStatus, TrailingStopPosition
    from tiger_client import TigerPaperClient
    from order_tracker_service import register_order
    
    try:
        group = OCAGroup.query.get(oca_group_id)
        if not group:
            return False, "group_not_found"
        
        if group.status != OCAStatus.ACTIVE:
            return False, f"group_not_active: {group.status.value}"
        
        if group.account_type != 'paper':
            return False, "not_paper_account"
        
        logger.info(f"Triggering soft stop for {group.symbol} at ${current_price}")
        
        client = TigerPaperClient()
        
        order_ids_to_cancel = []
        if group.stop_order_id:
            order_ids_to_cancel.append(group.stop_order_id)
        if group.take_profit_order_id:
            order_ids_to_cancel.append(group.take_profit_order_id)
        
        for order_id in order_ids_to_cancel:
            try:
                client.cancel_order(order_id)
                logger.info(f"Cancelled OCA order {order_id} for soft stop")
            except Exception as e:
                logger.warning(f"Error cancelling {order_id}: {e}")
        
        action = 'SELL' if group.side == 'long' else 'BUY'
        close_price = round(current_price * (0.998 if action == 'SELL' else 1.002), 2)
        
        close_result = client.place_limit_order(
            symbol=group.symbol,
            action=action,
            quantity=int(group.quantity),
            limit_price=close_price,
            outside_rth=True,
            time_in_force='DAY'
        )
        
        if close_result.get('success'):
            close_order_id = close_result.get('order_id')
            logger.info(f"Soft stop close order placed: {close_order_id} at ${close_price}")
            
            register_order(
                tiger_order_id=str(close_order_id),
                symbol=group.symbol,
                account_type='paper',
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
                    ts_pos.trigger_reason = "Soft stop (extended hours)"
            
            db.session.commit()
            return True, f"soft_stop_triggered_order_{close_order_id}"
        else:
            logger.error(f"Failed to place soft stop close order: {close_result.get('error')}")
            return False, f"close_order_failed: {close_result.get('error')}"
        
    except Exception as e:
        logger.error(f"Error triggering soft stop: {e}")
        db.session.rollback()
        return False, f"error: {str(e)}"


def get_active_oca_for_position(symbol: str, account_type: str) -> Optional[object]:
    """Get the active OCA group for a position.
    
    Args:
        symbol: Stock symbol
        account_type: 'real' or 'paper'
        
    Returns:
        OCAGroup record or None
    """
    from models import OCAGroup, OCAStatus
    
    return OCAGroup.query.filter_by(
        symbol=symbol,
        account_type=account_type,
        status=OCAStatus.ACTIVE
    ).first()


def sync_oca_with_tiger(app) -> Dict[str, Any]:
    """Sync OCA group status with Tiger API order status.
    
    This should be called on startup and periodically to ensure
    our records match the actual order status at Tiger.
    
    Args:
        app: Flask app context
        
    Returns:
        Dict with sync statistics
    """
    from models import OCAGroup, OCAStatus
    from tiger_client import TigerClient, TigerPaperClient
    
    results = {
        'checked': 0,
        'synced': 0,
        'expired': 0,
        'errors': []
    }
    
    try:
        with app.app_context():
            from app import db
            
            active_groups = OCAGroup.query.filter_by(status=OCAStatus.ACTIVE).all()
            results['checked'] = len(active_groups)
            
            for group in active_groups:
                try:
                    if group.account_type == 'paper':
                        client = TigerPaperClient()
                    else:
                        client = TigerClient()
                    
                    stop_status = None
                    tp_status = None
                    
                    if group.stop_order_id:
                        stop_result = client.get_order_status(group.stop_order_id)
                        stop_status = stop_result.get('status', '').lower()
                    
                    if group.take_profit_order_id:
                        tp_result = client.get_order_status(group.take_profit_order_id)
                        tp_status = tp_result.get('status', '').lower()
                    
                    if stop_status == 'filled':
                        group.status = OCAStatus.TRIGGERED_STOP
                        group.triggered_order_id = group.stop_order_id
                        group.triggered_at = datetime.utcnow()
                        results['synced'] += 1
                        logger.info(f"Synced OCA {group.id}: stop filled")
                    elif tp_status == 'filled':
                        group.status = OCAStatus.TRIGGERED_TP
                        group.triggered_order_id = group.take_profit_order_id
                        group.triggered_at = datetime.utcnow()
                        results['synced'] += 1
                        logger.info(f"Synced OCA {group.id}: TP filled")
                    elif stop_status in ['expired', 'cancelled'] and tp_status in ['expired', 'cancelled']:
                        group.status = OCAStatus.EXPIRED
                        results['expired'] += 1
                        logger.info(f"Synced OCA {group.id}: both orders expired/cancelled")
                        
                except Exception as e:
                    results['errors'].append(f"OCA {group.id}: {str(e)}")
                    logger.error(f"Error syncing OCA {group.id}: {e}")
            
            db.session.commit()
            
            logger.info(f"OCA sync: checked={results['checked']}, synced={results['synced']}, "
                       f"expired={results['expired']}")
            
            return results
            
    except Exception as e:
        logger.error(f"Error in sync_oca_with_tiger: {e}")
        results['errors'].append(str(e))
        return results


def verify_oca_stop_protection(trailing_stop_id: int, account_type: str) -> Dict[str, Any]:
    """Verify whether a position's OCA stop loss order is truly active.
    
    Checks local DB records (OrderTracker) to determine if the stop loss
    order within the OCA group is still in a live state. This is a fast,
    DB-only check (no Tiger API calls) designed to run in the monitoring loop.
    
    Returns:
        Dict with keys:
            - protected (bool): True if a live stop order exists on the broker
            - oca_group_id (int|None): The OCAGroup record ID if found
            - stop_order_status (str|None): Current status of the stop order
            - reason (str): Human-readable explanation
    """
    from models import OCAGroup, OCAStatus, OrderTracker
    
    result = {
        'protected': False,
        'oca_group_id': None,
        'stop_order_status': None,
        'reason': 'unknown'
    }
    
    try:
        oca_group = OCAGroup.query.filter_by(
            trailing_stop_id=trailing_stop_id,
            account_type=account_type,
            status=OCAStatus.ACTIVE
        ).first()
        
        if not oca_group:
            result['reason'] = 'no_active_oca_group'
            return result
        
        result['oca_group_id'] = oca_group.id
        
        if not oca_group.stop_order_id:
            try:
                from tiger_client import TigerClient, TigerPaperClient
                if account_type == 'paper':
                    client = TigerPaperClient()
                else:
                    client = TigerClient()
                clean_symbol = oca_group.symbol.replace('[PAPER]', '').strip()
                open_orders_result = client.get_open_orders_for_symbol(clean_symbol)
                if open_orders_result.get('success'):
                    for order in open_orders_result.get('orders', []):
                        order_type = str(getattr(order, 'order_type', '')).upper()
                        if 'STP' in order_type or 'STOP' in order_type:
                            found_stop_id = str(order.id)
                            logger.info(f"🔧 [{oca_group.symbol}] OCA group #{oca_group.id} missing stop_order_id, "
                                       f"found live STP order {found_stop_id} via API. Backfilling.")
                            oca_group.stop_order_id = found_stop_id
                            db.session.commit()
                            from order_tracker_service import register_order
                            existing_tracker = OrderTracker.query.filter_by(
                                tiger_order_id=found_stop_id
                            ).first()
                            if not existing_tracker:
                                try:
                                    register_order(
                                        tiger_order_id=found_stop_id,
                                        symbol=clean_symbol,
                                        account_type=account_type,
                                        role='stop_loss',
                                        side='SELL' if oca_group.side == 'long' else 'BUY',
                                        quantity=oca_group.quantity,
                                        order_type='STP_LMT',
                                        trailing_stop_id=trailing_stop_id
                                    )
                                except Exception as reg_err:
                                    logger.warning(f"Failed to register backfilled stop order: {reg_err}")
                            result['protected'] = True
                            result['reason'] = f'stop_order_live (backfilled from API)'
                            return result
                logger.warning(f"⚠️ [{oca_group.symbol}] OCA group #{oca_group.id} has no stop_order_id "
                              f"and no live STP order found via API")
            except Exception as api_err:
                logger.warning(f"⚠️ API check for missing stop_order_id failed: {api_err}")
            result['reason'] = 'oca_has_no_stop_order_id'
            return result
        
        stop_tracker = OrderTracker.query.filter_by(
            tiger_order_id=str(oca_group.stop_order_id)
        ).first()
        
        if not stop_tracker:
            result['reason'] = 'stop_order_not_in_tracker'
            return result
        
        result['stop_order_status'] = stop_tracker.status
        
        live_statuses = {'PENDING', 'SUBMITTED', 'INITIAL', 'NEW', 'HELD'}
        
        if stop_tracker.status in live_statuses:
            result['protected'] = True
            result['reason'] = f'stop_order_live ({stop_tracker.status})'
        elif stop_tracker.status == 'FILLED':
            result['reason'] = 'stop_order_already_filled'
        elif stop_tracker.status in {'CANCELLED', 'CANCELED'}:
            result['reason'] = 'stop_order_cancelled'
        elif stop_tracker.status in {'EXPIRED', 'REJECTED'}:
            result['reason'] = f'stop_order_{stop_tracker.status.lower()}'
        else:
            result['reason'] = f'stop_order_unknown_status ({stop_tracker.status})'
        
        return result
        
    except Exception as e:
        logger.error(f"Error verifying OCA stop protection for ts_id={trailing_stop_id}: {e}")
        result['reason'] = f'error: {str(e)}'
        return result
