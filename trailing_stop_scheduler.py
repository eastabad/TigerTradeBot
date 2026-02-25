import threading
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

_scheduler_thread = None
_scheduler_running = False
_last_check_time = None
_last_paper_oca_rebuild_date = None
_kline_backfill_done = False


def is_market_hours():
    """Check if current time is within US market hours (9:30 AM - 4:00 PM ET)"""
    import pytz
    
    try:
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)
        
        if now.weekday() >= 5:
            return False
        
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        
        extended_open = now.replace(hour=4, minute=0, second=0, microsecond=0)
        extended_close = now.replace(hour=20, minute=0, second=0, microsecond=0)
        
        return extended_open <= now <= extended_close
    except:
        return True


def _is_within_tradeable_session():
    """Check if current time is within any tradeable session (pre-market, regular, after-hours).
    
    Tiger API rejects orders outside 4:00 AM - 8:00 PM ET with '当前时段不支持下单'.
    """
    return is_market_hours()


def check_pending_orders_and_create_trailing_stops(app):
    """Check PENDING orders, if filled create trailing stop positions"""
    try:
        with app.app_context():
            from app import db
            from models import Trade, OrderStatus, TrailingStopPosition, TrailingStopMode
            from tiger_client import TigerClient, TigerPaperClient
            from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
            from datetime import datetime, timedelta
            
            ts_config = get_trailing_stop_config()
            ts_enabled = ts_config.is_enabled
            
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            pending_trades = Trade.query.filter(
                Trade.status == OrderStatus.PENDING,
                Trade.created_at >= cutoff_time
            ).all()
            
            if not pending_trades:
                return
            
            logger.debug(f"Checking {len(pending_trades)} pending orders for fill status (trailing_stop={'ON' if ts_enabled else 'OFF'})")
            
            for trade in pending_trades:
                try:
                    if not trade.tiger_order_id:
                        continue
                    
                    if trade.account_type == 'paper':
                        tiger_client = TigerPaperClient()
                    else:
                        tiger_client = TigerClient()
                    
                    status_result = tiger_client.get_order_status(trade.tiger_order_id)
                    
                    if not status_result.get('success'):
                        continue
                    
                    tiger_status = status_result.get('status')
                    
                    if tiger_status == 'filled':
                        trade.status = OrderStatus.FILLED
                        filled_price = status_result.get('filled_price', 0)
                        filled_qty = status_result.get('filled_quantity', trade.quantity)
                        
                        if filled_price:
                            trade.filled_price = filled_price
                        if filled_qty:
                            trade.filled_quantity = filled_qty
                        
                        logger.info(f"📦 Order {trade.tiger_order_id} ({trade.symbol}) filled at ${filled_price:.2f}")
                        
                        try:
                            from order_tracker_service import handle_fill_event
                            fill_result, fill_status = handle_fill_event(
                                tiger_order_id=str(trade.tiger_order_id),
                                filled_quantity=filled_qty or trade.quantity,
                                avg_fill_price=filled_price or 0,
                                realized_pnl=status_result.get('realized_pnl'),
                                commission=status_result.get('commission'),
                                fill_time=datetime.utcnow(),
                                source='scheduler_delayed_fill',
                            )
                            logger.info(f"📊 Scheduler fill → OrderTracker: {fill_status} for {trade.symbol}")
                        except Exception as fill_err:
                            logger.error(f"❌ Failed to process fill via OrderTracker for {trade.symbol}: {fill_err}")
                            logger.info(f"⏭️ Skipping remaining steps for {trade.symbol}, will retry next cycle")
                            continue
                        
                        try:
                            from models import EntrySignalRecord
                            existing_entry = EntrySignalRecord.query.filter_by(
                                entry_order_id=trade.tiger_order_id
                            ).first()
                            
                            if not existing_entry and not getattr(trade, 'is_close_position', False):
                                entry_record = EntrySignalRecord(
                                    symbol=trade.symbol,
                                    account_type=trade.account_type or 'real',
                                    side='long' if trade.side.value == 'buy' else 'short',
                                    entry_time=datetime.utcnow(),
                                    entry_price=filled_price or trade.reference_price or trade.price,
                                    quantity=filled_qty or trade.quantity,
                                    is_scaling=False,
                                    entry_order_id=trade.tiger_order_id,
                                    raw_json=trade.signal_data if hasattr(trade, 'signal_data') else None,
                                    signal_stop_loss=trade.stop_loss_price,
                                    signal_take_profit=trade.take_profit_price
                                )
                                db.session.add(entry_record)
                                db.session.flush()
                                logger.info(f"📝 Created EntrySignalRecord #{entry_record.id} for delayed fill: {trade.symbol}")
                        except Exception as entry_err:
                            logger.error(f"❌ Failed to create EntrySignalRecord for delayed fill: {entry_err}")
                        
                        is_entry = not getattr(trade, 'is_close_position', False)
                        ts_side = 'long' if trade.side.value == 'buy' else 'short'
                        entry_price = filled_price or trade.reference_price or trade.price
                        ts_symbol = trade.symbol
                        clean_symbol = ts_symbol.replace('[PAPER]', '').strip()
                        account_type = trade.account_type or 'real'
                        quantity = filled_qty or trade.quantity
                        trailing_position = None
                        
                        existing_ts = TrailingStopPosition.query.filter_by(
                            trade_id=trade.id,
                            is_active=True
                        ).first()
                        
                        if existing_ts:
                            trailing_position = existing_ts
                        elif is_entry and ts_enabled and entry_price and entry_price > 0:
                            if not trade.stop_loss_price and not trade.take_profit_price:
                                logger.info(f"⏭️ [{ts_symbol}] Skipping TrailingStop creation (entry_fill_handler): no SL/TP in entry signal")
                            else:
                                trailing_position = create_trailing_stop_for_trade(
                                    trade_id=trade.id,
                                    symbol=ts_symbol,
                                    side=ts_side,
                                    entry_price=entry_price,
                                    quantity=quantity,
                                    account_type=account_type,
                                    fixed_stop_loss=trade.stop_loss_price,
                                    fixed_take_profit=trade.take_profit_price,
                                    stop_loss_order_id=trade.stop_loss_order_id,
                                    take_profit_order_id=trade.take_profit_order_id,
                                    mode=TrailingStopMode.BALANCED,
                                    timeframe='15',
                                    creation_source='entry_fill_handler'
                                )
                                logger.info(f"🎯 Auto-created trailing stop for {ts_symbol}, side={ts_side}, entry=${entry_price:.2f}")
                        
                        if is_entry and trailing_position:
                            if trailing_position.stop_loss_order_id:
                                trade.stop_loss_order_id = trailing_position.stop_loss_order_id
                            if trailing_position.take_profit_order_id:
                                trade.take_profit_order_id = trailing_position.take_profit_order_id
                        
                        if hasattr(trade, 'needs_auto_protection') and trade.needs_auto_protection:
                            try:
                                import json as json_mod
                                protection_info = {}
                                if hasattr(trade, 'protection_info') and trade.protection_info:
                                    protection_info = json_mod.loads(trade.protection_info)
                                
                                sl_price = protection_info.get('stop_loss_price') or trade.stop_loss_price
                                tp_price = protection_info.get('take_profit_price') or trade.take_profit_price
                                
                                if sl_price or tp_price:
                                    has_switched = trailing_position.has_switched_to_trailing if trailing_position else False
                                    tp_for_oca = None if has_switched else tp_price
                                    
                                    from oca_service import create_oca_protection as create_oca_scaling
                                    scaling_ts_id = trailing_position.id if trailing_position else None
                                    oca_result, oca_status = create_oca_scaling(
                                        trailing_stop_id=scaling_ts_id,
                                        symbol=clean_symbol,
                                        side=ts_side,
                                        quantity=quantity,
                                        stop_price=sl_price,
                                        take_profit_price=tp_for_oca,
                                        account_type=account_type,
                                        trade_id=trade.id,
                                        entry_price=entry_price,
                                        force_replace=True,
                                        creation_source='entry_fill_handler'
                                    )
                                    
                                    if oca_result:
                                        logger.info(f"✅ Auto-protection applied via scheduler for {ts_symbol}: {oca_status}")
                                        trade.stop_loss_order_id = getattr(oca_result, 'stop_order_id', None)
                                        trade.take_profit_order_id = getattr(oca_result, 'take_profit_order_id', None)
                                        
                                        if trailing_position:
                                            from trailing_stop_engine import update_trailing_stop_on_position_increase
                                            from push_event_handlers import get_cached_position
                                            cached_pos = get_cached_position(clean_symbol, account_type, max_age_seconds=60)
                                            avg_cost = None
                                            current_qty = None
                                            if cached_pos and cached_pos.get('quantity'):
                                                avg_cost = cached_pos.get('average_cost', 0)
                                                current_qty = abs(cached_pos['quantity'])
                                            else:
                                                position_result = tiger_client.get_positions(symbol=clean_symbol)
                                                if position_result.get('success') and position_result.get('positions'):
                                                    avg_cost = position_result['positions'][0].get('average_cost', 0)
                                                    current_qty = abs(position_result['positions'][0]['quantity'])
                                            if avg_cost is not None and current_qty is not None:
                                                update_trailing_stop_on_position_increase(
                                                    symbol=trade.symbol,
                                                    account_type=account_type,
                                                    new_quantity=current_qty,
                                                    new_entry_price=avg_cost,
                                                    new_stop_loss_price=sl_price,
                                                    new_take_profit_price=tp_price,
                                                    new_stop_loss_order_id=getattr(oca_result, 'stop_order_id', None),
                                                    new_take_profit_order_id=getattr(oca_result, 'take_profit_order_id', None)
                                                )
                                    else:
                                        logger.error(f"❌ Auto-protection failed via scheduler: {oca_status}")
                                
                                trade.needs_auto_protection = False
                                trade.protection_info = None
                            except Exception as ap_err:
                                logger.error(f"❌ Auto-protection error in scheduler: {ap_err}")
                        
                        db.session.commit()
                    
                    elif tiger_status in ['cancelled', 'rejected', 'expired', 'invalid']:
                        if tiger_status == 'cancelled' or tiger_status == 'expired':
                            trade.status = OrderStatus.CANCELLED
                        else:
                            trade.status = OrderStatus.REJECTED
                        
                        # Record reject/cancel reason from Tiger API
                        reason = status_result.get('reason', '')
                        if reason:
                            trade.error_message = f"Tiger: {reason}"
                            logger.warning(f"⚠️ Order {trade.tiger_order_id} ({trade.symbol}) {tiger_status}: {reason}")
                        else:
                            logger.info(f"📋 Order {trade.tiger_order_id} ({trade.symbol}) status: {tiger_status}")
                        
                        db.session.commit()
                        
                        # Send Discord notification for rejected/cancelled orders
                        try:
                            from discord_notifier import get_discord_notifier
                            notifier = get_discord_notifier()
                            if notifier:
                                status_msg = f"❌ {tiger_status.upper()}"
                                if reason:
                                    status_msg += f" - {reason}"
                                notifier.send_order_notification(trade, status_msg, is_close=False)
                        except Exception as notify_err:
                            logger.debug(f"Discord notification skipped: {notify_err}")
                        
                except Exception as e:
                    logger.error(f"Error checking order {trade.tiger_order_id}: {str(e)}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error in check_pending_orders: {str(e)}")


def cleanup_stale_pending_orders(app):
    """Mark old PENDING orders (>24h) as EXPIRED to keep database clean"""
    try:
        with app.app_context():
            from app import db
            from models import Trade, OrderStatus
            from datetime import datetime, timedelta
            
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            stale_orders = Trade.query.filter(
                Trade.status == OrderStatus.PENDING,
                Trade.created_at < cutoff_time
            ).all()
            
            if stale_orders:
                for trade in stale_orders:
                    trade.status = OrderStatus.CANCELLED
                    logger.info(f"🧹 Auto-cancelled stale PENDING order: {trade.symbol} (created {trade.created_at})")
                db.session.commit()
                logger.info(f"🧹 Cleaned up {len(stale_orders)} stale PENDING orders")
    except Exception as e:
        logger.error(f"Error in cleanup_stale_pending_orders: {str(e)}")


def check_filled_trades_without_trailing_stop(app):
    """
    Fallback: Check for FILLED trades that don't have an active trailing stop.
    This handles cases where trailing stop creation failed during webhook processing.
    Uses shared cached positions (get_cached_tiger_positions) to avoid redundant API calls.
    """
    try:
        with app.app_context():
            from app import db
            from models import Trade, OrderStatus, TrailingStopPosition, TrailingStopMode
            from trailing_stop_engine import create_trailing_stop_for_trade, get_trailing_stop_config
            from datetime import datetime, timedelta
            
            ts_config = get_trailing_stop_config()
            if not ts_config.is_enabled:
                return
            
            # Only check trades from last 2 days
            cutoff_time = datetime.utcnow() - timedelta(days=2)
            
            # Find FILLED entry trades (not close positions)
            filled_trades = Trade.query.filter(
                Trade.status == OrderStatus.FILLED,
                Trade.created_at >= cutoff_time,
                Trade.is_close_position == False
            ).all()
            
            from trailing_stop_engine import get_cached_tiger_positions
            cached_positions = get_cached_tiger_positions(force_refresh=False)
            
            for trade in filled_trades:
                try:
                    existing_ts = TrailingStopPosition.query.filter_by(
                        trade_id=trade.id,
                        is_active=True
                    ).first()
                    
                    if not existing_ts:
                        existing_ts = TrailingStopPosition.query.filter_by(
                            symbol=trade.symbol,
                            account_type=trade.account_type or 'real',
                            is_active=True
                        ).first()
                    
                    if existing_ts:
                        has_oca = existing_ts.stop_loss_order_id or existing_ts.take_profit_order_id
                        needs_oca = trade.stop_loss_price or trade.take_profit_price
                        if has_oca or not needs_oca:
                            continue
                        
                        from models import OCAGroup
                        active_oca = OCAGroup.query.filter_by(
                            trailing_stop_id=existing_ts.id,
                            status='ACTIVE'
                        ).first()
                        if active_oca:
                            continue
                        
                        clean_sym = trade.symbol.replace('[PAPER]', '').strip()
                        ts_side = existing_ts.side or ('long' if trade.side.value == 'buy' else 'short')
                        oca_qty = None
                        for candidate_qty in [existing_ts.quantity, trade.filled_quantity, trade.quantity]:
                            if candidate_qty and candidate_qty > 0:
                                oca_qty = candidate_qty
                                break
                        
                        if not oca_qty or oca_qty <= 0:
                            account_key = trade.account_type or 'real'
                            if cached_positions and cached_positions.get(f'{account_key}_success', False):
                                account_positions = cached_positions.get(account_key, {})
                                if clean_sym in account_positions:
                                    pos = account_positions[clean_sym]
                                    pos_qty = abs(pos.get('quantity', 0) if isinstance(pos, dict) else getattr(pos, 'quantity', 0))
                                    if pos_qty > 0:
                                        oca_qty = pos_qty
                                        logger.info(f"📊 Using broker position qty={oca_qty} for {clean_sym} (TS/trade qty was 0)")
                        
                        if not oca_qty or oca_qty <= 0:
                            if not hasattr(ensure_oca_protection_for_trades, '_qty_zero_warned'):
                                ensure_oca_protection_for_trades._qty_zero_warned = set()
                            if clean_sym not in ensure_oca_protection_for_trades._qty_zero_warned:
                                logger.warning(f"⚠️ Cannot create OCA for {clean_sym}: quantity=0 in TS#{existing_ts.id}, trade#{trade.id}, and no broker position found")
                                ensure_oca_protection_for_trades._qty_zero_warned.add(clean_sym)
                            continue
                        
                        if (trade.account_type or 'real') == 'paper':
                            logger.debug(f"🔧 Skipping OCA creation for Paper account {trade.symbol}: Paper uses bracket-only architecture")
                            continue
                        
                        if not _is_within_tradeable_session():
                            logger.debug(f"🔧 Skipping OCA creation for {trade.symbol}: outside tradeable hours")
                            continue
                        
                        logger.info(f"🔧 Found TrailingStop #{existing_ts.id} for {trade.symbol} WITHOUT OCA protection, creating now (qty={oca_qty})...")
                        try:
                            from oca_service import create_oca_protection
                            oca_result, oca_status = create_oca_protection(
                                trailing_stop_id=existing_ts.id,
                                symbol=clean_sym,
                                side=ts_side,
                                quantity=oca_qty,
                                stop_price=existing_ts.fixed_stop_loss or trade.stop_loss_price,
                                take_profit_price=existing_ts.fixed_take_profit or trade.take_profit_price,
                                account_type=trade.account_type or 'real',
                                trade_id=trade.id,
                                entry_price=existing_ts.entry_price,
                                creation_source='scheduler_orphan'
                            )
                            if oca_result:
                                logger.info(f"✅ OCA protection created (orphan fix) for {trade.symbol}: {oca_status}")
                                existing_ts.stop_loss_order_id = oca_result.stop_order_id
                                existing_ts.take_profit_order_id = oca_result.take_profit_order_id
                                trade.stop_loss_order_id = oca_result.stop_order_id
                                trade.take_profit_order_id = oca_result.take_profit_order_id
                                db.session.commit()
                            else:
                                logger.warning(f"⚠️ OCA creation failed (orphan fix) for {trade.symbol}: {oca_status}")
                        except Exception as oca_err:
                            logger.error(f"❌ OCA creation error (orphan fix) for {trade.symbol}: {oca_err}")
                        continue
                    
                    clean_symbol = trade.symbol.replace('[PAPER]', '').strip()
                    account_type = trade.account_type or 'real'
                    
                    any_triggered = TrailingStopPosition.query.filter(
                        TrailingStopPosition.account_type == account_type,
                        TrailingStopPosition.is_active == False,
                        TrailingStopPosition.is_triggered == True,
                        db.or_(
                            TrailingStopPosition.symbol == clean_symbol,
                            TrailingStopPosition.symbol == f'[PAPER]{clean_symbol}'
                        ),
                    ).order_by(TrailingStopPosition.updated_at.desc()).first()
                    
                    from models import Position as PositionModel

                    if any_triggered:
                        open_pos = PositionModel.query.filter(
                            PositionModel.symbol == clean_symbol,
                            PositionModel.account_type == account_type,
                            PositionModel.status == 'OPEN',
                        ).first()
                        if open_pos:
                            triggered_time = any_triggered.updated_at or any_triggered.created_at
                            pos_created = open_pos.created_at
                            if pos_created and triggered_time and pos_created > triggered_time:
                                logger.info(f"[{clean_symbol}] Old triggered TS#{any_triggered.id} "
                                            f"(triggered {triggered_time}) belongs to previous lifecycle, "
                                            f"current OPEN position created {pos_created} — allowing re-creation")
                            else:
                                logger.debug(f"⏭️ Skipping {clean_symbol}: trailing stop was triggered in current lifecycle")
                                continue
                        else:
                            logger.debug(f"⏭️ Skipping {clean_symbol}: trailing stop was previously triggered, no OPEN position")
                            continue
                    closed_position = PositionModel.query.filter(
                        PositionModel.symbol == clean_symbol,
                        PositionModel.account_type == account_type,
                        PositionModel.status == 'CLOSED',
                    ).order_by(PositionModel.closed_at.desc()).first()
                    
                    if closed_position and closed_position.closed_at and \
                       closed_position.closed_at >= trade.created_at:
                        logger.debug(f"⏭️ Skipping {clean_symbol}: position closed after trade creation")
                        continue
                    
                    account_key = account_type
                    if not cached_positions.get(f'{account_key}_success', False):
                        continue
                    
                    account_positions = cached_positions.get(account_key, {})
                    if clean_symbol not in account_positions:
                        continue
                    
                    ts_side = 'long' if trade.side.value == 'buy' else 'short'
                    entry_price = trade.filled_price or trade.reference_price or trade.price
                    
                    if entry_price and entry_price > 0:
                        if not trade.stop_loss_price and not trade.take_profit_price:
                            logger.info(f"⏭️ [{trade.symbol}] Skipping TrailingStop creation (scheduler_orphan): no SL/TP in entry signal")
                        else:
                            trailing_position = create_trailing_stop_for_trade(
                                trade_id=trade.id,
                                symbol=trade.symbol,
                                side=ts_side,
                                entry_price=entry_price,
                                quantity=trade.filled_quantity or trade.quantity,
                                account_type=trade.account_type or 'real',
                                fixed_stop_loss=trade.stop_loss_price,
                                fixed_take_profit=trade.take_profit_price,
                                stop_loss_order_id=trade.stop_loss_order_id,
                                take_profit_order_id=trade.take_profit_order_id,
                                mode=TrailingStopMode.BALANCED,
                                timeframe='15',
                                creation_source='scheduler_orphan'
                            )
                            logger.info(f"🔧 Created missing trailing stop for FILLED trade: {trade.symbol}, entry=${entry_price:.2f}")
                        
                        if (trade.stop_loss_price or trade.take_profit_price) and account_type != 'paper':
                            try:
                                from oca_service import create_oca_protection
                                clean_sym = trade.symbol.replace('[PAPER]', '').strip()
                                oca_ts_id = trailing_position.id if trailing_position else None
                                oca_result, oca_status = create_oca_protection(
                                    trailing_stop_id=oca_ts_id,
                                    symbol=clean_sym,
                                    side=ts_side,
                                    quantity=trade.filled_quantity or trade.quantity,
                                    stop_price=trade.stop_loss_price,
                                    take_profit_price=trade.take_profit_price,
                                    account_type=account_type,
                                    creation_source='scheduler_orphan'
                                )
                                if oca_result:
                                    logger.info(f"✅ OCA protection created (fallback) for {trade.symbol}: {oca_status}")
                                    if trailing_position:
                                        trailing_position.stop_loss_order_id = oca_result.stop_order_id
                                        trailing_position.take_profit_order_id = oca_result.take_profit_order_id
                                    trade.stop_loss_order_id = oca_result.stop_order_id
                                    trade.take_profit_order_id = oca_result.take_profit_order_id
                                    db.session.commit()
                                else:
                                    logger.warning(f"⚠️ OCA creation failed (fallback) for {trade.symbol}: {oca_status}")
                            except Exception as oca_err:
                                logger.error(f"❌ OCA creation error (fallback) for {trade.symbol}: {oca_err}")
                        elif account_type == 'paper' and (trade.stop_loss_price or trade.take_profit_price):
                            logger.debug(f"🔧 Skipping OCA creation for Paper {trade.symbol}: Paper uses bracket-only architecture")
                        
                except Exception as e:
                    logger.error(f"Error checking trade {trade.id}: {str(e)}")
                    continue
                    
    except Exception as e:
        logger.error(f"Error in check_filled_trades_without_trailing_stop: {str(e)}")


def verify_exit_position_closure(app):
    """Post-fill position verification: confirm that triggered trailing stops
    actually resulted in position closure at the broker.

    Checks all TS records where is_active=False and is_triggered=True with
    triggered_at between 30 seconds and 10 minutes ago. For each, verifies
    via cached Tiger positions whether the position is actually gone.

    If position still exists at broker:
    - Calculate remaining quantity
    - Reactivate TS with remaining qty for continued protection
    - Send Discord alert

    If position is gone: confirmed closed, no action needed.

    Timing window (30s-60min):
    - 30s minimum: gives broker time to settle the position after order fill
    - 60min maximum: extended from 10min to catch stuck partial exits and scaling scenarios
    """
    try:
        with app.app_context():
            from app import db
            from models import TrailingStopPosition, OrderTracker, OrderRole
            from trailing_stop_engine import get_cached_tiger_positions
            from datetime import timedelta

            now = datetime.utcnow()
            min_age = now - timedelta(seconds=30)
            max_age = now - timedelta(minutes=60)

            triggered_ts_list = TrailingStopPosition.query.filter(
                TrailingStopPosition.is_active == False,
                TrailingStopPosition.is_triggered == True,
                TrailingStopPosition.triggered_at != None,
                TrailingStopPosition.triggered_at <= min_age,
                TrailingStopPosition.triggered_at >= max_age,
            ).all()

            if not triggered_ts_list:
                return

            tiger_positions = get_cached_tiger_positions(force_refresh=False)

            reactivated = 0
            confirmed = 0

            for ts in triggered_ts_list:
                try:
                    clean_symbol = ts.symbol.replace('[PAPER]', '').strip()

                    if ts.account_type == 'paper':
                        broker_positions = tiger_positions.get('paper', {})
                        api_success = tiger_positions.get('paper_success', False)
                    else:
                        broker_positions = tiger_positions.get('real', {})
                        api_success = tiger_positions.get('real_success', False)

                    if not api_success:
                        logger.debug(f"[{ts.symbol}] Skip closure verification: position data unavailable")
                        continue

                    has_pending_exit = OrderTracker.query.filter(
                        OrderTracker.symbol == clean_symbol,
                        OrderTracker.account_type == ts.account_type,
                        OrderTracker.role == OrderRole.EXIT_TRAILING,
                        OrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD',
                                                 'PARTIALLY_FILLED', 'SUBMITTED']),
                    ).first()
                    if has_pending_exit:
                        logger.debug(f"[{ts.symbol}] Skip closure verification: "
                                    f"EXIT_TRAILING order {has_pending_exit.tiger_order_id} still pending")
                        continue

                    if clean_symbol not in broker_positions:
                        confirmed += 1
                        logger.debug(f"✅ [{ts.symbol}] TS #{ts.id} closure confirmed: "
                                    f"position no longer exists at broker")
                        continue

                    broker_pos = broker_positions[clean_symbol]
                    broker_qty = abs(broker_pos.get('quantity', 0))

                    if broker_qty <= 0:
                        confirmed += 1
                        continue

                    MAX_EXIT_RETRIES = 5
                    retry_count = ts.trigger_retry_count or 0
                    if retry_count >= MAX_EXIT_RETRIES:
                        logger.warning(f"⚠️ [{ts.symbol}] TS #{ts.id} position still open (qty={broker_qty}) "
                                      f"but max retries ({MAX_EXIT_RETRIES}) exhausted, skipping reactivation")
                        try:
                            from discord_notifier import send_trailing_stop_notification
                            send_trailing_stop_notification(
                                ts.symbol, 'error', 0, ts.entry_price, 0,
                                f"Position still open (qty={broker_qty}) after {MAX_EXIT_RETRIES} exit retries. "
                                f"Manual intervention needed. TS #{ts.id}"
                            )
                        except Exception:
                            pass
                        continue

                    ts.quantity = broker_qty
                    ts.is_active = True
                    ts.is_triggered = False
                    ts.triggered_price = None
                    ts.trigger_reason = None
                    ts.trigger_retry_count = retry_count + 1

                    reactivated += 1
                    logger.warning(f"🔄 [{ts.symbol}] TS #{ts.id} reactivated: position still open at broker "
                                  f"(qty={broker_qty}), retry {retry_count + 1}/{MAX_EXIT_RETRIES}")

                    try:
                        from discord_notifier import send_trailing_stop_notification
                        send_trailing_stop_notification(
                            ts.symbol, 'reactivate', 0, ts.entry_price, 0,
                            f"Position still open after exit fill (qty={broker_qty}). "
                            f"TS #{ts.id} reactivated, retry {retry_count + 1}/{MAX_EXIT_RETRIES}"
                        )
                    except Exception:
                        pass

                except Exception as e:
                    logger.error(f"Error verifying TS #{ts.id} ({ts.symbol}): {e}")

            if reactivated > 0 or confirmed > 0:
                db.session.commit()
                logger.info(f"📊 Exit closure verification: {confirmed} confirmed closed, "
                           f"{reactivated} reactivated (position still open)")

    except Exception as e:
        logger.error(f"Error in verify_exit_position_closure: {e}")


def detect_closed_positions_fallback(app):
    """
    Fallback mechanism: Compare active TrailingStopPositions against actual Tiger positions.
    If a position no longer exists in Tiger but is still active in our system,
    create a ClosedPosition record and deactivate the TrailingStopPosition.
    
    This handles cases where:
    - WebSocket order fill event was missed
    - Position was closed via Tiger APP directly
    - Stop loss / take profit triggered but we didn't get the push
    
    Uses shared cached positions (get_cached_tiger_positions) to avoid redundant API calls
    and rate limit issues. The cache is refreshed by process_all_active_positions in the
    same 60-second cycle.
    """
    try:
        with app.app_context():
            from app import db
            from models import TrailingStopPosition, ClosedPosition, EntrySignalRecord, ExitMethod, Position as PositionModel, PositionStatus
            from tiger_client import get_tiger_quote_client
            from trailing_stop_engine import get_cached_tiger_positions
            
            active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
            if not active_positions:
                return
            
            tiger_positions = get_cached_tiger_positions(force_refresh=False)
            
            real_api_success = tiger_positions.get('real_success', False)
            paper_api_success = tiger_positions.get('paper_success', False)
            tiger_real_positions = tiger_positions.get('real', {})
            tiger_paper_positions = tiger_positions.get('paper', {})
            
            for trailing_pos in active_positions:
                try:
                    clean_symbol = trailing_pos.symbol.replace('[PAPER]', '').strip()
                    
                    if trailing_pos.account_type == 'paper':
                        tiger_positions = tiger_paper_positions
                        api_success = paper_api_success
                    else:
                        tiger_positions = tiger_real_positions
                        api_success = real_api_success
                    
                    # Skip if API call failed for this account type
                    if not api_success:
                        logger.debug(f"Skipping {trailing_pos.symbol} ({trailing_pos.account_type}): API call failed")
                        continue
                    
                    if clean_symbol not in tiger_positions:
                        # Check 1: ClosedPosition already exists for this trailing_stop_id
                        existing_closed = ClosedPosition.query.filter_by(
                            trailing_stop_id=trailing_pos.id
                        ).first()
                        
                        if existing_closed:
                            trailing_pos.is_active = False
                            trailing_pos.is_triggered = True
                            trailing_pos.triggered_at = datetime.utcnow()
                            if not trailing_pos.trigger_reason:
                                trailing_pos.trigger_reason = "Position closed (ClosedPosition already exists)"
                            
                            _close_matching_position(
                                trailing_pos.symbol, clean_symbol, trailing_pos.account_type,
                                trailing_pos.side, trailing_stop_id=trailing_pos.id,
                                close_reason="ClosedPosition already exists",
                                exit_price=existing_closed.exit_price,
                                exit_quantity=existing_closed.exit_quantity,
                                exit_order_id=existing_closed.exit_order_id,
                                exit_method=existing_closed.exit_method,
                                close_source='ghost_detection'
                            )
                            
                            db.session.commit()
                            continue
                        
                        # Check 2: ClosedPosition created recently for same symbol/account (e.g., by webhook signal)
                        # If webhook already closed this position, don't create duplicate
                        from datetime import timedelta
                        time_window = datetime.utcnow() - timedelta(seconds=120)  # 2 minute window
                        recent_closed = ClosedPosition.query.filter(
                            ClosedPosition.symbol == clean_symbol,
                            ClosedPosition.account_type == trailing_pos.account_type,
                            ClosedPosition.created_at >= time_window
                        ).first()
                        
                        if recent_closed:
                            logger.info(f"📊 Fallback: Position {clean_symbol} already has ClosedPosition #{recent_closed.id} "
                                       f"(created {recent_closed.created_at}), just deactivating trailing stop")
                            trailing_pos.is_active = False
                            trailing_pos.is_triggered = True
                            trailing_pos.triggered_at = datetime.utcnow()
                            trailing_pos.trigger_reason = f"Position closed via {recent_closed.exit_method.value if recent_closed.exit_method else 'unknown'} (ClosedPosition #{recent_closed.id})"
                            
                            _close_matching_position(
                                trailing_pos.symbol, clean_symbol, trailing_pos.account_type,
                                trailing_pos.side, trailing_stop_id=trailing_pos.id,
                                close_reason=f"recent ClosedPosition #{recent_closed.id} exists",
                                exit_price=recent_closed.exit_price,
                                exit_quantity=recent_closed.exit_quantity,
                                exit_order_id=recent_closed.exit_order_id,
                                exit_method=recent_closed.exit_method,
                                close_source='ghost_detection'
                            )
                            
                            db.session.commit()
                            continue
                        
                        logger.info(f"📊 Fallback: Detected closed position for {trailing_pos.symbol} ({trailing_pos.account_type})")
                        
                        exit_price = None
                        exit_order_id = None
                        exit_quantity = trailing_pos.quantity
                        pnl = None
                        pnl_pct = None
                        real_data_found = False
                        commission = None
                        position_closed_via_ot = False
                        
                        exit_trackers = []
                        try:
                            exit_trackers = OrderTracker.query.filter(
                                OrderTracker.trailing_stop_id == trailing_pos.id,
                                OrderTracker.role.in_(['EXIT_TRAILING', 'EXIT_SIGNAL', 'STOP_LOSS', 'TAKE_PROFIT'])
                            ).order_by(OrderTracker.created_at.desc()).all()
                        except Exception as e:
                            logger.debug(f"Could not query OrderTracker for TS#{trailing_pos.id}: {e}")
                        
                        already_filled_tracker = None
                        for t in exit_trackers:
                            if t.status == 'FILLED' and t.avg_fill_price and t.avg_fill_price > 0:
                                already_filled_tracker = t
                                exit_price = float(t.avg_fill_price)
                                exit_quantity = float(t.filled_quantity) if t.filled_quantity else trailing_pos.quantity
                                exit_order_id = t.tiger_order_id
                                pnl = t.realized_pnl
                                commission = t.commission
                                real_data_found = True
                                logger.info(f"📊 Ghost detection: OrderTracker already FILLED for {clean_symbol}: "
                                           f"order={exit_order_id}, price=${exit_price:.2f}")
                                break
                        
                        if not real_data_found:
                            exit_order_ids = [t.tiger_order_id for t in exit_trackers if t.tiger_order_id and t.status != 'FILLED']
                            for eid in exit_order_ids:
                                try:
                                    order_status = tiger_client.get_order_status(eid)
                                    if order_status.get('success') and order_status.get('status') == 'filled':
                                        fill_price = order_status.get('filled_price', 0)
                                        fill_qty = order_status.get('filled_quantity', 0)
                                        if fill_price and fill_price > 0:
                                            exit_price = float(fill_price)
                                            exit_quantity = float(fill_qty) if fill_qty else trailing_pos.quantity
                                            exit_order_id = str(eid)
                                            pnl = order_status.get('realized_pnl') or None
                                            commission = order_status.get('commission') or None
                                            real_data_found = True
                                            
                                            from order_tracker_service import handle_fill_event
                                            ot_result, ot_status = handle_fill_event(
                                                tiger_order_id=eid,
                                                filled_quantity=exit_quantity,
                                                avg_fill_price=exit_price,
                                                realized_pnl=pnl,
                                                commission=commission,
                                                fill_time=datetime.utcnow(),
                                                source='ghost_detection'
                                            )
                                            if ot_status == 'filled':
                                                position_closed_via_ot = True
                                                logger.info(f"📊 Ghost detection routed fill through OrderTracker for {clean_symbol}: "
                                                           f"order={eid}, price=${exit_price:.2f}, Position closed via add_exit_leg")
                                            elif ot_status == 'already_filled':
                                                position_closed_via_ot = True
                                                logger.debug(f"📊 Ghost detection: OrderTracker already processed {eid}")
                                            else:
                                                logger.warning(f"📊 Ghost detection: handle_fill_event returned {ot_status} for {eid}")
                                            break
                                except Exception as e:
                                    logger.debug(f"Could not query order {eid}: {e}")
                        
                        if not real_data_found:
                            exit_price = trailing_pos.entry_price
                            try:
                                from trailing_stop_engine import get_realtime_price_with_websocket_fallback
                                quote_client = get_tiger_quote_client()
                                price_data = get_realtime_price_with_websocket_fallback(clean_symbol, quote_client)
                                if price_data and price_data.get('price'):
                                    exit_price = price_data['price']
                                    logger.debug(f"📊 Got estimated exit price for {clean_symbol}: ${exit_price:.2f} (source: {price_data.get('source', 'unknown')})")
                            except Exception as e:
                                logger.debug(f"Could not get realtime price for {clean_symbol}: {e}")
                                if trailing_pos.side == 'long' and trailing_pos.highest_price:
                                    exit_price = trailing_pos.highest_price
                                elif trailing_pos.side == 'short' and trailing_pos.lowest_price:
                                    exit_price = trailing_pos.lowest_price
                            logger.info(f"⚠️ Ghost detection using ESTIMATED data for {clean_symbol}: price=${exit_price:.2f}")
                        
                        exit_method = ExitMethod.TRAILING_STOP
                        if trailing_pos.fixed_stop_loss:
                            if trailing_pos.side == 'long' and exit_price <= trailing_pos.fixed_stop_loss * 1.01:
                                exit_method = ExitMethod.STOP_LOSS
                            elif trailing_pos.side == 'short' and exit_price >= trailing_pos.fixed_stop_loss * 0.99:
                                exit_method = ExitMethod.STOP_LOSS
                        if trailing_pos.fixed_take_profit:
                            if trailing_pos.side == 'long' and exit_price >= trailing_pos.fixed_take_profit * 0.99:
                                exit_method = ExitMethod.TAKE_PROFIT
                            elif trailing_pos.side == 'short' and exit_price <= trailing_pos.fixed_take_profit * 1.01:
                                exit_method = ExitMethod.TAKE_PROFIT
                        
                        if not real_data_found:
                            if exit_price and trailing_pos.entry_price and trailing_pos.quantity:
                                if trailing_pos.side == 'long':
                                    pnl = (exit_price - trailing_pos.entry_price) * trailing_pos.quantity
                                else:
                                    pnl = (trailing_pos.entry_price - exit_price) * trailing_pos.quantity
                        
                        if pnl is not None and trailing_pos.entry_price and trailing_pos.quantity:
                            pnl_pct = pnl / (trailing_pos.entry_price * trailing_pos.quantity) * 100
                        else:
                            pnl_pct = None
                        
                        from closed_position_service import create_closed_position as create_closed_position_service
                        
                        parent_oid_sched = None
                        try:
                            from sqlalchemy import or_
                            sym_v_s = [trailing_pos.symbol]
                            if trailing_pos.account_type == 'paper' and not trailing_pos.symbol.startswith('[PAPER]'):
                                sym_v_s.append(f'[PAPER]{trailing_pos.symbol}')
                            ue_s = EntrySignalRecord.query.filter(
                                or_(*[EntrySignalRecord.symbol == s for s in sym_v_s]),
                                EntrySignalRecord.account_type == trailing_pos.account_type,
                                EntrySignalRecord.side == trailing_pos.side,
                                EntrySignalRecord.closed_position_id == None
                            ).order_by(EntrySignalRecord.entry_time.asc()).first()
                            if ue_s and ue_s.entry_order_id:
                                parent_oid_sched = ue_s.entry_order_id
                        except Exception:
                            pass
                        
                        data_source = "real Tiger fill data" if real_data_found else "estimated market price"
                        closed_pos, status = create_closed_position_service(
                            symbol=trailing_pos.symbol,
                            account_type=trailing_pos.account_type,
                            side=trailing_pos.side,
                            exit_price=exit_price,
                            exit_quantity=exit_quantity,
                            exit_time=datetime.utcnow(),
                            exit_method=exit_method,
                            exit_order_id=exit_order_id,
                            trailing_stop_id=trailing_pos.id,
                            realized_pnl=pnl,
                            commission=commission,
                            avg_entry_price=trailing_pos.entry_price,
                            parent_order_id=parent_oid_sched,
                            exit_signal_content=f"Position closed (scheduler fallback, {exit_method.value}, {data_source})"
                        )
                        
                        trailing_pos.is_active = False
                        trailing_pos.is_triggered = True
                        trailing_pos.triggered_at = datetime.utcnow()
                        trailing_pos.trigger_reason = f"Position closed (scheduler fallback detected, {exit_method.value})"
                        
                        if not position_closed_via_ot:
                            _close_matching_position(
                                trailing_pos.symbol, clean_symbol, trailing_pos.account_type,
                                trailing_pos.side, trailing_stop_id=trailing_pos.id, pnl=pnl,
                                close_reason=f"scheduler fallback ({exit_method.value})",
                                exit_price=exit_price,
                                exit_quantity=exit_quantity,
                                exit_order_id=exit_order_id or (closed_pos.exit_order_id if closed_pos else None),
                                exit_method=exit_method,
                                close_source='ghost_detection'
                            )
                        
                        db.session.commit()
                        
                        logger.info(f"📊 Created ClosedPosition #{closed_pos.id} via scheduler fallback: "
                                   f"{trailing_pos.symbol} exit@${exit_price:.2f} method={exit_method.value} "
                                   f"P&L=${pnl if pnl is not None else 0:.2f} ({pnl_pct if pnl_pct is not None else 0:.2f}%), "
                                   f"data_source={'real' if real_data_found else 'estimated'}, "
                                   f"position_via={'OrderTracker' if position_closed_via_ot else '_close_matching_position'}")
                        
                    else:
                        tiger_pos = tiger_positions.get(clean_symbol, {})
                        tiger_qty = abs(tiger_pos.get('quantity', 0))
                        ts_qty = abs(trailing_pos.quantity or 0)
                        
                        if tiger_qty > 0 and ts_qty > 0 and tiger_qty != ts_qty:
                            logger.warning(f"⚠️ Qty mismatch for {clean_symbol} ({trailing_pos.account_type}): "
                                          f"DB trailing_stop={ts_qty}, Tiger={tiger_qty}")
                            
                            if tiger_qty < ts_qty:
                                partial_closed = ts_qty - tiger_qty
                                logger.info(f"📊 Partial close detected for {clean_symbol}: "
                                           f"{partial_closed} of {ts_qty} shares closed, updating TS qty to {tiger_qty}")
                                trailing_pos.quantity = tiger_qty
                                db.session.commit()
                                try:
                                    from datetime import timedelta
                                    recon_start = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                                    recon_end = datetime.now().strftime('%Y-%m-%d')
                                    reconcile_tiger_orders(app, account_type=trailing_pos.account_type,
                                                         start_date=recon_start, end_date=recon_end)
                                    logger.info(f"📊 Triggered reconciliation for {clean_symbol} partial close")
                                except Exception as recon_err:
                                    logger.error(f"Partial close reconciliation error for {clean_symbol}: {recon_err}")
                        
                        tiger_side = 'long' if tiger_pos.get('quantity', 0) > 0 else 'short'
                        if tiger_side != trailing_pos.side:
                            logger.warning(f"⚠️ Direction mismatch for {clean_symbol} ({trailing_pos.account_type}): "
                                          f"DB side={trailing_pos.side}, Tiger side={tiger_side} — position may have reversed")
                        
                except Exception as e:
                    logger.error(f"Error processing fallback for {trailing_pos.symbol}: {str(e)}")
                    db.session.rollback()
                    
    except Exception as e:
        logger.error(f"Error in detect_closed_positions_fallback: {str(e)}")


def run_trailing_stop_check_fast(app):
    """Fast trailing stop check - WebSocket prices preferred, batch API fallback for stale.
    Called every 5 seconds during market hours.
    """
    global _last_check_time
    
    try:
        with app.app_context():
            from trailing_stop_engine import process_active_positions_fast, get_trailing_stop_config, batch_refresh_stale_prices
            from models import TrailingStopPosition
            from tiger_push_client import get_push_manager
            
            config = get_trailing_stop_config()
            if not config.is_enabled:
                return
            
            active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
            if not active_positions:
                return
            
            push_manager = get_push_manager()
            all_symbols = [p.symbol.replace('[PAPER]', '').strip() for p in active_positions]
            max_age = push_manager.get_adaptive_cache_max_age()
            stale_symbols = push_manager.get_stale_symbols(all_symbols, max_age_seconds=max_age)
            
            if stale_symbols:
                for s in stale_symbols:
                    push_manager.record_symbol_api_fallback(s)
                
                try:
                    from tiger_client import get_tiger_quote_client
                    quote_client = get_tiger_quote_client()
                    batch_refresh_stale_prices(stale_symbols, quote_client)
                except Exception as e:
                    logger.warning(f"Batch price refresh failed: {e}")
            
            results = process_active_positions_fast()
            
            _last_check_time = datetime.utcnow()
            
            actions_taken = [r for r in results if r.get('action')]
            if actions_taken:
                logger.info(f"🔄 Fast check: {len(actions_taken)} actions taken")
                for r in actions_taken:
                    logger.info(f"  - {r.get('symbol')}: {r.get('action')} - {r.get('message')}")
            
            resub_symbols = push_manager.get_symbols_needing_resubscribe(threshold=36)
            if resub_symbols:
                try:
                    from tiger_push_client import subscribe_trailing_stop_symbols
                    subscribe_trailing_stop_symbols(resub_symbols)
                    logger.warning(f"🔄 Auto re-subscribed symbols with stale WebSocket data: {resub_symbols}")
                except Exception as e:
                    logger.error(f"Auto re-subscribe failed: {e}")
                
    except Exception as e:
        logger.error(f"Error in trailing stop fast check: {str(e)}")


def run_trailing_stop_check(app):
    """Full trailing stop check with API calls - position sync, order checks, etc.
    Called every 60 seconds as fallback/verification.
    """
    global _last_check_time
    
    try:
        check_pending_orders_and_create_trailing_stops(app)
        cleanup_stale_pending_orders(app)
        
        with app.app_context():
            from trailing_stop_engine import process_all_active_positions, get_trailing_stop_config, get_cached_tiger_positions
            from models import TrailingStopPosition
            
            config = get_trailing_stop_config()
            if not config.is_enabled:
                logger.debug("Trailing stop system is disabled")
                return
            
            active_count = TrailingStopPosition.query.filter_by(is_active=True).count()
            if active_count == 0:
                logger.debug("No active trailing stop positions to check")
                return
            
            get_cached_tiger_positions(force_refresh=True)
            
            logger.info(f"🔄 Full sync: Processing {active_count} active trailing stop positions (API verification)")
            
            results = process_all_active_positions()
            
            _last_check_time = datetime.utcnow()
            
            actions_taken = [r for r in results if r.get('action')]
            if actions_taken:
                logger.info(f"🔄 Full sync complete: {len(actions_taken)} actions taken")
                for r in actions_taken:
                    logger.info(f"  - {r.get('symbol')}: {r.get('action')} - {r.get('message')}")
            else:
                logger.debug(f"🔄 Full sync complete: No actions needed")
        
        check_filled_trades_without_trailing_stop(app)
        verify_exit_position_closure(app)
        detect_closed_positions_fallback(app)
        
        for acct in ['real', 'paper']:
            try:
                reconcile_tiger_orders(app, account_type=acct)
            except Exception as recon_err:
                logger.error(f"Tiger {acct} reconciliation error: {recon_err}")

        for acct in ['real', 'paper']:
            try:
                _cross_check_tiger_holdings_vs_positions(app, acct)
            except Exception as hc_err:
                logger.error(f"Tiger {acct} holdings cross-check error: {hc_err}")
        
        from order_tracker_service import poll_pending_orders
        poll_pending_orders(app)
                
    except Exception as e:
        logger.error(f"Error in trailing stop auto-check: {str(e)}")


def _get_last_paper_oca_rebuild_date(app) -> str:
    """Get last Paper OCA rebuild date from database (persistent storage)"""
    try:
        with app.app_context():
            from models import TradingConfig
            config = TradingConfig.query.filter_by(key='last_paper_oca_rebuild').first()
            if config:
                return config.value or ''
    except Exception as e:
        logger.debug(f"Could not get last rebuild date: {e}")
    return ''


def _set_last_paper_oca_rebuild_date(app, date_str: str):
    """Store last Paper OCA rebuild date to database (persistent storage)"""
    try:
        with app.app_context():
            from models import TradingConfig
            from app import db
            config = TradingConfig.query.filter_by(key='last_paper_oca_rebuild').first()
            if config:
                config.value = date_str
            else:
                config = TradingConfig(key='last_paper_oca_rebuild', value=date_str, description='Last paper OCA rebuild date')
                db.session.add(config)
            db.session.commit()
    except Exception as e:
        logger.error(f"Could not save last rebuild date: {e}")


def check_paper_bracket_day_expiry(app):
    """Check if Paper account bracket DAY sub-orders have expired (20:00 ET).
    
    Called every slow loop cycle. Once per day after 20:00 ET, clears bracket
    sub-order IDs so soft stop takes over for cross-day protection.
    No OCA rebuild needed - Paper uses bracket-only architecture.
    """
    global _last_paper_oca_rebuild_date
    import pytz
    
    try:
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)
        today = now.date()
        today_str = today.isoformat()
        
        if now.weekday() >= 5:
            return
        
        expiry_key = f"expiry_{today_str}"
        if _last_paper_oca_rebuild_date == expiry_key:
            return
        
        if now.hour >= 20:
            last_expiry_str = _get_last_paper_oca_rebuild_date(app)
            if last_expiry_str != expiry_key:
                logger.info(f"📊 Paper bracket DAY sub-orders expired at 20:00 ET, soft stop takes over")
                with app.app_context():
                    from trailing_stop_engine import clear_bracket_sub_order_ids_eod
                    bracket_result = clear_bracket_sub_order_ids_eod(app)
                    
                    try:
                        from oca_service import mark_paper_oca_expired_after_day_expiry
                        oca_count = mark_paper_oca_expired_after_day_expiry(app)
                        if oca_count > 0:
                            logger.info(f"🧹 Legacy: expired {oca_count} Paper OCA groups (no rebuild)")
                    except Exception as oca_err:
                        logger.debug(f"Legacy OCA expiry skip: {oca_err}")
                    
                    _last_paper_oca_rebuild_date = expiry_key
                    _set_last_paper_oca_rebuild_date(app, expiry_key)
                    if bracket_result.get('cleared_count', 0) > 0:
                        logger.info(f"🌙 Cleared bracket sub-order IDs for {bracket_result['cleared_count']} positions, soft stop active")
            else:
                _last_paper_oca_rebuild_date = expiry_key
                    
    except Exception as e:
        logger.error(f"❌ Paper bracket expiry check error: {str(e)}")


def gradual_paper_oca_rebuild(app):
    """Gradually rebuild Paper OCA protection, one position at a time.
    
    Called from the 60-second slow loop. Handles at most ONE position per call.
    This replaces the old bulk rebuild that created all orders at once.
    
    Only runs during trading hours (4:00 - 20:00 ET) on weekdays.
    """
    import pytz
    
    try:
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)
        
        if now.weekday() >= 5:
            return
        
        if now.hour < 4 or now.hour >= 20:
            return
        
        with app.app_context():
            from oca_service import rebuild_one_paper_oca
            result = rebuild_one_paper_oca(app)
            
            if result.get('action'):
                logger.info(f"📊 Gradual OCA rebuild: {result.get('symbol')} -> {result.get('status')} "
                           f"({result.get('pending_count', 0)} remaining)")
            elif result.get('pending_count', 0) > 0:
                logger.debug(f"📊 Gradual OCA rebuild: {result.get('pending_count')} pending, "
                            f"status={result.get('status')}")
                    
    except Exception as e:
        logger.error(f"❌ Gradual OCA rebuild error: {str(e)}")


_kline_backfill_thread = None

def run_kline_backfill(app):
    global _kline_backfill_done, _kline_backfill_thread
    if _kline_backfill_done:
        return
    if _kline_backfill_thread is not None and _kline_backfill_thread.is_alive():
        return
    
    def _do_backfill():
        global _kline_backfill_done
        try:
            with app.app_context():
                from kline_service import startup_backfill
                startup_backfill()
                _kline_backfill_done = True
                logger.info("[KlineScheduler] Startup backfill complete")
        except Exception as e:
            logger.error(f"[KlineScheduler] Startup backfill error (will retry next loop): {e}")
    
    _kline_backfill_thread = threading.Thread(target=_do_backfill, daemon=True, name="KlineBackfill")
    _kline_backfill_thread.start()
    logger.info("[KlineScheduler] Startup backfill started in background thread (non-blocking)")


def run_kline_update(app, timeframe: str):
    try:
        with app.app_context():
            from kline_service import update_all_symbols_for_timeframe
            results = update_all_symbols_for_timeframe(timeframe)
            if results.get('errors', 0) > 0:
                logger.warning(f"[KlineScheduler] {timeframe} update had {results['errors']} errors")
    except Exception as e:
        logger.error(f"[KlineScheduler] {timeframe} update error: {e}")


def run_kline_daily_cleanup(app):
    try:
        with app.app_context():
            from kline_service import cleanup_old_data
            deleted = cleanup_old_data(days=7)
            logger.info(f"[KlineScheduler] Daily cleanup: {deleted} old bars removed")
    except Exception as e:
        logger.error(f"[KlineScheduler] Daily cleanup error: {e}")


def scheduler_loop(app, interval_seconds=5):
    """Background loop with two-tier scheduling:
    - Fast loop (every 5s): WebSocket price-based trailing stop checks (no API calls)
    - Slow loop (every 60s): Full API verification, position sync, order checks
    - K-line updates: 5min/150s, 15min/600s, 1hour/1800s
    """
    global _scheduler_running
    
    FAST_INTERVAL = 5
    SLOW_INTERVAL = 60
    WATCHLIST_CLEANUP_INTERVAL = 3600
    KLINE_5MIN_INTERVAL = 150
    KLINE_15MIN_INTERVAL = 600
    KLINE_1HOUR_INTERVAL = 1800
    KLINE_DAILY_CLEANUP_INTERVAL = 86400
    ERROR_DIGEST_INTERVAL = 14400
    
    logger.info(f"📊 Trailing stop scheduler started (fast: {FAST_INTERVAL}s, full sync: {SLOW_INTERVAL}s)")
    
    first_run = True
    slow_counter = 0
    watchlist_cleanup_counter = 0
    kline_5min_counter = 0
    kline_15min_counter = 0
    kline_1hour_counter = 0
    kline_daily_counter = 0
    error_digest_counter = 0
    
    while _scheduler_running:
        try:
            run_kline_backfill(app)
            
            check_paper_bracket_day_expiry(app)
            
            if is_market_hours():
                run_trailing_stop_check_fast(app)
                
                if first_run or slow_counter >= SLOW_INTERVAL:
                    run_trailing_stop_check(app)
                    slow_counter = 0
            else:
                if first_run or slow_counter >= SLOW_INTERVAL:
                    try:
                        with app.app_context():
                            from trailing_stop_engine import sync_trailing_stop_from_holdings
                            sync_trailing_stop_from_holdings()
                    except Exception as sync_err:
                        logger.error(f"Holdings sync error outside market hours: {sync_err}")
                    try:
                        from order_tracker_service import poll_pending_orders
                        poll_pending_orders(app)
                    except Exception as poll_err:
                        logger.error(f"Off-hours order poll error: {poll_err}")
                    try:
                        detect_closed_positions_fallback(app)
                        for acct in ['real', 'paper']:
                            try:
                                reconcile_tiger_orders(app, account_type=acct)
                            except Exception as recon_err:
                                logger.error(f"Off-hours Tiger {acct} reconciliation error: {recon_err}")
                    except Exception as cleanup_err:
                        logger.error(f"Off-hours position cleanup error: {cleanup_err}")
                    slow_counter = 0
                else:
                    logger.debug("Outside market hours, skipping check")
            
            first_run = False
            
            if watchlist_cleanup_counter >= WATCHLIST_CLEANUP_INTERVAL:
                try:
                    with app.app_context():
                        from watchlist_service import cleanup_inactive_symbols
                        deactivated = cleanup_inactive_symbols(inactive_days=3)
                        if deactivated:
                            from tiger_push_client import get_push_manager
                            manager = get_push_manager()
                            manager.unsubscribe_quotes(deactivated)
                            logger.info(f"🧹 Watchlist cleanup: unsubscribed {deactivated}")
                except Exception as cleanup_err:
                    logger.debug(f"Watchlist cleanup error: {cleanup_err}")
                watchlist_cleanup_counter = 0
            
            if is_market_hours():
                if kline_5min_counter >= KLINE_5MIN_INTERVAL:
                    run_kline_update(app, '5min')
                    kline_5min_counter = 0
                elif kline_15min_counter >= KLINE_15MIN_INTERVAL:
                    run_kline_update(app, '15min')
                    kline_15min_counter = 0
                elif kline_1hour_counter >= KLINE_1HOUR_INTERVAL:
                    run_kline_update(app, '1hour')
                    kline_1hour_counter = 0
            
            if kline_daily_counter >= KLINE_DAILY_CLEANUP_INTERVAL:
                run_kline_daily_cleanup(app)
                kline_daily_counter = 0
            
            if error_digest_counter >= ERROR_DIGEST_INTERVAL:
                try:
                    with app.app_context():
                        from error_analyzer import send_error_digest
                        send_error_digest(hours=4)
                except Exception as digest_err:
                    logger.debug(f"Error digest failed: {digest_err}")
                error_digest_counter = 0
            
            with app.app_context():
                from trailing_stop_engine import get_trailing_stop_config
                config = get_trailing_stop_config()
                FAST_INTERVAL = config.check_interval_seconds or 5
            
        except Exception as e:
            logger.error(f"Scheduler loop error: {str(e)}")
        
        for _ in range(FAST_INTERVAL):
            if not _scheduler_running:
                break
            time.sleep(1)
        
        slow_counter += FAST_INTERVAL
        watchlist_cleanup_counter += FAST_INTERVAL
        kline_5min_counter += FAST_INTERVAL
        kline_15min_counter += FAST_INTERVAL
        kline_1hour_counter += FAST_INTERVAL
        kline_daily_counter += FAST_INTERVAL
        error_digest_counter += FAST_INTERVAL
    
    logger.info("📊 Trailing stop scheduler stopped")


def _close_matching_position(symbol, clean_symbol, account_type, side, trailing_stop_id=None, pnl=None, close_reason="",
                             exit_price=None, exit_quantity=None, exit_order_id=None, exit_method=None, close_source=None):
    """Helper to find and close the matching Position record via add_exit_leg.
    
    Delegates to position_service.add_exit_leg for proper accounting:
    - Weighted average exit price calculation
    - Cumulative P&L tracking
    - Proper status transition (OPEN → CLOSED when fully exited)
    - Idempotent EXIT leg creation (by tiger_order_id)
    
    Lookup priority:
    1. trailing_stop_id → Position.trailing_stop_id (most precise)
    2. symbol + account_type + side (OPEN positions)
    3. Recently CLOSED positions missing EXIT legs (race condition recovery)
    """
    from models import Position as PositionModel, PositionStatus, PositionLeg, LegType
    from position_service import add_exit_leg
    
    open_pos = None
    
    if trailing_stop_id:
        open_pos = PositionModel.query.filter_by(
            trailing_stop_id=trailing_stop_id,
            status=PositionStatus.OPEN
        ).first()
    
    if not open_pos and side:
        for sym in [symbol, clean_symbol] if symbol != clean_symbol else [symbol]:
            open_pos = PositionModel.query.filter_by(
                symbol=sym,
                account_type=account_type,
                side=side,
                status=PositionStatus.OPEN
            ).first()
            if open_pos:
                break
    
    if not open_pos and trailing_stop_id:
        closed_pos = PositionModel.query.filter_by(
            trailing_stop_id=trailing_stop_id,
            status=PositionStatus.CLOSED
        ).order_by(PositionModel.closed_at.desc()).first()
        if closed_pos:
            has_exit_leg = PositionLeg.query.filter_by(
                position_id=closed_pos.id, leg_type=LegType.EXIT
            ).first()
            if not has_exit_leg:
                open_pos = closed_pos
                logger.info(f"📦 Found recently-closed Position #{closed_pos.id} missing EXIT leg, adding it")
    
    if open_pos:
        qty = exit_quantity or open_pos.total_entry_quantity
        
        exit_leg = add_exit_leg(
            position=open_pos,
            tiger_order_id=str(exit_order_id) if exit_order_id else None,
            price=exit_price,
            quantity=qty,
            filled_at=datetime.utcnow(),
            exit_method=exit_method,
            realized_pnl=pnl,
            close_source=close_source,
        )
        
        if exit_leg:
            logger.info(f"📦 Position #{open_pos.id} ({open_pos.symbol}) closed via add_exit_leg: {close_reason}")
        elif open_pos.status == PositionStatus.OPEN:
            open_pos.status = PositionStatus.CLOSED
            open_pos.closed_at = datetime.utcnow()
            if close_source:
                open_pos.close_source = close_source
            if pnl is not None:
                open_pos.realized_pnl = pnl
            logger.info(f"📦 Position #{open_pos.id} ({open_pos.symbol}) force-closed (add_exit_leg skipped): {close_reason}")
    
    return open_pos


def cleanup_orphaned_open_positions(app):
    """One-time cleanup: find Position records stuck in OPEN state where the trailing stop
    is already triggered/deactivated. This fixes ghost positions from past bugs where the
    fallback/exit paths created ClosedPosition but didn't update Position.
    
    Also tries to populate exit data from ClosedPosition table when closing orphans.
    """
    try:
        with app.app_context():
            from app import db
            from models import (TrailingStopPosition, Position as PositionModel, PositionStatus,
                                PositionLeg, LegType, ClosedPosition)
            from sqlalchemy import func
            
            all_open = PositionModel.query.filter(
                PositionModel.status == PositionStatus.OPEN,
            ).all()
            fixed_count = 0
            
            for pos in all_open:
                ts = None
                close_reason = None
                
                if pos.trailing_stop_id:
                    ts = TrailingStopPosition.query.get(pos.trailing_stop_id)
                    if ts and not ts.is_active and (ts.is_triggered or ts.trigger_reason):
                        close_reason = (f"TS #{ts.id} inactive "
                                       f"(triggered={ts.is_triggered}, reason={ts.trigger_reason})")
                
                if not close_reason and not pos.trailing_stop_id:
                    logger.debug(f"🔧 Skipping Position #{pos.id} ({pos.symbol}): "
                                f"no trailing_stop_id link, deferring to detect_closed_positions_fallback")
                
                if close_reason:
                    pos.status = PositionStatus.CLOSED
                    pos.closed_at = (ts.triggered_at if ts and ts.triggered_at 
                                    else ts.updated_at if ts else datetime.utcnow())
                    
                    clean_symbol = pos.symbol.replace('[PAPER]', '').strip() if pos.symbol else pos.symbol
                    cp_match = None
                    if pos.trailing_stop_id:
                        cp_match = ClosedPosition.query.filter_by(
                            trailing_stop_id=pos.trailing_stop_id,
                            account_type=pos.account_type
                        ).order_by(ClosedPosition.created_at.desc()).first()
                    if not cp_match:
                        cp_match = ClosedPosition.query.filter(
                            ClosedPosition.symbol == clean_symbol,
                            ClosedPosition.account_type == pos.account_type,
                            ClosedPosition.side == pos.side,
                            ClosedPosition.exit_price != None
                        ).order_by(ClosedPosition.created_at.desc()).first()
                        if cp_match and pos.closed_at:
                            time_diff = abs((cp_match.created_at - pos.closed_at).total_seconds()) if cp_match.created_at else 999999
                            if time_diff > 3600:
                                cp_match = None
                    
                    if cp_match and cp_match.exit_price:
                        pos.avg_exit_price = cp_match.exit_price
                        pos.total_exit_quantity = cp_match.exit_quantity or pos.total_entry_quantity
                        if cp_match.total_pnl is not None:
                            pos.realized_pnl = cp_match.total_pnl
                        if cp_match.commission is not None:
                            pos.commission = cp_match.commission
                        
                        existing_exit = PositionLeg.query.filter_by(
                            position_id=pos.id, leg_type=LegType.EXIT
                        ).first()
                        if not existing_exit:
                            exit_leg = PositionLeg(
                                position_id=pos.id,
                                leg_type=LegType.EXIT,
                                tiger_order_id=cp_match.exit_order_id,
                                price=cp_match.exit_price,
                                quantity=cp_match.exit_quantity or pos.total_entry_quantity,
                                filled_at=cp_match.exit_time or pos.closed_at,
                                exit_method=cp_match.exit_method,
                                realized_pnl=cp_match.total_pnl
                            )
                            db.session.add(exit_leg)
                        logger.info(f"🔧 Fixed orphan #{pos.id} with exit data from CP #{cp_match.id}: "
                                   f"exit@${cp_match.exit_price}")
                    
                    fixed_count += 1
                    logger.info(f"🔧 Fixed orphaned Position #{pos.id} ({pos.symbol}, {pos.side}): {close_reason}")
            
            if fixed_count > 0:
                db.session.commit()
                logger.info(f"🔧 Startup cleanup: fixed {fixed_count} orphaned OPEN positions")
            else:
                logger.debug("🔧 Startup cleanup: no orphaned positions found")
    except Exception as e:
        logger.error(f"Error in cleanup_orphaned_open_positions: {e}")


def backfill_closed_position_exit_data(app):
    """One-time backfill: for CLOSED Positions missing exit data, try to populate from ClosedPosition table.
    
    Matches by:
    1. trailing_stop_id (most precise)
    2. symbol + account_type + close time proximity (within 5 minutes)
    
    Creates EXIT PositionLegs and updates Position.avg_exit_price/total_exit_quantity/realized_pnl.
    """
    try:
        with app.app_context():
            from app import db
            from sqlalchemy import func
            from models import (Position as PositionModel, PositionStatus, PositionLeg, LegType,
                                ClosedPosition, ExitMethod, CompletedTrade)
            
            positions_needing_exit = PositionModel.query.filter(
                PositionModel.status == PositionStatus.CLOSED,
                PositionModel.avg_exit_price == None
            ).all()
            
            if not positions_needing_exit:
                logger.debug("🔧 Backfill: no closed positions missing exit data")
                return
            
            logger.info(f"🔧 Backfill: found {len(positions_needing_exit)} closed positions missing exit data")
            fixed_count = 0
            
            for pos in positions_needing_exit:
                existing_exit = PositionLeg.query.filter_by(
                    position_id=pos.id, leg_type=LegType.EXIT
                ).first()
                if existing_exit:
                    if existing_exit.price:
                        pos.avg_exit_price = existing_exit.price
                        pos.total_exit_quantity = existing_exit.quantity or pos.total_entry_quantity
                        fixed_count += 1
                        logger.info(f"🔧 Backfill Position #{pos.id} ({pos.symbol}): "
                                   f"updated exit data from existing EXIT leg")
                    continue
                
                clean_symbol = pos.symbol.replace('[PAPER]', '').strip() if pos.symbol else pos.symbol
                
                cp_match = None
                if pos.trailing_stop_id:
                    cp_match = ClosedPosition.query.filter_by(
                        trailing_stop_id=pos.trailing_stop_id,
                        account_type=pos.account_type
                    ).order_by(ClosedPosition.created_at.desc()).first()
                
                if not cp_match and pos.closed_at:
                    cp_candidates = ClosedPosition.query.filter(
                        ClosedPosition.symbol == clean_symbol,
                        ClosedPosition.account_type == pos.account_type,
                        ClosedPosition.side == pos.side,
                        ClosedPosition.exit_price != None,
                        func.abs(func.extract('epoch', ClosedPosition.created_at - pos.closed_at)) < 300
                    ).order_by(
                        func.abs(func.extract('epoch', ClosedPosition.created_at - pos.closed_at))
                    ).all()
                    
                    if cp_candidates:
                        cp_match = cp_candidates[0]
                
                if not cp_match and pos.closed_at:
                    ct_match = CompletedTrade.query.filter(
                        CompletedTrade.symbol == clean_symbol,
                        CompletedTrade.account_type == pos.account_type,
                        CompletedTrade.side == pos.side,
                        CompletedTrade.exit_price != None,
                        func.abs(func.extract('epoch', CompletedTrade.created_at - pos.closed_at)) < 300
                    ).order_by(
                        func.abs(func.extract('epoch', CompletedTrade.created_at - pos.closed_at))
                    ).first()
                    if ct_match:
                        pos.avg_exit_price = ct_match.exit_price
                        pos.total_exit_quantity = ct_match.exit_quantity or pos.total_entry_quantity
                        if ct_match.pnl_amount is not None and pos.realized_pnl is None:
                            pos.realized_pnl = ct_match.pnl_amount
                        
                        exit_method_val = ct_match.exit_method
                        exit_leg = PositionLeg(
                            position_id=pos.id,
                            leg_type=LegType.EXIT,
                            price=ct_match.exit_price,
                            quantity=ct_match.exit_quantity or pos.total_entry_quantity,
                            filled_at=ct_match.exit_time or pos.closed_at,
                            exit_method=exit_method_val,
                            realized_pnl=ct_match.pnl_amount
                        )
                        db.session.add(exit_leg)
                        fixed_count += 1
                        logger.info(f"🔧 Backfill Position #{pos.id} ({pos.symbol}): "
                                   f"from CompletedTrade #{ct_match.id}, exit@${ct_match.exit_price}")
                        continue
                    continue
                
                pos.avg_exit_price = cp_match.exit_price
                pos.total_exit_quantity = cp_match.exit_quantity or pos.total_entry_quantity
                if cp_match.total_pnl is not None and pos.realized_pnl is None:
                    pos.realized_pnl = cp_match.total_pnl
                if cp_match.commission is not None and pos.commission is None:
                    pos.commission = cp_match.commission
                
                exit_method_val = cp_match.exit_method
                exit_leg = PositionLeg(
                    position_id=pos.id,
                    leg_type=LegType.EXIT,
                    tiger_order_id=cp_match.exit_order_id,
                    price=cp_match.exit_price,
                    quantity=cp_match.exit_quantity or pos.total_entry_quantity,
                    filled_at=cp_match.exit_time or pos.closed_at,
                    exit_method=exit_method_val,
                    realized_pnl=cp_match.total_pnl
                )
                db.session.add(exit_leg)
                fixed_count += 1
                logger.info(f"🔧 Backfill Position #{pos.id} ({pos.symbol}): "
                           f"from ClosedPosition #{cp_match.id}, exit@${cp_match.exit_price}, "
                           f"method={exit_method_val}, P&L={cp_match.total_pnl}")
            
            if fixed_count > 0:
                db.session.commit()
                logger.info(f"🔧 Backfill complete: fixed {fixed_count}/{len(positions_needing_exit)} positions")
            else:
                logger.info(f"🔧 Backfill: no matching exit data found for {len(positions_needing_exit)} positions")
    except Exception as e:
        logger.error(f"Error in backfill_closed_position_exit_data: {e}")
        import traceback
        logger.error(traceback.format_exc())


def start_scheduler(app, interval_seconds=5):
    """Start the background scheduler thread"""
    global _scheduler_thread, _scheduler_running
    
    if _scheduler_running:
        logger.info("Scheduler already running")
        return False
    
    cleanup_orphaned_open_positions(app)
    backfill_closed_position_exit_data(app)
    
    _scheduler_running = True
    _scheduler_thread = threading.Thread(
        target=scheduler_loop,
        args=(app, interval_seconds),
        daemon=True,
        name="TrailingStopScheduler"
    )
    _scheduler_thread.start()
    
    logger.info(f"📊 Started trailing stop scheduler thread")
    return True


def stop_scheduler():
    """Stop the background scheduler"""
    global _scheduler_running
    
    _scheduler_running = False
    logger.info("📊 Stopping trailing stop scheduler...")


_tiger_holdings_grace_tracker = {}

def _cross_check_tiger_holdings_vs_positions(app, account_type='real'):
    """Tiger holdings-vs-position cross-check: dual-confirmation safety net.
    
    Same logic as Alpaca's _cross_check_holdings_vs_positions:
    1. For each broker holding, check if DB has matching OPEN position with correct side
    2. If no position: grace period, then create external position
    3. If position exists but no active TS: create TS (dual confirmation: order + holding)
    """
    global _tiger_holdings_grace_tracker
    with app.app_context():
        from app import db
        from models import TigerHolding, Position as PositionModel, PositionStatus, TrailingStopPosition
        from trailing_stop_engine import get_trailing_stop_config, create_trailing_stop_for_trade

        ts_config = get_trailing_stop_config()
        if not ts_config.is_enabled:
            return

        holdings = TigerHolding.query.filter(
            TigerHolding.account_type == account_type,
            TigerHolding.quantity != 0,
        ).all()
        if not holdings:
            return

        actions = {'ts_created': 0, 'missing_position': 0, 'external_created': 0}

        for holding in holdings:
            try:
                symbol = holding.symbol
                broker_qty = abs(holding.quantity)
                broker_side = 'long' if holding.quantity > 0 else 'short'

                open_pos = PositionModel.query.filter_by(
                    symbol=symbol,
                    account_type=account_type,
                    side=broker_side,
                    status=PositionStatus.OPEN,
                ).first()

                if not open_pos:
                    mismatch_pos = PositionModel.query.filter_by(
                        symbol=symbol,
                        account_type=account_type,
                        status=PositionStatus.OPEN,
                    ).first()
                    if mismatch_pos and mismatch_pos.side != broker_side:
                        logger.warning(f"[{symbol}] Tiger holdings cross-check ({account_type}): "
                                      f"DB position #{mismatch_pos.id} side={mismatch_pos.side} != broker side={broker_side} — "
                                      f"position may have reversed, skipping (reconciliation will handle)")
                        continue

                if not open_pos:
                    grace_key = f"missing_{account_type}_{symbol}"
                    first_seen = _tiger_holdings_grace_tracker.get(grace_key)
                    if not first_seen:
                        _tiger_holdings_grace_tracker[grace_key] = datetime.utcnow()
                        logger.info(f"[{symbol}] Tiger holdings cross-check ({account_type}): broker has "
                                   f"{broker_side} {broker_qty} shares but no DB position. Starting grace period.")
                        actions['missing_position'] += 1
                        continue

                    elapsed = (datetime.utcnow() - first_seen).total_seconds()
                    if elapsed < 300:
                        continue

                    logger.warning(f"[{symbol}] Tiger holdings cross-check ({account_type}): grace period expired ({elapsed:.0f}s). "
                                  f"Creating external position: {broker_side} {broker_qty}@{holding.average_cost}")
                    from models import EntrySignalRecord
                    new_entry = EntrySignalRecord(
                        symbol=symbol,
                        account_type=account_type,
                        entry_time=datetime.utcnow(),
                        entry_price=holding.average_cost or 0,
                        quantity=broker_qty,
                        side=broker_side,
                        is_scaling=False,
                        entry_order_id=f"external_holdings_{symbol}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                        raw_json="External: detected from Tiger holdings sync",
                        indicator_trigger="holdings_cross_check"
                    )
                    db.session.add(new_entry)

                    from position_service import get_or_create_position
                    new_pos, is_new = get_or_create_position(
                        symbol=symbol,
                        account_type=account_type,
                        side=broker_side,
                        entry_price=holding.average_cost or 0,
                        entry_quantity=broker_qty,
                    )
                    if is_new:
                        new_pos.total_entry_quantity = broker_qty
                        new_pos.avg_entry_price = holding.average_cost or 0
                        db.session.flush()

                    if new_pos:
                        logger.info(f"[{symbol}] Tiger holdings cross-check ({account_type}): created external position #{new_pos.id} (TS auto-creation disabled)")
                        actions['external_created'] += 1

                    _tiger_holdings_grace_tracker.pop(grace_key, None)
                    continue

                active_ts = TrailingStopPosition.query.filter(
                    TrailingStopPosition.account_type == account_type,
                    TrailingStopPosition.symbol == symbol,
                    TrailingStopPosition.is_active == True,
                ).first()

                if not active_ts:
                    logger.debug(f"[{symbol}] Tiger holdings cross-check ({account_type}): position #{open_pos.id} has no active TS (TS auto-creation disabled, use signal or manual)")

                grace_key = f"missing_{account_type}_{symbol}"
                _tiger_holdings_grace_tracker.pop(grace_key, None)

            except Exception as e:
                logger.error(f"[{holding.symbol}] Tiger holdings cross-check error: {e}")
                try:
                    db.session.rollback()
                except Exception:
                    pass

        stale_keys = [k for k in _tiger_holdings_grace_tracker
                      if (datetime.utcnow() - _tiger_holdings_grace_tracker[k]).total_seconds() > 600]
        for k in stale_keys:
            _tiger_holdings_grace_tracker.pop(k, None)

        total = sum(actions.values())
        if total > 0:
            logger.info(f"📊 Tiger holdings cross-check ({account_type}): {actions}")
            db.session.commit()


def get_scheduler_status():
    """Get current scheduler status"""
    global _scheduler_running, _last_check_time
    
    return {
        'running': _scheduler_running,
        'last_check': _last_check_time.isoformat() if _last_check_time else None
    }


def reconcile_tiger_orders(app, account_type='paper', start_date=None, end_date=None):
    """
    统一订单对账函数：基于Tiger已成交订单，补全和修正所有交易记录。
    合并了原 check_filled_attached_orders 和旧 reconcile_tiger_orders 的功能。
    
    功能:
    1. 获取Tiger已成交订单（24h，权威数据源）
    2. 补全缺失的EntrySignalRecord（开仓记录）
    3. 补全缺失的ClosedPosition（平仓记录）
    4. 修正已有ClosedPosition的exit_method和total_pnl
    5. 关联未匹配的EntrySignalRecord到ClosedPosition
    6. 停用相关TrailingStopPosition（原check_filled_attached_orders的功能）
    7. 更新CompletedTrade记录（原check_filled_attached_orders的功能）
    8. 关闭Position记录
    
    Called every 60s from scheduler slow loop for both real and paper accounts.
    
    Returns:
        dict: 对账结果统计
    """
    try:
        with app.app_context():
            from app import db
            from models import (ClosedPosition, EntrySignalRecord, ExitMethod, Trade, OrderStatus,
                                CompletedTrade, TrailingStopPosition, Position as PositionModel, PositionStatus)
            from tiger_client import TigerClient, TigerPaperClient
            from datetime import datetime, timedelta
            
            if not start_date:
                start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            logger.debug(f"📊 Reconciliation for {account_type} account ({start_date} to {end_date})")
            
            if account_type == 'paper':
                client = TigerPaperClient()
            else:
                client = TigerClient()
            
            result = client.get_filled_orders(start_date=start_date, end_date=end_date, limit=500)
            
            if not result.get('success'):
                logger.debug(f"Failed to get Tiger orders for {account_type}: {result.get('error')}")
                return {'success': False, 'error': result.get('error')}
            
            orders = result.get('orders', [])
            if not orders:
                return {'success': True, 'account_type': account_type, 'stats': {'total_orders': 0}}
            
            logger.debug(f"📊 Retrieved {len(orders)} filled orders for {account_type}")
            
            stats = {
                'total_orders': len(orders),
                'entries_created': 0,
                'entries_existed': 0,
                'exits_created': 0,
                'exits_existed': 0,
                'exits_updated': 0,
                'entries_linked': 0,
                'ts_deactivated': 0,
                'completed_trades_updated': 0,
                'errors': []
            }
            
            by_symbol = {}
            for order in orders:
                symbol = order.get('symbol', '')
                if symbol not in by_symbol:
                    by_symbol[symbol] = []
                by_symbol[symbol].append(order)
            
            for symbol, symbol_orders in by_symbol.items():
                try:
                    entries = [o for o in symbol_orders if o.get('is_open') == True]
                    exits = [o for o in symbol_orders if o.get('is_open') == False]
                    
                    entries.sort(key=lambda x: x.get('order_time', 0))
                    exits.sort(key=lambda x: x.get('order_time', 0))
                    
                    for entry in entries:
                        order_id = str(entry.get('order_id', ''))
                        if not order_id:
                            continue
                        
                        existing = EntrySignalRecord.query.filter_by(entry_order_id=order_id).first()
                        if existing:
                            stats['entries_existed'] += 1
                            continue
                        
                        action = entry.get('action', '').upper()
                        side = 'long' if 'BUY' in action else 'short'
                        
                        order_time = entry.get('order_time')
                        entry_time = None
                        if order_time:
                            if isinstance(order_time, (int, float)):
                                entry_time = datetime.fromtimestamp(order_time / 1000)
                            else:
                                entry_time = order_time
                        
                        new_entry = EntrySignalRecord(
                            symbol=symbol,
                            account_type=account_type,
                            entry_time=entry_time,
                            entry_price=entry.get('avg_fill_price', 0),
                            quantity=entry.get('filled', 0),
                            side=side,
                            is_scaling=False,
                            entry_order_id=order_id,
                            raw_json=f"Tiger order: {entry.get('order_type', 'N/A')}",
                            indicator_trigger="Tiger API reconciliation"
                        )
                        db.session.add(new_entry)
                        db.session.flush()
                        stats['entries_created'] += 1
                        logger.info(f"📊 Created EntrySignalRecord for {symbol} order {order_id}")

                        try:
                            existing_ts = TrailingStopPosition.query.filter(
                                TrailingStopPosition.account_type == account_type,
                                TrailingStopPosition.is_active == True,
                                TrailingStopPosition.symbol == symbol,
                            ).first()
                            if not existing_ts:
                                from models import TigerHolding
                                broker_holding = TigerHolding.query.filter(
                                    TigerHolding.account_type == account_type,
                                    TigerHolding.symbol == symbol,
                                    TigerHolding.quantity != 0,
                                ).first()
                                open_pos = PositionModel.query.filter_by(
                                    symbol=symbol,
                                    account_type=account_type,
                                    status=PositionStatus.OPEN,
                                ).first()
                                if open_pos and broker_holding:
                                    logger.info(f"⏭️ [{symbol}] Tiger reconciliation: skipping TS creation for entry {order_id} — no SL/TP from reconciliation data")
                                elif open_pos and not broker_holding:
                                    logger.info(f"[{symbol}] Tiger reconciliation: entry order {order_id} has DB position but no broker holding — deferring TS to holdings cross-check")
                        except Exception as ts_err:
                            logger.error(f"[{symbol}] Tiger reconciliation: failed to create TS for entry {order_id}: {ts_err}")

                    for exit_order in exits:
                        order_id = str(exit_order.get('order_id', ''))
                        if not order_id:
                            continue
                        
                        action = exit_order.get('action', '').upper()
                        position_side = 'short' if 'BUY' in action else 'long'
                        
                        avg_fill_price = exit_order.get('avg_fill_price', 0)
                        filled_qty = exit_order.get('filled', 0)
                        realized_pnl = exit_order.get('realized_pnl', 0) or 0
                        commission = exit_order.get('commission', 0)
                        order_type_str = str(exit_order.get('order_type', '')).upper()
                        parent_id = exit_order.get('parent_id')
                        is_stop_order = 'STOP' in order_type_str or 'STP' in order_type_str
                        is_limit_order = 'LIMIT' in order_type_str or 'LMT' in order_type_str
                        
                        exit_time = None
                        order_time = exit_order.get('order_time')
                        if order_time:
                            if isinstance(order_time, (int, float)):
                                exit_time = datetime.fromtimestamp(order_time / 1000)
                            else:
                                exit_time = order_time
                        
                        existing = ClosedPosition.query.filter_by(exit_order_id=order_id).first()
                        
                        if existing:
                            updated = False
                            if existing.total_pnl != realized_pnl and realized_pnl != 0:
                                existing.total_pnl = realized_pnl
                                updated = True
                            if updated:
                                stats['exits_updated'] += 1
                                logger.debug(f"📊 Updated ClosedPosition #{existing.id} P&L for {symbol}")
                            else:
                                stats['exits_existed'] += 1
                            continue
                        
                        exit_method = _determine_exit_method(
                            symbol=symbol,
                            account_type=account_type,
                            position_side=position_side,
                            avg_fill_price=avg_fill_price,
                            realized_pnl=realized_pnl,
                            order_type_str=order_type_str,
                            is_stop_order=is_stop_order,
                            is_limit_order=is_limit_order,
                            parent_id=parent_id,
                        )
                        
                        current_open_pos = PositionModel.query.filter_by(
                            symbol=symbol,
                            account_type=account_type,
                            side=position_side,
                            status=PositionStatus.OPEN
                        ).order_by(PositionModel.opened_at.desc()).first()
                        
                        if current_open_pos and exit_time and current_open_pos.opened_at:
                            if exit_time < current_open_pos.opened_at:
                                logger.info(f"📊 Skipping exit order {order_id} for {symbol}: "
                                          f"exit_time={exit_time} < position opened_at={current_open_pos.opened_at} "
                                          f"(old lifecycle fill)")
                                stats['exits_existed'] += 1
                                continue
                        
                        trailing_pos = TrailingStopPosition.query.filter_by(
                            symbol=symbol,
                            account_type=account_type,
                            side=position_side,
                            is_active=True
                        ).first()
                        
                        if trailing_pos and exit_time and trailing_pos.created_at:
                            if exit_time < trailing_pos.created_at:
                                logger.info(f"📊 Skipping exit order {order_id} for {symbol}: "
                                          f"exit_time={exit_time} < TS #{trailing_pos.id} created_at={trailing_pos.created_at} "
                                          f"(old lifecycle fill, not deactivating current TS)")
                                trailing_pos = None
                        
                        trailing_stop_id = trailing_pos.id if trailing_pos else None
                        
                        from models import OrderTracker as OT, OrderRole
                        from order_tracker_service import register_order as ot_register_order, handle_fill_event as ot_handle_fill
                        
                        precise_entry_order_ids = []
                        exit_tracker = OT.query.filter_by(tiger_order_id=order_id).first()
                        position_closed_via_ot = False
                        
                        exit_method_to_role = {
                            ExitMethod.STOP_LOSS: 'stop_loss',
                            ExitMethod.TAKE_PROFIT: 'take_profit',
                            ExitMethod.TRAILING_STOP: 'exit_trailing',
                            ExitMethod.WEBHOOK_SIGNAL: 'exit_signal',
                        }
                        
                        if not exit_tracker:
                            ot_role = exit_method_to_role.get(exit_method, 'exit_signal')
                            ot_side = action if action else ('SELL' if position_side == 'long' else 'BUY')
                            exit_tracker, reg_status = ot_register_order(
                                tiger_order_id=order_id,
                                symbol=symbol,
                                account_type=account_type,
                                role=ot_role,
                                side=ot_side,
                                quantity=filled_qty,
                                order_type=order_type_str,
                                parent_order_id=str(parent_id) if parent_id else None,
                                trailing_stop_id=trailing_stop_id,
                            )
                            if exit_tracker and reg_status == 'created':
                                logger.info(f"📊 Reconciliation: registered OrderTracker for exit {order_id} ({symbol}, {ot_role})")
                        
                        if exit_tracker and exit_tracker.status != 'FILLED':
                            ot_result, ot_status = ot_handle_fill(
                                tiger_order_id=order_id,
                                filled_quantity=filled_qty,
                                avg_fill_price=avg_fill_price,
                                realized_pnl=realized_pnl if realized_pnl != 0 else None,
                                commission=commission,
                                fill_time=exit_time,
                                source='reconciliation'
                            )
                            if ot_status in ('filled', 'already_filled'):
                                position_closed_via_ot = True
                                logger.info(f"📊 Reconciliation: routed exit {order_id} through OrderTracker → Position closed via add_exit_leg")
                        elif exit_tracker and exit_tracker.status == 'FILLED':
                            position_closed_via_ot = True
                        
                        if exit_tracker:
                            if exit_tracker.trade_id:
                                sibling_entries = OT.query.filter_by(
                                    trade_id=exit_tracker.trade_id,
                                    role=OrderRole.ENTRY,
                                    account_type=account_type
                                ).all()
                                precise_entry_order_ids = [e.tiger_order_id for e in sibling_entries if e.tiger_order_id]
                            
                            if not precise_entry_order_ids and exit_tracker.trailing_stop_id:
                                sibling_entries = OT.query.filter_by(
                                    trailing_stop_id=exit_tracker.trailing_stop_id,
                                    role=OrderRole.ENTRY,
                                    account_type=account_type
                                ).all()
                                precise_entry_order_ids = [e.tiger_order_id for e in sibling_entries if e.tiger_order_id]
                            
                            if not precise_entry_order_ids and exit_tracker.parent_order_id:
                                parent_tracker = OT.query.filter_by(
                                    tiger_order_id=exit_tracker.parent_order_id
                                ).first()
                                if parent_tracker and parent_tracker.role == OrderRole.ENTRY:
                                    precise_entry_order_ids = [parent_tracker.tiger_order_id]
                                elif parent_tracker:
                                    if parent_tracker.trade_id:
                                        sibling_entries = OT.query.filter_by(
                                            trade_id=parent_tracker.trade_id,
                                            role=OrderRole.ENTRY,
                                            account_type=account_type
                                        ).all()
                                        precise_entry_order_ids = [e.tiger_order_id for e in sibling_entries if e.tiger_order_id]
                        
                        # Find matching entries: precise first, then fallback
                        from sqlalchemy import or_ as or_clause
                        symbol_vars = [symbol]
                        if account_type == 'paper' and not symbol.startswith('[PAPER]'):
                            symbol_vars.append(f'[PAPER]{symbol}')
                        elif symbol.startswith('[PAPER]'):
                            symbol_vars.append(symbol.replace('[PAPER]', '').strip())
                        
                        avg_entry_price = None
                        if precise_entry_order_ids:
                            unlinked_entries = EntrySignalRecord.query.filter(
                                EntrySignalRecord.entry_order_id.in_(precise_entry_order_ids),
                                or_clause(*[EntrySignalRecord.symbol == s for s in symbol_vars]),
                                EntrySignalRecord.account_type == account_type,
                                EntrySignalRecord.side == position_side,
                                EntrySignalRecord.closed_position_id == None
                            ).order_by(EntrySignalRecord.entry_time.asc()).all()
                            if unlinked_entries:
                                logger.info(f"📊 Precise entry match via OrderTracker: {len(unlinked_entries)} entries "
                                           f"for exit {order_id} (order IDs: {precise_entry_order_ids})")
                            else:
                                logger.debug(f"📊 OrderTracker found entry IDs {precise_entry_order_ids} "
                                            f"but no unlinked EntrySignalRecords, falling back to symbol+side")
                                unlinked_entries = EntrySignalRecord.query.filter(
                                    or_clause(*[EntrySignalRecord.symbol == s for s in symbol_vars]),
                                    EntrySignalRecord.account_type == account_type,
                                    EntrySignalRecord.side == position_side,
                                    EntrySignalRecord.closed_position_id == None
                                ).order_by(EntrySignalRecord.entry_time.asc()).all()
                        else:
                            unlinked_entries = EntrySignalRecord.query.filter(
                                or_clause(*[EntrySignalRecord.symbol == s for s in symbol_vars]),
                                EntrySignalRecord.account_type == account_type,
                                EntrySignalRecord.side == position_side,
                                EntrySignalRecord.closed_position_id == None
                            ).order_by(EntrySignalRecord.entry_time.asc()).all()
                        
                        if unlinked_entries:
                            total_cost = sum((e.entry_price or 0) * (e.quantity or 0) for e in unlinked_entries)
                            total_qty = sum(e.quantity or 0 for e in unlinked_entries)
                            if total_qty > 0:
                                avg_entry_price = total_cost / total_qty
                        
                        parent_oid = None
                        if parent_id:
                            parent_oid = str(parent_id)
                        elif precise_entry_order_ids:
                            parent_oid = precise_entry_order_ids[0]
                        elif unlinked_entries:
                            for ue in unlinked_entries:
                                if ue.entry_order_id:
                                    parent_oid = ue.entry_order_id
                                    break
                        
                        from closed_position_service import create_closed_position as create_closed_position_service
                        
                        new_closed, status = create_closed_position_service(
                            symbol=symbol,
                            account_type=account_type,
                            side=position_side,
                            exit_price=avg_fill_price,
                            exit_quantity=filled_qty,
                            exit_time=exit_time,
                            exit_method=exit_method,
                            exit_order_id=order_id,
                            parent_order_id=parent_oid,
                            realized_pnl=realized_pnl,
                            commission=commission,
                            avg_entry_price=avg_entry_price,
                            trailing_stop_id=trailing_stop_id,
                            exit_signal_content=f"Tiger order: {order_type_str}",
                            exit_indicator=f"Tiger {exit_method.value.replace('_', ' ').title()}"
                        )
                        
                        if not new_closed or status != "created":
                            if status == "already_exists":
                                stats['exits_existed'] += 1
                            continue
                        
                        stats['exits_created'] += 1
                        linked_count = EntrySignalRecord.query.filter_by(closed_position_id=new_closed.id).count()
                        stats['entries_linked'] += linked_count
                        
                        if trailing_pos:
                            trailing_pos.is_active = False
                            trailing_pos.is_triggered = True
                            trailing_pos.triggered_at = datetime.utcnow()
                            trailing_pos.trigger_reason = f"Order filled ({exit_method.value}, order {order_id})"
                            stats['ts_deactivated'] += 1
                            logger.info(f"📊 TrailingStop #{trailing_pos.id} deactivated via reconciliation")
                        
                        if not position_closed_via_ot:
                            if not current_open_pos or (exit_time and current_open_pos.opened_at and exit_time >= current_open_pos.opened_at):
                                _close_matching_position(
                                    symbol, symbol, account_type,
                                    side=position_side, trailing_stop_id=trailing_stop_id,
                                    pnl=realized_pnl,
                                    close_reason=f"reconciliation (order {order_id}, {exit_method.value})",
                                    exit_price=avg_fill_price,
                                    exit_quantity=filled_qty,
                                    exit_order_id=order_id,
                                    exit_method=exit_method,
                                    close_source='reconciliation'
                                )
                            else:
                                logger.warning(f"📊 ClosedPosition #{new_closed.id} created for {symbol} "
                                             f"but NOT closing Position #{current_open_pos.id}: "
                                             f"exit belongs to previous lifecycle")
                        
                        _update_completed_trades(
                            symbol=symbol,
                            account_type=account_type,
                            position_side=position_side,
                            avg_fill_price=avg_fill_price,
                            filled_qty=filled_qty,
                            exit_method=exit_method,
                        )
                        stats['completed_trades_updated'] += 1
                        
                        if avg_entry_price and avg_fill_price and filled_qty > 0:
                            if position_side == 'long':
                                new_closed.total_pnl_pct = (avg_fill_price - avg_entry_price) / avg_entry_price * 100
                            else:
                                new_closed.total_pnl_pct = (avg_entry_price - avg_fill_price) / avg_entry_price * 100
                        
                        logger.info(f"📊 Created ClosedPosition #{new_closed.id}: {symbol} {exit_method.value} "
                                   f"@ ${avg_fill_price:.2f}, P&L=${realized_pnl:.2f}, linked {linked_count} entries")
                    
                except Exception as symbol_err:
                    error_msg = f"Error processing {symbol}: {str(symbol_err)}"
                    logger.error(f"📊 {error_msg}")
                    stats['errors'].append(error_msg)
                    db.session.rollback()
                    continue
            
            db.session.commit()
            
            created = stats['exits_created'] + stats['entries_created']
            if created > 0:
                logger.info(f"📊 Reconciliation ({account_type}): "
                           f"entries={stats['entries_created']}, exits={stats['exits_created']}, "
                           f"updated={stats['exits_updated']}, linked={stats['entries_linked']}, "
                           f"ts_deactivated={stats['ts_deactivated']}")
            
            return {
                'success': True,
                'account_type': account_type,
                'date_range': f"{start_date} to {end_date}",
                'stats': stats
            }
            
    except Exception as e:
        logger.error(f"📊 Reconciliation failed ({account_type}): {str(e)}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}


def _determine_exit_method(symbol, account_type, position_side, avg_fill_price, realized_pnl,
                           order_type_str, is_stop_order, is_limit_order, parent_id=None):
    """Determine exit method using 4-priority chain.
    
    Priority:
    1. Compare exit price to Trade's stop_loss_price / take_profit_price (within 2%)
    2. Use realized P&L from Tiger (most accurate)
    3. Compare exit price to entry price
    4. Fallback to order type (STOP vs LIMIT)
    """
    from models import ExitMethod, Trade, OrderStatus, TrailingStopPosition
    
    parent_trade = None
    if parent_id:
        parent_trade = Trade.query.filter_by(tiger_order_id=str(parent_id)).first()
    
    if parent_trade and not getattr(parent_trade, 'is_close_position', False):
        sl = parent_trade.stop_loss_price
        tp = parent_trade.take_profit_price
        entry_price = parent_trade.filled_price or parent_trade.price or parent_trade.reference_price
        
        if sl and avg_fill_price:
            sl_diff = abs(avg_fill_price - sl) / sl if sl > 0 else float('inf')
            if sl_diff <= 0.02:
                return ExitMethod.STOP_LOSS
        
        if tp and avg_fill_price:
            tp_diff = abs(avg_fill_price - tp) / tp if tp > 0 else float('inf')
            if tp_diff <= 0.02:
                return ExitMethod.TAKE_PROFIT
        
        if entry_price and avg_fill_price:
            if position_side == 'long':
                return ExitMethod.TAKE_PROFIT if avg_fill_price > entry_price else ExitMethod.STOP_LOSS
            else:
                return ExitMethod.TAKE_PROFIT if avg_fill_price < entry_price else ExitMethod.STOP_LOSS
    
    if realized_pnl and realized_pnl != 0:
        return ExitMethod.TAKE_PROFIT if realized_pnl > 0 else ExitMethod.STOP_LOSS
    
    if is_stop_order:
        return ExitMethod.STOP_LOSS
    elif is_limit_order:
        return ExitMethod.TAKE_PROFIT
    
    return ExitMethod.MANUAL


def _update_completed_trades(symbol, account_type, position_side, avg_fill_price, filled_qty, exit_method):
    """Update CompletedTrade records when a position is closed via reconciliation."""
    from models import CompletedTrade
    
    open_completed_trades = CompletedTrade.query.filter_by(
        symbol=symbol,
        account_type=account_type,
        is_open=True
    ).order_by(CompletedTrade.entry_time.asc()).all()
    
    for ct in open_completed_trades:
        ct.is_open = False
        ct.exit_time = datetime.utcnow()
        ct.exit_price = avg_fill_price
        per_entry_qty = ct.entry_quantity or filled_qty
        ct.exit_quantity = per_entry_qty
        ct.exit_method = exit_method
        ct.remaining_quantity = 0
        ct.exited_quantity = per_entry_qty
        ct.avg_exit_price = avg_fill_price
        
        if ct.entry_price and avg_fill_price:
            if position_side == 'long':
                ct.pnl_amount = (avg_fill_price - ct.entry_price) * per_entry_qty
                ct.pnl_percent = ((avg_fill_price - ct.entry_price) / ct.entry_price) * 100
            else:
                ct.pnl_amount = (ct.entry_price - avg_fill_price) * per_entry_qty
                ct.pnl_percent = ((ct.entry_price - avg_fill_price) / ct.entry_price) * 100
        
        if ct.entry_time:
            hold_seconds = int((datetime.utcnow() - ct.entry_time).total_seconds())
            ct.hold_duration_seconds = hold_seconds
        
        logger.info(f"📊 Updated CompletedTrade #{ct.id}: {symbol} {exit_method.value} "
                   f"qty={per_entry_qty}, pnl=${ct.pnl_amount or 0:.2f}")
