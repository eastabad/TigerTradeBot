import threading
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

_scheduler_thread = None
_scheduler_running = False
_last_check_time = None


def is_market_hours() -> bool:
    try:
        import pytz
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)

        if now.weekday() >= 5:
            return False

        current_minutes = now.hour * 60 + now.minute
        return 240 <= current_minutes <= 1200
    except Exception:
        return True


def _fast_loop(app):
    global _last_check_time

    with app.app_context():
        try:
            from alpaca.trailing_stop_engine import get_trailing_stop_config
            config = get_trailing_stop_config()
            if not config.is_enabled:
                return

            from alpaca.models import AlpacaTrailingStopPosition
            active_positions = AlpacaTrailingStopPosition.query.filter_by(is_active=True).all()
            if active_positions:
                try:
                    from tiger_push_client import get_push_manager
                    from trailing_stop_engine import batch_refresh_stale_prices

                    push_manager = get_push_manager()
                    all_symbols = [p.symbol for p in active_positions]
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
                            logger.warning(f"Alpaca batch price refresh failed: {e}")
                except ImportError as e:
                    logger.debug(f"Shared market data not available: {e}")
                except Exception as e:
                    logger.debug(f"Shared market data refresh error: {e}")

            from alpaca.trailing_stop_engine import process_all_active_positions
            results = process_all_active_positions()

            _last_check_time = datetime.utcnow()

            if results['total'] > 0:
                logger.debug(f"Trailing stop check: {results}")
        except Exception as e:
            logger.error(f"Error in trailing stop fast loop: {e}")
            try:
                from alpaca.db_logger import log_error
                log_error('scheduler', f'Fast loop error: {str(e)}', category='error')
            except Exception:
                pass


def _slow_loop(app):
    with app.app_context():
        from app import db
        logger.info("Alpaca slow loop started")
        try:
            from alpaca.order_tracker import poll_all_pending_orders
            poll_results = poll_all_pending_orders()
            if poll_results.get('filled', 0) > 0:
                logger.info(f"Slow loop: polled orders, {poll_results['filled']} filled")
        except Exception as e:
            logger.error(f"Error polling pending orders: {e}")
            try:
                from alpaca.db_logger import log_error
                log_error('scheduler', f'Order polling error: {str(e)}', category='error')
            except Exception:
                pass

        try:
            from alpaca.oco_service import poll_oco_order_status
            oco_results = poll_oco_order_status()
            if oco_results.get('triggered', 0) > 0:
                logger.info(f"Slow loop: {oco_results['triggered']} OCO triggers detected")
        except Exception as e:
            logger.error(f"Error polling OCO status: {e}")
            try:
                from alpaca.db_logger import log_error
                log_error('scheduler', f'OCO polling error: {str(e)}', category='error')
            except Exception:
                pass

        try:
            from alpaca.holdings_sync import sync_holdings
            sync_result = sync_holdings()
            if sync_result.get('success') and sync_result.get('total', 0) > 0:
                logger.debug(f"Holdings synced: {sync_result.get('total')} positions")
        except Exception as e:
            logger.error(f"Error syncing holdings: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

        try:
            _cleanup_stale_oca_orders(app)
        except Exception as e:
            logger.error(f"Error cleaning up stale OCA orders: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

        try:
            _quick_fill_fetch(app)
        except Exception as e:
            logger.error(f"Error in quick fill fetch: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

        try:
            _check_filled_without_protection(app)
        except Exception as e:
            logger.error(f"Error checking fills without protection: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

        try:
            _cross_check_holdings_vs_positions(app)
        except Exception as e:
            logger.error(f"Error in holdings cross-check: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

        try:
            _verify_exit_position_closure(app)
        except Exception as e:
            logger.error(f"Error verifying exit position closure: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

        try:
            _reconcile_ghost_positions(app)
        except Exception as e:
            logger.error(f"Error reconciling ghost positions: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass


_holdings_grace_tracker = {}

def _cross_check_holdings_vs_positions(app):
    """Holdings-vs-Position cross-check: the dual-confirmation safety net.
    
    Ensures every broker holding has a matching DB position with correct side
    AND an active trailing stop. This is the entry-side counterpart to
    verify_exit_position_closure (exit-side dual confirmation).
    
    Logic:
    1. For each broker holding, check if DB has matching OPEN position with correct side
    2. If no position: try Activities reconciliation first, then external fallback after grace period
    3. If position exists but side wrong: close old position, let reconciliation create correct one
    4. If position exists but no active TS: create TS (dual confirmation: order record + broker position)
    """
    from alpaca.models import (AlpacaHolding, AlpacaPosition, AlpacaPositionStatus,
                               AlpacaTrailingStopPosition)
    from alpaca.trailing_stop_engine import get_trailing_stop_config, create_trailing_stop_for_entry
    from alpaca.position_service import link_trailing_stop_to_position

    config = get_trailing_stop_config()
    if not config.is_enabled:
        return

    holdings = AlpacaHolding.query.filter(AlpacaHolding.quantity != 0).all()
    if not holdings:
        return

    actions = {'ts_created': 0, 'side_mismatch': 0, 'missing_position': 0, 'reconciliation_triggered': 0}

    for holding in holdings:
        try:
            symbol = holding.symbol
            broker_qty = abs(holding.quantity)
            broker_side = 'long' if holding.quantity > 0 else 'short'

            open_pos = AlpacaPosition.query.filter_by(
                symbol=symbol,
                status=AlpacaPositionStatus.OPEN,
            ).first()

            if not open_pos:
                grace_key = f"missing_{symbol}"
                first_seen = _holdings_grace_tracker.get(grace_key)
                if not first_seen:
                    _holdings_grace_tracker[grace_key] = datetime.utcnow()
                    logger.info(f"[{symbol}] Holdings cross-check: broker has {broker_side} {broker_qty} shares but no DB position. "
                               f"Starting grace period, will try reconciliation next cycle.")
                    actions['missing_position'] += 1
                    continue

                elapsed = (datetime.utcnow() - first_seen).total_seconds()
                if elapsed < 300:
                    logger.debug(f"[{symbol}] Holdings cross-check: grace period {elapsed:.0f}s/300s for missing position")
                    continue

                logger.warning(f"[{symbol}] Holdings cross-check: grace period expired ({elapsed:.0f}s). "
                              f"Creating external position from holdings: {broker_side} {broker_qty}@{holding.average_cost}")
                from alpaca.position_service import get_or_create_position
                position, is_new = get_or_create_position(
                    symbol=symbol,
                    side=broker_side,
                    entry_price=holding.average_cost or 0,
                    entry_quantity=broker_qty,
                )
                if is_new:
                    from alpaca.models import AlpacaPositionLeg, AlpacaLegType
                    leg = AlpacaPositionLeg(
                        position_id=position.id,
                        leg_type=AlpacaLegType.ENTRY,
                        alpaca_order_id=f"external_holdings_{symbol}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                        price=holding.average_cost or 0,
                        quantity=broker_qty,
                        filled_at=datetime.utcnow(),
                    )
                    db.session.add(leg)
                    position.total_entry_quantity = broker_qty
                    position.avg_entry_price = holding.average_cost or 0
                    db.session.flush()

                    logger.info(f"[{symbol}] Holdings cross-check: created external position #{position.id} (TS auto-creation disabled)")

                _holdings_grace_tracker.pop(grace_key, None)
                continue

            if open_pos.side != broker_side:
                logger.warning(f"[{symbol}] Holdings cross-check: side mismatch — DB={open_pos.side}, broker={broker_side}. "
                              f"Closing DB position #{open_pos.id} to allow reconciliation.")
                from alpaca.position_service import add_exit_leg
                from alpaca.models import AlpacaExitMethod
                remaining = open_pos.total_entry_quantity - (open_pos.total_exit_quantity or 0)
                if remaining > 0:
                    close_price = holding.current_price or holding.average_cost or open_pos.avg_entry_price
                    add_exit_leg(
                        position=open_pos,
                        price=close_price,
                        quantity=remaining,
                        exit_method=AlpacaExitMethod.EXTERNAL,
                    )
                    try:
                        from alpaca.order_tracker import ensure_tracker_for_fill
                        from alpaca.models import AlpacaOrderRole
                        exit_side = 'sell' if open_pos.side == 'long' else 'buy'
                        ensure_tracker_for_fill(
                            alpaca_order_id=f"side_mismatch_{open_pos.id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                            symbol=symbol,
                            role=AlpacaOrderRole.EXIT_SIGNAL,
                            side=exit_side,
                            quantity=remaining,
                            fill_price=close_price,
                            source='holdings_side_mismatch',
                        )
                    except Exception as tracker_err:
                        logger.warning(f"[{symbol}] Side mismatch: failed to ensure exit tracker: {tracker_err}")
                db.session.flush()
                actions['side_mismatch'] += 1

                new_pos, is_new = get_or_create_position(
                    symbol=symbol,
                    side=broker_side,
                    entry_price=holding.average_cost or 0,
                    entry_quantity=broker_qty,
                )
                if is_new:
                    from alpaca.models import AlpacaPositionLeg, AlpacaLegType
                    leg = AlpacaPositionLeg(
                        position_id=new_pos.id,
                        leg_type=AlpacaLegType.ENTRY,
                        alpaca_order_id=f"reversal_holdings_{symbol}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                        price=holding.average_cost or 0,
                        quantity=broker_qty,
                        filled_at=datetime.utcnow(),
                    )
                    db.session.add(leg)
                    new_pos.total_entry_quantity = broker_qty
                    new_pos.avg_entry_price = holding.average_cost or 0
                    db.session.flush()

                logger.info(f"[{symbol}] Holdings cross-check: reversal → new position #{new_pos.id} (TS auto-creation disabled)")
                continue

            active_ts = AlpacaTrailingStopPosition.query.filter_by(
                symbol=symbol, is_active=True
            ).first()

            if not active_ts:
                logger.debug(f"[{symbol}] Holdings cross-check: position #{open_pos.id} has no active TS (TS auto-creation disabled, use signal or manual)")

            grace_key = f"missing_{symbol}"
            _holdings_grace_tracker.pop(grace_key, None)

        except Exception as e:
            logger.error(f"[{holding.symbol}] Holdings cross-check error: {e}")
            try:
                db.session.rollback()
            except Exception:
                pass

    stale_keys = [k for k in _holdings_grace_tracker
                  if (datetime.utcnow() - _holdings_grace_tracker[k]).total_seconds() > 600]
    for k in stale_keys:
        _holdings_grace_tracker.pop(k, None)

    total_actions = sum(actions.values())
    if total_actions > 0:
        logger.info(f"📊 Holdings cross-check: {actions}")
        db.session.commit()


def _verify_exit_position_closure(app):
    """Post-fill position verification: confirm that triggered Alpaca trailing stops
    actually resulted in position closure at the broker.

    Checks all TS records where is_active=False and is_triggered=True with
    triggered_at between 30 seconds and 60 minutes ago. For each, verifies
    via Alpaca API whether the position is actually gone.

    If position still exists at broker:
    - Reactivate TS with broker's remaining qty for continued protection
    - Send Discord alert

    If position is gone: confirmed closed, no action needed.

    Window extended from 10min to 60min to catch stuck partial exits from
    scaling scenarios where bracket/OCO quantity doesn't cover full position.
    """
    from datetime import timedelta
    from alpaca.models import AlpacaTrailingStopPosition, AlpacaOrderTracker, AlpacaOrderRole
    from alpaca.client import AlpacaClient
    from app import db

    now = datetime.utcnow()
    min_age = now - timedelta(seconds=30)
    max_age = now - timedelta(minutes=60)

    triggered_ts_list = AlpacaTrailingStopPosition.query.filter(
        AlpacaTrailingStopPosition.is_active == False,
        AlpacaTrailingStopPosition.is_triggered == True,
        AlpacaTrailingStopPosition.triggered_at != None,
        AlpacaTrailingStopPosition.triggered_at <= min_age,
        AlpacaTrailingStopPosition.triggered_at >= max_age,
    ).all()

    if not triggered_ts_list:
        return

    client = AlpacaClient()
    if not client.api_key:
        logger.warning("Exit closure verification: no Alpaca API key configured")
        return

    try:
        alpaca_positions = client.get_positions()
    except Exception as e:
        logger.warning(f"Exit closure verification: failed to get Alpaca positions: {e}")
        return

    broker_map = {}
    for ap in (alpaca_positions or []):
        sym = ap.get('symbol', '')
        if sym:
            broker_map[sym] = ap

    reactivated = 0
    confirmed = 0

    for ts in triggered_ts_list:
        try:
            has_pending_exit = AlpacaOrderTracker.query.filter(
                AlpacaOrderTracker.symbol == ts.symbol,
                AlpacaOrderTracker.role.in_([
                    AlpacaOrderRole.EXIT_TRAILING,
                    AlpacaOrderRole.EXIT_SIGNAL,
                ]),
                AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING',
                                                'HELD', 'PARTIALLY_FILLED']),
            ).first()
            if has_pending_exit:
                logger.debug(f"[{ts.symbol}] Skip closure verification: "
                            f"exit order {has_pending_exit.alpaca_order_id} still pending")
                continue

            if ts.symbol not in broker_map:
                confirmed += 1
                logger.debug(f"✅ [{ts.symbol}] TS #{ts.id} closure confirmed: "
                            f"position no longer exists at Alpaca")
                continue

            broker_pos = broker_map[ts.symbol]
            broker_qty = abs(float(broker_pos.get('qty', 0)))

            if broker_qty <= 0.001:
                confirmed += 1
                continue

            MAX_EXIT_RETRIES = 5
            retry_count = ts.trigger_retry_count or 0
            if retry_count >= MAX_EXIT_RETRIES:
                logger.warning(f"⚠️ [{ts.symbol}] TS #{ts.id} position still open (qty={broker_qty}) "
                              f"but max retries ({MAX_EXIT_RETRIES}) exhausted, skipping reactivation")
                try:
                    from alpaca.discord_notifier import alpaca_discord
                    alpaca_discord.send_trailing_stop_notification(
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
            logger.warning(f"🔄 [{ts.symbol}] Alpaca TS #{ts.id} reactivated: position still open "
                          f"(qty={broker_qty}), retry {retry_count + 1}/{MAX_EXIT_RETRIES}")

            try:
                from alpaca.discord_notifier import alpaca_discord
                alpaca_discord.send_trailing_stop_notification(
                    ts.symbol, 'reactivate', 0, ts.entry_price, 0,
                    f"Position still open after exit fill (qty={broker_qty}). "
                    f"TS #{ts.id} reactivated, retry {retry_count + 1}/{MAX_EXIT_RETRIES}"
                )
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Error verifying Alpaca TS #{ts.id} ({ts.symbol}): {e}")

    if reactivated > 0 or confirmed > 0:
        db.session.commit()
        logger.info(f"📊 Alpaca exit closure verification: {confirmed} confirmed closed, "
                   f"{reactivated} reactivated (position still open)")


def _cleanup_stale_oca_orders(app):
    """Clean up overnight PENDING OCA protection orders (STOP_LOSS/TAKE_PROFIT)
    whose trailing stop is no longer active or position is closed.
    Verifies order status with Alpaca API before cancelling."""
    from datetime import timedelta
    from alpaca.models import (AlpacaOrderTracker, AlpacaOrderRole,
                               AlpacaTrailingStopPosition, AlpacaPosition, AlpacaPositionStatus)
    from app import db

    overnight_cutoff = datetime.utcnow() - timedelta(hours=12)
    stale_oca = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD']),
        AlpacaOrderTracker.role.in_([AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT]),
        AlpacaOrderTracker.created_at < overnight_cutoff,
    ).all()

    if not stale_oca:
        return

    cleaned = 0
    for oca_order in stale_oca:
        locally_stale = False
        reason = ''

        if oca_order.trailing_stop_id:
            ts = AlpacaTrailingStopPosition.query.get(oca_order.trailing_stop_id)
            if ts and not ts.is_active:
                locally_stale = True
                reason = f'linked TS#{ts.id} inactive (triggered={ts.is_triggered})'
            elif not ts:
                locally_stale = True
                reason = f'linked TS#{oca_order.trailing_stop_id} not found'
        else:
            pos = AlpacaPosition.query.filter(
                AlpacaPosition.symbol == oca_order.symbol,
                AlpacaPosition.status == AlpacaPositionStatus.OPEN,
            ).first()
            if not pos:
                locally_stale = True
                reason = 'no OPEN position for symbol'

        if not locally_stale:
            continue

        if oca_order.alpaca_order_id:
            try:
                from alpaca.order_tracker import poll_order_status
                result = poll_order_status(oca_order.alpaca_order_id)
                if result:
                    broker_status = result.get('status', '').upper()
                    if broker_status in ['FILLED', 'CANCELED', 'EXPIRED', 'REJECTED', 'REPLACED']:
                        oca_order.status = broker_status if broker_status != 'REJECTED' else 'CANCELLED'
                        cleaned += 1
                        reason += f', broker confirmed {broker_status}'
                        logger.info(f"🧹 Synced stale OCA order: {oca_order.symbol} "
                                    f"{oca_order.role.value} → {broker_status} ({reason})")
                    elif broker_status in ['NEW', 'ACCEPTED', 'HELD', 'PENDING']:
                        logger.info(f"🧹 OCA order {oca_order.alpaca_order_id[:12]}... ({oca_order.symbol} "
                                    f"{oca_order.role.value}) locally stale ({reason}) but "
                                    f"still {broker_status} at broker - skipping")
                    else:
                        oca_order.status = 'CANCELLED'
                        cleaned += 1
                        reason += f', unknown broker_status={broker_status}'
                        logger.info(f"🧹 Auto-cancelled stale OCA order: {oca_order.symbol} "
                                    f"{oca_order.role.value} ({reason})")
                else:
                    oca_order.status = 'CANCELLED'
                    cleaned += 1
                    reason += ', order not found at broker'
                    logger.info(f"🧹 Auto-cancelled stale OCA order: {oca_order.symbol} "
                                f"{oca_order.role.value} ({reason})")
            except Exception as e:
                logger.warning(f"🧹 Error checking OCA order {oca_order.alpaca_order_id[:12]}...: {e}")
                continue
        else:
            oca_order.status = 'CANCELLED'
            cleaned += 1
            reason += ', no alpaca_order_id'
            logger.info(f"🧹 Auto-cancelled stale OCA order: {oca_order.symbol} "
                        f"{oca_order.role.value} ({reason})")

    if cleaned > 0:
        db.session.commit()
        logger.info(f"🧹 Cleaned up {cleaned} stale overnight OCA protection orders")

    stale_cutoff_3d = datetime.utcnow() - timedelta(days=3)
    very_old = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD']),
        AlpacaOrderTracker.created_at < stale_cutoff_3d,
    ).all()
    if very_old:
        for old_order in very_old:
            old_order.status = 'CANCELLED'
            logger.info(f"🧹 Auto-expired very old PENDING order: {old_order.symbol} "
                        f"{old_order.role.value} (created {old_order.created_at})")
        db.session.commit()
        logger.info(f"🧹 Expired {len(very_old)} orders older than 3 days")


def _quick_fill_fetch(app):
    """Lightweight fill fetch that runs every slow loop (before ghost recon).
    Only fetches new fills without running full reconciliation.
    This ensures ghost recon has up-to-date fill data to link exits properly."""
    with app.app_context():
        try:
            from alpaca.reconciliation import fetch_and_store_filled_orders
            total_fetched, new_stored = fetch_and_store_filled_orders()
            if new_stored > 0:
                logger.info(f"Quick fill fetch: {new_stored} new fills stored (total={total_fetched})")
        except Exception as e:
            logger.error(f"Quick fill fetch error: {e}")
            try:
                from app import db
                db.session.rollback()
            except Exception:
                pass


def _activities_reconciliation(app):
    with app.app_context():
        logger.info("Activities reconciliation started (periodic)")
        try:
            from alpaca.reconciliation import fetch_and_store_filled_orders, reconcile_today
            total_fetched, new_stored = fetch_and_store_filled_orders()
            logger.info(f"Activities fetch: {total_fetched} fetched, {new_stored} new fills stored")

            if new_stored > 0:
                run = reconcile_today(run_type='scheduled')
                logger.info(f"Activities reconciliation: status={run.status}, "
                           f"matched={run.positions_matched}, corrected={run.records_corrected}, "
                           f"created={run.records_created}")

                try:
                    from alpaca.db_logger import log_info
                    log_info('scheduler', f'Activities recon: {new_stored} new fills, '
                            f'matched={run.positions_matched}, corrected={run.records_corrected}',
                            category='reconciliation')
                except Exception:
                    pass
            else:
                logger.debug("Activities reconciliation: no new fills, skipping reconcile")
        except Exception as e:
            logger.error(f"Activities reconciliation error: {e}", exc_info=True)
            try:
                from app import db
                db.session.rollback()
            except Exception:
                pass


_ghost_recon_failures = {}

def _reconcile_ghost_positions(app):
    """Ghost reconciliation: ONLY closes DB OPEN positions that no longer exist at Alpaca.
    Does NOT trigger activities reconciliation or handle qty mismatches.
    Qty/direction mismatches are handled by the periodic activities reconciliation
    and the daily EOD alignment.
    """
    from datetime import timedelta
    from alpaca.models import (AlpacaPosition, AlpacaPositionStatus, AlpacaExitMethod,
                               AlpacaFilledOrder)
    from alpaca.position_service import find_all_open_positions, add_exit_leg
    from alpaca.client import AlpacaClient

    logger.info("Ghost position reconciliation started")
    try:
        open_positions = find_all_open_positions()
        if not open_positions:
            logger.info("Ghost reconciliation: no open positions in DB")
            return

        logger.info(f"Ghost reconciliation: {len(open_positions)} open positions in DB: {[p.symbol for p in open_positions]}")
        client = AlpacaClient()
        if not client.api_key:
            logger.warning("Ghost reconciliation: no Alpaca API key configured")
            return

        try:
            alpaca_positions = client.get_positions()
        except Exception as e:
            logger.warning(f"Ghost reconciliation: failed to get Alpaca positions: {e}")
            return

        alpaca_symbols = set()
        for ap in (alpaca_positions or []):
            sym = ap.get('symbol', '')
            if sym:
                alpaca_symbols.add(sym)

        logger.info(f"Ghost reconciliation: Alpaca has {len(alpaca_symbols)} positions: {alpaca_symbols}")

        ghost_count = 0
        error_count = 0
        from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole, AlpacaTrailingStopPosition
        from app import db

        for pos in open_positions:
            remaining = pos.total_entry_quantity - (pos.total_exit_quantity or 0)
            if remaining <= 0.001:
                continue

            if pos.symbol in alpaca_symbols:
                if pos.id in _ghost_recon_failures:
                    del _ghost_recon_failures[pos.id]
                continue

            pos_fail_count = _ghost_recon_failures.get(pos.id, 0)
            if pos_fail_count >= 5:
                if pos_fail_count == 5:
                    logger.error(f"Ghost reconciliation: {pos.symbol} #{pos.id} failed {pos_fail_count} times, "
                                f"stopping retries. Manual intervention needed.")
                    _ghost_recon_failures[pos.id] = pos_fail_count + 1
                continue

            has_pending_orders = AlpacaOrderTracker.query.filter(
                AlpacaOrderTracker.symbol == pos.symbol,
                AlpacaOrderTracker.role.in_([
                    AlpacaOrderRole.EXIT_SIGNAL,
                    AlpacaOrderRole.EXIT_TRAILING,
                    AlpacaOrderRole.ENTRY,
                ]),
                AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD', 'PARTIALLY_FILLED']),
            ).first()
            if has_pending_orders:
                logger.info(f"Ghost reconciliation: {pos.symbol} #{pos.id} has pending {has_pending_orders.role}, skip")
                continue

            recent_cutoff = datetime.utcnow() - timedelta(hours=6)
            recent_unreconciled = AlpacaFilledOrder.query.filter(
                AlpacaFilledOrder.symbol == pos.symbol,
                AlpacaFilledOrder.reconciled == False,
                AlpacaFilledOrder.filled_at >= recent_cutoff.isoformat(),
            ).count()
            if recent_unreconciled > 0 and pos_fail_count < 2:
                logger.info(f"Ghost reconciliation: {pos.symbol} #{pos.id} has {recent_unreconciled} "
                           f"unreconciled fills, deferring to activities reconciliation "
                           f"(attempt #{pos_fail_count + 1})")
                _ghost_recon_failures[pos.id] = pos_fail_count + 1
                continue

            try:
                exit_fill = _find_exit_fill_for_ghost(pos)

                if exit_fill:
                    last_price = exit_fill.filled_avg_price or pos.avg_entry_price or 0
                    exit_order_id = exit_fill.alpaca_order_id
                    fill_time = None
                    if exit_fill.filled_at:
                        try:
                            from alpaca.reconciliation import _parse_timestamp
                            fill_time = _parse_timestamp(exit_fill.filled_at)
                        except Exception:
                            pass
                    exit_fill.reconciled = True
                    exit_fill.reconciled_at = datetime.utcnow()
                else:
                    last_price = pos.avg_entry_price or 0
                    exit_order_id = None
                    fill_time = None
                    filled_exits = AlpacaOrderTracker.query.filter(
                        AlpacaOrderTracker.symbol == pos.symbol,
                        AlpacaOrderTracker.role.in_([
                            AlpacaOrderRole.EXIT_SIGNAL,
                            AlpacaOrderRole.EXIT_TRAILING,
                            AlpacaOrderRole.STOP_LOSS,
                            AlpacaOrderRole.TAKE_PROFIT,
                        ]),
                        AlpacaOrderTracker.status == 'FILLED',
                    ).order_by(AlpacaOrderTracker.fill_time.desc()).first()
                    if filled_exits and filled_exits.avg_fill_price:
                        last_price = filled_exits.avg_fill_price
                        exit_order_id = filled_exits.alpaca_order_id

                logger.warning(f"Ghost reconciliation: GHOST DETECTED {pos.symbol} #{pos.id}, "
                              f"remaining={remaining}, closing @ ${last_price:.2f} "
                              f"(order_id={exit_order_id}, attempt #{pos_fail_count + 1})")
                add_exit_leg(
                    position=pos,
                    alpaca_order_id=exit_order_id,
                    price=last_price,
                    quantity=remaining,
                    filled_at=fill_time or datetime.utcnow(),
                    exit_method=AlpacaExitMethod.EXTERNAL,
                )

                try:
                    from alpaca.order_tracker import ensure_tracker_for_fill
                    exit_side = 'sell' if pos.side == 'long' else 'buy'
                    ensure_tracker_for_fill(
                        alpaca_order_id=exit_order_id,
                        symbol=pos.symbol,
                        role=AlpacaOrderRole.EXIT_SIGNAL,
                        side=exit_side,
                        quantity=remaining,
                        fill_price=last_price,
                        fill_time=fill_time or datetime.utcnow(),
                        source='ghost_reconciliation',
                    )
                except Exception as tracker_err:
                    logger.warning(f"[{pos.symbol}] Ghost recon: failed to ensure exit tracker: {tracker_err}")

                active_ts = AlpacaTrailingStopPosition.query.filter_by(
                    symbol=pos.symbol, is_active=True
                ).first()
                if active_ts:
                    active_ts.is_active = False
                    active_ts.is_triggered = True
                    active_ts.triggered_at = datetime.utcnow()
                    active_ts.trigger_reason = f"Ghost reconciliation: position closed externally"

                remaining_unreconciled = AlpacaFilledOrder.query.filter(
                    AlpacaFilledOrder.symbol == pos.symbol,
                    AlpacaFilledOrder.reconciled == False,
                ).all()
                exit_sides = ['sell', 'sell_short'] if pos.side == 'long' else ['buy', 'buy_to_cover']
                for uf in remaining_unreconciled:
                    fill_side = (uf.side or '').lower()
                    if fill_side in exit_sides:
                        uf.reconciled = True
                        uf.reconciled_at = datetime.utcnow()
                if remaining_unreconciled:
                    marked = sum(1 for uf in remaining_unreconciled if uf.reconciled)
                    logger.info(f"Ghost recon: marked {marked} exit-direction unreconciled fills "
                               f"as reconciled for {pos.symbol}")

                db.session.commit()
                ghost_count += 1
                logger.warning(f"Ghost position closed: {pos.symbol} #{pos.id}, {remaining} shares @ ${last_price:.2f}")

                if pos.id in _ghost_recon_failures:
                    del _ghost_recon_failures[pos.id]

                try:
                    from alpaca.db_logger import log_warning
                    log_warning('scheduler', f'Ghost position closed: {pos.symbol} #{pos.id}',
                                category='ghost_recon', symbol=pos.symbol,
                                extra_data={'remaining': remaining, 'price': last_price})
                except Exception:
                    pass

            except Exception as e:
                error_count += 1
                _ghost_recon_failures[pos.id] = pos_fail_count + 1
                logger.error(f"Ghost reconciliation: FAILED to close {pos.symbol} #{pos.id} "
                            f"(attempt #{pos_fail_count + 1}): {e}", exc_info=True)
                try:
                    db.session.rollback()
                except Exception:
                    pass

        logger.info(f"Ghost reconciliation completed: {ghost_count} ghosts closed, {error_count} errors")

    except Exception as e:
        logger.error(f"Ghost position reconciliation error: {e}", exc_info=True)
        try:
            db.session.rollback()
        except Exception:
            pass


def _find_exit_fill_for_ghost(pos):
    """Find the most likely unreconciled fill that caused this position to disappear from Alpaca.
    Looks for exit-direction fills (sell for long, buy for short) matching the symbol
    within the last 24 hours.
    """
    from alpaca.models import AlpacaFilledOrder
    from datetime import timedelta

    if pos.side == 'long':
        exit_sides = ['sell', 'sell_short']
    else:
        exit_sides = ['buy', 'buy_to_cover']

    cutoff = datetime.utcnow() - timedelta(hours=24)

    best_fill = AlpacaFilledOrder.query.filter(
        AlpacaFilledOrder.symbol == pos.symbol,
        AlpacaFilledOrder.reconciled == False,
        AlpacaFilledOrder.side.in_(exit_sides),
        AlpacaFilledOrder.filled_at >= cutoff.isoformat(),
    ).order_by(AlpacaFilledOrder.filled_at.desc()).first()

    return best_fill


def _check_filled_without_protection(app):
    from alpaca.models import AlpacaTrade, AlpacaOrderStatus, AlpacaTrailingStopPosition, AlpacaOCOGroup, AlpacaOCOStatus
    from alpaca.trailing_stop_engine import get_trailing_stop_config, create_trailing_stop_for_entry
    from alpaca.oco_service import create_oco_for_entry
    from datetime import timedelta

    config = get_trailing_stop_config()

    cutoff = datetime.utcnow() - timedelta(hours=24)
    filled_trades = AlpacaTrade.query.filter(
        AlpacaTrade.status == AlpacaOrderStatus.FILLED,
        AlpacaTrade.created_at >= cutoff,
        AlpacaTrade.is_close_position == False,
    ).all()

    for trade in filled_trades:
        try:
            symbol = trade.symbol

            existing_ts = AlpacaTrailingStopPosition.query.filter_by(
                symbol=symbol, is_active=True
            ).first()

            any_ts_for_trade = AlpacaTrailingStopPosition.query.filter_by(
                trade_id=trade.id
            ).first()

            any_triggered = AlpacaTrailingStopPosition.query.filter(
                AlpacaTrailingStopPosition.symbol == symbol,
                AlpacaTrailingStopPosition.is_active == False,
                AlpacaTrailingStopPosition.is_triggered == True,
            ).order_by(AlpacaTrailingStopPosition.updated_at.desc()).first()

            from alpaca.models import AlpacaPosition, AlpacaPositionStatus

            if any_triggered:
                is_ghost = any_triggered.trigger_reason and 'ghost prevention' in any_triggered.trigger_reason
                if not is_ghost:
                    open_pos = AlpacaPosition.query.filter(
                        AlpacaPosition.symbol == symbol,
                        AlpacaPosition.status == AlpacaPositionStatus.OPEN,
                    ).first()
                    if open_pos:
                        triggered_time = any_triggered.triggered_at or any_triggered.created_at
                        pos_created = open_pos.created_at
                        if pos_created and triggered_time and pos_created > triggered_time:
                            logger.info(f"[{symbol}] Old triggered TS#{any_triggered.id} "
                                        f"(triggered {triggered_time}) belongs to previous lifecycle, "
                                        f"current OPEN position created {pos_created} — allowing re-creation")
                        else:
                            logger.debug(f"⏭️ Skipping {symbol}: trailing stop was triggered in current lifecycle")
                            continue
                    else:
                        logger.debug(f"⏭️ Skipping {symbol}: trailing stop was previously triggered, no OPEN position")
                        continue
                else:
                    logger.info(f"[{symbol}] Previous ghost prevention detected, checking if position still exists for re-creation")
            closed_position = AlpacaPosition.query.filter(
                AlpacaPosition.symbol == symbol,
                AlpacaPosition.status == AlpacaPositionStatus.CLOSED,
            ).order_by(AlpacaPosition.closed_at.desc()).first()

            if closed_position and closed_position.closed_at and \
               closed_position.closed_at >= trade.created_at:
                logger.debug(f"⏭️ Skipping {symbol}: position closed after trade creation")
                continue

            existing_oco = AlpacaOCOGroup.query.filter_by(
                symbol=symbol, status=AlpacaOCOStatus.ACTIVE
            ).first()

            entry_price = trade.filled_price or trade.price
            if not entry_price or entry_price <= 0:
                continue

            side = 'long' if trade.side.value == 'buy' else 'short'
            quantity = trade.filled_quantity or trade.quantity

            ghost_recovery = any_triggered and any_triggered.trigger_reason and 'ghost prevention' in any_triggered.trigger_reason
            old_ts_inactive = any_ts_for_trade and not any_ts_for_trade.is_active
            should_create = not existing_ts and config.is_enabled and (not any_ts_for_trade or ghost_recovery or old_ts_inactive)
            if should_create:
                from alpaca.trailing_stop_engine import was_manually_deactivated_alpaca
                if was_manually_deactivated_alpaca(symbol):
                    logger.warning(f"⛔ [{symbol}] Skipping auto TS creation (_check_filled_without_protection): manually deactivated")
                    should_create = False
            if should_create and (ghost_recovery or old_ts_inactive):
                try:
                    from alpaca.client import AlpacaClient
                    _client = AlpacaClient()
                    _alpaca_pos = _client.get_position(symbol)
                    if _alpaca_pos and _alpaca_pos.get('_no_position'):
                        logger.warning(f"[{symbol}] Skipping trailing stop re-creation: "
                                      f"no position at Alpaca (old_ts_inactive={old_ts_inactive}, ghost_recovery={ghost_recovery})")
                        should_create = False
                    elif _alpaca_pos is None:
                        logger.warning(f"[{symbol}] Alpaca API error checking position, allowing TS re-creation as safety fallback")
                    else:
                        logger.info(f"[{symbol}] Alpaca position confirmed ({_alpaca_pos.get('qty')} shares), "
                                   f"proceeding with trailing stop re-creation")
                except Exception as e:
                    logger.warning(f"[{symbol}] Failed to verify Alpaca position, allowing TS re-creation as fallback: {e}")
            if should_create:
                if not trade.stop_loss_price and not trade.take_profit_price:
                    logger.info(f"⏭️ [{symbol}] Skipping TrailingStop creation (_check_filled_without_protection): no SL/TP in entry signal")
                    should_create = False
            if should_create:
                ts_pos = create_trailing_stop_for_entry(
                    symbol=symbol,
                    side=side,
                    entry_price=entry_price,
                    quantity=quantity,
                    stop_loss_price=trade.stop_loss_price,
                    take_profit_price=trade.take_profit_price,
                    trade_id=trade.id,
                    timeframe=trade.signal_timeframe,
                )
                if ts_pos:
                    logger.info(f"Created missing trailing stop for {symbol}")
                    from app import db
                    db.session.flush()

            has_bracket_legs = not trade.needs_auto_protection
            if not existing_oco and trade.stop_loss_price and trade.take_profit_price and not has_bracket_legs:
                ts_id = None
                if existing_ts:
                    ts_id = existing_ts.id
                elif AlpacaTrailingStopPosition.query.filter_by(symbol=symbol, is_active=True).first():
                    ts_id = AlpacaTrailingStopPosition.query.filter_by(symbol=symbol, is_active=True).first().id

                oco, status = create_oco_for_entry(
                    symbol=symbol,
                    quantity=quantity,
                    entry_price=entry_price,
                    stop_loss_price=trade.stop_loss_price,
                    take_profit_price=trade.take_profit_price,
                    trade_id=trade.id,
                    trailing_stop_id=ts_id,
                    side=side,
                )
                if oco:
                    logger.info(f"Created missing OCO for {symbol}: {status}")

        except Exception as e:
            logger.error(f"Error checking protection for trade #{trade.id}: {e}")

    _check_open_positions_without_protection(app, config)


def _find_sl_tp_for_position(pos):
    """Find original SL/TP from entry signal for a position.
    Only checks signal-origin data (entry leg or Trade record), NOT broker order prices.
    Returns (stop_loss_price, take_profit_price, source_description).
    """
    from alpaca.models import (AlpacaPositionLeg, AlpacaLegType, AlpacaTrade,
                               AlpacaSide)
    from datetime import timedelta

    sl, tp, source = None, None, None

    entry_legs = AlpacaPositionLeg.query.filter(
        AlpacaPositionLeg.position_id == pos.id,
        AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD]),
    ).order_by(AlpacaPositionLeg.filled_at.desc()).all()

    for leg in entry_legs:
        if leg.stop_price and leg.take_profit_price:
            sl, tp = leg.stop_price, leg.take_profit_price
            source = f"entry_leg #{leg.id}"
            break
        if leg.stop_price and not sl:
            sl = leg.stop_price
            source = f"entry_leg #{leg.id} (SL only)"
        if leg.take_profit_price and not tp:
            tp = leg.take_profit_price
            if source and 'SL only' in source:
                source = f"entry_legs (SL+TP from different legs)"

    if sl and tp:
        if _validate_sl_tp(pos.side, pos.avg_entry_price, sl, tp):
            logger.info(f"[{pos.symbol}] Recovered SL/TP from {source}: SL={sl}, TP={tp}")
            return sl, tp, source
        else:
            logger.warning(f"[{pos.symbol}] SL/TP from {source} failed validation (SL={sl}, TP={tp}, entry={pos.avg_entry_price}, side={pos.side})")
            sl, tp, source = None, None, None

    if not (sl and tp):
        time_window = timedelta(hours=2)
        opened_at = pos.opened_at or pos.created_at
        trade_side = AlpacaSide.BUY if pos.side == 'long' else AlpacaSide.SELL

        trade = AlpacaTrade.query.filter(
            AlpacaTrade.symbol == pos.symbol,
            AlpacaTrade.side == trade_side,
            AlpacaTrade.stop_loss_price != None,
            AlpacaTrade.take_profit_price != None,
        ).filter(
            AlpacaTrade.created_at >= opened_at - time_window,
            AlpacaTrade.created_at <= opened_at + time_window,
        ).order_by(AlpacaTrade.created_at.desc()).first()

        if trade and trade.stop_loss_price and trade.take_profit_price:
            if _validate_sl_tp(pos.side, pos.avg_entry_price, trade.stop_loss_price, trade.take_profit_price):
                sl, tp = trade.stop_loss_price, trade.take_profit_price
                source = f"alpaca_trade #{trade.id} (signal)"
                logger.info(f"[{pos.symbol}] Recovered SL/TP from {source}: SL={sl}, TP={tp}")
                return sl, tp, source
            else:
                logger.warning(f"[{pos.symbol}] SL/TP from trade #{trade.id} failed validation")

    if sl or tp:
        logger.info(f"[{pos.symbol}] Partial SL/TP recovered: SL={sl}, TP={tp}, source={source}")
    else:
        logger.info(f"[{pos.symbol}] No SL/TP in entry signal for position #{pos.id}, skipping TS creation")
    return sl, tp, source


def _validate_sl_tp(side, entry_price, sl, tp):
    if not entry_price or not sl or not tp:
        return False
    if side == 'long':
        return sl < entry_price < tp
    else:
        return tp < entry_price < sl


def _check_open_positions_without_protection(app, config):
    """Fallback: create trailing stops for OPEN positions that have no active
    trailing stop protection — either trailing_stop_id is NULL, or it points
    to an inactive/dead TS record."""
    from alpaca.models import (AlpacaPosition, AlpacaPositionStatus,
                               AlpacaTrailingStopPosition)
    from alpaca.trailing_stop_engine import create_trailing_stop_for_entry
    from alpaca.position_service import link_trailing_stop_to_position

    if not config.is_enabled:
        return

    no_ts_positions = AlpacaPosition.query.filter(
        AlpacaPosition.status == AlpacaPositionStatus.OPEN,
        AlpacaPosition.trailing_stop_id == None,
    ).all()

    has_ts_positions = AlpacaPosition.query.filter(
        AlpacaPosition.status == AlpacaPositionStatus.OPEN,
        AlpacaPosition.trailing_stop_id != None,
    ).all()

    dead_ts_positions = []
    for pos in has_ts_positions:
        linked_ts = AlpacaTrailingStopPosition.query.get(pos.trailing_stop_id)
        if not linked_ts or not linked_ts.is_active:
            active_ts = AlpacaTrailingStopPosition.query.filter_by(
                symbol=pos.symbol, is_active=True
            ).first()
            if active_ts:
                link_trailing_stop_to_position(pos, active_ts.id)
                logger.info(f"[{pos.symbol}] Re-linked position #{pos.id} to active TS #{active_ts.id} (old TS was dead)")
            else:
                dead_ts_positions.append(pos)
                logger.info(f"[{pos.symbol}] Position #{pos.id} has dead TS #{pos.trailing_stop_id}, needs re-creation")

    open_positions = no_ts_positions + dead_ts_positions

    for pos in open_positions:
        try:
            existing_ts = AlpacaTrailingStopPosition.query.filter_by(
                symbol=pos.symbol, is_active=True
            ).first()

            if existing_ts:
                link_trailing_stop_to_position(pos, existing_ts.id)
                logger.info(f"[{pos.symbol}] Linked existing active TS #{existing_ts.id} to position #{pos.id}")
                continue

            remaining = pos.total_entry_quantity - (pos.total_exit_quantity or 0)
            if remaining <= 0:
                continue

            entry_price = pos.avg_entry_price
            if not entry_price or entry_price <= 0:
                continue

            any_triggered = AlpacaTrailingStopPosition.query.filter(
                AlpacaTrailingStopPosition.symbol == pos.symbol,
                AlpacaTrailingStopPosition.is_active == False,
                AlpacaTrailingStopPosition.is_triggered == True,
            ).order_by(AlpacaTrailingStopPosition.triggered_at.desc()).first()

            if any_triggered:
                is_ghost = any_triggered.trigger_reason and 'ghost prevention' in any_triggered.trigger_reason
                if not is_ghost:
                    triggered_time = any_triggered.triggered_at or any_triggered.created_at
                    pos_created = pos.created_at
                    if pos_created and triggered_time and pos_created > triggered_time:
                        logger.info(f"[{pos.symbol}] Old triggered TS belongs to previous lifecycle, allowing re-creation for position #{pos.id}")
                    else:
                        logger.debug(f"⏭️ Skipping {pos.symbol} position #{pos.id}: trailing stop was triggered in current lifecycle")
                        continue

            from alpaca.trailing_stop_engine import was_manually_deactivated_alpaca
            if was_manually_deactivated_alpaca(pos.symbol):
                logger.warning(f"⛔ [{pos.symbol}] Skipping auto TS creation (_check_open_positions): manually deactivated")
                continue

            sl, tp, sl_tp_source = _find_sl_tp_for_position(pos)

            if not sl and not tp:
                logger.info(f"⏭️ [{pos.symbol}] Skipping TrailingStop creation (_check_open_positions): no SL/TP found for position #{pos.id}")
                continue

            ts_pos = create_trailing_stop_for_entry(
                symbol=pos.symbol,
                side=pos.side,
                entry_price=entry_price,
                quantity=remaining,
                stop_loss_price=sl,
                take_profit_price=tp,
            )
            if ts_pos:
                link_trailing_stop_to_position(pos, ts_pos.id)
                sl_tp_info = f", SL={sl}, TP={tp} from {sl_tp_source}" if sl_tp_source else ", default params"
                logger.info(f"[{pos.symbol}] Created trailing stop #{ts_pos.id} for reconciled position #{pos.id} "
                           f"(entry={entry_price}, qty={remaining}, side={pos.side}{sl_tp_info})")
                from app import db
                db.session.flush()

        except Exception as e:
            logger.error(f"Error creating protection for position #{pos.id} ({pos.symbol}): {e}")


def _process_ws_events(app):
    with app.app_context():
        try:
            from alpaca.trade_stream import get_trade_stream
            from alpaca.order_tracker import apply_trade_update

            stream = get_trade_stream()
            events = stream.get_pending_events(max_events=50)

            if not events:
                return 0

            from app import db as app_db

            processed = 0
            errors = 0
            for event in events:
                try:
                    result = apply_trade_update(event)
                    processed += 1
                    if result.get('action', '').endswith('_processed') and not result.get('skipped'):
                        logger.info(f"WS event applied: {result['event']} {result.get('symbol', '')} -> {result['action']}")
                except Exception as e:
                    errors += 1
                    logger.error(f"Error processing WS event {event.get('event', '?')} for {event.get('symbol', '?')}: {e}")
                    try:
                        app_db.session.rollback()
                    except Exception:
                        pass
                    try:
                        from alpaca.db_logger import log_error
                        log_error('ws_event', f"Error processing {event.get('event', '?')}: {str(e)}", category='error')
                    except Exception:
                        pass

            if processed > 0:
                logger.info(f"Processed {processed} WS events ({errors} errors)")
            return processed
        except Exception as e:
            logger.error(f"Error in WS event processing: {e}")
            return 0


def _get_slow_loop_interval() -> int:
    try:
        from alpaca.trade_stream import get_trade_stream
        stream = get_trade_stream()
        if stream.is_connected:
            return 300
    except Exception:
        pass
    return 60


def _scheduler_worker(app):
    global _scheduler_running
    logger.info("Alpaca trailing stop scheduler started")

    fast_interval = 5
    last_slow_check = 0
    last_activities_recon = 0
    _first_slow_loop_done = False
    ACTIVITIES_RECON_INTERVAL = 600

    while _scheduler_running:
        try:
            _process_ws_events(app)

            now = time.time()
            slow_interval = _get_slow_loop_interval()
            elapsed = now - last_slow_check
            if not _first_slow_loop_done or elapsed >= slow_interval:
                logger.info(f"Slow loop running: first={not _first_slow_loop_done} elapsed={elapsed:.0f}s interval={slow_interval}s")
                _slow_loop(app)
                last_slow_check = now
                _first_slow_loop_done = True
                logger.info("Slow loop completed")

            activities_elapsed = now - last_activities_recon
            if is_market_hours() and activities_elapsed >= ACTIVITIES_RECON_INTERVAL:
                try:
                    _activities_reconciliation(app)
                except Exception as e:
                    logger.error(f"Error in activities reconciliation: {e}")
                last_activities_recon = now

            if not is_market_hours():
                time.sleep(30)
                continue

            _fast_loop(app)

            time.sleep(fast_interval)
        except Exception as e:
            logger.error(f"Scheduler worker error: {e}")
            try:
                from alpaca.db_logger import log_error
                log_error('scheduler', f'Scheduler error: {str(e)}', category='error')
            except Exception:
                pass
            time.sleep(10)

    logger.info("Alpaca trailing stop scheduler stopped")


def start_scheduler(app):
    global _scheduler_thread, _scheduler_running

    if _scheduler_running:
        logger.info("Alpaca scheduler already running")
        return

    try:
        from alpaca.trade_stream import start_trade_stream
        start_trade_stream()
        logger.info("Alpaca trade stream started")
    except Exception as e:
        logger.error(f"Failed to start Alpaca trade stream: {e}")

    _scheduler_running = True
    _scheduler_thread = threading.Thread(
        target=_scheduler_worker,
        args=(app,),
        daemon=True,
        name='alpaca-trailing-stop-scheduler'
    )
    _scheduler_thread.start()
    logger.info("Alpaca trailing stop scheduler thread started")


def stop_scheduler():
    global _scheduler_running
    _scheduler_running = False

    try:
        from alpaca.trade_stream import stop_trade_stream
        stop_trade_stream()
    except Exception as e:
        logger.error(f"Error stopping trade stream: {e}")

    logger.info("Alpaca trailing stop scheduler stopping...")


def get_scheduler_status() -> dict:
    ws_status = {}
    try:
        from alpaca.trade_stream import get_trade_stream_status
        ws_status = get_trade_stream_status()
    except Exception:
        pass

    return {
        'running': _scheduler_running,
        'last_check': _last_check_time,
        'market_hours': is_market_hours(),
        'thread_alive': _scheduler_thread.is_alive() if _scheduler_thread else False,
        'trade_stream': ws_status,
    }
