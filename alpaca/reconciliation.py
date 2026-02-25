import logging
import json
import threading
from datetime import datetime, date, timedelta
from typing import Optional, Tuple, List, Dict
from sqlalchemy.exc import IntegrityError
from app import db
from alpaca.models import (
    AlpacaFilledOrder, AlpacaReconciliationRun,
    AlpacaPosition, AlpacaPositionLeg, AlpacaPositionStatus,
    AlpacaLegType, AlpacaExitMethod, AlpacaOrderTracker, AlpacaOrderRole,
    AlpacaTrade
)

logger = logging.getLogger(__name__)

_reconciliation_lock = threading.Lock()


def _extract_signal_from_order(alpaca_order_id: str) -> dict:
    from alpaca.signal_utils import parse_signal_fields
    result = {
        'signal_content': None, 'signal_grade': None, 'signal_score': None,
        'signal_timeframe': None, 'signal_indicator': None,
        'stop_price': None, 'take_profit_price': None,
    }
    try:
        tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=alpaca_order_id).first()
        if not tracker or not tracker.trade_id:
            return result
        trade = AlpacaTrade.query.get(tracker.trade_id)
        if not trade or not trade.signal_data:
            return result

        parsed = parse_signal_fields(trade.signal_data)
        result.update(parsed)
        result['stop_price'] = trade.stop_loss_price
        result['take_profit_price'] = trade.take_profit_price
    except Exception as e:
        logger.debug(f"Signal extraction failed for order {alpaca_order_id}: {e}")
    return result


def fetch_and_store_filled_orders(after: str = None, until: str = None,
                                   page_size: int = 100) -> Tuple[int, int]:
    from alpaca.client import AlpacaClient

    try:
        client = AlpacaClient()
        if not client.api_key:
            logger.warning("Alpaca API keys not configured, skipping fetch")
            return 0, 0

        all_activities = []
        page_token = None
        max_pages = 20

        for page_num in range(max_pages):
            params = {'activity_type': 'FILL', 'page_size': page_size, 'direction': 'desc'}
            if after:
                params['after'] = after
            if until:
                params['until'] = until
            if page_token:
                params['page_token'] = page_token

            activities = client._request('GET', '/v2/account/activities/FILL', params=params)

            if not isinstance(activities, list):
                break

            all_activities.extend(activities)

            if len(activities) < page_size:
                break

            if activities:
                page_token = activities[-1].get('id')
            else:
                break

        total_fetched = len(all_activities)
        new_stored = 0

        for activity in all_activities:
            order_id = activity.get('order_id', '')
            if not order_id:
                continue

            existing = AlpacaFilledOrder.query.filter_by(alpaca_order_id=order_id).first()

            if existing:
                existing.filled_qty = float(activity.get('qty', 0) or 0)
                existing.filled_avg_price = float(activity.get('price', 0) or 0)
                existing.status = 'filled'
                continue

            try:
                filled_order = AlpacaFilledOrder(
                    alpaca_order_id=order_id,
                    client_order_id=activity.get('client_order_id', ''),
                    symbol=activity.get('symbol', ''),
                    side=activity.get('side', ''),
                    quantity=float(activity.get('qty', 0) or 0),
                    filled_qty=float(activity.get('qty', 0) or 0),
                    filled_avg_price=float(activity.get('price', 0) or 0),
                    order_type=activity.get('type', ''),
                    order_class=activity.get('order_class', ''),
                    time_in_force='',
                    extended_hours=False,
                    status='filled',
                    submitted_at=activity.get('transaction_time', ''),
                    filled_at=activity.get('transaction_time', ''),
                    raw_json=json.dumps(activity, default=str),
                    reconciled=False,
                )
                db.session.add(filled_order)
                db.session.flush()
                new_stored += 1
            except IntegrityError:
                db.session.rollback()
                logger.debug(f"Duplicate fill {order_id} skipped")
                continue

        db.session.commit()
        logger.info(f"Alpaca fills fetch: fetched={total_fetched}, new={new_stored}")
        return total_fetched, new_stored

    except Exception as e:
        logger.error(f"Error fetching Alpaca fills: {str(e)}")
        db.session.rollback()
        return 0, 0


def reconcile_today(run_type: str = 'manual') -> AlpacaReconciliationRun:
    return reconcile_date(target_date=date.today(), run_type=run_type)


def reconcile_date(target_date: date = None, run_type: str = 'manual') -> AlpacaReconciliationRun:
    if not _reconciliation_lock.acquire(blocking=False):
        logger.warning("Reconciliation already in progress")
        run = AlpacaReconciliationRun(
            run_date=target_date or date.today(),
            run_type=run_type,
            status='skipped',
            details='Another reconciliation is already running',
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
        )
        db.session.add(run)
        db.session.commit()
        return run

    try:
        recon_date = target_date or date.today()

        run = AlpacaReconciliationRun(
            run_date=recon_date,
            run_type=run_type,
            status='running',
            started_at=datetime.utcnow(),
        )
        db.session.add(run)
        db.session.commit()

        details = []

        today_str = recon_date.isoformat()
        next_day_str = (recon_date + timedelta(days=1)).isoformat()
        total_fetched, new_stored = fetch_and_store_filled_orders(
            after=today_str + 'T00:00:00Z',
            until=next_day_str + 'T00:00:00Z',
        )
        run.total_activities_fetched = total_fetched
        run.new_fills_stored = new_stored
        details.append(f"Fetched {total_fetched} activities, {new_stored} new fills stored")

        day_fills = AlpacaFilledOrder.query.filter(
            AlpacaFilledOrder.filled_at >= today_str,
            AlpacaFilledOrder.filled_at < next_day_str,
            AlpacaFilledOrder.reconciled == False,
        ).order_by(AlpacaFilledOrder.filled_at.asc()).all()

        if not day_fills:
            day_fills = AlpacaFilledOrder.query.filter(
                AlpacaFilledOrder.reconciled == False,
            ).order_by(AlpacaFilledOrder.filled_at.asc()).all()

        if not day_fills:
            details.append("No unreconciled fills found")
            run.status = 'completed'
            run.details = '\n'.join(details)
            run.finished_at = datetime.utcnow()
            db.session.commit()
            return run

        symbols = set(f.symbol for f in day_fills)
        details.append(f"Found {len(day_fills)} unreconciled fills for {len(symbols)} symbols: {', '.join(sorted(symbols))}")

        positions_matched = 0
        records_corrected = 0
        records_created = 0

        for symbol in sorted(symbols):
            symbol_fills = [f for f in day_fills if f.symbol == symbol]
            result = _reconcile_symbol_fills(symbol, symbol_fills)
            positions_matched += result['matched']
            records_corrected += result['corrected']
            records_created += result['created']
            if result.get('detail'):
                details.append(result['detail'])

        run.positions_matched = positions_matched
        run.records_corrected = records_corrected
        run.records_created = records_created
        run.status = 'completed'
        run.details = '\n'.join(details)
        run.finished_at = datetime.utcnow()
        db.session.commit()

        logger.info(f"Reconciliation completed for {recon_date}: "
                    f"matched={positions_matched}, corrected={records_corrected}, created={records_created}")
        return run

    except Exception as e:
        logger.error(f"Reconciliation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        try:
            run.status = 'failed'
            run.error_message = str(e)
            run.finished_at = datetime.utcnow()
            db.session.commit()
        except Exception:
            db.session.rollback()
        return run

    finally:
        _reconciliation_lock.release()


def reconcile_history(days_back: int = 30) -> AlpacaReconciliationRun:
    if not _reconciliation_lock.acquire(blocking=False):
        logger.warning("Reconciliation already in progress")
        run = AlpacaReconciliationRun(
            run_date=date.today(),
            run_type='full_history',
            status='skipped',
            details='Another reconciliation is already running',
            started_at=datetime.utcnow(),
            finished_at=datetime.utcnow(),
        )
        db.session.add(run)
        db.session.commit()
        return run

    try:
        run = AlpacaReconciliationRun(
            run_date=date.today(),
            run_type='full_history',
            status='running',
            started_at=datetime.utcnow(),
        )
        db.session.add(run)
        db.session.commit()

        details = []

        start_date = (date.today() - timedelta(days=days_back)).isoformat()
        end_date = (date.today() + timedelta(days=1)).isoformat()

        total_fetched, new_stored = fetch_and_store_filled_orders(
            after=start_date + 'T00:00:00Z',
            until=end_date + 'T00:00:00Z',
        )
        run.total_activities_fetched = total_fetched
        run.new_fills_stored = new_stored
        details.append(f"Fetched {total_fetched} activities ({days_back} days), {new_stored} new fills")

        AlpacaFilledOrder.query.filter(
            AlpacaFilledOrder.reconciled == True
        ).update({'reconciled': False, 'reconciled_at': None})
        db.session.commit()
        details.append("Reset all reconciliation flags")

        all_fills = AlpacaFilledOrder.query.order_by(
            AlpacaFilledOrder.filled_at.asc()
        ).all()

        if not all_fills:
            details.append("No fills found in history")
            run.status = 'completed'
            run.details = '\n'.join(details)
            run.finished_at = datetime.utcnow()
            db.session.commit()
            return run

        symbols = set(f.symbol for f in all_fills)
        details.append(f"Processing {len(all_fills)} fills for {len(symbols)} symbols")

        total_matched = 0
        total_corrected = 0
        total_created = 0

        for symbol in sorted(symbols):
            symbol_fills = [f for f in all_fills if f.symbol == symbol]
            result = _reconcile_symbol_fills(symbol, symbol_fills)
            total_matched += result['matched']
            total_corrected += result['corrected']
            total_created += result['created']
            if result.get('detail'):
                details.append(f"  {result['detail']}")

        run.positions_matched = total_matched
        run.records_corrected = total_corrected
        run.records_created = total_created
        run.status = 'completed'
        run.details = '\n'.join(details)
        run.finished_at = datetime.utcnow()
        db.session.commit()

        logger.info(f"Full history reconciliation: matched={total_matched}, "
                    f"corrected={total_corrected}, created={total_created}")
        return run

    except Exception as e:
        logger.error(f"History reconciliation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        try:
            run.status = 'failed'
            run.error_message = str(e)
            run.finished_at = datetime.utcnow()
            db.session.commit()
        except Exception:
            db.session.rollback()
        return run

    finally:
        _reconciliation_lock.release()


def _reconcile_symbol_fills(symbol: str, fills: List[AlpacaFilledOrder]) -> Dict:
    result = {'matched': 0, 'corrected': 0, 'created': 0, 'detail': ''}

    try:
        for fill in fills:
            existing_leg = AlpacaPositionLeg.query.filter_by(
                alpaca_order_id=fill.alpaca_order_id
            ).first()

            if existing_leg:
                if existing_leg.price != fill.filled_avg_price and fill.filled_avg_price:
                    existing_leg.price = fill.filled_avg_price
                    result['corrected'] += 1
                fill.reconciled = True
                fill.reconciled_at = datetime.utcnow()
                result['matched'] += 1
                continue

            side = (fill.side or '').lower()

            open_position = AlpacaPosition.query.filter_by(
                symbol=symbol,
                status=AlpacaPositionStatus.OPEN,
            ).order_by(AlpacaPosition.opened_at.asc(), AlpacaPosition.id.asc()).first()

            is_entry = False
            if open_position is None:
                is_entry = True
            elif open_position.side == 'long' and side == 'buy':
                is_entry = True
            elif open_position.side == 'short' and side == 'sell':
                is_entry = True

            if is_entry and open_position is None:
                tracker = AlpacaOrderTracker.query.filter_by(
                    alpaca_order_id=fill.alpaca_order_id
                ).first()
                if tracker and tracker.role in (
                    AlpacaOrderRole.EXIT_SIGNAL, AlpacaOrderRole.EXIT_TRAILING,
                    AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT,
                ):
                    fill.reconciled = True
                    fill.reconciled_at = datetime.utcnow()
                    result['matched'] += 1
                    logger.info(f"Reconcile: {symbol} fill {fill.alpaca_order_id[:12]}... "
                               f"is exit order (role={tracker.role.value}), no OPEN position, skipping phantom creation")
                    continue

                if _fill_belongs_to_closed_position(symbol, fill):
                    fill.reconciled = True
                    fill.reconciled_at = datetime.utcnow()
                    result['matched'] += 1
                    logger.info(f"Reconcile: {symbol} fill {fill.alpaca_order_id[:12]}... "
                               f"linked to recently closed position, skipping phantom creation")
                    continue

            if is_entry:
                from alpaca.position_service import get_or_create_position, add_entry_leg

                position_side = 'long' if side == 'buy' else 'short'
                fill_time = _parse_timestamp(fill.filled_at)

                position, is_new = get_or_create_position(
                    symbol=symbol,
                    side=position_side,
                    entry_price=fill.filled_avg_price or 0,
                    entry_quantity=fill.filled_qty or fill.quantity or 0,
                    filled_at=fill_time,
                )

                sig_data = _extract_signal_from_order(fill.alpaca_order_id)

                add_entry_leg(
                    position=position,
                    alpaca_order_id=fill.alpaca_order_id,
                    price=fill.filled_avg_price,
                    quantity=fill.filled_qty or fill.quantity,
                    filled_at=fill_time,
                    signal_content=sig_data['signal_content'],
                    signal_grade=sig_data['signal_grade'],
                    signal_score=sig_data['signal_score'],
                    signal_indicator=sig_data['signal_indicator'],
                    signal_timeframe=sig_data['signal_timeframe'],
                    stop_price=sig_data['stop_price'],
                    take_profit_price=sig_data['take_profit_price'],
                )

                try:
                    from alpaca.order_tracker import ensure_tracker_for_fill
                    entry_role = AlpacaOrderRole.ADD if not is_new else AlpacaOrderRole.ENTRY
                    ensure_tracker_for_fill(
                        alpaca_order_id=fill.alpaca_order_id,
                        symbol=symbol,
                        role=entry_role,
                        side=side,
                        quantity=fill.filled_qty or fill.quantity,
                        fill_price=fill.filled_avg_price,
                        fill_time=fill_time,
                        source='activities_reconciliation',
                    )
                except Exception as tracker_err:
                    logger.warning(f"[{symbol}] Failed to ensure entry tracker: {tracker_err}")

                if is_new:
                    try:
                        from alpaca.models import AlpacaHolding
                        broker_holding = AlpacaHolding.query.filter(
                            AlpacaHolding.symbol == symbol,
                            AlpacaHolding.quantity != 0,
                        ).first()
                        if broker_holding:
                            from alpaca.trailing_stop_engine import create_trailing_stop_for_entry
                            from alpaca.position_service import link_trailing_stop_to_position
                            ts = create_trailing_stop_for_entry(
                                symbol=symbol,
                                side=position_side,
                                entry_price=fill.filled_avg_price or 0,
                                quantity=fill.filled_qty or fill.quantity or 0,
                                stop_loss_price=sig_data.get('stop_price'),
                                take_profit_price=sig_data.get('take_profit_price'),
                                from_reconciliation=True,
                            )
                            if ts:
                                link_trailing_stop_to_position(position, ts.id)
                                logger.info(f"[{symbol}] Reconciliation: created TS #{ts.id} for new position #{position.id} (dual confirmed: fill + broker holding)")
                        else:
                            logger.info(f"[{symbol}] Reconciliation: created position #{position.id} but no broker holding found yet — deferring TS to holdings cross-check")
                    except Exception as ts_err:
                        logger.error(f"[{symbol}] Reconciliation: failed to create TS for position #{position.id}: {ts_err}")

                fill.reconciled = True
                fill.reconciled_at = datetime.utcnow()
                result['created'] += 1
                result['matched'] += 1

            else:
                remaining = open_position.total_entry_quantity - (open_position.total_exit_quantity or 0)
                fill_qty = fill.filled_qty or fill.quantity or 0
                exit_qty = min(fill_qty, remaining)

                if exit_qty <= 0.001:
                    fill.reconciled = True
                    fill.reconciled_at = datetime.utcnow()
                    result['matched'] += 1
                    continue

                from alpaca.position_service import add_exit_leg

                fill_time = _parse_timestamp(fill.filled_at)
                exit_method = _determine_exit_method(fill)

                add_exit_leg(
                    position=open_position,
                    alpaca_order_id=fill.alpaca_order_id,
                    price=fill.filled_avg_price,
                    quantity=exit_qty,
                    filled_at=fill_time,
                    exit_method=exit_method,
                )

                try:
                    from alpaca.order_tracker import ensure_tracker_for_fill
                    exit_side = 'sell' if open_position.side == 'long' else 'buy'
                    exit_role = _exit_method_to_role(exit_method)
                    ensure_tracker_for_fill(
                        alpaca_order_id=fill.alpaca_order_id,
                        symbol=symbol,
                        role=exit_role,
                        side=exit_side,
                        quantity=exit_qty,
                        fill_price=fill.filled_avg_price,
                        fill_time=fill_time,
                        source='activities_reconciliation',
                    )
                except Exception as tracker_err:
                    logger.warning(f"[{symbol}] Failed to ensure exit tracker: {tracker_err}")

                overflow_qty = fill_qty - exit_qty
                if overflow_qty > 0.001:
                    reverse_side = 'short' if side == 'sell' else 'long'
                    trade_date = (fill_time or datetime.utcnow()).date() if fill_time else date.today()
                    seq = _next_seq(symbol, trade_date)
                    position_key = f"{symbol}_{trade_date.isoformat()}_{seq}"

                    new_pos = AlpacaPosition(
                        position_key=position_key,
                        symbol=symbol,
                        trade_date=trade_date,
                        sequence_number=seq,
                        side=reverse_side,
                        status=AlpacaPositionStatus.OPEN,
                        total_entry_quantity=overflow_qty,
                        total_exit_quantity=0,
                        avg_entry_price=fill.filled_avg_price or 0,
                        opened_at=fill_time or datetime.utcnow(),
                    )
                    db.session.add(new_pos)
                    db.session.flush()

                    overflow_leg = AlpacaPositionLeg(
                        position_id=new_pos.id,
                        leg_type=AlpacaLegType.ENTRY,
                        alpaca_order_id=fill.alpaca_order_id + '_overflow',
                        price=fill.filled_avg_price,
                        quantity=overflow_qty,
                        filled_at=fill_time,
                    )
                    db.session.add(overflow_leg)
                    result['created'] += 1
                    logger.info(f"Reversal overflow: {overflow_qty} shares created new {reverse_side} "
                               f"position {position_key}")

                    try:
                        from alpaca.models import AlpacaHolding
                        broker_holding = AlpacaHolding.query.filter(
                            AlpacaHolding.symbol == symbol,
                            AlpacaHolding.quantity != 0,
                        ).first()
                        if broker_holding:
                            from alpaca.trailing_stop_engine import create_trailing_stop_for_entry
                            from alpaca.position_service import link_trailing_stop_to_position
                            ts = create_trailing_stop_for_entry(
                                symbol=symbol,
                                side=reverse_side,
                                entry_price=fill.filled_avg_price or 0,
                                quantity=overflow_qty,
                                from_reconciliation=True,
                            )
                            if ts:
                                link_trailing_stop_to_position(new_pos, ts.id)
                                logger.info(f"[{symbol}] Reconciliation: created TS #{ts.id} for reversal position #{new_pos.id} (dual confirmed)")
                        else:
                            logger.info(f"[{symbol}] Reconciliation: created reversal position #{new_pos.id} but no broker holding — deferring TS")
                    except Exception as ts_err:
                        logger.error(f"[{symbol}] Reconciliation: failed to create TS for reversal position #{new_pos.id}: {ts_err}")

                fill.reconciled = True
                fill.reconciled_at = datetime.utcnow()
                result['created'] += 1
                result['matched'] += 1

            db.session.flush()

        total_fills = len(fills)
        total_qty = sum(f.filled_qty or f.quantity or 0 for f in fills)
        result['detail'] = (f"{symbol}: {total_fills} fills ({total_qty:.0f} shares) -> "
                           f"matched={result['matched']}, corrected={result['corrected']}, created={result['created']}")

        return result

    except Exception as e:
        logger.error(f"Error reconciling {symbol}: {str(e)}")
        import traceback
        traceback.print_exc()
        db.session.rollback()
        return result


def _fill_belongs_to_closed_position(symbol: str, fill: AlpacaFilledOrder) -> bool:
    """Check if an unmatched fill belongs to a recently closed position.
    This prevents phantom position creation from exit fills that were already
    handled by ghost reconciliation or other mechanisms.
    
    Returns True if the fill should be marked reconciled without creating a new position.
    """
    from datetime import timedelta

    recently_closed = AlpacaPosition.query.filter(
        AlpacaPosition.symbol == symbol,
        AlpacaPosition.status == AlpacaPositionStatus.CLOSED,
        AlpacaPosition.closed_at >= datetime.utcnow() - timedelta(hours=24),
    ).order_by(AlpacaPosition.closed_at.desc()).all()

    if not recently_closed:
        return False

    fill_side = (fill.side or '').lower()

    for closed_pos in recently_closed:
        if closed_pos.side == 'long' and fill_side in ('sell', 'sell_short'):
            exit_legs_without_order = AlpacaPositionLeg.query.filter(
                AlpacaPositionLeg.position_id == closed_pos.id,
                AlpacaPositionLeg.leg_type == AlpacaLegType.EXIT,
                AlpacaPositionLeg.alpaca_order_id == None,
            ).all()
            if exit_legs_without_order:
                best_leg = exit_legs_without_order[0]
                best_leg.alpaca_order_id = fill.alpaca_order_id
                if fill.filled_avg_price:
                    best_leg.price = fill.filled_avg_price
                logger.info(f"Reconcile: linked fill {fill.alpaca_order_id[:12]}... to "
                           f"exit leg #{best_leg.id} of closed position #{closed_pos.id}")
                return True

            logger.info(f"Reconcile: fill {fill.alpaca_order_id[:12]}... matches closed {symbol} "
                       f"#{closed_pos.id} direction (sell vs long), marking reconciled")
            return True

        elif closed_pos.side == 'short' and fill_side in ('buy', 'buy_to_cover'):
            exit_legs_without_order = AlpacaPositionLeg.query.filter(
                AlpacaPositionLeg.position_id == closed_pos.id,
                AlpacaPositionLeg.leg_type == AlpacaLegType.EXIT,
                AlpacaPositionLeg.alpaca_order_id == None,
            ).all()
            if exit_legs_without_order:
                best_leg = exit_legs_without_order[0]
                best_leg.alpaca_order_id = fill.alpaca_order_id
                if fill.filled_avg_price:
                    best_leg.price = fill.filled_avg_price
                logger.info(f"Reconcile: linked fill {fill.alpaca_order_id[:12]}... to "
                           f"exit leg #{best_leg.id} of closed position #{closed_pos.id}")
                return True

            logger.info(f"Reconcile: fill {fill.alpaca_order_id[:12]}... matches closed {symbol} "
                       f"#{closed_pos.id} direction (buy vs short), marking reconciled")
            return True

        if closed_pos.side == 'long' and fill_side == 'buy':
            entry_legs_without_order = AlpacaPositionLeg.query.filter(
                AlpacaPositionLeg.position_id == closed_pos.id,
                AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD]),
                AlpacaPositionLeg.alpaca_order_id == None,
            ).all()
            if entry_legs_without_order:
                best_leg = entry_legs_without_order[0]
                best_leg.alpaca_order_id = fill.alpaca_order_id
                if fill.filled_avg_price:
                    best_leg.price = fill.filled_avg_price
                logger.info(f"Reconcile: linked fill {fill.alpaca_order_id[:12]}... to "
                           f"entry leg #{best_leg.id} of closed position #{closed_pos.id}")
                return True

        elif closed_pos.side == 'short' and fill_side in ('sell', 'sell_short'):
            entry_legs_without_order = AlpacaPositionLeg.query.filter(
                AlpacaPositionLeg.position_id == closed_pos.id,
                AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD]),
                AlpacaPositionLeg.alpaca_order_id == None,
            ).all()
            if entry_legs_without_order:
                best_leg = entry_legs_without_order[0]
                best_leg.alpaca_order_id = fill.alpaca_order_id
                if fill.filled_avg_price:
                    best_leg.price = fill.filled_avg_price
                logger.info(f"Reconcile: linked fill {fill.alpaca_order_id[:12]}... to "
                           f"entry leg #{best_leg.id} of closed position #{closed_pos.id}")
                return True

    return False


def _exit_method_to_role(exit_method) -> AlpacaOrderRole:
    from alpaca.models import AlpacaExitMethod
    method_to_role = {
        AlpacaExitMethod.WEBHOOK_SIGNAL: AlpacaOrderRole.EXIT_SIGNAL,
        AlpacaExitMethod.TRAILING_STOP: AlpacaOrderRole.EXIT_TRAILING,
        AlpacaExitMethod.OCO_STOP: AlpacaOrderRole.STOP_LOSS,
        AlpacaExitMethod.OCO_TAKE_PROFIT: AlpacaOrderRole.TAKE_PROFIT,
        AlpacaExitMethod.STOP_LOSS: AlpacaOrderRole.STOP_LOSS,
    }
    return method_to_role.get(exit_method, AlpacaOrderRole.EXIT_SIGNAL)


def _determine_exit_method(fill: AlpacaFilledOrder) -> AlpacaExitMethod:
    tracker = AlpacaOrderTracker.query.filter_by(
        alpaca_order_id=fill.alpaca_order_id
    ).first()

    if tracker:
        from alpaca.models import AlpacaOrderRole
        role_map = {
            AlpacaOrderRole.EXIT_SIGNAL: AlpacaExitMethod.WEBHOOK_SIGNAL,
            AlpacaOrderRole.EXIT_TRAILING: AlpacaExitMethod.TRAILING_STOP,
            AlpacaOrderRole.STOP_LOSS: AlpacaExitMethod.OCO_STOP,
            AlpacaOrderRole.TAKE_PROFIT: AlpacaExitMethod.OCO_TAKE_PROFIT,
        }
        return role_map.get(tracker.role, AlpacaExitMethod.EXTERNAL)

    order_type = (fill.order_type or '').lower()
    if 'stop' in order_type:
        return AlpacaExitMethod.STOP_LOSS

    return AlpacaExitMethod.EXTERNAL


def _parse_timestamp(ts_str: str) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        ts_str = ts_str.replace('Z', '+00:00')
        return datetime.fromisoformat(ts_str)
    except Exception:
        try:
            return datetime.strptime(ts_str[:19], '%Y-%m-%dT%H:%M:%S')
        except Exception:
            return datetime.utcnow()


def _next_seq(symbol: str, trade_date) -> int:
    max_seq = db.session.query(db.func.max(AlpacaPosition.sequence_number)).filter_by(
        symbol=symbol,
        trade_date=trade_date,
    ).scalar()
    return (max_seq or 0) + 1


def get_reconciliation_summary() -> Dict:
    total_fills = AlpacaFilledOrder.query.count()
    reconciled_fills = AlpacaFilledOrder.query.filter_by(reconciled=True).count()
    unreconciled_fills = total_fills - reconciled_fills

    latest_run = AlpacaReconciliationRun.query.order_by(
        AlpacaReconciliationRun.started_at.desc()
    ).first()

    recent_runs = AlpacaReconciliationRun.query.order_by(
        AlpacaReconciliationRun.started_at.desc()
    ).limit(20).all()

    return {
        'total_fills': total_fills,
        'reconciled_fills': reconciled_fills,
        'unreconciled_fills': unreconciled_fills,
        'reconciliation_pct': (reconciled_fills / total_fills * 100) if total_fills > 0 else 0,
        'latest_run': latest_run,
        'recent_runs': recent_runs,
    }


def fetch_fills_only() -> Tuple[int, int]:
    return fetch_and_store_filled_orders()


