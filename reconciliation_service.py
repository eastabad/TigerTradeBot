import logging
import json
import threading
from datetime import datetime, date, timedelta
from sqlalchemy.exc import IntegrityError
from app import db
from models import (
    TigerFilledOrder, ReconciliationRun, ClosedPosition, CompletedTrade,
    EntrySignalRecord, ExitMethod, Trade
)

logger = logging.getLogger(__name__)

_last_fetch_hour = {}
_reconciliation_lock = threading.Lock()


def fetch_and_store_filled_orders(account_type='real', start_date=None, end_date=None):
    """Fetch filled orders from Tiger API and store in database (deduplicated by order_id).
    
    Returns: (total_fetched, new_stored)
    """
    try:
        if account_type == 'paper':
            from tiger_client import TigerPaperClient
            client = TigerPaperClient()
        else:
            from tiger_client import TigerClient
            client = TigerClient()
        
        if not client.client:
            logger.warning(f"Tiger {account_type} client not initialized, skipping fetch")
            return 0, 0
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = today_str
        if not end_date:
            end_date = today_str
        
        result = client.get_filled_orders(start_date=start_date, end_date=end_date)
        
        if not result.get('success'):
            logger.error(f"Failed to fetch filled orders for {account_type}: {result.get('error')}")
            return 0, 0
        
        orders = result.get('orders', [])
        total_fetched = len(orders)
        new_stored = 0
        
        for order_data in orders:
            order_id = str(order_data.get('order_id', ''))
            if not order_id:
                continue
            
            existing = TigerFilledOrder.query.filter_by(
                order_id=order_id,
                account_type=account_type
            ).first()
            
            if existing:
                existing.symbol = order_data.get('symbol', existing.symbol)
                existing.action = order_data.get('action', existing.action)
                existing.is_open = order_data.get('is_open', existing.is_open)
                existing.quantity = order_data.get('quantity', existing.quantity)
                existing.filled = order_data.get('filled', existing.filled)
                existing.avg_fill_price = order_data.get('avg_fill_price', existing.avg_fill_price)
                existing.limit_price = order_data.get('limit_price', existing.limit_price)
                existing.realized_pnl = order_data.get('realized_pnl', existing.realized_pnl)
                existing.commission = order_data.get('commission', existing.commission)
                existing.order_time = order_data.get('order_time', existing.order_time)
                existing.trade_time = order_data.get('trade_time', existing.trade_time)
                existing.order_time_str = order_data.get('order_time_str', existing.order_time_str)
                existing.trade_time_str = order_data.get('trade_time_str', existing.trade_time_str)
                existing.status = order_data.get('status', existing.status)
                existing.order_type = order_data.get('order_type', existing.order_type)
                existing.outside_rth = order_data.get('outside_rth', existing.outside_rth)
                existing.parent_id = str(order_data.get('parent_id', '')) if order_data.get('parent_id') else existing.parent_id
                existing.raw_json = json.dumps(order_data, default=str)
                continue
            
            try:
                filled_order = TigerFilledOrder(
                    order_id=order_id,
                    account_type=account_type,
                    symbol=order_data.get('symbol', 'N/A'),
                    action=order_data.get('action', 'N/A'),
                    is_open=order_data.get('is_open', True),
                    quantity=order_data.get('quantity', 0),
                    filled=order_data.get('filled', 0),
                    avg_fill_price=order_data.get('avg_fill_price', 0),
                    limit_price=order_data.get('limit_price', 0),
                    realized_pnl=order_data.get('realized_pnl', 0),
                    commission=order_data.get('commission', 0),
                    order_time=order_data.get('order_time'),
                    trade_time=order_data.get('trade_time'),
                    order_time_str=order_data.get('order_time_str', ''),
                    trade_time_str=order_data.get('trade_time_str', ''),
                    status=order_data.get('status', ''),
                    order_type=order_data.get('order_type', ''),
                    outside_rth=order_data.get('outside_rth', False),
                    parent_id=str(order_data.get('parent_id', '')) if order_data.get('parent_id') else None,
                    raw_json=json.dumps(order_data, default=str)
                )
                db.session.add(filled_order)
                db.session.flush()
                new_stored += 1
            except IntegrityError:
                db.session.rollback()
                logger.debug(f"Duplicate order {order_id} skipped (concurrent insert)")
                continue
        
        db.session.commit()
        logger.info(f"📊 Tiger filled orders fetch: {account_type} - fetched={total_fetched}, new={new_stored}")
        return total_fetched, new_stored
        
    except Exception as e:
        logger.error(f"❌ Error fetching filled orders for {account_type}: {str(e)}")
        db.session.rollback()
        return 0, 0


def reconcile_today(account_type='real', run_type='scheduled', target_date=None):
    """Run reconciliation for today's closed positions.
    
    1. Fetch latest filled orders from Tiger
    2. Match opening and closing orders by symbol
    3. Update/correct ClosedPosition and CompletedTrade records
    
    Returns: ReconciliationRun record
    """
    recon_date = target_date or date.today()
    
    run = ReconciliationRun(
        run_date=recon_date,
        account_type=account_type,
        run_type=run_type,
        status='running',
        started_at=datetime.utcnow()
    )
    db.session.add(run)
    db.session.commit()
    
    details = []
    
    try:
        today_str = recon_date.strftime('%Y-%m-%d')
        total_fetched, new_stored = fetch_and_store_filled_orders(
            account_type=account_type,
            start_date=today_str,
            end_date=today_str
        )
        run.total_orders_fetched = total_fetched
        run.new_orders_stored = new_stored
        details.append(f"Fetched {total_fetched} orders, {new_stored} new")
        
        import pytz
        et = pytz.timezone('US/Eastern')
        day_start_et = et.localize(datetime.combine(recon_date, datetime.min.time()))
        day_end_et = et.localize(datetime.combine(recon_date + timedelta(days=1), datetime.min.time()))
        day_start_ms = int(day_start_et.timestamp() * 1000)
        day_end_ms = int(day_end_et.timestamp() * 1000)
        
        closing_orders = TigerFilledOrder.query.filter(
            TigerFilledOrder.account_type == account_type,
            TigerFilledOrder.is_open == False,
            TigerFilledOrder.trade_time >= day_start_ms,
            TigerFilledOrder.trade_time < day_end_ms
        ).order_by(TigerFilledOrder.trade_time.asc()).all()
        
        if not closing_orders:
            details.append("No closing orders found for today")
            run.status = 'completed'
            run.details = '\n'.join(details)
            run.finished_at = datetime.utcnow()
            db.session.commit()
            return run
        
        symbols_with_closes = set(co.symbol for co in closing_orders)
        details.append(f"Found {len(closing_orders)} closing orders for symbols: {', '.join(symbols_with_closes)}")
        
        positions_matched = 0
        records_corrected = 0
        records_created = 0
        
        for symbol in symbols_with_closes:
            result = _reconcile_symbol(symbol, account_type, day_start_ms, day_end_ms)
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
        
        logger.info(f"📊 Reconciliation completed for {account_type} on {recon_date}: "
                    f"matched={positions_matched}, corrected={records_corrected}, created={records_created}")
        return run
        
    except Exception as e:
        logger.error(f"❌ Reconciliation failed for {account_type}: {str(e)}")
        import traceback
        traceback.print_exc()
        run.status = 'failed'
        run.error_message = str(e)
        run.details = '\n'.join(details)
        run.finished_at = datetime.utcnow()
        db.session.commit()
        return run


def reconcile_all_history(account_type='paper', days_back=90):
    """Full re-reconciliation of all historical filled orders.
    
    1. Clean up invalid ClosedPosition records (exit_order_id pointing to opening orders)
    2. Reset all reconciliation flags
    3. Find all unique dates with closing orders
    4. Run reconciliation for each date
    
    Returns: ReconciliationRun record with aggregate stats
    """
    run = ReconciliationRun(
        run_date=date.today(),
        account_type=account_type,
        run_type='full_history',
        status='running',
        started_at=datetime.utcnow()
    )
    db.session.add(run)
    db.session.commit()
    
    details = []
    
    try:
        invalid_cps = db.session.query(ClosedPosition).join(
            TigerFilledOrder,
            db.and_(
                ClosedPosition.exit_order_id == TigerFilledOrder.order_id,
                ClosedPosition.account_type == TigerFilledOrder.account_type
            )
        ).filter(
            ClosedPosition.account_type == account_type,
            TigerFilledOrder.is_open == True
        ).all()
        
        if invalid_cps:
            invalid_ids = [cp.id for cp in invalid_cps]
            EntrySignalRecord.query.filter(
                EntrySignalRecord.closed_position_id.in_(invalid_ids)
            ).update({EntrySignalRecord.closed_position_id: None}, synchronize_session='fetch')
            
            for cp in invalid_cps:
                logger.info(f"🗑️ Removing invalid ClosedPosition #{cp.id} {cp.symbol} "
                           f"(exit_order {cp.exit_order_id} is actually an opening order)")
                db.session.delete(cp)
            
            db.session.commit()
            details.append(f"Removed {len(invalid_cps)} invalid ClosedPosition records (exit_order_id → opening orders)")
        
        reset_count = TigerFilledOrder.query.filter(
            TigerFilledOrder.account_type == account_type,
            TigerFilledOrder.reconciled == True
        ).update({'reconciled': False, 'reconciled_at': None, 'matched_order_id': None})
        db.session.commit()
        details.append(f"Reset {reset_count} reconciliation flags")
        
        today_str = date.today().strftime('%Y-%m-%d')
        start_str = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        total_fetched, new_stored = fetch_and_store_filled_orders(
            account_type=account_type,
            start_date=start_str,
            end_date=today_str
        )
        run.total_orders_fetched = total_fetched
        run.new_orders_stored = new_stored
        details.append(f"Fetched {total_fetched} orders ({days_back} days), {new_stored} new")
        
        import pytz
        from sqlalchemy import func
        et = pytz.timezone('US/Eastern')
        
        all_closing = TigerFilledOrder.query.filter(
            TigerFilledOrder.account_type == account_type,
            TigerFilledOrder.is_open == False
        ).all()
        
        if not all_closing:
            details.append("No closing orders found in history")
            run.status = 'completed'
            run.details = '\n'.join(details)
            run.finished_at = datetime.utcnow()
            db.session.commit()
            return run
        
        trade_dates = set()
        for co in all_closing:
            if co.trade_time:
                dt = datetime.utcfromtimestamp(co.trade_time / 1000)
                et_dt = dt.replace(tzinfo=pytz.utc).astimezone(et)
                trade_dates.add(et_dt.date())
        
        trade_dates = sorted(trade_dates)
        details.append(f"Found {len(trade_dates)} unique trading dates with closings")
        
        total_matched = 0
        total_corrected = 0
        total_created = 0
        
        for td in trade_dates:
            day_start_et = et.localize(datetime.combine(td, datetime.min.time()))
            day_end_et = et.localize(datetime.combine(td + timedelta(days=1), datetime.min.time()))
            day_start_ms = int(day_start_et.timestamp() * 1000)
            day_end_ms = int(day_end_et.timestamp() * 1000)
            
            day_closing = TigerFilledOrder.query.filter(
                TigerFilledOrder.account_type == account_type,
                TigerFilledOrder.is_open == False,
                TigerFilledOrder.trade_time >= day_start_ms,
                TigerFilledOrder.trade_time < day_end_ms
            ).all()
            
            symbols = set(co.symbol for co in day_closing)
            
            for sym in symbols:
                result = _reconcile_symbol(sym, account_type, day_start_ms, day_end_ms)
                total_matched += result['matched']
                total_corrected += result['corrected']
                total_created += result['created']
                if result.get('detail'):
                    details.append(f"  [{td}] {result['detail']}")
        
        run.positions_matched = total_matched
        run.records_corrected = total_corrected
        run.records_created = total_created
        run.status = 'completed'
        run.details = '\n'.join(details)
        run.finished_at = datetime.utcnow()
        db.session.commit()
        
        logger.info(f"📊 Full history reconciliation for {account_type}: "
                    f"matched={total_matched}, corrected={total_corrected}, created={total_created}")
        return run
        
    except Exception as e:
        logger.error(f"❌ Full history reconciliation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        run.status = 'failed'
        run.error_message = str(e)
        run.details = '\n'.join(details)
        run.finished_at = datetime.utcnow()
        db.session.commit()
        return run


def _reconcile_symbol(symbol, account_type, day_start_ms, day_end_ms):
    """Reconcile all opening and closing orders for a single symbol on a given day.
    
    Core logic:
    - closing SELL (is_open=False, action=SELL) → closing a LONG → match with BUY opens
    - closing BUY (is_open=False, action=BUY) → closing a SHORT → match with SELL opens
    
    Matching strategy (per direction):
    1. Exact quantity match first
    2. FIFO fallback for remaining
    
    Opening orders searched across ALL days (unreconciled), closing scoped to target day.
    """
    result = {'matched': 0, 'corrected': 0, 'created': 0, 'detail': ''}
    
    try:
        closing_orders = TigerFilledOrder.query.filter(
            TigerFilledOrder.account_type == account_type,
            TigerFilledOrder.symbol == symbol,
            TigerFilledOrder.is_open == False,
            TigerFilledOrder.trade_time >= day_start_ms,
            TigerFilledOrder.trade_time < day_end_ms
        ).order_by(TigerFilledOrder.trade_time.asc()).all()
        
        if not closing_orders:
            return result
        
        close_sells = [o for o in closing_orders if (o.action or '').upper() in ('SELL', 'SELL_CLOSE')]
        close_buys = [o for o in closing_orders if (o.action or '').upper() in ('BUY', 'BUY_CLOSE')]
        
        matches = []
        
        if close_sells:
            buy_opens = TigerFilledOrder.query.filter(
                TigerFilledOrder.account_type == account_type,
                TigerFilledOrder.symbol == symbol,
                TigerFilledOrder.is_open == True,
                TigerFilledOrder.action.in_(['BUY', 'BUY_OPEN', 'Buy', 'buy']),
                TigerFilledOrder.reconciled == False,
                TigerFilledOrder.trade_time < day_end_ms
            ).order_by(TigerFilledOrder.trade_time.asc()).all()
            
            if buy_opens:
                m, r_inc = _match_direction(close_sells, buy_opens)
                matches.extend(m)
                result['matched'] += r_inc
            else:
                logger.warning(f"⚠️ {symbol}: {len(close_sells)} closing SELLs but no BUY opens found")
        
        if close_buys:
            sell_opens = TigerFilledOrder.query.filter(
                TigerFilledOrder.account_type == account_type,
                TigerFilledOrder.symbol == symbol,
                TigerFilledOrder.is_open == True,
                TigerFilledOrder.action.in_(['SELL', 'SELL_OPEN', 'Sell', 'sell']),
                TigerFilledOrder.reconciled == False,
                TigerFilledOrder.trade_time < day_end_ms
            ).order_by(TigerFilledOrder.trade_time.asc()).all()
            
            if sell_opens:
                m, r_inc = _match_direction(close_buys, sell_opens)
                matches.extend(m)
                result['matched'] += r_inc
            else:
                logger.warning(f"⚠️ {symbol}: {len(close_buys)} closing BUYs but no SELL opens found")
        
        for match in matches:
            close_order = match['close_order']
            allocations = match['allocations']
            r = _apply_reconciliation(close_order, allocations, account_type, symbol)
            result['corrected'] += r['corrected']
            result['created'] += r['created']
        
        db.session.commit()
        
        total_closing = sum((c.filled or c.quantity or 0) for c in closing_orders)
        result['detail'] = (f"{symbol}: {len(closing_orders)} closes ({total_closing} shares) → "
                          f"{result['matched']} matched, {result['corrected']} corrected, {result['created']} created")
        
        logger.info(f"📊 Reconciliation {result['detail']}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error reconciling {symbol}/{account_type}: {str(e)}")
        import traceback
        traceback.print_exc()
        db.session.rollback()
        return result


def _match_direction(closing_orders, opening_orders):
    """Match closing orders with opening orders of the correct direction.
    
    Args:
        closing_orders: list of closing TigerFilledOrder (same direction)
        opening_orders: list of opening TigerFilledOrder (complementary direction)
        
    Returns:
        (matches_list, matched_count)
    """
    matches = []
    matched_count = 0
    
    opening_lots = []
    for o in opening_orders:
        opening_lots.append({
            'order': o,
            'remaining_qty': o.filled or o.quantity or 0,
            'entry_price': o.avg_fill_price or 0,
            'order_id': o.order_id,
            'trade_time': o.trade_time
        })
    
    exact_matched_close_ids = set()
    exact_matched_lot_indices = set()
    
    for ci, close_order in enumerate(closing_orders):
        close_qty = close_order.filled or close_order.quantity or 0
        if close_qty <= 0:
            continue
        
        for li, lot in enumerate(opening_lots):
            if li in exact_matched_lot_indices:
                continue
            if abs(lot['remaining_qty'] - close_qty) < 0.001:
                matches.append({
                    'close_order': close_order,
                    'allocations': [{
                        'lot': lot,
                        'qty': close_qty,
                        'entry_price': lot['entry_price']
                    }]
                })
                lot['remaining_qty'] = 0
                exact_matched_close_ids.add(ci)
                exact_matched_lot_indices.add(li)
                
                close_order.matched_order_id = lot['order_id']
                close_order.reconciled = True
                close_order.reconciled_at = datetime.utcnow()
                lot['order'].reconciled = True
                lot['order'].reconciled_at = datetime.utcnow()
                lot['order'].matched_order_id = close_order.order_id
                
                matched_count += 1
                break
    
    remaining_closes = [c for i, c in enumerate(closing_orders) if i not in exact_matched_close_ids]
    
    for close_order in remaining_closes:
        close_qty = close_order.filled or close_order.quantity or 0
        if close_qty <= 0:
            continue
        
        allocations = []
        remaining_to_fill = close_qty
        
        for lot in opening_lots:
            if remaining_to_fill <= 0.001:
                break
            if lot['remaining_qty'] <= 0.001:
                continue
            
            alloc_qty = min(remaining_to_fill, lot['remaining_qty'])
            allocations.append({
                'lot': lot,
                'qty': alloc_qty,
                'entry_price': lot['entry_price']
            })
            lot['remaining_qty'] -= alloc_qty
            remaining_to_fill -= alloc_qty
            
            if lot['remaining_qty'] <= 0.001:
                lot['order'].reconciled = True
                lot['order'].reconciled_at = datetime.utcnow()
                lot['order'].matched_order_id = close_order.order_id
        
        if allocations:
            matches.append({
                'close_order': close_order,
                'allocations': allocations
            })
            close_order.reconciled = True
            close_order.reconciled_at = datetime.utcnow()
            if len(allocations) == 1:
                close_order.matched_order_id = allocations[0]['lot']['order_id']
            
            matched_count += 1
    
    return matches, matched_count


def _apply_reconciliation(close_order, allocations, account_type, symbol):
    """Apply reconciliation result: update/create ClosedPosition and CompletedTrade records.
    
    Args:
        close_order: TigerFilledOrder (closing/exit order)
        allocations: list of {lot, qty, entry_price} from matching
        account_type: 'real' or 'paper'
        symbol: stock symbol
    """
    r = {'corrected': 0, 'created': 0}
    
    try:
        exit_price = close_order.avg_fill_price or 0
        exit_qty = close_order.filled or close_order.quantity or 0
        exit_order_id = close_order.order_id
        
        import pytz
        exit_time = None
        if close_order.trade_time:
            exit_time = datetime.utcfromtimestamp(close_order.trade_time / 1000)
        
        action = (close_order.action or '').upper()
        if 'SELL' in action:
            position_side = 'long'
        else:
            position_side = 'short'
        
        total_entry_qty = sum(a['qty'] for a in allocations)
        weighted_entry_price = 0
        if total_entry_qty > 0:
            weighted_entry_price = sum(a['entry_price'] * a['qty'] for a in allocations) / total_entry_qty
        
        if position_side == 'long':
            total_pnl = (exit_price - weighted_entry_price) * exit_qty
        else:
            total_pnl = (weighted_entry_price - exit_price) * exit_qty
        
        tiger_pnl = close_order.realized_pnl
        if tiger_pnl and abs(tiger_pnl) > 0.01:
            total_pnl = tiger_pnl
        
        total_pnl_pct = 0
        if weighted_entry_price > 0:
            if position_side == 'long':
                total_pnl_pct = ((exit_price - weighted_entry_price) / weighted_entry_price) * 100
            else:
                total_pnl_pct = ((weighted_entry_price - exit_price) / weighted_entry_price) * 100
        
        exit_method = _determine_exit_method(close_order)
        
        closed_pos = ClosedPosition.query.filter_by(
            exit_order_id=exit_order_id,
            account_type=account_type
        ).first()
        
        if closed_pos:
            changed = False
            if closed_pos.side != position_side:
                logger.info(f"📊 Fixing side: {closed_pos.side} → {position_side} for #{closed_pos.id}")
                closed_pos.side = position_side
                changed = True
            if closed_pos.exit_price != exit_price:
                closed_pos.exit_price = exit_price
                changed = True
            if closed_pos.exit_quantity != exit_qty:
                closed_pos.exit_quantity = exit_qty
                changed = True
            if closed_pos.avg_entry_price != round(weighted_entry_price, 4):
                closed_pos.avg_entry_price = round(weighted_entry_price, 4)
                changed = True
            if closed_pos.total_pnl != round(total_pnl, 4):
                closed_pos.total_pnl = round(total_pnl, 4)
                changed = True
            if closed_pos.total_pnl_pct != round(total_pnl_pct, 4):
                closed_pos.total_pnl_pct = round(total_pnl_pct, 4)
                changed = True
            if close_order.commission and closed_pos.commission != close_order.commission:
                closed_pos.commission = close_order.commission
                changed = True
            if exit_time and closed_pos.exit_time != exit_time:
                closed_pos.exit_time = exit_time
                changed = True
            
            if changed:
                r['corrected'] += 1
                logger.info(f"📊 Reconciliation corrected ClosedPosition #{closed_pos.id} for {symbol} "
                           f"entry={round(weighted_entry_price, 4)}, exit={exit_price}, pnl={round(total_pnl, 2)}")
        else:
            closed_pos = ClosedPosition(
                symbol=symbol,
                account_type=account_type,
                exit_order_id=exit_order_id,
                exit_time=exit_time,
                exit_price=exit_price,
                exit_quantity=exit_qty,
                side=position_side,
                exit_method=exit_method,
                total_pnl=total_pnl,
                total_pnl_pct=round(total_pnl_pct, 2),
                avg_entry_price=round(weighted_entry_price, 4),
                commission=close_order.commission
            )
            db.session.add(closed_pos)
            db.session.flush()
            r['created'] += 1
            logger.info(f"📊 Reconciliation created ClosedPosition #{closed_pos.id} for {symbol}")
            
            try:
                from closed_position_service import _sync_position_exit_data
                _sync_position_exit_data(
                    symbol=symbol,
                    account_type=account_type,
                    side=position_side,
                    exit_price=exit_price,
                    exit_quantity=exit_qty,
                    exit_time=exit_time,
                    exit_method=exit_method,
                    exit_order_id=str(exit_order_id) if exit_order_id else None,
                    realized_pnl=total_pnl,
                    commission=close_order.commission,
                )
            except Exception as sync_err:
                logger.warning(f"📊 Position sync failed (non-fatal): {sync_err}")
        
        for alloc in allocations:
            lot = alloc['lot']
            lot_order = lot['order']
            alloc_qty = alloc['qty']
            entry_price = alloc['entry_price']
            
            if position_side == 'long':
                alloc_pnl = (exit_price - entry_price) * alloc_qty
            else:
                alloc_pnl = (entry_price - exit_price) * alloc_qty
            alloc_pnl_pct = 0
            if entry_price > 0:
                if position_side == 'long':
                    alloc_pnl_pct = ((exit_price - entry_price) / entry_price) * 100
                else:
                    alloc_pnl_pct = ((entry_price - exit_price) / entry_price) * 100
            
            entry_time = None
            if lot_order.trade_time:
                entry_time = datetime.utcfromtimestamp(lot_order.trade_time / 1000)
            
            entry_record = _find_entry_signal_record(
                symbol, account_type, lot_order.order_id, entry_price, alloc_qty, entry_time
            )
            
            if entry_record:
                if entry_record.closed_position_id != closed_pos.id:
                    entry_record.closed_position_id = closed_pos.id
                    r['corrected'] += 1
                if entry_record.contribution_pnl != round(alloc_pnl, 2):
                    entry_record.contribution_pnl = round(alloc_pnl, 2)
                    entry_record.contribution_pct = round(alloc_pnl_pct, 2)
            
            completed_trade = _find_completed_trade(
                symbol, account_type, lot_order.order_id, entry_price, entry_time
            )
            
            if completed_trade:
                changed = False
                if completed_trade.is_open:
                    completed_trade.is_open = False
                    changed = True
                if completed_trade.exit_price != exit_price:
                    completed_trade.exit_price = exit_price
                    changed = True
                if completed_trade.exit_quantity != alloc_qty:
                    completed_trade.exit_quantity = alloc_qty
                    changed = True
                if completed_trade.pnl_amount != round(alloc_pnl, 2):
                    completed_trade.pnl_amount = round(alloc_pnl, 2)
                    changed = True
                if completed_trade.pnl_percent != round(alloc_pnl_pct, 2):
                    completed_trade.pnl_percent = round(alloc_pnl_pct, 2)
                    changed = True
                if exit_time and completed_trade.exit_time != exit_time:
                    completed_trade.exit_time = exit_time
                    changed = True
                if completed_trade.remaining_quantity != 0:
                    completed_trade.remaining_quantity = 0
                    changed = True
                if completed_trade.exited_quantity != alloc_qty:
                    completed_trade.exited_quantity = alloc_qty
                    changed = True
                if completed_trade.avg_exit_price != exit_price:
                    completed_trade.avg_exit_price = exit_price
                    changed = True
                if not completed_trade.exit_method:
                    completed_trade.exit_method = exit_method
                    changed = True
                if entry_time and completed_trade.exit_time:
                    hold_seconds = int((completed_trade.exit_time - entry_time).total_seconds())
                    if completed_trade.hold_duration_seconds != hold_seconds:
                        completed_trade.hold_duration_seconds = hold_seconds
                        changed = True
                
                if changed:
                    r['corrected'] += 1
                    logger.info(f"📊 Reconciliation corrected CompletedTrade #{completed_trade.id} for {symbol}")
            else:
                new_ct = CompletedTrade(
                    symbol=symbol,
                    account_type=account_type,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    entry_quantity=alloc_qty,
                    side=position_side,
                    exit_method=exit_method,
                    exit_time=exit_time,
                    exit_price=exit_price,
                    exit_quantity=alloc_qty,
                    pnl_amount=round(alloc_pnl, 2),
                    pnl_percent=round(alloc_pnl_pct, 2),
                    is_open=False,
                    remaining_quantity=0,
                    exited_quantity=alloc_qty,
                    avg_exit_price=exit_price,
                    signal_indicator=f"Reconciled from Tiger (order {lot_order.order_id})"
                )
                if entry_time and exit_time:
                    new_ct.hold_duration_seconds = int((exit_time - entry_time).total_seconds())
                db.session.add(new_ct)
                r['created'] += 1
                logger.info(f"📊 Reconciliation created CompletedTrade for {symbol} "
                           f"(entry={entry_price}, exit={exit_price}, qty={alloc_qty})")
        
        return r
        
    except Exception as e:
        logger.error(f"❌ Error applying reconciliation for {symbol}: {str(e)}")
        import traceback
        traceback.print_exc()
        return r


def _determine_exit_method(close_order):
    """Determine ExitMethod from Tiger order data."""
    order_type = (close_order.order_type or '').upper()
    
    if 'STP' in order_type:
        return ExitMethod.STOP_LOSS
    elif 'LMT' in order_type:
        return ExitMethod.TAKE_PROFIT
    elif 'MKT' in order_type:
        return ExitMethod.MANUAL
    
    return ExitMethod.EXTERNAL


def _find_entry_signal_record(symbol, account_type, entry_order_id, entry_price, qty, entry_time):
    """Find matching EntrySignalRecord for an opening order.
    
    Multi-pass matching strategy (progressively relaxed):
    1. Exact order_id match
    2. Symbol + price(±0.5%) + qty + time(±10min) 
    3. Symbol + price(±1%) + time(±60min)
    4. Symbol + price(±2%) + unlinked only (no closed_position_id)
    """
    clean_symbol = symbol.replace('[PAPER]', '').strip()
    
    if entry_order_id:
        record = EntrySignalRecord.query.filter_by(
            entry_order_id=str(entry_order_id),
            account_type=account_type
        ).first()
        if record:
            return record
    
    from sqlalchemy import or_
    symbol_variants = [clean_symbol]
    if account_type == 'paper':
        symbol_variants.append(f'[PAPER]{clean_symbol}')
    
    base_filter = [
        or_(*[EntrySignalRecord.symbol == s for s in symbol_variants]),
        EntrySignalRecord.account_type == account_type
    ]
    
    if entry_price and qty and entry_time:
        result = EntrySignalRecord.query.filter(
            *base_filter,
            EntrySignalRecord.entry_price.between(entry_price * 0.995, entry_price * 1.005),
            EntrySignalRecord.quantity == qty,
            EntrySignalRecord.entry_time.between(entry_time - timedelta(minutes=10), entry_time + timedelta(minutes=10))
        ).first()
        if result:
            return result
    
    if entry_price and entry_time:
        result = EntrySignalRecord.query.filter(
            *base_filter,
            EntrySignalRecord.entry_price.between(entry_price * 0.99, entry_price * 1.01),
            EntrySignalRecord.entry_time.between(entry_time - timedelta(minutes=60), entry_time + timedelta(minutes=60))
        ).first()
        if result:
            return result
    
    if entry_price:
        result = EntrySignalRecord.query.filter(
            *base_filter,
            EntrySignalRecord.entry_price.between(entry_price * 0.98, entry_price * 1.02),
            EntrySignalRecord.closed_position_id == None
        ).order_by(EntrySignalRecord.entry_time.desc()).first()
        if result:
            return result
    
    return None


def _find_completed_trade(symbol, account_type, entry_order_id, entry_price, entry_time):
    """Find matching CompletedTrade for an opening order.
    
    Multi-pass matching strategy:
    1. Match via Trade.tiger_order_id -> CompletedTrade.trade_id
    2. Symbol + price(±0.5%) + time(±10min)
    3. Symbol + price(±1%) + time(±60min)
    4. Symbol + price(±2%) + still open
    """
    clean_symbol = symbol.replace('[PAPER]', '').strip()
    
    from sqlalchemy import or_
    symbol_variants = [clean_symbol]
    if account_type == 'paper':
        symbol_variants.append(f'[PAPER]{clean_symbol}')
    
    if entry_order_id:
        trade = Trade.query.filter_by(
            tiger_order_id=str(entry_order_id)
        ).first()
        if trade:
            ct = CompletedTrade.query.filter_by(trade_id=trade.id).first()
            if ct:
                return ct
    
    base_filter = [
        or_(*[CompletedTrade.symbol == s for s in symbol_variants]),
        CompletedTrade.account_type == account_type
    ]
    
    if entry_price and entry_time:
        result = CompletedTrade.query.filter(
            *base_filter,
            CompletedTrade.entry_price.between(entry_price * 0.995, entry_price * 1.005),
            CompletedTrade.entry_time.between(entry_time - timedelta(minutes=10), entry_time + timedelta(minutes=10))
        ).order_by(CompletedTrade.entry_time.asc()).first()
        if result:
            return result
    
    if entry_price and entry_time:
        result = CompletedTrade.query.filter(
            *base_filter,
            CompletedTrade.entry_price.between(entry_price * 0.99, entry_price * 1.01),
            CompletedTrade.entry_time.between(entry_time - timedelta(minutes=60), entry_time + timedelta(minutes=60))
        ).order_by(CompletedTrade.entry_time.asc()).first()
        if result:
            return result
    
    if entry_price:
        result = CompletedTrade.query.filter(
            *base_filter,
            CompletedTrade.entry_price.between(entry_price * 0.98, entry_price * 1.02),
            CompletedTrade.is_open == True
        ).order_by(CompletedTrade.entry_time.asc()).first()
        if result:
            return result
    
    return None


def cleanup_old_filled_orders(days=7):
    """Delete TigerFilledOrder records older than `days` days."""
    try:
        cutoff_ms = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
        deleted = TigerFilledOrder.query.filter(
            TigerFilledOrder.trade_time < cutoff_ms
        ).delete(synchronize_session=False)
        if deleted > 0:
            db.session.commit()
            logger.info(f"🗑️ Cleaned up {deleted} filled orders older than {days} days")
        return deleted
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error cleaning up old filled orders: {e}")
        return 0


def scheduled_fetch_filled_orders(app):
    """Scheduled job: fetch filled orders every hour for both accounts. Store only, no reconciliation.
    Fetches last 7 days of data to ensure completeness, deduplicates via UniqueConstraint."""
    global _last_fetch_hour
    
    if not _reconciliation_lock.acquire(blocking=False):
        logger.debug("Reconciliation fetch skipped - another run in progress")
        return
    
    try:
        import pytz
        et = pytz.timezone('US/Eastern')
        now_et = datetime.now(et)
        current_hour = now_et.strftime('%Y-%m-%d-%H')
        
        if now_et.weekday() >= 5:
            return
        
        if now_et.hour < 4 or now_et.hour >= 21:
            return
        
        start_7d = (now_et - timedelta(days=7)).strftime('%Y-%m-%d')
        end_today = now_et.strftime('%Y-%m-%d')
        
        with app.app_context():
            for acct in ['real', 'paper']:
                cache_key = f"{acct}_{current_hour}"
                if _last_fetch_hour.get(cache_key):
                    continue
                
                try:
                    total, new = fetch_and_store_filled_orders(
                        account_type=acct,
                        start_date=start_7d,
                        end_date=end_today
                    )
                    _last_fetch_hour[cache_key] = True
                    if new > 0:
                        logger.info(f"📊 Hourly fetch: {acct} - {new} new orders stored (7-day window)")
                except Exception as e:
                    logger.error(f"❌ Hourly fetch failed for {acct}: {str(e)}")
            
            try:
                cleanup_old_filled_orders(days=7)
            except Exception as e:
                logger.error(f"❌ Cleanup old filled orders failed: {e}")
    finally:
        _reconciliation_lock.release()


def scheduled_reconciliation(app):
    """Scheduled job: run reconciliation after market close (21:00 ET)."""
    if not _reconciliation_lock.acquire(blocking=False):
        logger.debug("Reconciliation skipped - another run in progress")
        return
    
    try:
        import pytz
        et = pytz.timezone('US/Eastern')
        now_et = datetime.now(et)
        
        if now_et.weekday() >= 5:
            return
        
        if now_et.hour != 21:
            return
        
        with app.app_context():
            for acct in ['real', 'paper']:
                try:
                    run = reconcile_today(account_type=acct, run_type='scheduled')
                    logger.info(f"📊 Scheduled reconciliation: {acct} - status={run.status}, "
                               f"matched={run.positions_matched}, corrected={run.records_corrected}")
                except Exception as e:
                    logger.error(f"❌ Scheduled reconciliation failed for {acct}: {str(e)}")
    finally:
        _reconciliation_lock.release()
