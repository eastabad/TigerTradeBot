import json
import logging
from datetime import datetime
from flask import render_template, request, jsonify, flash, redirect, url_for
from alpaca import alpaca_bp
from app import db

logger = logging.getLogger(__name__)


@alpaca_bp.route('/')
def alpaca_index():
    from alpaca.models import AlpacaTrade, AlpacaPosition, AlpacaPositionStatus, AlpacaHolding, AlpacaOrderTracker
    from alpaca.holdings_sync import get_sync_status

    open_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.OPEN).count()
    closed_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.CLOSED).count()
    total_trades = AlpacaTrade.query.count()
    pending_orders = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.status.in_(['NEW', 'PENDING', 'ACCEPTED', 'PARTIALLY_FILLED'])
    ).count()

    recent_trades = AlpacaTrade.query.order_by(AlpacaTrade.created_at.desc()).limit(10).all()

    holdings = AlpacaHolding.query.all()

    if not holdings and open_positions > 0:
        try:
            from alpaca.holdings_sync import sync_holdings
            sync_result = sync_holdings()
            if sync_result.get('success'):
                holdings = AlpacaHolding.query.all()
        except Exception:
            pass

    total_unrealized_pnl = sum(h.unrealized_pnl or 0 for h in holdings)
    total_market_value = sum(h.market_value or 0 for h in holdings)

    closed = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.CLOSED).all()
    total_realized_pnl = sum(p.realized_pnl or 0 for p in closed)
    win_count = sum(1 for p in closed if (p.realized_pnl or 0) > 0)
    win_rate = (win_count / len(closed) * 100) if closed else 0

    sync_status = get_sync_status()

    account_info = None
    day_change = None
    day_change_pct = None
    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        if client.api_key:
            account_info = client.get_account()
            if account_info:
                equity = float(account_info.get('equity', 0) or 0)
                last_equity = float(account_info.get('last_equity', 0) or 0)
                if last_equity > 0:
                    day_change = equity - last_equity
                    day_change_pct = (day_change / last_equity) * 100
    except Exception as e:
        logger.debug(f"Could not fetch account info: {e}")

    return render_template('alpaca/index.html',
                           open_positions=open_positions,
                           closed_positions=closed_positions,
                           total_trades=total_trades,
                           pending_orders=pending_orders,
                           recent_trades=recent_trades,
                           holdings=holdings,
                           total_unrealized_pnl=total_unrealized_pnl,
                           total_market_value=total_market_value,
                           total_realized_pnl=total_realized_pnl,
                           win_rate=win_rate,
                           sync_status=sync_status,
                           account_info=account_info,
                           day_change=day_change,
                           day_change_pct=day_change_pct)


@alpaca_bp.route('/trades')
def alpaca_trades():
    from alpaca.models import AlpacaTrade

    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    symbol_filter = request.args.get('symbol', '')
    status_filter = request.args.get('status', '')

    query = AlpacaTrade.query

    if symbol_filter:
        query = query.filter(AlpacaTrade.symbol.ilike(f'%{symbol_filter}%'))
    if status_filter:
        from alpaca.models import AlpacaOrderStatus
        try:
            status_enum = AlpacaOrderStatus(status_filter)
            query = query.filter_by(status=status_enum)
        except ValueError:
            pass

    trades = query.order_by(AlpacaTrade.created_at.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )

    return render_template('alpaca/trades.html',
                           trades=trades,
                           symbol_filter=symbol_filter,
                           status_filter=status_filter)


@alpaca_bp.route('/settings', methods=['GET', 'POST'])
def alpaca_settings():
    from alpaca.models import AlpacaTradingConfig, AlpacaTrailingStopConfig
    import os

    if request.method == 'POST':
        for key in request.form:
            if key.startswith('config_'):
                config_key = key[7:]
                config_value = request.form[key]

                existing = AlpacaTradingConfig.query.filter_by(key=config_key).first()
                if existing:
                    existing.value = config_value
                else:
                    new_config = AlpacaTradingConfig(key=config_key, value=config_value)
                    db.session.add(new_config)

        db.session.commit()
        flash('Settings saved successfully', 'success')
        return redirect(url_for('alpaca.alpaca_settings'))

    configs = {c.key: c.value for c in AlpacaTradingConfig.query.all()}

    has_api_key = bool(os.environ.get('ALPACA_API_KEY2') or os.environ.get('ALPACA_API_KEY'))
    has_secret_key = bool(os.environ.get('ALPACA_SECRET_KEY2') or os.environ.get('ALPACA_SECRET_KEY'))

    ts_config = AlpacaTrailingStopConfig.query.first()
    if not ts_config:
        ts_config = AlpacaTrailingStopConfig()
        db.session.add(ts_config)
        db.session.commit()

    return render_template('alpaca/settings.html',
                           configs=configs,
                           has_api_key=has_api_key,
                           has_secret_key=has_secret_key,
                           ts_config=ts_config)


@alpaca_bp.route('/positions')
def alpaca_positions():
    from alpaca.models import AlpacaPosition, AlpacaPositionStatus, AlpacaHolding

    status_filter = request.args.get('status', 'all')
    symbol_filter = request.args.get('symbol', '')
    page = request.args.get('page', 1, type=int)

    query = AlpacaPosition.query

    if status_filter == 'open':
        query = query.filter_by(status=AlpacaPositionStatus.OPEN)
    elif status_filter == 'closed':
        query = query.filter_by(status=AlpacaPositionStatus.CLOSED)

    if symbol_filter:
        query = query.filter(AlpacaPosition.symbol.ilike(f'%{symbol_filter}%'))

    positions = query.order_by(AlpacaPosition.created_at.desc()).paginate(
        page=page, per_page=50, error_out=False
    )

    open_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.OPEN).all()

    holdings_map = {}
    for h in AlpacaHolding.query.all():
        holdings_map[h.symbol] = h

    if open_positions and not holdings_map:
        try:
            from alpaca.holdings_sync import sync_holdings
            sync_result = sync_holdings()
            if sync_result.get('success'):
                holdings_map = {}
                for h in AlpacaHolding.query.all():
                    holdings_map[h.symbol] = h
        except Exception:
            pass

    if open_positions:
        missing_symbols = [p.symbol for p in open_positions if p.symbol not in holdings_map]
        if missing_symbols:
            try:
                from alpaca.client import AlpacaClient
                client = AlpacaClient()
                for sym in missing_symbols:
                    try:
                        pos_data = client._request('GET', f'/v2/positions/{sym}')
                        if pos_data and isinstance(pos_data, dict):
                            unrealized_pl = float(pos_data.get('unrealized_pl', 0) or 0)
                            unrealized_plpc = float(pos_data.get('unrealized_plpc', 0) or 0) * 100
                            current_price = float(pos_data.get('current_price', 0) or 0)

                            class HoldingProxy:
                                def __init__(self, unrealized_pnl, unrealized_pnl_pct, current_price):
                                    self.unrealized_pnl = unrealized_pnl
                                    self.unrealized_pnl_pct = unrealized_pnl_pct
                                    self.current_price = current_price

                            holdings_map[sym] = HoldingProxy(unrealized_pl, unrealized_plpc, current_price)
                    except Exception:
                        pass
            except Exception:
                pass

    closed_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.CLOSED).all()

    total_unrealized = sum(
        (holdings_map.get(p.symbol).unrealized_pnl or 0)
        for p in open_positions
        if p.symbol in holdings_map
    )
    total_realized = sum(p.realized_pnl or 0 for p in closed_positions)

    return render_template('alpaca/positions.html',
                           positions=positions,
                           holdings_map=holdings_map,
                           status_filter=status_filter,
                           symbol_filter=symbol_filter,
                           total_unrealized=total_unrealized,
                           total_realized=total_realized,
                           open_count=len(open_positions),
                           closed_count=len(closed_positions))


@alpaca_bp.route('/positions/<int:position_id>')
def alpaca_position_detail(position_id):
    from alpaca.models import AlpacaPosition, AlpacaHolding, AlpacaOrderTracker

    position = AlpacaPosition.query.get_or_404(position_id)

    holding = AlpacaHolding.query.filter_by(symbol=position.symbol).first()

    entry_legs = position.entry_legs
    exit_legs = position.exit_legs

    order_ids = set()
    for leg in entry_legs + exit_legs:
        if leg.alpaca_order_id:
            order_ids.add(leg.alpaca_order_id)

    tracked_orders = []
    if order_ids:
        tracked_orders = AlpacaOrderTracker.query.filter(
            AlpacaOrderTracker.alpaca_order_id.in_(order_ids)
        ).all()

    return render_template('alpaca/position_detail.html',
                           position=position,
                           holding=holding,
                           entry_legs=entry_legs,
                           exit_legs=exit_legs,
                           tracked_orders=tracked_orders)


@alpaca_bp.route('/webhook', methods=['POST'])
def alpaca_webhook():
    from alpaca.signal_parser import AlpacaSignalParser
    from alpaca.models import AlpacaSignalLog, AlpacaTrade, AlpacaOrderStatus, AlpacaSide, AlpacaOrderType, AlpacaOrderRole
    from alpaca.order_tracker import register_order

    try:
        from alpaca.db_logger import log_info, log_warning, log_error, log_critical

        raw_data = request.get_data(as_text=True)
        logger.info(f"Alpaca webhook received: {raw_data[:200]}")

        try:
            signal_data = json.loads(raw_data)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in webhook: {raw_data[:200]}")
            return jsonify({'error': 'Invalid JSON'}), 400

        signal_log = AlpacaSignalLog(
            source='tradingview',
            raw_data=raw_data,
            status='received'
        )
        db.session.add(signal_log)
        db.session.flush()

        parser = AlpacaSignalParser()
        parsed = parser.parse(signal_data)

        signal_log.symbol = parsed.get('symbol')
        signal_log.action = parsed.get('side')
        signal_log.parsed_data = json.dumps(parsed)

        log_info('webhook', f'Signal received: {parsed["symbol"]} {parsed["side"]}', category='signal', symbol=parsed.get('symbol'), extra_data={'raw': raw_data[:500]})

        try:
            from watchlist_service import on_signal_received
            on_signal_received(parsed['symbol'], source_broker='alpaca')
        except Exception as wl_err:
            logger.debug(f"Watchlist update failed: {wl_err}")

        side_enum = AlpacaSide.BUY if parsed['side'] == 'buy' else AlpacaSide.SELL
        order_type_str = parsed.get('order_type', 'limit')
        order_type_map = {
            'limit': AlpacaOrderType.LIMIT,
            'market': AlpacaOrderType.MARKET,
            'stop': AlpacaOrderType.STOP,
            'stop_limit': AlpacaOrderType.STOP_LIMIT,
        }
        order_type_enum = order_type_map.get(order_type_str, AlpacaOrderType.LIMIT)

        quantity = parsed['quantity']
        if quantity == 'all':
            quantity = 0

        trade = AlpacaTrade(
            symbol=parsed['symbol'],
            side=side_enum,
            quantity=quantity,
            price=parsed.get('price'),
            order_type=order_type_enum,
            status=AlpacaOrderStatus.PENDING,
            signal_data=raw_data,
            stop_loss_price=round(float(parsed['stop_loss']), 2) if parsed.get('stop_loss') else None,
            take_profit_price=round(float(parsed['take_profit']), 2) if parsed.get('take_profit') else None,
            is_close_position=parsed.get('is_close', False),
            extended_hours=parsed.get('extended_hours', False),
            reference_price=parsed.get('reference_price'),
            signal_timeframe=parsed.get('timeframe'),
        )
        db.session.add(trade)
        db.session.flush()

        signal_log.trade_id = trade.id
        signal_log.status = 'parsed'

        try:
            is_close = parsed.get('is_close', False)
            if is_close:
                from alpaca.models import AlpacaOrderTracker as OT, AlpacaOrderRole as OR

                pending_exits = OT.query.filter(
                    OT.symbol == parsed['symbol'],
                    OT.role.in_([OR.EXIT_SIGNAL, OR.EXIT_TRAILING]),
                    OT.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD', 'PARTIALLY_FILLED']),
                ).all()

                from alpaca.client import AlpacaClient as AC
                cancel_client = AC()

                if pending_exits:
                    for pe in pending_exits:
                        if pe.alpaca_order_id:
                            try:
                                cancel_result = cancel_client.cancel_order(pe.alpaca_order_id)
                                if cancel_result.get('success'):
                                    pe.status = 'CANCELLED'
                                    logger.info(f"🔄 Auto-cancelled old exit order {pe.alpaca_order_id} (role={pe.role.value}) for {parsed['symbol']} - new close signal arrived")
                                else:
                                    try:
                                        broker_order = cancel_client._request('GET', f'/v2/orders/{pe.alpaca_order_id}')
                                        real_status = (broker_order.get('status', '') or '').upper()
                                        if real_status == 'FILLED':
                                            pe.status = 'FILLED'
                                            filled_qty = broker_order.get('filled_qty')
                                            filled_price = broker_order.get('filled_avg_price')
                                            if filled_qty:
                                                pe.filled_quantity = float(filled_qty)
                                            logger.info(f"🔄 Old exit order {pe.alpaca_order_id} already FILLED at broker for {parsed['symbol']} (qty={filled_qty}, price={filled_price})")
                                        elif real_status in ('CANCELED', 'CANCELLED', 'EXPIRED', 'REJECTED'):
                                            pe.status = 'CANCELLED'
                                            logger.info(f"🔄 Old exit order {pe.alpaca_order_id} already {real_status} at broker")
                                        else:
                                            pe.status = 'CANCELLED'
                                            logger.warning(f"⚠️ Old exit order {pe.alpaca_order_id} broker status={real_status}, marking CANCELLED")
                                    except Exception as fetch_err:
                                        pe.status = 'CANCELLED'
                                        logger.warning(f"⚠️ Could not fetch broker status for {pe.alpaca_order_id}: {fetch_err}")
                            except Exception as ce:
                                pe.status = 'CANCELLED'
                                logger.warning(f"⚠️ Failed to cancel old exit order {pe.alpaca_order_id}: {ce}")
                        else:
                            pe.status = 'CANCELLED'

                    old_exit_trades = AlpacaTrade.query.filter(
                        AlpacaTrade.symbol == parsed['symbol'],
                        AlpacaTrade.is_close_position == True,
                        AlpacaTrade.status.in_([AlpacaOrderStatus.NEW, AlpacaOrderStatus.PENDING, AlpacaOrderStatus.ACCEPTED]),
                        AlpacaTrade.alpaca_order_id.in_([pe.alpaca_order_id for pe in pending_exits if pe.alpaca_order_id])
                    ).all()
                    for old_trade in old_exit_trades:
                        matching_pe = next((pe for pe in pending_exits if pe.alpaca_order_id == old_trade.alpaca_order_id), None)
                        if matching_pe and matching_pe.status == 'FILLED':
                            old_trade.status = AlpacaOrderStatus.FILLED
                            if hasattr(matching_pe, 'filled_quantity') and matching_pe.filled_quantity:
                                old_trade.filled_quantity = matching_pe.filled_quantity
                        else:
                            old_trade.status = AlpacaOrderStatus.CANCELLED
                    db.session.flush()
                    logger.info(f"🔄 [{parsed['symbol']}] Processed {len(pending_exits)} old exit order(s), placing new close order")

                try:
                    broker_orders = cancel_client._request('GET', f'/v2/orders?status=open&symbols={parsed["symbol"]}&limit=50')
                    if broker_orders:
                        for bo in broker_orders:
                            bo_id = bo.get('id', '')
                            bo_side = bo.get('side', '')
                            bo_type = bo.get('type', '')
                            bo_status = bo.get('status', '')
                            if bo_status == 'partially_filled':
                                logger.warning(f"⚠️ Skipping partially filled order {bo_id} for {parsed['symbol']}")
                                continue
                            try:
                                cancel_client.cancel_order(bo_id)
                                logger.info(f"🔧 Auto-cancelled broker order {bo_id} ({bo_type}/{bo_side}) for {parsed['symbol']} webhook close")
                            except Exception as ce:
                                logger.warning(f"⚠️ Failed to cancel broker order {bo_id}: {ce}")

                            tracker = OT.query.filter_by(alpaca_order_id=bo_id).first()
                            if tracker and tracker.status not in ('FILLED', 'CANCELLED', 'EXPIRED', 'REJECTED'):
                                tracker.status = 'CANCELLED'
                        db.session.flush()
                        import time
                        time.sleep(0.3)
                except Exception as e:
                    logger.warning(f"⚠️ Error fetching/cancelling open orders for {parsed['symbol']}: {e}")

            from alpaca.client import AlpacaClient
            client = AlpacaClient()
            result = client.place_order(parsed)

            if result.get('success'):
                trade.alpaca_order_id = result.get('order_id')
                trade.client_order_id = result.get('client_order_id')
                trade.status = AlpacaOrderStatus.NEW
                trade.alpaca_response = json.dumps(result.get('order_data', {}))
                actual_qty = float(result.get('order_data', {}).get('qty', 0) or 0)
                if actual_qty > 0 and trade.quantity == 0:
                    trade.quantity = actual_qty
                signal_log.status = 'executed'

                role = AlpacaOrderRole.EXIT_SIGNAL if is_close else AlpacaOrderRole.ENTRY

                entry_signal = None
                if is_close:
                    from alpaca.models import AlpacaTrade as AT
                    entry_trade = AT.query.filter_by(
                        symbol=parsed['symbol'],
                        is_close_position=False,
                    ).order_by(AT.created_at.desc()).first()
                    if entry_trade:
                        entry_signal = entry_trade.signal_data
                else:
                    entry_signal = trade.signal_data

                register_order(
                    alpaca_order_id=result.get('order_id'),
                    symbol=parsed['symbol'],
                    role=role,
                    side=parsed['side'],
                    quantity=float(result.get('order_data', {}).get('qty', 0) or 0),
                    order_type=order_type_str,
                    limit_price=parsed.get('price'),
                    client_order_id=result.get('client_order_id'),
                    trade_id=trade.id,
                    signal_content=entry_signal,
                )

                order_data = result.get('order_data', {})
                legs = order_data.get('legs') or []
                for leg in legs:
                    leg_id = leg.get('id')
                    leg_type = leg.get('type', '')
                    if leg_id:
                        if 'stop' in leg_type.lower():
                            leg_role = AlpacaOrderRole.STOP_LOSS
                        else:
                            leg_role = AlpacaOrderRole.TAKE_PROFIT
                        register_order(
                            alpaca_order_id=leg_id,
                            symbol=parsed['symbol'],
                            role=leg_role,
                            side='sell' if parsed['side'] == 'buy' else 'buy',
                            quantity=float(result.get('order_data', {}).get('qty', 0) or 0),
                            order_type=leg_type,
                            stop_price=float(leg.get('stop_price', 0) or 0) if leg.get('stop_price') else None,
                            limit_price=float(leg.get('limit_price', 0) or 0) if leg.get('limit_price') else None,
                            parent_order_id=result.get('order_id'),
                            trade_id=trade.id,
                            leg_role=leg_role.value,
                        )

                trade.needs_auto_protection = bool(
                    not is_close and (trade.stop_loss_price or trade.take_profit_price)
                    and not legs
                )

                logger.info(f"Alpaca order placed: {result.get('order_id')} for {parsed['symbol']}")
                log_info('webhook', f'Order placed: {result.get("order_id")}', category='order', symbol=parsed['symbol'], extra_data={'order_id': result.get('order_id'), 'side': parsed['side']})

                try:
                    from alpaca.discord_notifier import alpaca_discord
                    alpaca_discord.send_signal_notification(parsed, 'executed')
                except Exception as de:
                    logger.debug(f"Discord notification error: {de}")
            else:
                trade.status = AlpacaOrderStatus.REJECTED
                trade.error_message = result.get('error', 'Unknown error')
                signal_log.status = 'error'
                signal_log.error_message = result.get('error')
                logger.error(f"Alpaca order failed: {result.get('error')}")
                log_error('webhook', f'Order failed: {result.get("error")}', category='order', symbol=parsed['symbol'])

                try:
                    from alpaca.discord_notifier import alpaca_discord
                    alpaca_discord.send_signal_notification(parsed, 'error', error=result.get('error'))
                except Exception as de:
                    logger.debug(f"Discord notification error: {de}")
        except Exception as e:
            trade.status = AlpacaOrderStatus.REJECTED
            trade.error_message = str(e)
            signal_log.status = 'error'
            signal_log.error_message = str(e)
            logger.error(f"Alpaca order execution error: {str(e)}")
            log_error('webhook', f'Execution error: {str(e)}', category='order', symbol=parsed.get('symbol', 'unknown'))

            try:
                from alpaca.discord_notifier import alpaca_discord
                alpaca_discord.send_signal_notification(parsed, 'error', error=str(e))
            except Exception as de:
                logger.debug(f"Discord notification error: {de}")

        db.session.commit()

        return jsonify({
            'status': 'ok',
            'trade_id': trade.id,
            'order_id': trade.alpaca_order_id,
            'symbol': parsed['symbol'],
            'side': parsed['side'],
            'quantity': parsed['quantity'],
        })

    except Exception as e:
        db.session.rollback()
        logger.error(f"Alpaca webhook error: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@alpaca_bp.route('/signals')
def alpaca_signals():
    from alpaca.models import AlpacaSignalLog

    page = request.args.get('page', 1, type=int)
    symbol_filter = request.args.get('symbol', '').strip()
    status_filter = request.args.get('status', '').strip()
    action_filter = request.args.get('action', '').strip()

    query = AlpacaSignalLog.query

    if symbol_filter:
        query = query.filter(AlpacaSignalLog.symbol.ilike(f'%{symbol_filter}%'))
    if status_filter:
        query = query.filter(AlpacaSignalLog.status == status_filter)
    if action_filter:
        query = query.filter(AlpacaSignalLog.action == action_filter)

    signals = query.order_by(
        AlpacaSignalLog.created_at.desc()
    ).paginate(page=page, per_page=50, error_out=False)

    total_signals = AlpacaSignalLog.query.count()
    executed_count = AlpacaSignalLog.query.filter_by(status='executed').count()
    error_count = AlpacaSignalLog.query.filter_by(status='error').count()
    success_rate = (executed_count / total_signals * 100) if total_signals > 0 else 0

    return render_template('alpaca/signals.html',
                           signals=signals,
                           symbol_filter=symbol_filter,
                           status_filter=status_filter,
                           action_filter=action_filter,
                           total_signals=total_signals,
                           executed_count=executed_count,
                           error_count=error_count,
                           success_rate=success_rate)


@alpaca_bp.route('/signals/<int:signal_id>')
def alpaca_signal_detail(signal_id):
    from alpaca.models import AlpacaSignalLog, AlpacaTrade

    signal = AlpacaSignalLog.query.get_or_404(signal_id)
    trade = AlpacaTrade.query.get(signal.trade_id) if signal.trade_id else None

    parsed = None
    if signal.parsed_data:
        try:
            parsed = json.loads(signal.parsed_data)
        except Exception:
            parsed = None

    raw_formatted = None
    if signal.raw_data:
        try:
            raw_formatted = json.dumps(json.loads(signal.raw_data), indent=2)
        except Exception:
            raw_formatted = signal.raw_data

    return render_template('alpaca/signal_detail.html',
                           signal=signal,
                           trade=trade,
                           parsed=parsed,
                           raw_formatted=raw_formatted)


@alpaca_bp.route('/sync_holdings', methods=['POST'])
def alpaca_sync_holdings():
    from alpaca.holdings_sync import sync_holdings

    result = sync_holdings()
    if result.get('success'):
        flash(f"Holdings synced: {result.get('total', 0)} positions", 'success')
    else:
        flash(f"Sync failed: {result.get('error', 'Unknown error')}", 'danger')

    return redirect(url_for('alpaca.alpaca_index'))


@alpaca_bp.route('/poll_orders', methods=['POST'])
def alpaca_poll_orders():
    from alpaca.order_tracker import poll_all_pending_orders

    result = poll_all_pending_orders()
    flash(f"Polled {result['polled']} orders: {result['filled']} filled", 'info')

    return redirect(url_for('alpaca.alpaca_index'))


@alpaca_bp.route('/orders')
def alpaca_orders():
    from alpaca.models import AlpacaOrderTracker, AlpacaPosition

    page = request.args.get('page', 1, type=int)
    status_filter = request.args.get('status', '')
    symbol_filter = request.args.get('symbol', '')
    role_filter = request.args.get('role', '')

    query = AlpacaOrderTracker.query

    if status_filter:
        query = query.filter_by(status=status_filter.upper())
    if symbol_filter:
        query = query.filter(AlpacaOrderTracker.symbol.ilike(f'%{symbol_filter}%'))
    if role_filter:
        query = query.filter(AlpacaOrderTracker.role == role_filter)

    orders = query.order_by(AlpacaOrderTracker.created_at.desc()).paginate(
        page=page, per_page=50, error_out=False
    )

    position_map = {}
    for order in orders.items:
        if order.position_id and order.position_id not in position_map:
            from alpaca.models import AlpacaPosition
            pos = AlpacaPosition.query.get(order.position_id)
            if pos:
                position_map[order.position_id] = pos

    return render_template('alpaca/orders.html',
                           orders=orders,
                           status_filter=status_filter,
                           symbol_filter=symbol_filter,
                           role_filter=role_filter,
                           position_map=position_map)


@alpaca_bp.route('/trailing_stops')
def alpaca_trailing_stops():
    from alpaca.models import AlpacaTrailingStopPosition, AlpacaTrailingStopLog, AlpacaOCOGroup, AlpacaOCOStatus, AlpacaTrailingStopConfig

    active_stops = AlpacaTrailingStopPosition.query.filter_by(is_active=True).order_by(
        AlpacaTrailingStopPosition.created_at.desc()
    ).all()

    inactive_stops = AlpacaTrailingStopPosition.query.filter_by(is_active=False).order_by(
        AlpacaTrailingStopPosition.updated_at.desc()
    ).limit(20).all()

    active_ocos = AlpacaOCOGroup.query.filter_by(status=AlpacaOCOStatus.ACTIVE).order_by(
        AlpacaOCOGroup.created_at.desc()
    ).all()

    from sqlalchemy.orm import joinedload
    recent_logs = AlpacaTrailingStopLog.query.options(
        joinedload(AlpacaTrailingStopLog.trailing_stop)
    ).order_by(
        AlpacaTrailingStopLog.created_at.desc()
    ).limit(50).all()

    ts_config = AlpacaTrailingStopConfig.query.first()

    from alpaca.trailing_stop_scheduler import get_scheduler_status
    scheduler_status = get_scheduler_status()

    return render_template('alpaca/trailing_stops.html',
                           active_stops=active_stops,
                           inactive_stops=inactive_stops,
                           active_ocos=active_ocos,
                           recent_logs=recent_logs,
                           ts_config=ts_config,
                           scheduler_status=scheduler_status)


@alpaca_bp.route('/trailing_stops/<int:ts_id>')
def alpaca_trailing_stop_detail(ts_id):
    from alpaca.models import AlpacaTrailingStopPosition, AlpacaTrailingStopLog, AlpacaOCOGroup, AlpacaOCOStatus

    ts = AlpacaTrailingStopPosition.query.get_or_404(ts_id)

    logs = AlpacaTrailingStopLog.query.filter_by(
        trailing_stop_id=ts_id
    ).order_by(AlpacaTrailingStopLog.created_at.desc()).all()

    oco = AlpacaOCOGroup.query.filter_by(
        symbol=ts.symbol,
        status=AlpacaOCOStatus.ACTIVE
    ).first()

    return render_template('alpaca/trailing_stop_detail.html',
                           ts=ts, logs=logs, oco=oco)


@alpaca_bp.route('/trailing_stops/<int:ts_id>/deactivate', methods=['POST'])
def alpaca_deactivate_trailing_stop(ts_id):
    from alpaca.models import AlpacaTrailingStopPosition

    ts = AlpacaTrailingStopPosition.query.get_or_404(ts_id)
    from alpaca.trailing_stop_engine import deactivate_trailing_stop
    deactivate_trailing_stop(ts.symbol, reason='manual_deactivate')
    flash(f'Trailing stop for {ts.symbol} deactivated', 'warning')
    return redirect(url_for('alpaca.alpaca_trailing_stops'))


@alpaca_bp.route('/trailing_stops/run_cycle', methods=['POST'])
def alpaca_run_trailing_stop_cycle():
    from alpaca.trailing_stop_engine import process_all_active_positions
    result = process_all_active_positions()
    flash(f"Trailing stop cycle: processed {result.get('processed', 0)} positions", 'info')
    return redirect(url_for('alpaca.alpaca_trailing_stops'))


@alpaca_bp.route('/api/trailing-stops/data')
def api_alpaca_trailing_stops_data():
    from alpaca.models import AlpacaTrailingStopPosition
    from alpaca.trailing_stop_engine import _fetch_current_price
    from flask import jsonify

    try:
        positions = AlpacaTrailingStopPosition.query.filter_by(is_active=True).all()
        stops_data = []
        for ts in positions:
            current_price = _fetch_current_price(ts.symbol)
            if current_price is None:
                current_price = ts.current_price

            profit_pct = None
            if current_price and ts.entry_price:
                if ts.side == 'long':
                    profit_pct = round((current_price - ts.entry_price) / ts.entry_price * 100, 2)
                else:
                    profit_pct = round((ts.entry_price - current_price) / ts.entry_price * 100, 2)

            stops_data.append({
                'id': ts.id,
                'symbol': ts.symbol,
                'side': ts.side,
                'entry_price': round(ts.entry_price, 2) if ts.entry_price else None,
                'current_price': round(current_price, 2) if current_price else None,
                'highest_price': round(ts.highest_price, 2) if ts.highest_price else None,
                'trailing_stop_price': round(ts.trailing_stop_price, 2) if ts.trailing_stop_price else None,
                'stop_loss_price': round(ts.stop_loss_price, 2) if ts.stop_loss_price else None,
                'phase': ts.phase,
                'profit_pct': profit_pct,
                'quantity': ts.quantity,
            })

        return jsonify({'success': True, 'stops': stops_data})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@alpaca_bp.route('/reconciliation')
def alpaca_reconciliation():
    from alpaca.reconciliation import get_reconciliation_summary
    from alpaca.models import AlpacaFilledOrder, AlpacaPosition, AlpacaPositionStatus
    from alpaca.models import AlpacaHolding

    summary = get_reconciliation_summary()

    unreconciled_fills = AlpacaFilledOrder.query.filter_by(
        reconciled=False
    ).order_by(AlpacaFilledOrder.filled_at.desc()).limit(100).all()

    open_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.OPEN).all()

    ghost_positions = []
    ghost_source = 'alpaca_api'
    ghost_error = None
    alpaca_symbol_count = 0
    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        if client.api_key:
            live_positions = client.get_positions() or []
            alpaca_symbols = {p.get('symbol', '') for p in live_positions}
            alpaca_symbol_count = len(alpaca_symbols)
        else:
            alpaca_symbols = {h.symbol for h in AlpacaHolding.query.all()}
            alpaca_symbol_count = len(alpaca_symbols)
            ghost_source = 'holdings_cache'
            ghost_error = 'No Alpaca API key configured, using cached holdings data'
    except Exception as e:
        alpaca_symbols = {h.symbol for h in AlpacaHolding.query.all()}
        alpaca_symbol_count = len(alpaca_symbols)
        ghost_source = 'holdings_cache'
        ghost_error = f'Alpaca API error: {str(e)}, using cached holdings data'

    for pos in open_positions:
        remaining = pos.total_entry_quantity - (pos.total_exit_quantity or 0)
        if pos.symbol not in alpaca_symbols and remaining > 0.001:
            ghost_positions.append({
                'id': pos.id,
                'symbol': pos.symbol,
                'side': pos.side,
                'remaining': remaining,
                'avg_entry_price': pos.avg_entry_price,
                'created_at': pos.created_at,
            })

    return render_template('alpaca/reconciliation.html',
                           summary=summary,
                           unreconciled_fills=unreconciled_fills,
                           ghost_positions=ghost_positions,
                           ghost_source=ghost_source,
                           ghost_error=ghost_error,
                           open_count=len(open_positions),
                           holdings_count=alpaca_symbol_count)


@alpaca_bp.route('/reconciliation/action', methods=['POST'])
def alpaca_reconciliation_action():
    action = request.form.get('action', '')

    if action == 'fetch_fills':
        from alpaca.reconciliation import fetch_fills_only
        total, new = fetch_fills_only()
        flash(f'Fetched {total} activities, {new} new fills stored', 'success')

    elif action == 'reconcile_today':
        from alpaca.reconciliation import reconcile_today
        run = reconcile_today(run_type='manual')
        if run.status == 'completed':
            flash(f'Reconciliation completed: {run.positions_matched or 0} matched, '
                  f'{run.records_corrected or 0} corrected, {run.records_created or 0} created', 'success')
        elif run.status == 'skipped':
            flash('Reconciliation skipped - another run is in progress', 'warning')
        else:
            flash(f'Reconciliation failed: {run.error_message or "unknown error"}', 'error')

    elif action == 'reconcile_history':
        days_back = int(request.form.get('days_back', 30))
        from alpaca.reconciliation import reconcile_history
        run = reconcile_history(days_back=days_back)
        if run.status == 'completed':
            flash(f'History reconciliation ({days_back} days): {run.positions_matched or 0} matched, '
                  f'{run.records_corrected or 0} corrected, {run.records_created or 0} created', 'success')
        elif run.status == 'skipped':
            flash('Reconciliation skipped - another run is in progress', 'warning')
        else:
            flash(f'History reconciliation failed: {run.error_message or "unknown error"}', 'error')

    elif action == 'backfill':
        clear_existing = request.form.get('clear_existing') == 'on'
        from alpaca.position_backfill import rebuild_positions_from_fills
        result = rebuild_positions_from_fills(clear_existing=clear_existing)
        if result['status'] == 'completed':
            flash(f'Backfill completed: {result["positions_created"]} positions, '
                  f'{result["entry_legs_created"]} entries, {result["exit_legs_created"]} exits', 'success')
        else:
            flash(f'Backfill failed: check details', 'error')

    elif action == 'ghost_reconcile':
        from alpaca.models import AlpacaPosition, AlpacaPositionStatus, AlpacaExitMethod
        from alpaca.models import AlpacaOrderTracker, AlpacaOrderRole, AlpacaTrailingStopPosition
        from alpaca.position_service import find_all_open_positions, add_exit_leg
        from alpaca.client import AlpacaClient

        try:
            open_positions = find_all_open_positions()
            if not open_positions:
                flash('No open positions found in database.', 'info')
            else:
                client = AlpacaClient()
                if not client.api_key:
                    flash('Alpaca API key not configured. Cannot run ghost reconciliation.', 'error')
                else:
                    try:
                        alpaca_positions = client.get_positions()
                    except Exception as e:
                        flash(f'Failed to get Alpaca positions: {str(e)}', 'error')
                        return redirect(url_for('alpaca.alpaca_reconciliation'))

                    alpaca_symbols = {p.get('symbol', '') for p in (alpaca_positions or [])}

                    ghost_count = 0
                    error_count = 0
                    skipped = []
                    closed_symbols = []
                    errors = []

                    for pos in open_positions:
                        if pos.symbol in alpaca_symbols:
                            continue

                        remaining = pos.total_entry_quantity - (pos.total_exit_quantity or 0)
                        if remaining <= 0.001:
                            skipped.append(f"{pos.symbol}(no remaining)")
                            continue

                        has_pending = AlpacaOrderTracker.query.filter(
                            AlpacaOrderTracker.symbol == pos.symbol,
                            AlpacaOrderTracker.role.in_([AlpacaOrderRole.EXIT_SIGNAL, AlpacaOrderRole.EXIT_TRAILING, AlpacaOrderRole.ENTRY]),
                            AlpacaOrderTracker.status.in_(['NEW', 'ACCEPTED', 'PENDING', 'HELD', 'PARTIALLY_FILLED']),
                        ).first()
                        if has_pending:
                            skipped.append(f"{pos.symbol}(pending {has_pending.role})")
                            continue

                        try:
                            last_price = pos.avg_entry_price or 0
                            filled_exit = AlpacaOrderTracker.query.filter(
                                AlpacaOrderTracker.symbol == pos.symbol,
                                AlpacaOrderTracker.role.in_([AlpacaOrderRole.EXIT_SIGNAL, AlpacaOrderRole.EXIT_TRAILING, AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT]),
                                AlpacaOrderTracker.status == 'FILLED',
                            ).order_by(AlpacaOrderTracker.fill_time.desc()).first()
                            if filled_exit and filled_exit.avg_fill_price:
                                last_price = filled_exit.avg_fill_price

                            add_exit_leg(
                                position=pos, price=last_price, quantity=remaining,
                                filled_at=datetime.utcnow(), exit_method=AlpacaExitMethod.EXTERNAL,
                            )

                            try:
                                from alpaca.order_tracker import ensure_tracker_for_fill
                                exit_side = 'sell' if pos.side == 'long' else 'buy'
                                ghost_exit_id = filled_exit.alpaca_order_id if filled_exit else f"manual_ghost_{pos.id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
                                ensure_tracker_for_fill(
                                    alpaca_order_id=ghost_exit_id,
                                    symbol=pos.symbol,
                                    role=AlpacaOrderRole.EXIT_SIGNAL,
                                    side=exit_side,
                                    quantity=remaining,
                                    fill_price=last_price,
                                    source='manual_ghost_reconciliation',
                                )
                            except Exception:
                                pass

                            active_ts = AlpacaTrailingStopPosition.query.filter_by(symbol=pos.symbol, is_active=True).first()
                            if active_ts:
                                active_ts.is_active = False
                                active_ts.is_triggered = True
                                active_ts.triggered_at = datetime.utcnow()
                                active_ts.trigger_reason = "Ghost reconciliation: position closed externally"

                            db.session.commit()
                            ghost_count += 1
                            closed_symbols.append(f"{pos.symbol}({remaining}@${last_price:.2f})")
                        except Exception as e:
                            error_count += 1
                            errors.append(f"{pos.symbol}: {str(e)}")
                            try:
                                db.session.rollback()
                            except Exception:
                                pass

                    msg_parts = [f"DB: {len(open_positions)} open, Alpaca: {len(alpaca_symbols)} positions ({', '.join(alpaca_symbols)})."]
                    if closed_symbols:
                        msg_parts.append(f"Closed {ghost_count}: {', '.join(closed_symbols)}.")
                    if skipped:
                        msg_parts.append(f"Skipped: {', '.join(skipped)}.")
                    if errors:
                        msg_parts.append(f"Errors ({error_count}): {'; '.join(errors)}.")
                    if not closed_symbols and not errors:
                        msg_parts.append("No ghost positions found to close.")

                    flash_type = 'success' if ghost_count > 0 and error_count == 0 else ('warning' if error_count > 0 else 'info')
                    flash(' '.join(msg_parts), flash_type)
        except Exception as e:
            logger.error(f"Manual ghost reconciliation error: {e}", exc_info=True)
            flash(f'Ghost reconciliation error: {str(e)}', 'error')

    else:
        flash('Unknown action', 'error')

    return redirect(url_for('alpaca.alpaca_reconciliation'))


@alpaca_bp.route('/analytics')
def alpaca_analytics():
    from alpaca.models import (AlpacaPosition, AlpacaPositionStatus, AlpacaPositionLeg,
                               AlpacaLegType, AlpacaExitMethod, AlpacaHolding,
                               AlpacaTrailingStopPosition)
    from datetime import datetime, timedelta
    import pytz
    import json

    signal_grade_filter = request.args.get('signal_grade', '')
    exit_method_filter = request.args.get('exit_method', '')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    symbol_search = request.args.get('symbol', '').strip().upper()
    status_filter = request.args.get('status', '')
    page = request.args.get('page', 1, type=int)
    per_page = 30

    query = AlpacaPositionLeg.query.join(AlpacaPosition).filter(
        AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD])
    )

    if status_filter == 'open':
        query = query.filter(AlpacaPosition.status == AlpacaPositionStatus.OPEN)
    elif status_filter == 'closed':
        query = query.filter(AlpacaPosition.status == AlpacaPositionStatus.CLOSED)

    if symbol_search:
        query = query.filter(AlpacaPosition.symbol.ilike(f'%{symbol_search}%'))

    if signal_grade_filter:
        query = query.filter(AlpacaPositionLeg.signal_grade == signal_grade_filter)

    if exit_method_filter:
        try:
            em = AlpacaExitMethod(exit_method_filter)
            exit_leg_exists = AlpacaPositionLeg.query.filter(
                AlpacaPositionLeg.position_id == AlpacaPosition.id,
                AlpacaPositionLeg.leg_type == AlpacaLegType.EXIT,
                AlpacaPositionLeg.exit_method == em
            ).exists()
            query = query.filter(exit_leg_exists)
        except ValueError:
            pass

    if start_date:
        try:
            sd = datetime.strptime(start_date, '%Y-%m-%d')
            query = query.filter(AlpacaPositionLeg.filled_at >= sd)
        except ValueError:
            pass

    if end_date:
        try:
            ed = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            query = query.filter(AlpacaPositionLeg.filled_at < ed)
        except ValueError:
            pass

    query = query.order_by(AlpacaPositionLeg.filled_at.desc().nullslast(), AlpacaPositionLeg.created_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    entry_legs = pagination.items

    eastern = pytz.timezone('US/Eastern')
    holdings_map = {}
    for h in AlpacaHolding.query.all():
        holdings_map[h.symbol] = h

    position_ids = list(set(leg.position_id for leg in entry_legs))
    positions_map = {}
    if position_ids:
        for pos in AlpacaPosition.query.filter(AlpacaPosition.id.in_(position_ids)).all():
            positions_map[pos.id] = pos

    exit_legs_map = {}
    if position_ids:
        all_exit_legs = AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.position_id.in_(position_ids),
            AlpacaPositionLeg.leg_type == AlpacaLegType.EXIT
        ).order_by(AlpacaPositionLeg.filled_at.asc()).all()
        for el in all_exit_legs:
            exit_legs_map.setdefault(el.position_id, []).append(el)

    ts_map = {}
    ts_ids = [p.trailing_stop_id for p in positions_map.values() if p.trailing_stop_id]
    if ts_ids:
        trailing_stops = AlpacaTrailingStopPosition.query.filter(
            AlpacaTrailingStopPosition.id.in_(ts_ids)
        ).all()
        for ts in trailing_stops:
            ts_map[ts.id] = ts

    display_entries = []
    for leg in entry_legs:
        pos = positions_map.get(leg.position_id)
        if not pos:
            continue
        is_open = pos.status == AlpacaPositionStatus.OPEN

        entry_time_et = None
        if leg.filled_at:
            try:
                entry_time_et = leg.filled_at.replace(tzinfo=pytz.UTC).astimezone(eastern)
            except:
                entry_time_et = leg.filled_at

        pos_exit_legs = exit_legs_map.get(pos.id, [])
        exit_price = pos.avg_exit_price
        exit_time_et = None
        primary_exit_method = None
        if pos_exit_legs:
            last_exit = pos_exit_legs[-1]
            if last_exit.filled_at:
                try:
                    exit_time_et = last_exit.filled_at.replace(tzinfo=pytz.UTC).astimezone(eastern)
                except:
                    exit_time_et = last_exit.filled_at
            for el in pos_exit_legs:
                if el.exit_method:
                    primary_exit_method = el.exit_method
                    break

        display_signal_type = _alpaca_extract_signal_type(leg.signal_content, leg.signal_indicator)

        current_price = None
        unrealized_pnl = None
        unrealized_pnl_pct = None
        if is_open:
            holding = holdings_map.get(pos.symbol)
            if holding:
                current_price = holding.current_price
                unrealized_pnl = holding.unrealized_pnl
                unrealized_pnl_pct = holding.unrealized_pnl_pct

        hold_duration_seconds = None
        if not is_open and pos.opened_at and pos.closed_at:
            hold_duration_seconds = (pos.closed_at - pos.opened_at).total_seconds()

        pnl_amount = pos.realized_pnl if not is_open else None
        pnl_percent = None
        if pnl_amount is not None and pos.avg_entry_price and pos.total_entry_quantity:
            cost_basis = pos.avg_entry_price * pos.total_entry_quantity
            if cost_basis > 0:
                pnl_percent = (pnl_amount / cost_basis) * 100

        display_entry = {
            'id': leg.id,
            'position_id': pos.id,
            'position_key': pos.position_key,
            'symbol': pos.symbol,
            'side': pos.side,
            'quantity': leg.quantity,
            'is_scaling': leg.leg_type == AlpacaLegType.ADD,
            'is_open': is_open,
            'entry_price': leg.price,
            'entry_time_et': entry_time_et,
            'entry_order_id': leg.alpaca_order_id,
            'signal_grade': leg.signal_grade,
            'signal_score': leg.signal_score,
            'signal_timeframe': leg.signal_timeframe,
            'display_signal_type': display_signal_type,
            'signal_indicator': leg.signal_indicator,
            'raw_json': leg.signal_content,
            'stop_price': leg.stop_price or (ts_map.get(pos.trailing_stop_id, None) and ts_map[pos.trailing_stop_id].stop_loss_price),
            'take_profit_price': leg.take_profit_price or (ts_map.get(pos.trailing_stop_id, None) and ts_map[pos.trailing_stop_id].take_profit_price),
            'current_price': current_price,
            'unrealized_pnl': unrealized_pnl,
            'unrealized_pnl_pct': unrealized_pnl_pct,
            'exit_price': exit_price,
            'exit_time_et': exit_time_et,
            'exit_method': primary_exit_method,
            'pnl_amount': pnl_amount,
            'pnl_percent': pnl_percent,
            'hold_duration_seconds': hold_duration_seconds,
        }
        display_entries.append(display_entry)

    stats_query = AlpacaPosition.query.filter_by(
        status=AlpacaPositionStatus.CLOSED
    ).filter(AlpacaPosition.realized_pnl != None)

    if symbol_search:
        stats_query = stats_query.filter(AlpacaPosition.symbol.ilike(f'%{symbol_search}%'))

    if start_date:
        try:
            sd = datetime.strptime(start_date, '%Y-%m-%d')
            stats_query = stats_query.filter(AlpacaPosition.closed_at >= sd)
        except ValueError:
            pass

    if end_date:
        try:
            ed = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            stats_query = stats_query.filter(AlpacaPosition.closed_at < ed)
        except ValueError:
            pass

    if signal_grade_filter:
        stats_query = stats_query.filter(
            AlpacaPosition.id.in_(
                db.session.query(AlpacaPositionLeg.position_id).filter(
                    AlpacaPositionLeg.leg_type == AlpacaLegType.ENTRY,
                    AlpacaPositionLeg.signal_grade == signal_grade_filter
                )
            )
        )

    if exit_method_filter:
        try:
            em = AlpacaExitMethod(exit_method_filter)
            stats_query = stats_query.filter(
                AlpacaPosition.id.in_(
                    db.session.query(AlpacaPositionLeg.position_id).filter(
                        AlpacaPositionLeg.leg_type == AlpacaLegType.EXIT,
                        AlpacaPositionLeg.exit_method == em
                    )
                )
            )
        except ValueError:
            pass

    closed_positions = stats_query.all()

    all_closed_ids = [p.id for p in closed_positions]
    all_entry_legs_for_stats = []
    all_exit_legs_for_stats = []
    if all_closed_ids:
        all_entry_legs_for_stats = AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.position_id.in_(all_closed_ids),
            AlpacaPositionLeg.leg_type == AlpacaLegType.ENTRY
        ).order_by(AlpacaPositionLeg.filled_at.asc().nullslast(), AlpacaPositionLeg.created_at.asc()).all()
        all_exit_legs_for_stats = AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.position_id.in_(all_closed_ids),
            AlpacaPositionLeg.leg_type == AlpacaLegType.EXIT,
            AlpacaPositionLeg.exit_method != None
        ).order_by(AlpacaPositionLeg.filled_at.asc().nullslast(), AlpacaPositionLeg.created_at.asc()).all()

    first_entry_by_pos = {}
    for el in all_entry_legs_for_stats:
        if el.position_id not in first_entry_by_pos:
            first_entry_by_pos[el.position_id] = el

    first_exit_by_pos = {}
    for el in all_exit_legs_for_stats:
        if el.position_id not in first_exit_by_pos:
            first_exit_by_pos[el.position_id] = el

    total_pnl = 0
    winning_pnls = []
    losing_pnls = []
    grade_data = {}
    exit_method_data = {}

    for pos in closed_positions:
        pnl = pos.realized_pnl
        total_pnl += pnl
        if pnl >= 0:
            winning_pnls.append(pnl)
        else:
            losing_pnls.append(pnl)

        first_entry = first_entry_by_pos.get(pos.id)
        grade_key = first_entry.signal_grade if first_entry and first_entry.signal_grade else 'Unknown'
        if grade_key not in grade_data:
            grade_data[grade_key] = {'count': 0, 'pnl': 0, 'wins': 0}
        grade_data[grade_key]['count'] += 1
        grade_data[grade_key]['pnl'] += pnl
        if pnl > 0:
            grade_data[grade_key]['wins'] += 1

        exit_leg = first_exit_by_pos.get(pos.id)
        method_key = exit_leg.exit_method.value if exit_leg else 'unknown'
        if method_key not in exit_method_data:
            exit_method_data[method_key] = {'count': 0, 'pnl': 0, 'wins': 0}
        exit_method_data[method_key]['count'] += 1
        exit_method_data[method_key]['pnl'] += pnl
        if pnl > 0:
            exit_method_data[method_key]['wins'] += 1

    total_closed = len(closed_positions)
    win_rate = (len(winning_pnls) / total_closed * 100) if total_closed else 0
    avg_win = sum(winning_pnls) / len(winning_pnls) if winning_pnls else 0
    avg_loss = abs(sum(losing_pnls) / len(losing_pnls)) if losing_pnls else 0
    gross_win = sum(winning_pnls) if winning_pnls else 0
    gross_loss = abs(sum(losing_pnls)) if losing_pnls else 0
    profit_factor = gross_win / gross_loss if gross_loss > 0 else 99999.0 if gross_win > 0 else 0.0

    grade_stats = {}
    for g in ['A', 'B', 'C', 'Unknown']:
        if g in grade_data:
            d = grade_data[g]
            grade_stats[g] = {
                'count': d['count'],
                'pnl': d['pnl'],
                'win_rate': d['wins'] / d['count'] * 100 if d['count'] else 0
            }

    exit_stats = {}
    for mk, md in exit_method_data.items():
        exit_stats[mk] = {
            'count': md['count'],
            'pnl': md['pnl'],
            'win_rate': md['wins'] / md['count'] * 100 if md['count'] else 0
        }

    return render_template('alpaca/analytics.html',
        trades=display_entries,
        pagination=pagination,
        signal_grade=signal_grade_filter,
        exit_method_filter=exit_method_filter,
        start_date=start_date,
        end_date=end_date,
        symbol_search=symbol_search,
        status_filter=status_filter,
        total_pnl=total_pnl,
        total_trades=total_closed,
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        profit_factor=profit_factor,
        grade_stats=grade_stats,
        exit_stats=exit_stats,
        AlpacaExitMethod=AlpacaExitMethod
    )


def _alpaca_extract_signal_type(entry_signal_content, fallback_type):
    import json
    if not entry_signal_content:
        return fallback_type or 'Unknown'
    content = entry_signal_content.strip()
    try:
        data = json.loads(content)
        if isinstance(data, dict):
            extras = data.get('extras', {})
            indicator = None
            if isinstance(extras, dict) and extras.get('indicator'):
                indicator = extras['indicator']
            elif data.get('indicator'):
                indicator = data['indicator']
            if indicator:
                parts = indicator.split()
                if len(parts) > 2:
                    return ' '.join(parts[:2])
                return indicator
            if data.get('signal'):
                return data['signal']
        return fallback_type or 'Unknown'
    except:
        pass
    return fallback_type or content[:30] if content else 'Unknown'


@alpaca_bp.route('/account')
def alpaca_account():
    from alpaca.models import AlpacaHolding
    from alpaca.holdings_sync import get_sync_status

    account_info = None
    error_msg = None
    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        if client.api_key:
            account_info = client.get_account()
    except Exception as e:
        error_msg = str(e)

    holdings = AlpacaHolding.query.order_by(AlpacaHolding.symbol).all()
    sync_status = get_sync_status()

    return render_template('alpaca/account.html',
                           account_info=account_info,
                           error_msg=error_msg,
                           holdings=holdings,
                           sync_status=sync_status)


@alpaca_bp.route('/logs')
def alpaca_system_logs():
    from alpaca.models import AlpacaSystemLog

    page = request.args.get('page', 1, type=int)
    per_page = 100

    filters = {
        'level': request.args.get('level', ''),
        'source': request.args.get('source', ''),
        'category': request.args.get('category', ''),
        'symbol': request.args.get('symbol', ''),
    }

    query = AlpacaSystemLog.query

    if filters['level']:
        query = query.filter(AlpacaSystemLog.level == filters['level'])
    if filters['source']:
        query = query.filter(AlpacaSystemLog.source == filters['source'])
    if filters['category']:
        query = query.filter(AlpacaSystemLog.category == filters['category'])
    if filters['symbol']:
        query = query.filter(AlpacaSystemLog.symbol.ilike(f"%{filters['symbol']}%"))

    total = query.count()
    total_pages = max(1, (total + per_page - 1) // per_page)

    logs = query.order_by(AlpacaSystemLog.timestamp.desc()).offset(
        (page - 1) * per_page
    ).limit(per_page).all()

    sources = [r[0] for r in db.session.query(AlpacaSystemLog.source).distinct().filter(
        AlpacaSystemLog.source.isnot(None)
    ).all()]
    categories = [r[0] for r in db.session.query(AlpacaSystemLog.category).distinct().filter(
        AlpacaSystemLog.category.isnot(None)
    ).all()]

    return render_template('alpaca/logs.html',
                           logs=logs,
                           total=total,
                           page=page,
                           total_pages=total_pages,
                           filters=filters,
                           sources=sources,
                           categories=categories)


@alpaca_bp.route('/logs/cleanup', methods=['POST'])
def alpaca_cleanup_logs():
    from alpaca.db_logger import cleanup_old_logs
    deleted = cleanup_old_logs(days=7)
    flash(f'Cleaned up {deleted} old log entries', 'success')
    return redirect(url_for('alpaca.alpaca_system_logs'))


@alpaca_bp.route('/monitor')
def alpaca_monitor():
    from alpaca.monitor_service import build_lifecycle, get_global_health, get_recent_closed_lifecycles

    symbol = request.args.get('symbol', '').strip().upper()
    order_id = request.args.get('order_id', '').strip()
    position_id = request.args.get('position_id', '', type=str).strip()

    lifecycles = None
    search_performed = False

    if symbol or order_id or position_id:
        search_performed = True
        pid = int(position_id) if position_id and position_id.isdigit() else None
        result = build_lifecycle(
            symbol=symbol if symbol else None,
            order_id=order_id if order_id else None,
            position_id=pid,
        )
        if result is None:
            lifecycles = []
        elif isinstance(result, list):
            lifecycles = result
        else:
            lifecycles = [result]

    health = get_global_health()

    recent_closed = []
    if not search_performed:
        recent_closed = get_recent_closed_lifecycles(limit=20)

    return render_template('alpaca/monitor.html',
                           symbol=symbol,
                           order_id=order_id,
                           position_id=position_id,
                           lifecycles=lifecycles,
                           search_performed=search_performed,
                           health=health,
                           recent_closed=recent_closed)


@alpaca_bp.route('/backfill-signal-data', methods=['POST'])
def alpaca_backfill_signal_data():
    from app import db
    from alpaca.models import (AlpacaPositionLeg, AlpacaLegType,
                               AlpacaOrderTracker, AlpacaTrade)
    from alpaca.signal_utils import parse_signal_fields
    from sqlalchemy import or_

    updated = 0
    skipped = 0
    no_data = 0
    grade_fixed = 0
    try:
        target_legs = AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD]),
            or_(
                AlpacaPositionLeg.signal_content == None,
                AlpacaPositionLeg.signal_grade == None,
            ),
        ).all()

        for leg in target_legs:
            if leg.signal_content and not leg.signal_grade:
                parsed = parse_signal_fields(leg.signal_content)
                if parsed['signal_grade']:
                    leg.signal_grade = parsed['signal_grade']
                    if parsed['signal_score'] is not None:
                        leg.signal_score = parsed['signal_score']
                    if parsed['signal_indicator'] and not leg.signal_indicator:
                        leg.signal_indicator = parsed['signal_indicator']
                    if parsed['signal_timeframe'] and not leg.signal_timeframe:
                        leg.signal_timeframe = parsed['signal_timeframe']
                    grade_fixed += 1
                continue

            if not leg.alpaca_order_id:
                no_data += 1
                continue

            order_id = leg.alpaca_order_id
            if order_id.endswith('_overflow'):
                order_id = order_id.replace('_overflow', '')

            tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=order_id).first()
            if not tracker:
                tracker = AlpacaOrderTracker.query.filter(
                    AlpacaOrderTracker.alpaca_order_id.like(f'{order_id[:12]}%')
                ).first()

            if not tracker or not tracker.trade_id:
                no_data += 1
                continue

            trade = AlpacaTrade.query.get(tracker.trade_id)
            if not trade or not trade.signal_data:
                no_data += 1
                continue

            try:
                parsed = parse_signal_fields(trade.signal_data)
                leg.signal_content = parsed['signal_content']
                leg.signal_indicator = parsed['signal_indicator']
                leg.signal_grade = parsed['signal_grade']
                leg.signal_score = parsed['signal_score']
                leg.signal_timeframe = parsed['signal_timeframe']

                if not leg.stop_price and trade.stop_loss_price:
                    leg.stop_price = trade.stop_loss_price
                if not leg.take_profit_price and trade.take_profit_price:
                    leg.take_profit_price = trade.take_profit_price

                updated += 1
            except Exception as e:
                logger.warning(f"Backfill parse error for leg {leg.id}: {e}")
                skipped += 1

        db.session.commit()
        flash(f'Signal backfill: {updated} content filled, {grade_fixed} grades fixed, {no_data} no source, {skipped} errors', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Backfill error: {str(e)}', 'danger')
        logger.error(f"Signal backfill failed: {e}", exc_info=True)

    return redirect(url_for('alpaca.alpaca_analytics'))


@alpaca_bp.route('/sync-entry-records', methods=['POST'])
def alpaca_sync_entry_records():
    """Sync alpaca_entry_signal_record table with all display fields from AlpacaPositionLeg + AlpacaPosition + AlpacaTrailingStopPosition"""
    from app import db
    from alpaca.models import (AlpacaPosition, AlpacaPositionStatus, AlpacaPositionLeg,
                               AlpacaLegType, AlpacaEntrySignalRecord,
                               AlpacaTrailingStopPosition)

    created = 0
    updated = 0
    errors = 0

    try:
        entry_legs = AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD])
        ).all()

        pos_ids = list(set(leg.position_id for leg in entry_legs))
        positions_map = {}
        for pos in AlpacaPosition.query.filter(AlpacaPosition.id.in_(pos_ids)).all():
            positions_map[pos.id] = pos

        ts_map = {}
        ts_ids = [p.trailing_stop_id for p in positions_map.values() if p.trailing_stop_id]
        if ts_ids:
            for ts in AlpacaTrailingStopPosition.query.filter(AlpacaTrailingStopPosition.id.in_(ts_ids)).all():
                ts_map[ts.id] = ts

        exit_legs_map = {}
        if pos_ids:
            all_exit_legs = AlpacaPositionLeg.query.filter(
                AlpacaPositionLeg.position_id.in_(pos_ids),
                AlpacaPositionLeg.leg_type == AlpacaLegType.EXIT
            ).order_by(AlpacaPositionLeg.filled_at.asc()).all()
            for el in all_exit_legs:
                exit_legs_map.setdefault(el.position_id, []).append(el)

        for leg in entry_legs:
            try:
                pos = positions_map.get(leg.position_id)
                if not pos:
                    continue

                existing = AlpacaEntrySignalRecord.query.filter_by(
                    entry_order_id=leg.alpaca_order_id,
                    position_id=pos.id
                ).first() if leg.alpaca_order_id else None

                if not existing:
                    existing = AlpacaEntrySignalRecord.query.filter_by(
                        symbol=pos.symbol,
                        entry_price=leg.price,
                        quantity=leg.quantity,
                        position_id=pos.id
                    ).first()

                is_open = pos.status == AlpacaPositionStatus.OPEN

                exit_price = None
                exit_time = None
                exit_method_str = None
                hold_duration = None
                pnl_amount = None
                pnl_percent = None

                if not is_open:
                    exit_price = pos.avg_exit_price
                    pos_exit_legs = exit_legs_map.get(pos.id, [])
                    if pos_exit_legs:
                        last_exit = pos_exit_legs[-1]
                        exit_time = last_exit.filled_at
                        for el in pos_exit_legs:
                            if el.exit_method:
                                exit_method_str = el.exit_method.value if hasattr(el.exit_method, 'value') else str(el.exit_method)
                                break

                    if exit_price and leg.price and leg.quantity:
                        if pos.side == 'long':
                            pnl_amount = (exit_price - leg.price) * leg.quantity
                        else:
                            pnl_amount = (leg.price - exit_price) * leg.quantity
                        if leg.price > 0:
                            pnl_percent = (pnl_amount / (leg.price * leg.quantity)) * 100

                    if pos.opened_at and pos.closed_at:
                        hold_duration = (pos.closed_at - pos.opened_at).total_seconds()

                ts = ts_map.get(pos.trailing_stop_id) if pos.trailing_stop_id else None
                stop_price = leg.stop_price or (ts.stop_loss_price if ts else None)
                tp_price = leg.take_profit_price or (ts.take_profit_price if ts else None)

                if existing:
                    existing.position_key = pos.position_key
                    existing.stop_price = stop_price
                    existing.take_profit_price = tp_price
                    existing.exit_price = exit_price
                    existing.exit_time = exit_time
                    existing.exit_method = exit_method_str
                    existing.hold_duration_seconds = hold_duration
                    if pnl_amount is not None:
                        existing.contribution_pnl = pnl_amount
                    if pnl_percent is not None:
                        existing.contribution_pct = pnl_percent
                    if not existing.entry_order_id and leg.alpaca_order_id:
                        existing.entry_order_id = leg.alpaca_order_id
                    if not existing.raw_json and leg.signal_content:
                        existing.raw_json = leg.signal_content
                    if not existing.indicator_trigger and leg.signal_indicator:
                        existing.indicator_trigger = leg.signal_indicator
                    if not existing.signal_grade and leg.signal_grade:
                        existing.signal_grade = leg.signal_grade
                    if not existing.signal_score and leg.signal_score:
                        existing.signal_score = leg.signal_score
                    if not existing.timeframe and leg.signal_timeframe:
                        existing.timeframe = leg.signal_timeframe
                    updated += 1
                else:
                    new_record = AlpacaEntrySignalRecord(
                        position_id=pos.id,
                        position_key=pos.position_key,
                        symbol=pos.symbol,
                        entry_time=leg.filled_at,
                        entry_price=leg.price,
                        quantity=leg.quantity,
                        side=pos.side,
                        is_scaling=(leg.leg_type == AlpacaLegType.ADD),
                        entry_order_id=leg.alpaca_order_id,
                        raw_json=leg.signal_content,
                        indicator_trigger=leg.signal_indicator,
                        signal_grade=leg.signal_grade,
                        signal_score=leg.signal_score,
                        timeframe=leg.signal_timeframe,
                        signal_stop_loss=leg.stop_price,
                        signal_take_profit=leg.take_profit_price,
                        stop_price=stop_price,
                        take_profit_price=tp_price,
                        exit_price=exit_price,
                        exit_time=exit_time,
                        exit_method=exit_method_str,
                        hold_duration_seconds=hold_duration,
                        contribution_pnl=pnl_amount,
                        contribution_pct=pnl_percent,
                    )
                    db.session.add(new_record)
                    created += 1
            except Exception as e:
                logger.warning(f"Alpaca sync entry record error for leg {leg.id}: {e}")
                errors += 1

        db.session.commit()
        flash(f'Alpaca entry records sync: {created} created, {updated} updated, {errors} errors', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Sync error: {str(e)}', 'danger')
        logger.error(f"Alpaca entry records sync failed: {e}", exc_info=True)

    return redirect(url_for('alpaca.alpaca_analytics'))


@alpaca_bp.route('/reset-all-data', methods=['POST'])
def alpaca_reset_all_data():
    from app import db
    from alpaca.models import (AlpacaPosition, AlpacaPositionLeg, AlpacaOrderTracker,
                               AlpacaTrade, AlpacaTrailingStopPosition, AlpacaTrailingStopLog,
                               AlpacaHolding, AlpacaOCOGroup, AlpacaSignalLog,
                               AlpacaSystemLog, AlpacaFilledOrder, AlpacaEntrySignalRecord,
                               AlpacaReconciliationRun)
    try:
        from sqlalchemy import text
        tables = [
            'alpaca_position_leg', 'alpaca_trailing_stop_log', 'alpaca_order_tracker',
            'alpaca_oco_group', 'alpaca_position', 'alpaca_trailing_stop_position',
            'alpaca_holding', 'alpaca_filled_order', 'alpaca_signal_log',
            'alpaca_system_log', 'alpaca_entry_signal_record', 'alpaca_reconciliation_run',
            'alpaca_trade'
        ]
        for t in tables:
            db.session.execute(text(f'TRUNCATE TABLE {t} CASCADE'))
        db.session.commit()
        flash('All Alpaca data has been cleared successfully', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Error clearing data: {str(e)}', 'danger')
    return redirect(url_for('alpaca.alpaca_settings'))


@alpaca_bp.route('/admin/fix-positions', methods=['GET', 'POST'])
def fix_alpaca_positions():
    import os
    token = request.args.get('token', '')
    expected = os.environ.get('SESSION_SECRET', '')
    if not expected or token != expected:
        return jsonify({'error': 'unauthorized'}), 403

    from alpaca.models import (AlpacaPosition, AlpacaPositionStatus,
                               AlpacaTrailingStopPosition, AlpacaOrderTracker)

    close_pos_ids = [173, 129, 151, 167, 138]
    fix_qty_id = 143
    smh_order_ids = [1025, 1026]
    deactivate_ts_ids = [687]

    if request.method == 'GET':
        positions = AlpacaPosition.query.filter(AlpacaPosition.id.in_(close_pos_ids + [fix_qty_id])).all()
        orders = AlpacaOrderTracker.query.filter(AlpacaOrderTracker.id.in_(smh_order_ids)).all()
        return jsonify({
            'positions_to_close': [{
                'id': p.id, 'symbol': p.symbol, 'side': p.side, 'status': p.status.value,
                'qty': p.total_entry_quantity, 'trailing_stop_id': p.trailing_stop_id
            } for p in positions if p.id in close_pos_ids],
            'qty_fix': [{
                'id': p.id, 'symbol': p.symbol, 'current_qty': p.total_entry_quantity, 'new_qty': 2
            } for p in positions if p.id == fix_qty_id],
            'smh_orders_to_cancel': [{
                'id': o.id, 'order_id': o.alpaca_order_id, 'role': o.role.value, 'status': o.status
            } for o in orders],
            'action': 'POST to execute'
        })

    results = []
    from datetime import datetime

    for pos in AlpacaPosition.query.filter(AlpacaPosition.id.in_(close_pos_ids)).all():
        pos.status = AlpacaPositionStatus.CLOSED
        pos.closed_at = datetime.utcnow()
        results.append(f'Position #{pos.id} {pos.symbol}: CLOSED')
        if pos.trailing_stop_id:
            ts = AlpacaTrailingStopPosition.query.get(pos.trailing_stop_id)
            if ts and ts.is_active:
                ts.is_active = False
                ts.is_triggered = True
                ts.trigger_reason = ts.trigger_reason or 'Manual fix: no broker position'
                results.append(f'TS #{ts.id} {ts.symbol}: deactivated')

    for ts_id in deactivate_ts_ids:
        ts = AlpacaTrailingStopPosition.query.get(ts_id)
        if ts and ts.is_active:
            ts.is_active = False
            ts.is_triggered = True
            ts.trigger_reason = ts.trigger_reason or 'Manual fix: no broker position'
            results.append(f'TS #{ts.id} {ts.symbol}: deactivated')

    fico = AlpacaPosition.query.get(fix_qty_id)
    if fico:
        old_qty = fico.total_entry_quantity
        fico.total_entry_quantity = 2
        results.append(f'FICO #{fico.id}: qty {old_qty} -> 2')

    from alpaca.client import AlpacaClient as AC
    cancel_client = AC()
    for ot in AlpacaOrderTracker.query.filter(AlpacaOrderTracker.id.in_(smh_order_ids)).all():
        old_status = ot.status
        if old_status not in ('FILLED', 'CANCELLED', 'EXPIRED', 'REJECTED'):
            try:
                cancel_client.cancel_order(ot.alpaca_order_id)
                results.append(f'OrderTracker #{ot.id} {ot.symbol}: API cancel sent')
            except Exception as e:
                results.append(f'OrderTracker #{ot.id} {ot.symbol}: API cancel failed ({e})')
        ot.status = 'CANCELLED'
        results.append(f'OrderTracker #{ot.id} {ot.symbol} {ot.role.value}: {old_status} -> CANCELLED')

    db.session.commit()
    results.append('All Alpaca fixes committed')
    return jsonify({'results': results})


@alpaca_bp.route('/admin/create-oco/<int:pos_id>', methods=['POST'])
def admin_create_oco(pos_id):
    import os
    token = request.args.get('token', '')
    expected = os.environ.get('SESSION_SECRET', '')
    if not expected or token != expected:
        return jsonify({'error': 'unauthorized'}), 403

    from alpaca.models import AlpacaPosition, AlpacaTrailingStopPosition, AlpacaPositionStatus

    pos = AlpacaPosition.query.get(pos_id)
    if not pos or pos.status != AlpacaPositionStatus.OPEN:
        return jsonify({'error': f'No open position #{pos_id}'}), 404

    sl_price = request.args.get('sl', type=float)
    tp_price = request.args.get('tp', type=float)
    if not sl_price or not tp_price:
        return jsonify({'error': 'Missing sl or tp parameter'}), 400

    ts = AlpacaTrailingStopPosition.query.filter_by(symbol=pos.symbol, is_active=True).first()
    ts_id = ts.id if ts else None

    if ts:
        ts.take_profit_price = tp_price
        if not ts.stop_loss_price or (pos.side == 'long' and sl_price > ts.stop_loss_price) or (pos.side == 'short' and sl_price < ts.stop_loss_price):
            ts.stop_loss_price = sl_price
            ts.trailing_stop_price = sl_price

    from alpaca.oco_service import create_oco_for_entry
    oco_group, oco_status = create_oco_for_entry(
        symbol=pos.symbol,
        quantity=pos.total_entry_quantity,
        entry_price=float(pos.avg_entry_price),
        stop_loss_price=sl_price,
        take_profit_price=tp_price,
        trade_id=None,
        trailing_stop_id=ts_id,
        side=pos.side,
    )
    db.session.commit()

    if oco_group:
        return jsonify({'result': f'OCO created for {pos.symbol}: {oco_status}', 'ts_id': ts_id})
    else:
        return jsonify({'error': f'OCO failed: {oco_status}'}), 500


@alpaca_bp.route('/admin/fix-duplicate-fill/<int:pos_id>', methods=['GET', 'POST'])
def fix_duplicate_fill(pos_id):
    import os
    token = request.args.get('token', '')
    expected = os.environ.get('SESSION_SECRET', '')
    if not expected or token != expected:
        return jsonify({'error': 'unauthorized'}), 403

    from alpaca.models import (AlpacaPosition, AlpacaPositionLeg, AlpacaLegType,
                               AlpacaTrailingStopPosition, AlpacaPositionStatus)

    pos = AlpacaPosition.query.get(pos_id)
    if not pos:
        return jsonify({'error': f'Position #{pos_id} not found'}), 404

    legs = AlpacaPositionLeg.query.filter_by(
        position_id=pos.id
    ).filter(
        AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD])
    ).order_by(AlpacaPositionLeg.id).all()

    seen_orders = {}
    duplicate_legs = []
    for leg in legs:
        if leg.alpaca_order_id and leg.alpaca_order_id in seen_orders:
            duplicate_legs.append(leg)
        elif leg.alpaca_order_id:
            seen_orders[leg.alpaca_order_id] = leg

    if request.method == 'GET':
        return jsonify({
            'position': {
                'id': pos.id, 'symbol': pos.symbol, 'side': pos.side,
                'total_entry_quantity': pos.total_entry_quantity,
                'avg_entry_price': float(pos.avg_entry_price) if pos.avg_entry_price else None,
            },
            'legs': [{'id': l.id, 'type': l.leg_type.value, 'order_id': l.alpaca_order_id,
                       'qty': l.quantity, 'price': float(l.price) if l.price else None} for l in legs],
            'duplicates': [{'id': l.id, 'order_id': l.alpaca_order_id, 'qty': l.quantity} for l in duplicate_legs],
            'trailing_stop': None,
            'action': 'POST to fix'
        })

    results = []

    for dup in duplicate_legs:
        dup_qty = dup.quantity or 0
        dup_price = float(dup.price) if dup.price else 0
        old_cost = (pos.avg_entry_price or 0) * pos.total_entry_quantity
        new_cost = old_cost - dup_price * dup_qty
        new_qty = pos.total_entry_quantity - dup_qty
        pos.total_entry_quantity = new_qty
        pos.avg_entry_price = new_cost / new_qty if new_qty > 0 else 0
        results.append(f'Removed duplicate leg #{dup.id} (order {dup.alpaca_order_id[:8]}..., qty={dup_qty})')
        db.session.delete(dup)

    results.append(f'Position #{pos.id} {pos.symbol}: qty={pos.total_entry_quantity}, avg=${pos.avg_entry_price:.2f}')

    existing_ts = AlpacaTrailingStopPosition.query.filter_by(
        symbol=pos.symbol, is_active=True
    ).first()
    if not existing_ts and pos.status == AlpacaPositionStatus.OPEN:
        first_leg = legs[0] if legs else None
        sl_price = float(first_leg.stop_price) if first_leg and first_leg.stop_price else None
        tp_price = float(first_leg.take_profit_price) if first_leg and first_leg.take_profit_price else None
        trade_id = first_leg.trade_id if first_leg else None

        if not sl_price:
            from alpaca.trailing_stop_engine import get_trailing_stop_config
            config = get_trailing_stop_config()
            if pos.side == 'long':
                sl_price = round(float(pos.avg_entry_price) * (1 - config.initial_stop_pct), 2)
            else:
                sl_price = round(float(pos.avg_entry_price) * (1 + config.initial_stop_pct), 2)

        from alpaca.trailing_stop_engine import create_trailing_stop_for_entry
        ts = create_trailing_stop_for_entry(
            symbol=pos.symbol,
            side=pos.side,
            entry_price=float(pos.avg_entry_price),
            quantity=pos.total_entry_quantity,
            stop_loss_price=sl_price,
            take_profit_price=tp_price,
            trade_id=trade_id,
        )
        if ts:
            pos.trailing_stop_id = ts.id
            results.append(f'Created trailing stop #{ts.id}: SL=${sl_price}, TP=${tp_price}')

            try:
                from alpaca.oco_service import create_oco_for_entry
                oco_group, oco_status = create_oco_for_entry(
                    symbol=pos.symbol,
                    quantity=pos.total_entry_quantity,
                    entry_price=float(pos.avg_entry_price),
                    stop_loss_price=sl_price,
                    take_profit_price=tp_price,
                    trade_id=trade_id,
                    trailing_stop_id=ts.id,
                    side=pos.side,
                )
                if oco_group:
                    results.append(f'Created OCO protection: {oco_status}')
                else:
                    results.append(f'OCO creation failed: {oco_status}')
            except Exception as e:
                results.append(f'OCO creation error: {str(e)}')

    db.session.commit()
    logger.info(f'🔧 Duplicate fill fix: {results}')
    return jsonify({'results': results})
