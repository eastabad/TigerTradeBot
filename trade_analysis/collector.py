import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


def _safe_enum_value(val):
    if val is None:
        return None
    if hasattr(val, 'value'):
        return val.value
    return str(val)


def _normalize_side(side_val) -> Optional[str]:
    if side_val is None:
        return None
    s = _safe_enum_value(side_val)
    if s is None:
        return None
    s = s.upper()
    if 'BUY' in s:
        return 'BUY'
    if 'SELL' in s:
        return 'SELL'
    return s


def _normalize_role(role_val) -> Optional[str]:
    if role_val is None:
        return None
    return _safe_enum_value(role_val)


def collect_tiger_data(target_date: datetime.date, account_types: List[str] = None) -> Dict[str, List[Dict]]:
    from app import db
    from models import SignalLog, Trade, OrderTracker, ClosedPosition

    if account_types is None:
        account_types = ['real', 'paper']

    day_start = datetime.combine(target_date, datetime.min.time())
    day_end = datetime.combine(target_date, datetime.max.time())

    signals = []
    for sig in SignalLog.query.filter(
        SignalLog.created_at.between(day_start, day_end)
    ).all():
        signals.append({
            'broker': 'tiger',
            'record_type': 'signal',
            'source_table': 'signal_log',
            'source_id': sig.id,
            'order_id': sig.tiger_order_id,
            'symbol': None,
            'side': None,
            'quantity': None,
            'price': None,
            'time': sig.created_at,
            'role': None,
            'trade_id': sig.trade_id,
            'account_type': sig.account_type,
            'raw_signal': sig.raw_signal,
            'parsed_successfully': sig.parsed_successfully,
            'endpoint': sig.endpoint,
            'raw_record': sig,
        })

    trades = []
    for t in Trade.query.filter(
        Trade.created_at.between(day_start, day_end)
    ).all():
        trades.append({
            'broker': 'tiger',
            'record_type': 'trade',
            'source_table': 'trade',
            'source_id': t.id,
            'order_id': t.tiger_order_id,
            'symbol': t.symbol,
            'side': _normalize_side(t.side),
            'quantity': t.quantity,
            'price': t.filled_price or t.price,
            'time': t.created_at,
            'role': 'entry' if not t.is_close_position else 'exit',
            'account_type': t.account_type or 'real',
            'signal_data': t.signal_data,
            'status': _safe_enum_value(t.status),
            'stop_loss_price': t.stop_loss_price,
            'take_profit_price': t.take_profit_price,
            'raw_record': t,
        })

    trackers = []
    for ot in OrderTracker.query.filter(
        OrderTracker.created_at.between(day_start, day_end)
    ).all():
        trackers.append({
            'broker': 'tiger',
            'record_type': 'tracker',
            'source_table': 'order_tracker',
            'source_id': ot.id,
            'order_id': ot.tiger_order_id,
            'symbol': ot.symbol,
            'side': _normalize_side(ot.side),
            'quantity': ot.quantity,
            'price': ot.avg_fill_price,
            'time': ot.fill_time or ot.created_at,
            'role': _normalize_role(ot.role),
            'account_type': ot.account_type,
            'status': ot.status,
            'trade_id': ot.trade_id,
            'closed_position_id': ot.closed_position_id,
            'trailing_stop_id': ot.trailing_stop_id,
            'fill_source': ot.fill_source,
            'realized_pnl': ot.realized_pnl,
            'commission': ot.commission,
            'raw_record': ot,
        })

    closed_positions = []
    for cp in ClosedPosition.query.filter(
        ClosedPosition.exit_time.between(day_start, day_end)
    ).all():
        closed_positions.append({
            'broker': 'tiger',
            'record_type': 'closed',
            'source_table': 'closed_position',
            'source_id': cp.id,
            'order_id': cp.exit_order_id,
            'symbol': cp.symbol,
            'side': cp.side,
            'quantity': cp.exit_quantity,
            'price': cp.exit_price,
            'time': cp.exit_time,
            'role': 'exit',
            'account_type': cp.account_type,
            'exit_method': _safe_enum_value(cp.exit_method),
            'total_pnl': cp.total_pnl,
            'total_pnl_pct': cp.total_pnl_pct,
            'avg_entry_price': cp.avg_entry_price,
            'commission': cp.commission,
            'raw_record': cp,
        })

    api_fills = []
    for acct in account_types:
        try:
            if acct == 'paper':
                from tiger_client import TigerPaperClient
                client = TigerPaperClient()
            else:
                from tiger_client import TigerClient
                client = TigerClient()

            date_str = target_date.strftime('%Y-%m-%d')
            result = client.get_filled_orders(start_date=date_str, end_date=date_str, limit=500)
            if result.get('success'):
                for order in result.get('orders', []):
                    trade_time = order.get('trade_time')
                    if isinstance(trade_time, (int, float)):
                        fill_dt = datetime.fromtimestamp(trade_time / 1000)
                    elif isinstance(trade_time, datetime):
                        fill_dt = trade_time
                    else:
                        fill_dt = None

                    api_fills.append({
                        'broker': 'tiger',
                        'record_type': 'api_fill',
                        'source_table': 'tiger_api',
                        'source_id': None,
                        'order_id': str(order.get('order_id', '')),
                        'symbol': order.get('symbol', ''),
                        'side': 'BUY' if 'BUY' in str(order.get('action', '')).upper() else 'SELL',
                        'quantity': order.get('filled', order.get('quantity', 0)),
                        'price': order.get('avg_fill_price', 0),
                        'time': fill_dt,
                        'role': None,
                        'account_type': acct,
                        'realized_pnl': order.get('realized_pnl', 0),
                        'commission': order.get('commission', 0),
                        'is_open': order.get('is_open', True),
                        'parent_id': order.get('parent_id'),
                        'raw_record': order,
                    })
            else:
                logger.warning(f"Tiger API get_filled_orders failed for {acct}: {result.get('error')}")
        except Exception as e:
            logger.error(f"Error collecting Tiger API fills for {acct}: {e}")

    return {
        'signals': signals,
        'trades': trades,
        'trackers': trackers,
        'closed_positions': closed_positions,
        'api_fills': api_fills,
    }


def collect_alpaca_data(target_date: datetime.date) -> Dict[str, List[Dict]]:
    from app import db

    try:
        from alpaca.models import (
            AlpacaSignalLog, AlpacaTrade, AlpacaOrderTracker,
            AlpacaPosition, AlpacaPositionLeg, AlpacaPositionStatus, AlpacaLegType
        )
    except ImportError:
        logger.warning("Alpaca models not available, skipping Alpaca data collection")
        return {'signals': [], 'trades': [], 'trackers': [], 'closed_positions': [], 'api_fills': []}

    day_start = datetime.combine(target_date, datetime.min.time())
    day_end = datetime.combine(target_date, datetime.max.time())

    signals = []
    for sig in AlpacaSignalLog.query.filter(
        AlpacaSignalLog.created_at.between(day_start, day_end)
    ).all():
        signals.append({
            'broker': 'alpaca',
            'record_type': 'signal',
            'source_table': 'alpaca_signal_log',
            'source_id': sig.id,
            'order_id': None,
            'symbol': sig.symbol,
            'side': sig.action.upper() if sig.action else None,
            'quantity': None,
            'price': None,
            'time': sig.created_at,
            'role': None,
            'trade_id': sig.trade_id,
            'account_type': 'paper',
            'raw_signal': sig.raw_data,
            'parsed_successfully': sig.status == 'processed',
            'endpoint': None,
            'raw_record': sig,
        })

    trades = []
    for t in AlpacaTrade.query.filter(
        AlpacaTrade.created_at.between(day_start, day_end)
    ).all():
        trades.append({
            'broker': 'alpaca',
            'record_type': 'trade',
            'source_table': 'alpaca_trade',
            'source_id': t.id,
            'order_id': t.alpaca_order_id,
            'symbol': t.symbol,
            'side': _normalize_side(t.side),
            'quantity': t.quantity,
            'price': t.filled_price or t.price,
            'time': t.created_at,
            'role': 'entry' if not t.is_close_position else 'exit',
            'account_type': 'paper',
            'signal_data': t.signal_data,
            'status': _safe_enum_value(t.status),
            'stop_loss_price': t.stop_loss_price,
            'take_profit_price': t.take_profit_price,
            'raw_record': t,
        })

    order_to_position = {}
    try:
        all_legs = AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.alpaca_order_id.isnot(None)
        ).all()
        for leg in all_legs:
            order_to_position[leg.alpaca_order_id] = leg.position_id
    except Exception as e:
        logger.warning(f"Failed to build order->position map: {e}")

    closed_position_ids = set()
    try:
        for pos in AlpacaPosition.query.filter(
            AlpacaPosition.status == AlpacaPositionStatus.CLOSED
        ).all():
            closed_position_ids.add(pos.id)
    except Exception as e:
        logger.warning(f"Failed to query closed position IDs: {e}")

    today_closed_pos_ids = set()
    try:
        for pos in AlpacaPosition.query.filter(
            AlpacaPosition.status == AlpacaPositionStatus.CLOSED,
            AlpacaPosition.closed_at.between(day_start, day_end)
        ).all():
            today_closed_pos_ids.add(pos.id)
    except Exception as e:
        logger.warning(f"Failed to query today's closed positions: {e}")

    linked_order_ids_for_today = set()
    for order_id, pos_id in order_to_position.items():
        if pos_id in today_closed_pos_ids:
            linked_order_ids_for_today.add(order_id)

    trackers = []
    seen_tracker_ids = set()
    today_trackers = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.created_at.between(day_start, day_end)
    ).all()
    for ot in today_trackers:
        seen_tracker_ids.add(ot.id)
        linked_position_id = order_to_position.get(ot.alpaca_order_id)
        cp_id = linked_position_id if linked_position_id and linked_position_id in closed_position_ids else None

        trackers.append({
            'broker': 'alpaca',
            'record_type': 'tracker',
            'source_table': 'alpaca_order_tracker',
            'source_id': ot.id,
            'order_id': ot.alpaca_order_id,
            'symbol': ot.symbol,
            'side': _normalize_side(ot.side),
            'quantity': ot.quantity,
            'price': ot.avg_fill_price,
            'time': ot.fill_time or ot.created_at,
            'role': _normalize_role(ot.role),
            'account_type': 'paper',
            'status': ot.status,
            'trade_id': ot.trade_id,
            'closed_position_id': cp_id,
            'trailing_stop_id': ot.trailing_stop_id,
            'fill_source': None,
            'realized_pnl': ot.realized_pnl,
            'commission': ot.commission,
            'raw_record': ot,
        })

    if linked_order_ids_for_today:
        cross_day_trackers = AlpacaOrderTracker.query.filter(
            AlpacaOrderTracker.alpaca_order_id.in_(list(linked_order_ids_for_today)),
            AlpacaOrderTracker.created_at < day_start
        ).all()
        for ot in cross_day_trackers:
            if ot.id in seen_tracker_ids:
                continue
            seen_tracker_ids.add(ot.id)
            linked_position_id = order_to_position.get(ot.alpaca_order_id)
            cp_id = linked_position_id if linked_position_id and linked_position_id in closed_position_ids else None

            trackers.append({
                'broker': 'alpaca',
                'record_type': 'tracker',
                'source_table': 'alpaca_order_tracker',
                'source_id': ot.id,
                'order_id': ot.alpaca_order_id,
                'symbol': ot.symbol,
                'side': _normalize_side(ot.side),
                'quantity': ot.quantity,
                'price': ot.avg_fill_price,
                'time': ot.fill_time or ot.created_at,
                'role': _normalize_role(ot.role),
                'account_type': 'paper',
                'status': ot.status,
                'trade_id': ot.trade_id,
                'closed_position_id': cp_id,
                'trailing_stop_id': ot.trailing_stop_id,
                'fill_source': None,
                'realized_pnl': ot.realized_pnl,
                'commission': ot.commission,
                'raw_record': ot,
            })
        if cross_day_trackers:
            logger.info(f"Added {len(cross_day_trackers)} cross-day entry trackers linked to today's closed positions")

    closed_positions = []
    closed_pos_query = AlpacaPosition.query.filter(
        AlpacaPosition.status == AlpacaPositionStatus.CLOSED,
        AlpacaPosition.closed_at.between(day_start, day_end)
    ).all()
    for pos in closed_pos_query:
        exit_legs = AlpacaPositionLeg.query.filter_by(
            position_id=pos.id, leg_type=AlpacaLegType.EXIT
        ).all()
        exit_method = None
        if exit_legs:
            exit_method = _safe_enum_value(exit_legs[-1].exit_method)

        closed_positions.append({
            'broker': 'alpaca',
            'record_type': 'closed',
            'source_table': 'alpaca_position',
            'source_id': pos.id,
            'order_id': exit_legs[-1].alpaca_order_id if exit_legs else None,
            'symbol': pos.symbol,
            'side': pos.side,
            'quantity': pos.total_exit_quantity,
            'price': pos.avg_exit_price,
            'time': pos.closed_at,
            'role': 'exit',
            'account_type': 'paper',
            'exit_method': exit_method,
            'total_pnl': pos.realized_pnl,
            'total_pnl_pct': None,
            'avg_entry_price': pos.avg_entry_price,
            'commission': pos.commission,
            'raw_record': pos,
        })

    api_fills = []
    try:
        from alpaca.client import AlpacaClient
        client = AlpacaClient()
        date_str = target_date.strftime('%Y-%m-%dT00:00:00Z')
        next_date_str = (target_date + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')
        activities = client.get_activities('FILL', after=date_str, until=next_date_str, page_size=500)
        for act in activities:
            api_fills.append({
                'broker': 'alpaca',
                'record_type': 'api_fill',
                'source_table': 'alpaca_api',
                'source_id': None,
                'order_id': act.get('order_id', ''),
                'symbol': act.get('symbol', ''),
                'side': act.get('side', '').upper(),
                'quantity': float(act.get('qty', 0)),
                'price': float(act.get('price', 0)),
                'time': act.get('transaction_time'),
                'role': None,
                'account_type': 'paper',
                'realized_pnl': None,
                'commission': None,
                'raw_record': act,
            })
    except Exception as e:
        logger.error(f"Error collecting Alpaca API fills: {e}")

    return {
        'signals': signals,
        'trades': trades,
        'trackers': trackers,
        'closed_positions': closed_positions,
        'api_fills': api_fills,
    }


def collect_all_data(target_date: datetime.date, brokers: List[str] = None) -> Dict[str, Dict]:
    if brokers is None:
        brokers = ['tiger', 'alpaca']

    result = {}
    if 'tiger' in brokers:
        logger.info(f"Collecting Tiger data for {target_date}")
        result['tiger'] = collect_tiger_data(target_date)
        tiger_counts = {k: len(v) for k, v in result['tiger'].items()}
        logger.info(f"Tiger data collected: {tiger_counts}")

    if 'alpaca' in brokers:
        logger.info(f"Collecting Alpaca data for {target_date}")
        result['alpaca'] = collect_alpaca_data(target_date)
        alpaca_counts = {k: len(v) for k, v in result['alpaca'].items()}
        logger.info(f"Alpaca data collected: {alpaca_counts}")

    return result
