import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from app import db
from models import (
    SignalLog, Trade, OrderTracker, Position, PositionLeg,
    OCAGroup, TrailingStopPosition, TrailingStopLog,
    PositionStatus, OrderRole, OCAStatus, LegType, ExitMethod,
    OrderStatus, SystemLog,
)

logger = logging.getLogger(__name__)

STAGE_SIGNAL = 'signal'
STAGE_TRADE = 'trade'
STAGE_ENTRY_ORDER = 'entry_order'
STAGE_POSITION = 'position'
STAGE_PROTECTION = 'protection'
STAGE_TRAILING_STOP = 'trailing_stop'
STAGE_EXIT = 'exit'
STAGE_CLOSE = 'close'

STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_WARNING = 'warning'
STATUS_PENDING = 'pending'
STATUS_SKIPPED = 'skipped'


def _fmt_time(dt):
    if not dt:
        return ''
    if isinstance(dt, str):
        return dt
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def _fmt_price(p):
    if p is None:
        return ''
    return f'${p:,.2f}'


def _fmt_qty(q):
    if q is None:
        return ''
    if q == int(q):
        return str(int(q))
    return f'{q:.2f}'


def _minutes_since(dt):
    if not dt:
        return None
    now = datetime.utcnow()
    return round((now - dt).total_seconds() / 60, 1)


SOURCE_LABELS = {
    'websocket': 'WebSocket推送',
    'websocket_oca': 'WebSocket(OCA)',
    'websocket_attached': 'WebSocket(附属单)',
    'websocket_external': 'WebSocket(外部)',
    'polling': 'API轮询',
    'reconciliation': '对账修复',
    'webhook_immediate': 'Webhook即时',
    'entry_fill_handler': '成交处理器',
    'scheduler_orphan': '调度器修复',
    'ts_creation_auto': 'TS自动创建',
    'oca_rebuild': 'OCA重建',
    'manual': '手动创建',
    'websocket_auto_protection': 'WebSocket自动保护',
    'ghost_detection': '幽灵检测',
    'soft_stop': '软止损',
    'websocket_fill': 'WebSocket成交',
    'websocket_oca_fill': 'WebSocket(OCA)成交',
    'websocket_attached_fill': 'WebSocket(附属单)成交',
    'websocket_external_fill': 'WebSocket(外部)成交',
    'polling_fill': '轮询成交',
}

def _source_label(source: str) -> str:
    if not source:
        return ''
    return SOURCE_LABELS.get(source, source)

TERMINAL_STATUSES = {'FILLED', 'CANCELLED', 'CANCELED', 'EXPIRED', 'REJECTED', 'REPLACED'}

STAGE_SOURCE_MAP = {
    STAGE_SIGNAL: {
        'sources': ['routes'],
        'categories': ['general'],
        'keywords': ['webhook', 'signal', 'parse'],
    },
    STAGE_TRADE: {
        'sources': ['routes', 'tiger_client'],
        'categories': ['order'],
        'keywords': ['trade', 'order_type', 'submit'],
    },
    STAGE_ENTRY_ORDER: {
        'sources': ['tiger_client', 'push_event_handlers', 'order_tracker_service'],
        'categories': ['order'],
        'keywords': ['entry', 'place_order', 'ENTRY'],
    },
    STAGE_POSITION: {
        'sources': ['push_event_handlers', 'holdings_sync'],
        'categories': ['position'],
        'keywords': ['position', 'holdings'],
    },
    STAGE_PROTECTION: {
        'sources': ['oca_service', 'tiger_client', 'order_tracker_service'],
        'categories': ['order'],
        'keywords': ['OCA', 'oca', 'stop_loss', 'take_profit', 'protection', 'SL', 'TP'],
    },
    STAGE_TRAILING_STOP: {
        'sources': ['trailing_stop_engine', 'trailing_stop_scheduler'],
        'categories': ['trailing_stop'],
        'keywords': ['trailing', 'stop', 'breach', 'tier'],
    },
    STAGE_EXIT: {
        'sources': ['tiger_client', 'push_event_handlers', 'order_tracker_service', 'trailing_stop_engine'],
        'categories': ['order'],
        'keywords': ['exit', 'EXIT', 'close', 'sell', 'buy_to_cover'],
    },
    STAGE_CLOSE: {
        'sources': ['push_event_handlers', 'reconciliation_service', 'order_tracker_service'],
        'categories': ['position'],
        'keywords': ['close', 'closed', 'reconcil', 'P&L', 'pnl'],
    },
}


def _fetch_related_logs(symbol: str, stage: str, account_type: str = None,
                        time_start: datetime = None, time_end: datetime = None,
                        limit: int = 10) -> List[Dict]:
    mapping = STAGE_SOURCE_MAP.get(stage)
    if not mapping:
        return []

    if not time_start:
        time_start = datetime.utcnow() - timedelta(hours=48)
    if not time_end:
        time_end = datetime.utcnow() + timedelta(minutes=5)

    source_conditions = []
    if mapping.get('sources'):
        source_conditions.append(SystemLog.source.in_(mapping['sources']))
    if mapping.get('categories'):
        source_conditions.append(SystemLog.category.in_(mapping['categories']))

    keyword_conditions = []
    for kw in mapping.get('keywords', []):
        keyword_conditions.append(SystemLog.message.ilike(f'%{kw}%'))

    query = SystemLog.query.filter(
        SystemLog.level.in_(['ERROR', 'WARNING', 'CRITICAL']),
        SystemLog.timestamp >= time_start,
        SystemLog.timestamp <= time_end,
        db.or_(
            SystemLog.symbol == symbol,
            SystemLog.message.ilike(f'%{symbol}%'),
        ),
        db.or_(*source_conditions) if source_conditions else db.true(),
    )

    if account_type:
        query = query.filter(
            db.or_(
                SystemLog.account_type == account_type,
                SystemLog.account_type.is_(None),
            )
        )

    logs = query.order_by(SystemLog.timestamp.desc()).limit(limit * 3).all()

    if keyword_conditions and logs:
        scored = []
        for log in logs:
            msg_lower = (log.message or '').lower()
            kw_hits = sum(1 for kw in mapping.get('keywords', []) if kw.lower() in msg_lower)
            source_hit = 1 if log.source in (mapping.get('sources') or []) else 0
            scored.append((kw_hits + source_hit, log))
        scored.sort(key=lambda x: (-x[0], x[1].timestamp), reverse=False)
        scored.sort(key=lambda x: -x[0])
        logs = [s[1] for s in scored[:limit]]

    result = []
    for log in logs[:limit]:
        result.append({
            'id': log.id,
            'timestamp': _fmt_time(log.timestamp),
            'level': log.level,
            'source': log.source or '',
            'category': log.category or '',
            'message': log.message or '',
            'symbol': log.symbol or '',
        })
    return result


def build_lifecycle(symbol: str = None, order_id: str = None, position_id: int = None, account_type: str = None) -> Optional:
    if position_id:
        position = Position.query.get(position_id)
        if not position:
            return None
        return _build_lifecycle_for_position(position)

    if order_id:
        tracker = OrderTracker.query.filter_by(tiger_order_id=order_id).first()
        if tracker:
            position = _find_position_for_tracker(tracker)
            if position:
                return _build_lifecycle_for_position(position)
            return _build_lifecycle_for_orphan_tracker(tracker)
        trade = Trade.query.filter_by(tiger_order_id=order_id).first()
        if trade:
            leg = PositionLeg.query.filter_by(trade_id=trade.id).first()
            if leg:
                position = Position.query.get(leg.position_id)
                if position:
                    return _build_lifecycle_for_position(position)
        return None

    if symbol:
        query = Position.query.filter_by(symbol=symbol.upper())
        if account_type:
            query = query.filter_by(account_type=account_type)
        positions = query.order_by(Position.opened_at.desc()).all()
        if positions:
            results = []
            for pos in positions:
                lc = _build_lifecycle_for_position(pos)
                if lc:
                    results.append(lc)
            return results if results else None

        t_query = OrderTracker.query.filter_by(symbol=symbol.upper())
        if account_type:
            t_query = t_query.filter_by(account_type=account_type)
        trackers = t_query.order_by(OrderTracker.created_at.desc()).all()
        if trackers:
            results = []
            seen_ids = set()
            for t in trackers:
                if t.id not in seen_ids:
                    seen_ids.add(t.id)
                    results.append(_build_lifecycle_for_orphan_tracker(t))
            return results if results else None

    return None


def _find_position_for_tracker(tracker: OrderTracker) -> Optional[Position]:
    leg = PositionLeg.query.filter_by(tiger_order_id=tracker.tiger_order_id).first()
    if leg:
        return Position.query.get(leg.position_id)
    if tracker.trade_id:
        leg2 = PositionLeg.query.filter_by(trade_id=tracker.trade_id).first()
        if leg2:
            return Position.query.get(leg2.position_id)
    if tracker.trailing_stop_id:
        pos = Position.query.filter_by(trailing_stop_id=tracker.trailing_stop_id).first()
        if pos:
            return pos
    return None


def _build_lifecycle_for_position(position: Position) -> Dict:
    stages = []
    all_records = {}

    legs = PositionLeg.query.filter_by(position_id=position.id).order_by(
        PositionLeg.filled_at.asc()
    ).all()
    all_records['legs'] = legs

    entry_legs = [l for l in legs if l.leg_type in (LegType.ENTRY, LegType.ADD)]
    exit_legs = [l for l in legs if l.leg_type == LegType.EXIT]

    trade_ids = set()
    order_ids = set()
    for l in legs:
        if l.trade_id:
            trade_ids.add(l.trade_id)
        if l.tiger_order_id:
            order_ids.add(l.tiger_order_id)

    if position.trailing_stop_id:
        ts = TrailingStopPosition.query.get(position.trailing_stop_id)
    else:
        ts = TrailingStopPosition.query.filter_by(
            symbol=position.symbol,
            account_type=position.account_type
        ).order_by(TrailingStopPosition.created_at.desc()).first()
        if ts and ts.trade_id:
            if ts.trade_id not in trade_ids:
                ts = None
    all_records['trailing_stop'] = ts

    if ts and ts.trade_id:
        trade_ids.add(ts.trade_id)

    trades = []
    if trade_ids:
        trades = Trade.query.filter(Trade.id.in_(trade_ids)).order_by(
            Trade.created_at.asc()
        ).all()
        for t in trades:
            if t.tiger_order_id:
                order_ids.add(t.tiger_order_id)
    all_records['trades'] = trades

    signal_logs = []
    if trade_ids:
        signal_logs = SignalLog.query.filter(SignalLog.trade_id.in_(trade_ids)).order_by(
            SignalLog.created_at.asc()
        ).all()
        failed_signals = SignalLog.query.filter(
            SignalLog.raw_signal.ilike(f'%{position.symbol}%'),
            SignalLog.parsed_successfully == False,
        ).all()
        existing_ids = {s.id for s in signal_logs}
        for fs in failed_signals:
            if fs.id not in existing_ids:
                if trades and fs.created_at:
                    earliest_trade = min(t.created_at for t in trades if t.created_at)
                    if earliest_trade and fs.created_at >= earliest_trade - timedelta(hours=2):
                        signal_logs.append(fs)
        signal_logs.sort(key=lambda s: s.created_at or datetime.min)
    all_records['signal_logs'] = signal_logs

    trackers = OrderTracker.query.filter(
        OrderTracker.symbol == position.symbol,
        OrderTracker.account_type == position.account_type,
        db.or_(
            OrderTracker.tiger_order_id.in_(order_ids) if order_ids else db.false(),
            OrderTracker.trade_id.in_(trade_ids) if trade_ids else db.false(),
            OrderTracker.trailing_stop_id == position.trailing_stop_id if position.trailing_stop_id else db.false(),
        )
    ).order_by(OrderTracker.created_at.asc()).all()
    for t in trackers:
        if t.tiger_order_id:
            order_ids.add(t.tiger_order_id)
    all_records['trackers'] = trackers

    oca_group_ids = set()
    for t in trackers:
        if t.oca_group_id:
            oca_group_ids.add(t.oca_group_id)
    for l in legs:
        if l.oca_group_id:
            oca_group_ids.add(l.oca_group_id)

    oca_groups = []
    if oca_group_ids:
        oca_groups = OCAGroup.query.filter(OCAGroup.id.in_(oca_group_ids)).order_by(
            OCAGroup.created_at.asc()
        ).all()
    else:
        if trade_ids:
            oca_groups = OCAGroup.query.filter(
                OCAGroup.trade_id.in_(trade_ids)
            ).order_by(OCAGroup.created_at.asc()).all()
        if not oca_groups and ts:
            oca_groups = OCAGroup.query.filter_by(trailing_stop_id=ts.id).order_by(
                OCAGroup.created_at.asc()
            ).all()
    all_records['oca_groups'] = oca_groups

    ts_logs = []
    if ts:
        ts_logs = TrailingStopLog.query.filter_by(trailing_stop_id=ts.id).order_by(
            TrailingStopLog.created_at.asc()
        ).all()
    all_records['ts_logs'] = ts_logs

    stages = _evaluate_stages(position, all_records)

    time_start = position.opened_at - timedelta(hours=1) if position.opened_at else None
    time_end = (position.closed_at + timedelta(hours=1)) if position.closed_at else None

    for stage in stages:
        if stage['status'] in (STATUS_ERROR, STATUS_WARNING):
            try:
                stage['related_logs'] = _fetch_related_logs(
                    symbol=position.symbol,
                    stage=stage['stage'],
                    account_type=position.account_type,
                    time_start=time_start,
                    time_end=time_end,
                    limit=8,
                )
            except Exception as e:
                logger.debug(f"Failed to fetch related logs for stage {stage['stage']}: {e}")
                stage['related_logs'] = []
        else:
            stage['related_logs'] = []

    has_error = any(s['status'] == STATUS_ERROR for s in stages)
    has_warning = any(s['status'] == STATUS_WARNING for s in stages)
    if has_error:
        overall = STATUS_ERROR
    elif has_warning:
        overall = STATUS_WARNING
    else:
        overall = STATUS_OK

    return {
        'type': 'position',
        'position': position,
        'stages': stages,
        'records': all_records,
        'overall_status': overall,
        'summary': _build_summary(position, all_records),
    }


def _evaluate_stages(position: Position, records: Dict) -> List[Dict]:
    stages = []

    stages.append(_eval_signal_stage(records))
    stages.append(_eval_trade_stage(records))
    stages.append(_eval_entry_order_stage(records, position))
    stages.append(_eval_position_stage(position, records))
    stages.append(_eval_protection_stage(position, records))
    stages.append(_eval_trailing_stop_stage(position, records))

    if position.status == PositionStatus.CLOSED or (records.get('trailing_stop') and records['trailing_stop'].is_triggered):
        stages.append(_eval_exit_stage(position, records))
        stages.append(_eval_close_stage(position, records))
    elif position.status == PositionStatus.OPEN:
        exit_trackers = [t for t in records.get('trackers', []) if t.role in (
            OrderRole.EXIT_SIGNAL, OrderRole.EXIT_TRAILING
        )]
        if exit_trackers:
            stages.append(_eval_exit_stage(position, records))

    return stages


def _eval_signal_stage(records: Dict) -> Dict:
    signal_logs = records.get('signal_logs', [])
    if not signal_logs:
        return {
            'stage': STAGE_SIGNAL,
            'name': '信号接收',
            'status': STATUS_SKIPPED,
            'message': '无信号记录（可能通过手动/API下单）',
            'time': None,
            'details': [],
        }

    executed = [s for s in signal_logs if s.parsed_successfully and s.trade_id]
    failed = [s for s in signal_logs if not s.parsed_successfully or s.tiger_status == 'error']

    details = []
    for s in signal_logs:
        icon = '✅' if s.parsed_successfully and s.trade_id else '❌'
        status_str = s.tiger_status or ('ok' if s.parsed_successfully else 'error')
        details.append(f'{icon} #{s.id} [{status_str}] {_fmt_time(s.created_at)}')
        if s.error_message:
            details.append(f'   原因: {s.error_message}')
        if s.endpoint:
            details.append(f'   端点: {s.endpoint}')

    if executed:
        status = STATUS_OK
        msg = f'信号已接收并执行'
        if failed:
            status = STATUS_WARNING
            msg += f'（前 {len(failed)} 次失败后成功）'
    else:
        status = STATUS_ERROR
        msg = f'所有 {len(signal_logs)} 个信号均失败'

    return {
        'stage': STAGE_SIGNAL,
        'name': '信号接收',
        'status': status,
        'message': msg,
        'time': _fmt_time(signal_logs[0].created_at) if signal_logs else None,
        'details': details,
    }


def _eval_trade_stage(records: Dict) -> Dict:
    trades = records.get('trades', [])
    if not trades:
        return {
            'stage': STAGE_TRADE,
            'name': '交易记录',
            'status': STATUS_SKIPPED,
            'message': '无交易记录（手动/API下单）',
            'time': None,
            'details': [],
        }

    details = []
    errors = []

    for t in trades:
        side_str = t.side.value.upper() if t.side else '?'
        status_str = t.status.value.upper() if t.status else '?'
        icon = '✅' if status_str == 'FILLED' else ('❌' if status_str == 'REJECTED' else '⏳')
        line = f'{icon} Trade #{t.id} {side_str} {_fmt_qty(t.quantity)} @ {_fmt_price(t.price)} [{status_str}]'
        if t.tiger_order_id:
            line += f' (order={t.tiger_order_id})'
        details.append(line)
        if t.error_message:
            errors.append(f'Trade #{t.id}: {t.error_message}')

        if t.stop_loss_price or t.take_profit_price:
            protection_info = []
            if t.stop_loss_price:
                protection_info.append(f'SL={_fmt_price(t.stop_loss_price)}')
            if t.take_profit_price:
                protection_info.append(f'TP={_fmt_price(t.take_profit_price)}')
            details.append(f'   保护参数: {", ".join(protection_info)}')

        if t.account_type:
            details.append(f'   账户: {t.account_type}')

    filled_trades = [t for t in trades if t.status == OrderStatus.FILLED]
    rejected_trades = [t for t in trades if t.status == OrderStatus.REJECTED]

    if filled_trades:
        status = STATUS_OK
        msg = f'交易记录已创建'
        if rejected_trades:
            status = STATUS_WARNING
            msg += f'（{len(rejected_trades)} 条被拒绝）'
    elif rejected_trades:
        status = STATUS_ERROR
        msg = f'交易记录全部被拒绝'
        if errors:
            msg += f': {errors[0]}'
    else:
        status = STATUS_WARNING
        msg = f'交易记录状态异常: {trades[0].status.value if trades[0].status else "unknown"}'

    return {
        'stage': STAGE_TRADE,
        'name': '交易记录',
        'status': status,
        'message': msg,
        'time': _fmt_time(trades[0].created_at) if trades else None,
        'details': details,
    }


def _eval_entry_order_stage(records: Dict, position: Position) -> Dict:
    trackers = records.get('trackers', [])
    entry_trackers = [t for t in trackers if t.role == OrderRole.ENTRY]

    if not entry_trackers:
        return {
            'stage': STAGE_ENTRY_ORDER,
            'name': '入场订单',
            'status': STATUS_WARNING,
            'message': '无入场订单追踪记录',
            'time': None,
            'details': [],
        }

    details = []
    errors = []

    for t in entry_trackers:
        icon = '✅' if t.status == 'FILLED' else ('❌' if t.status in TERMINAL_STATUSES - {'FILLED'} else '⏳')
        line = f'{icon} {t.side} {_fmt_qty(t.quantity)} [{t.order_type or "?"}] → {t.status}'
        if t.status == 'FILLED':
            line += f' @ {_fmt_price(t.avg_fill_price)} × {_fmt_qty(t.filled_quantity)}'
        details.append(line)
        details.append(f'   Tiger Order: {t.tiger_order_id}')
        if hasattr(t, 'fill_source') and t.fill_source:
            details.append(f'   成交检测: {_source_label(t.fill_source)}')

        if t.status not in TERMINAL_STATUSES:
            mins = _minutes_since(t.created_at)
            if mins and mins > 5:
                errors.append(f'订单卡在 {t.status} 已 {mins:.0f} 分钟')

    filled = [t for t in entry_trackers if t.status == 'FILLED']
    stuck = [t for t in entry_trackers if t.status not in TERMINAL_STATUSES]

    if filled:
        status = STATUS_OK
        msg = f'入场成交'
        total_qty = sum(t.filled_quantity or 0 for t in filled)
        avg_prices = [t.avg_fill_price for t in filled if t.avg_fill_price]
        if avg_prices and total_qty:
            wavg = sum((t.avg_fill_price or 0) * (t.filled_quantity or 0) for t in filled) / total_qty
            msg += f' {_fmt_qty(total_qty)} @ {_fmt_price(wavg)}'
    elif stuck:
        status = STATUS_ERROR
        msg = errors[0] if errors else f'入场订单未成交，状态: {stuck[0].status}'
    else:
        status = STATUS_ERROR
        msg = f'入场订单异常: {entry_trackers[0].status}'

    return {
        'stage': STAGE_ENTRY_ORDER,
        'name': '入场订单',
        'status': status,
        'message': msg,
        'time': _fmt_time(entry_trackers[0].created_at) if entry_trackers else None,
        'details': details,
    }


def _eval_position_stage(position: Position, records: Dict) -> Dict:
    details = []
    legs = records.get('legs', [])
    entry_legs = [l for l in legs if l.leg_type in (LegType.ENTRY, LegType.ADD)]

    if entry_legs:
        for l in entry_legs:
            label = '首次入场' if l.leg_type == LegType.ENTRY else '加仓'
            grade_info = ''
            if l.signal_grade:
                grade_info = f' [Grade={l.signal_grade}'
                if l.signal_score is not None:
                    grade_info += f' Score={l.signal_score}'
                grade_info += ']'
            details.append(f'✅ {label}: {_fmt_qty(l.quantity)} @ {_fmt_price(l.price)} ({_fmt_time(l.filled_at)}){grade_info}')

    details.append(f'仓位: {position.position_key} | 方向: {position.side} | 总入场: {_fmt_qty(position.total_entry_quantity)} @ {_fmt_price(position.avg_entry_price)}')
    details.append(f'账户: {position.account_type}')

    if position.status == PositionStatus.OPEN and position.total_entry_quantity <= 0:
        return {
            'stage': STAGE_POSITION,
            'name': '仓位建立',
            'status': STATUS_ERROR,
            'message': f'仓位已创建但入场数量为 0',
            'time': _fmt_time(position.opened_at),
            'details': details,
        }

    return {
        'stage': STAGE_POSITION,
        'name': '仓位建立',
        'status': STATUS_OK,
        'message': f'仓位已建立 ({position.side} {_fmt_qty(position.total_entry_quantity)} @ {_fmt_price(position.avg_entry_price)})',
        'time': _fmt_time(position.opened_at),
        'details': details,
    }


def _eval_protection_stage(position: Position, records: Dict) -> Dict:
    oca_groups = records.get('oca_groups', [])
    ts = records.get('trailing_stop')
    trackers = records.get('trackers', [])
    sl_trackers = [t for t in trackers if t.role == OrderRole.STOP_LOSS]
    tp_trackers = [t for t in trackers if t.role == OrderRole.TAKE_PROFIT]

    details = []

    if not oca_groups and not sl_trackers and not tp_trackers:
        if position.status == PositionStatus.OPEN:
            return {
                'stage': STAGE_PROTECTION,
                'name': 'OCA 保护',
                'status': STATUS_ERROR,
                'message': '仓位无任何保护（无 OCA/SL/TP）',
                'time': None,
                'details': ['该仓位没有止损或止盈保护'],
            }
        else:
            return {
                'stage': STAGE_PROTECTION,
                'name': 'OCA 保护',
                'status': STATUS_SKIPPED,
                'message': '未设置 OCA 保护',
                'time': None,
                'details': [],
            }

    for og in oca_groups:
        status_str = og.status.value if og.status else '?'
        icon = '✅' if status_str in ('active', 'cancelled') else ('⚠️' if status_str in ('triggered_stop', 'triggered_tp') else '❓')
        details.append(f'{icon} OCA #{og.id} [{status_str}] SL={_fmt_price(og.stop_price)} TP={_fmt_price(og.take_profit_price)}')
        details.append(f'   OCA Group ID: {og.oca_group_id}')
        details.append(f'   SL Order: {og.stop_order_id} | TP Order: {og.take_profit_order_id}')
        details.append(f'   TIF: {og.time_in_force} | RTH_SL: {og.outside_rth_stop} | RTH_TP: {og.outside_rth_tp}')
        if hasattr(og, 'creation_source') and og.creation_source:
            details.append(f'   创建通道: {_source_label(og.creation_source)}')
        if og.rebuild_count and og.rebuild_count > 0:
            details.append(f'   重建次数: {og.rebuild_count}')
        if og.triggered_at:
            details.append(f'   触发: @ {_fmt_price(og.triggered_price)} ({_fmt_time(og.triggered_at)})')

    for t in sl_trackers:
        icon = '✅' if t.status == 'FILLED' else ('⏳' if t.status not in TERMINAL_STATUSES else '⊘')
        details.append(f'{icon} SL 单 [{t.status}] stop={_fmt_price(t.stop_price)} order={t.tiger_order_id}')

    for t in tp_trackers:
        icon = '✅' if t.status == 'FILLED' else ('⏳' if t.status not in TERMINAL_STATUSES else '⊘')
        details.append(f'{icon} TP 单 [{t.status}] limit={_fmt_price(t.limit_price)} order={t.tiger_order_id}')

    active_oca = [og for og in oca_groups if og.status == OCAStatus.ACTIVE]
    if active_oca and position.status == PositionStatus.OPEN:
        return {
            'stage': STAGE_PROTECTION,
            'name': 'OCA 保护',
            'status': STATUS_OK,
            'message': f'OCA 保护激活中 (SL={_fmt_price(active_oca[0].stop_price)} TP={_fmt_price(active_oca[0].take_profit_price)})',
            'time': _fmt_time(oca_groups[0].created_at) if oca_groups else None,
            'details': details,
        }

    return {
        'stage': STAGE_PROTECTION,
        'name': 'OCA 保护',
        'status': STATUS_OK,
        'message': f'保护已建立 (SL={_fmt_price(oca_groups[0].stop_price) if oca_groups else "?"} TP={_fmt_price(oca_groups[0].take_profit_price) if oca_groups else "?"})',
        'time': _fmt_time(oca_groups[0].created_at) if oca_groups else None,
        'details': details,
    }


def _eval_trailing_stop_stage(position: Position, records: Dict) -> Dict:
    ts = records.get('trailing_stop')
    ts_logs = records.get('ts_logs', [])

    if not ts:
        if position.status == PositionStatus.OPEN:
            return {
                'stage': STAGE_TRAILING_STOP,
                'name': 'Trailing Stop',
                'status': STATUS_ERROR,
                'message': '仓位无 Trailing Stop',
                'time': None,
                'details': [],
            }
        return {
            'stage': STAGE_TRAILING_STOP,
            'name': 'Trailing Stop',
            'status': STATUS_SKIPPED,
            'message': '未创建 Trailing Stop',
            'time': None,
            'details': [],
        }

    details = []
    details.append(f'Entry: {_fmt_price(ts.entry_price)} | Fixed SL: {_fmt_price(ts.fixed_stop_loss)} | TS: {_fmt_price(ts.current_trailing_stop)} | TP: {_fmt_price(ts.fixed_take_profit)}')
    details.append(f'Mode: {ts.mode.value if ts.mode else "?"} | Active: {ts.is_active} | Triggered: {ts.is_triggered}')
    details.append(f'Profit Tier: {ts.profit_tier} | Progressive Tier: {ts.progressive_stop_tier} | Adjustments: {ts.stop_adjustment_count}')
    if hasattr(ts, 'creation_source') and ts.creation_source:
        details.append(f'创建通道: {_source_label(ts.creation_source)}')

    if ts.highest_price:
        details.append(f'Highest: {_fmt_price(ts.highest_price)}')
    if ts.lowest_price:
        details.append(f'Lowest: {_fmt_price(ts.lowest_price)}')

    if ts.trend_strength is not None:
        details.append(f'Trend Strength: {ts.trend_strength:.1f} | ATR Convergence: {ts.atr_convergence or "-"} | Momentum: {ts.momentum_score or "-"}')

    if ts.has_switched_to_trailing:
        details.append(f'Switched to trailing: {_fmt_time(ts.switch_triggered_at)} 原因: {ts.switch_reason or "?"}')

    if ts.breach_detected_at:
        details.append(f'⚠️ Breach detected: {_fmt_time(ts.breach_detected_at)} @ {_fmt_price(ts.breach_price)}')

    if ts_logs:
        details.append(f'调整记录: {len(ts_logs)} 条')
        for log in ts_logs[-5:]:
            details.append(f'  {_fmt_time(log.created_at)} [{log.event_type}] price={_fmt_price(log.current_price)} stop={_fmt_price(log.trailing_stop_price)}')

    if ts.is_triggered:
        status = STATUS_OK
        msg = f'Trailing Stop 已触发 @ {_fmt_price(ts.triggered_price)} ({_fmt_time(ts.triggered_at)})'
        if ts.trigger_reason:
            details.append(f'触发原因: {ts.trigger_reason}')
    elif ts.is_active:
        status = STATUS_OK
        profit_info = f' profit={ts.current_profit_pct:.2f}%' if ts.current_profit_pct else ''
        msg = f'Trailing Stop 运行中 (tier={ts.profit_tier}, mode={ts.mode.value if ts.mode else "?"}){profit_info}'
    elif not ts.is_active and not ts.is_triggered:
        status = STATUS_WARNING
        msg = f'Trailing Stop 已停用但未触发（异常停用）'
    else:
        status = STATUS_OK
        msg = f'Trailing Stop 状态: active={ts.is_active}, triggered={ts.is_triggered}'

    if ts.is_triggered and position.status == PositionStatus.OPEN:
        exit_trackers = [t for t in records.get('trackers', []) if t.role in (
            OrderRole.EXIT_TRAILING, OrderRole.EXIT_SIGNAL
        ) and t.status == 'FILLED']
        if not exit_trackers:
            status = STATUS_ERROR
            msg = f'Trailing Stop 已触发但仓位仍未关闭'
            mins = _minutes_since(ts.triggered_at)
            if mins:
                msg += f' (已 {mins:.0f} 分钟)'

    return {
        'stage': STAGE_TRAILING_STOP,
        'name': 'Trailing Stop',
        'status': status,
        'message': msg,
        'time': _fmt_time(ts.created_at),
        'details': details,
    }


def _eval_exit_stage(position: Position, records: Dict) -> Dict:
    trackers = records.get('trackers', [])
    exit_trackers = [t for t in trackers if t.role in (
        OrderRole.EXIT_SIGNAL, OrderRole.EXIT_TRAILING,
        OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT,
    ) and t.status == 'FILLED']

    all_exit_trackers = [t for t in trackers if t.role in (
        OrderRole.EXIT_SIGNAL, OrderRole.EXIT_TRAILING,
        OrderRole.STOP_LOSS, OrderRole.TAKE_PROFIT,
    )]

    details = []
    for t in all_exit_trackers:
        icon = '✅' if t.status == 'FILLED' else ('⊘' if t.status in TERMINAL_STATUSES else '⏳')
        line = f'{icon} {t.role.value} {t.side} {_fmt_qty(t.quantity)} [{t.status}]'
        if t.status == 'FILLED':
            line += f' @ {_fmt_price(t.avg_fill_price)}'
            if t.realized_pnl is not None:
                line += f' P&L={_fmt_price(t.realized_pnl)}'
        if hasattr(t, 'fill_source') and t.fill_source:
            line += f' [{_source_label(t.fill_source)}]'
        details.append(line)

    if exit_trackers:
        t = exit_trackers[-1]
        return {
            'stage': STAGE_EXIT,
            'name': '出场执行',
            'status': STATUS_OK,
            'message': f'出场成交 ({t.role.value}) {_fmt_qty(t.filled_quantity)} @ {_fmt_price(t.avg_fill_price)}',
            'time': _fmt_time(t.fill_time),
            'details': details,
        }

    stuck = [t for t in all_exit_trackers if t.status not in TERMINAL_STATUSES]
    if stuck:
        t = stuck[0]
        mins = _minutes_since(t.created_at)
        msg = f'出场订单未成交，状态: {t.status}'
        if mins and mins > 5:
            msg += f' (已 {mins:.0f} 分钟)'
        return {
            'stage': STAGE_EXIT,
            'name': '出场执行',
            'status': STATUS_ERROR,
            'message': msg,
            'time': _fmt_time(t.created_at),
            'details': details,
        }

    ts = records.get('trailing_stop')
    if ts and ts.is_triggered:
        return {
            'stage': STAGE_EXIT,
            'name': '出场执行',
            'status': STATUS_ERROR,
            'message': f'Trailing Stop 已触发但无出场订单记录',
            'time': _fmt_time(ts.triggered_at),
            'details': details,
        }

    return {
        'stage': STAGE_EXIT,
        'name': '出场执行',
        'status': STATUS_PENDING,
        'message': '等待出场',
        'time': None,
        'details': details,
    }


def _eval_close_stage(position: Position, records: Dict) -> Dict:
    legs = records.get('legs', [])
    exit_legs = [l for l in legs if l.leg_type == LegType.EXIT]

    details = []
    for l in exit_legs:
        method = l.exit_method.value if l.exit_method else '未知'
        details.append(f'✅ 出场: {_fmt_qty(l.quantity)} @ {_fmt_price(l.price)} 方式={method} ({_fmt_time(l.filled_at)})')
        if l.realized_pnl is not None:
            details.append(f'   P&L: {_fmt_price(l.realized_pnl)} commission={_fmt_price(l.commission)}')

    if position.status == PositionStatus.CLOSED:
        pnl_str = _fmt_price(position.realized_pnl) if position.realized_pnl is not None else '未计算'
        pnl_pct = ''
        if position.pnl_percent is not None:
            pnl_pct = f' ({position.pnl_percent:+.2f}%)'

        hold_secs = position.hold_duration_seconds
        hold_str = ''
        if hold_secs:
            if hold_secs >= 3600:
                hold_str = f' | 持仓 {hold_secs//3600}h{(hold_secs%3600)//60}m'
            else:
                hold_str = f' | 持仓 {hold_secs//60}m{hold_secs%60}s'

        details.append(f'P&L: {pnl_str}{pnl_pct}{hold_str}')
        if hasattr(position, 'close_source') and position.close_source:
            details.append(f'关闭通道: {_source_label(position.close_source)}')

        if not exit_legs:
            return {
                'stage': STAGE_CLOSE,
                'name': '仓位关闭',
                'status': STATUS_WARNING,
                'message': f'仓位已关闭但无出场 Leg 记录 (P&L={pnl_str}{pnl_pct})',
                'time': _fmt_time(position.closed_at),
                'details': details,
            }

        return {
            'stage': STAGE_CLOSE,
            'name': '仓位关闭',
            'status': STATUS_OK,
            'message': f'仓位已关闭 P&L={pnl_str}{pnl_pct}{hold_str}',
            'time': _fmt_time(position.closed_at),
            'details': details,
        }

    if position.status == PositionStatus.OPEN:
        return {
            'stage': STAGE_CLOSE,
            'name': '仓位关闭',
            'status': STATUS_ERROR,
            'message': '仓位仍然 OPEN（应该已关闭）',
            'time': None,
            'details': details,
        }

    return {
        'stage': STAGE_CLOSE,
        'name': '仓位关闭',
        'status': STATUS_PENDING,
        'message': '等待关闭',
        'time': None,
        'details': details,
    }


def _build_lifecycle_for_orphan_tracker(tracker: OrderTracker) -> Dict:
    stages = []

    stages.append({
        'stage': STAGE_SIGNAL,
        'name': '信号接收',
        'status': STATUS_SKIPPED,
        'message': '无关联信号记录',
        'time': None,
        'details': [],
    })

    stages.append({
        'stage': STAGE_ENTRY_ORDER,
        'name': '入场订单',
        'status': STATUS_OK if tracker.status == 'FILLED' else (STATUS_ERROR if tracker.status in TERMINAL_STATUSES else STATUS_PENDING),
        'message': f'订单 [{tracker.status}] {tracker.side} {_fmt_qty(tracker.quantity)} @ {_fmt_price(tracker.avg_fill_price) if tracker.avg_fill_price else tracker.order_type}',
        'time': _fmt_time(tracker.created_at),
        'details': [f'Tiger Order ID: {tracker.tiger_order_id}', f'Role: {tracker.role.value}'],
    })

    stages.append({
        'stage': STAGE_POSITION,
        'name': '仓位建立',
        'status': STATUS_ERROR,
        'message': '未找到关联的仓位记录（孤立订单）',
        'time': None,
        'details': [],
    })

    return {
        'type': 'orphan_tracker',
        'tracker': tracker,
        'stages': stages,
        'records': {'trackers': [tracker]},
        'overall_status': STATUS_ERROR,
        'summary': {
            'symbol': tracker.symbol,
            'side': tracker.side or '?',
            'status': tracker.status,
            'entry': _fmt_price(tracker.avg_fill_price),
            'exit': '',
            'pnl': '',
            'hold_time': '',
        },
    }


def _build_summary(position: Position, records: Dict) -> Dict:
    hold_secs = position.hold_duration_seconds
    hold_str = ''
    if hold_secs:
        if hold_secs >= 3600:
            hold_str = f'{hold_secs // 3600}h{(hold_secs % 3600) // 60}m'
        else:
            hold_str = f'{hold_secs // 60}m{hold_secs % 60}s'

    pnl_str = ''
    if position.realized_pnl is not None:
        pnl_str = f'{_fmt_price(position.realized_pnl)}'
        if position.pnl_percent is not None:
            pnl_str += f' ({position.pnl_percent:+.2f}%)'

    return {
        'symbol': position.symbol,
        'side': position.side,
        'status': position.status.value,
        'account_type': position.account_type,
        'position_key': position.position_key,
        'entry': f'{_fmt_qty(position.total_entry_quantity)} @ {_fmt_price(position.avg_entry_price)}',
        'exit': f'{_fmt_qty(position.total_exit_quantity)} @ {_fmt_price(position.avg_exit_price)}' if position.total_exit_quantity else '',
        'pnl': pnl_str,
        'hold_time': hold_str,
        'opened_at': _fmt_time(position.opened_at),
        'closed_at': _fmt_time(position.closed_at),
    }


def get_global_health(account_type: str = None) -> Dict:
    query = Position.query.filter_by(status=PositionStatus.OPEN)
    if account_type:
        query = query.filter_by(account_type=account_type)
    open_positions = query.all()

    issues = []
    ok_count = 0
    warning_count = 0
    error_count = 0

    for pos in open_positions:
        pos_issues = _check_position_health(pos)
        if pos_issues:
            issues.extend(pos_issues)
            has_error = any(i['severity'] == 'error' for i in pos_issues)
            has_warning = any(i['severity'] == 'warning' for i in pos_issues)
            if has_error:
                error_count += 1
            elif has_warning:
                warning_count += 1
            else:
                ok_count += 1
        else:
            ok_count += 1

    stuck_query = OrderTracker.query.filter(
        OrderTracker.status.notin_(list(TERMINAL_STATUSES)),
        OrderTracker.created_at < datetime.utcnow() - timedelta(minutes=5),
    )
    if account_type:
        stuck_query = stuck_query.filter_by(account_type=account_type)
    stuck_orders = stuck_query.all()

    for order in stuck_orders:
        mins = _minutes_since(order.created_at)
        in_open_pos = any(pos.symbol == order.symbol and pos.account_type == order.account_type for pos in open_positions)
        if not in_open_pos:
            issues.append({
                'severity': 'warning',
                'symbol': order.symbol,
                'message': f'订单 {order.role.value} 卡在 {order.status} 已 {mins:.0f} 分钟',
                'position_id': None,
                'detail': f'Tiger Order: {order.tiger_order_id}',
                'account_type': order.account_type,
            })

    issues.sort(key=lambda x: (0 if x['severity'] == 'error' else 1 if x['severity'] == 'warning' else 2))

    return {
        'total_open': len(open_positions),
        'ok_count': ok_count,
        'warning_count': warning_count,
        'error_count': error_count,
        'issues': issues,
    }


def _check_position_health(position: Position) -> List[Dict]:
    issues = []

    if not position.trailing_stop_id:
        issues.append({
            'severity': 'error',
            'symbol': position.symbol,
            'message': '仓位无 Trailing Stop',
            'position_id': position.id,
            'detail': f'{position.side} {_fmt_qty(position.total_entry_quantity)} @ {_fmt_price(position.avg_entry_price)} [{position.account_type}]',
            'account_type': position.account_type,
        })
        return issues

    ts = TrailingStopPosition.query.get(position.trailing_stop_id)
    if not ts:
        issues.append({
            'severity': 'error',
            'symbol': position.symbol,
            'message': f'Trailing Stop #{position.trailing_stop_id} 记录不存在',
            'position_id': position.id,
            'detail': '',
            'account_type': position.account_type,
        })
        return issues

    if ts.is_triggered and position.status == PositionStatus.OPEN:
        mins = _minutes_since(ts.triggered_at)
        msg = f'TS 已触发但仓位未关闭'
        if mins:
            msg += f' (已 {mins:.0f} 分钟)'
        issues.append({
            'severity': 'error',
            'symbol': position.symbol,
            'message': msg,
            'position_id': position.id,
            'detail': f'触发原因: {ts.trigger_reason or "未知"}',
            'account_type': position.account_type,
        })

    if not ts.is_active and not ts.is_triggered:
        issues.append({
            'severity': 'warning',
            'symbol': position.symbol,
            'message': 'Trailing Stop 异常停用（未触发）',
            'position_id': position.id,
            'detail': f'TS #{ts.id}, mode={ts.mode.value if ts.mode else "?"}',
            'account_type': position.account_type,
        })

    if position.account_type != 'paper':
        oca_groups = OCAGroup.query.filter_by(
            trailing_stop_id=ts.id,
            status=OCAStatus.ACTIVE,
        ).all()
        if not oca_groups and position.status == PositionStatus.OPEN:
            cancelled_oca = OCAGroup.query.filter(
                OCAGroup.trailing_stop_id == ts.id,
                OCAGroup.status != OCAStatus.ACTIVE,
            ).first()
            if cancelled_oca:
                pass
            else:
                issues.append({
                    'severity': 'warning',
                    'symbol': position.symbol,
                    'message': '无激活的 OCA 保护组',
                    'position_id': position.id,
                    'detail': f'TS #{ts.id}',
                    'account_type': position.account_type,
                })

    return issues


def get_recent_closed_lifecycles(account_type: str = None, limit: int = 20) -> List[Dict]:
    query = Position.query.filter_by(status=PositionStatus.CLOSED)
    if account_type:
        query = query.filter_by(account_type=account_type)
    positions = query.order_by(Position.closed_at.desc()).limit(limit).all()

    results = []
    for pos in positions:
        try:
            lc = _build_lifecycle_for_position(pos)
            if lc:
                results.append(lc)
        except Exception as e:
            logger.warning(f"Failed to build lifecycle for closed position {pos.id}: {e}")
    return results
