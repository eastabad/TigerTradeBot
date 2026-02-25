import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from app import db
from alpaca.models import (
    AlpacaSignalLog, AlpacaTrade, AlpacaOrderTracker, AlpacaPosition,
    AlpacaPositionLeg, AlpacaOCOGroup, AlpacaTrailingStopPosition,
    AlpacaTrailingStopLog, AlpacaPositionStatus, AlpacaOrderRole,
    AlpacaOCOStatus, AlpacaLegType, AlpacaSystemLog,
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


TERMINAL_STATUSES = {'FILLED', 'CANCELLED', 'CANCELED', 'EXPIRED', 'REJECTED', 'REPLACED'}

STAGE_SOURCE_MAP = {
    STAGE_SIGNAL: {
        'sources': ['routes', 'webhook'],
        'categories': ['general'],
        'keywords': ['webhook', 'signal', 'parse'],
    },
    STAGE_TRADE: {
        'sources': ['routes', 'alpaca_client'],
        'categories': ['order'],
        'keywords': ['trade', 'order_type', 'submit'],
    },
    STAGE_ENTRY_ORDER: {
        'sources': ['alpaca_client', 'order_tracker', 'scheduler'],
        'categories': ['order'],
        'keywords': ['entry', 'place_order', 'ENTRY', 'bracket'],
    },
    STAGE_POSITION: {
        'sources': ['scheduler', 'order_tracker'],
        'categories': ['position'],
        'keywords': ['position', 'holdings'],
    },
    STAGE_PROTECTION: {
        'sources': ['scheduler', 'order_tracker', 'trailing_stop'],
        'categories': ['order'],
        'keywords': ['OCO', 'oco', 'stop_loss', 'take_profit', 'protection', 'SL', 'TP', 'bracket'],
    },
    STAGE_TRAILING_STOP: {
        'sources': ['trailing_stop', 'scheduler'],
        'categories': ['trailing_stop'],
        'keywords': ['trailing', 'stop', 'breach', 'tier'],
    },
    STAGE_EXIT: {
        'sources': ['trailing_stop', 'order_tracker', 'scheduler'],
        'categories': ['order'],
        'keywords': ['exit', 'EXIT', 'close', 'sell', 'buy_to_cover'],
    },
    STAGE_CLOSE: {
        'sources': ['scheduler', 'order_tracker'],
        'categories': ['position'],
        'keywords': ['close', 'closed', 'reconcil', 'P&L', 'pnl', 'ghost'],
    },
}


def _fetch_related_logs(symbol: str, stage: str,
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
        source_conditions.append(AlpacaSystemLog.source.in_(mapping['sources']))
    if mapping.get('categories'):
        source_conditions.append(AlpacaSystemLog.category.in_(mapping['categories']))

    query = AlpacaSystemLog.query.filter(
        AlpacaSystemLog.level.in_(['ERROR', 'WARNING', 'CRITICAL']),
        AlpacaSystemLog.timestamp >= time_start,
        AlpacaSystemLog.timestamp <= time_end,
        db.or_(
            AlpacaSystemLog.symbol == symbol,
            AlpacaSystemLog.message.ilike(f'%{symbol}%'),
        ),
        db.or_(*source_conditions) if source_conditions else db.true(),
    )

    logs = query.order_by(AlpacaSystemLog.timestamp.desc()).limit(limit * 3).all()

    if logs:
        scored = []
        for log in logs:
            msg_lower = (log.message or '').lower()
            kw_hits = sum(1 for kw in mapping.get('keywords', []) if kw.lower() in msg_lower)
            source_hit = 1 if log.source in (mapping.get('sources') or []) else 0
            scored.append((kw_hits + source_hit, log))
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


def build_lifecycle(symbol: str = None, order_id: str = None, position_id: int = None) -> Optional[Dict]:
    if position_id:
        position = AlpacaPosition.query.get(position_id)
        if not position:
            return None
        symbol = position.symbol
        return _build_lifecycle_for_position(position)

    if order_id:
        tracker = AlpacaOrderTracker.query.filter_by(alpaca_order_id=order_id).first()
        if tracker:
            symbol = tracker.symbol
            position = _find_position_for_tracker(tracker)
            if position:
                return _build_lifecycle_for_position(position)
            return _build_lifecycle_for_orphan_tracker(tracker)
        return None

    if symbol:
        positions = AlpacaPosition.query.filter_by(symbol=symbol.upper()).order_by(
            AlpacaPosition.opened_at.desc()
        ).all()
        if positions:
            results = []
            for pos in positions:
                lc = _build_lifecycle_for_position(pos)
                if lc:
                    results.append(lc)
            return results if results else None
        trackers = AlpacaOrderTracker.query.filter_by(symbol=symbol.upper()).order_by(
            AlpacaOrderTracker.created_at.desc()
        ).all()
        if trackers:
            results = []
            seen_ids = set()
            for t in trackers:
                if t.id not in seen_ids:
                    seen_ids.add(t.id)
                    results.append(_build_lifecycle_for_orphan_tracker(t))
            return results if results else None

    return None


def _find_position_for_tracker(tracker: AlpacaOrderTracker) -> Optional[AlpacaPosition]:
    leg = AlpacaPositionLeg.query.filter_by(alpaca_order_id=tracker.alpaca_order_id).first()
    if leg:
        return AlpacaPosition.query.get(leg.position_id)
    if tracker.trade_id:
        leg2 = AlpacaPositionLeg.query.filter_by(trade_id=tracker.trade_id).first()
        if leg2:
            return AlpacaPosition.query.get(leg2.position_id)
    if tracker.trailing_stop_id:
        pos = AlpacaPosition.query.filter_by(trailing_stop_id=tracker.trailing_stop_id).first()
        if pos:
            return pos
    return None


def _build_lifecycle_for_position(position: AlpacaPosition) -> Dict:
    stages = []
    all_records = {}

    legs = AlpacaPositionLeg.query.filter_by(position_id=position.id).order_by(
        AlpacaPositionLeg.filled_at.asc()
    ).all()
    all_records['legs'] = legs

    entry_legs = [l for l in legs if l.leg_type in (AlpacaLegType.ENTRY, AlpacaLegType.ADD)]
    exit_legs = [l for l in legs if l.leg_type == AlpacaLegType.EXIT]

    trade_ids = set()
    order_ids = set()
    for l in legs:
        if l.trade_id:
            trade_ids.add(l.trade_id)
        if l.alpaca_order_id:
            order_ids.add(l.alpaca_order_id)

    if position.trailing_stop_id:
        ts = AlpacaTrailingStopPosition.query.get(position.trailing_stop_id)
    else:
        ts = AlpacaTrailingStopPosition.query.filter_by(symbol=position.symbol).order_by(
            AlpacaTrailingStopPosition.created_at.desc()
        ).first()
        if ts and ts.trade_id:
            if ts.trade_id not in trade_ids:
                ts = None
    all_records['trailing_stop'] = ts

    if ts and ts.trade_id:
        trade_ids.add(ts.trade_id)

    trades = []
    if trade_ids:
        trades = AlpacaTrade.query.filter(AlpacaTrade.id.in_(trade_ids)).order_by(
            AlpacaTrade.created_at.asc()
        ).all()
        for t in trades:
            if t.alpaca_order_id:
                order_ids.add(t.alpaca_order_id)
    all_records['trades'] = trades

    signal_logs = []
    if trade_ids:
        signal_logs = AlpacaSignalLog.query.filter(AlpacaSignalLog.trade_id.in_(trade_ids)).order_by(
            AlpacaSignalLog.created_at.asc()
        ).all()
        failed_signals = AlpacaSignalLog.query.filter(
            AlpacaSignalLog.symbol == position.symbol,
            AlpacaSignalLog.status.in_(['error', 'rejected']),
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

    trackers = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.symbol == position.symbol,
        db.or_(
            AlpacaOrderTracker.alpaca_order_id.in_(order_ids) if order_ids else db.false(),
            AlpacaOrderTracker.trade_id.in_(trade_ids) if trade_ids else db.false(),
            AlpacaOrderTracker.trailing_stop_id == position.trailing_stop_id if position.trailing_stop_id else db.false(),
        )
    ).order_by(AlpacaOrderTracker.created_at.asc()).all()
    for t in trackers:
        if t.alpaca_order_id:
            order_ids.add(t.alpaca_order_id)
        if t.oco_group_id:
            pass
    all_records['trackers'] = trackers

    oco_group_ids = set()
    for t in trackers:
        if t.oco_group_id:
            oco_group_ids.add(t.oco_group_id)
    for l in legs:
        if l.oco_group_id:
            oco_group_ids.add(l.oco_group_id)

    oco_groups = []
    if oco_group_ids:
        oco_groups = AlpacaOCOGroup.query.filter(AlpacaOCOGroup.id.in_(oco_group_ids)).order_by(
            AlpacaOCOGroup.created_at.asc()
        ).all()
    else:
        if trade_ids:
            oco_groups = AlpacaOCOGroup.query.filter(
                AlpacaOCOGroup.trade_id.in_(trade_ids)
            ).order_by(AlpacaOCOGroup.created_at.asc()).all()
        if not oco_groups and ts:
            oco_groups = AlpacaOCOGroup.query.filter_by(trailing_stop_id=ts.id).order_by(
                AlpacaOCOGroup.created_at.asc()
            ).all()
    all_records['oco_groups'] = oco_groups

    ts_logs = []
    if ts:
        ts_logs = AlpacaTrailingStopLog.query.filter_by(trailing_stop_id=ts.id).order_by(
            AlpacaTrailingStopLog.created_at.asc()
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


def _evaluate_stages(position: AlpacaPosition, records: Dict) -> List[Dict]:
    stages = []

    stages.append(_eval_signal_stage(records))
    stages.append(_eval_trade_stage(records))
    stages.append(_eval_entry_order_stage(records, position))
    stages.append(_eval_position_stage(position, records))
    stages.append(_eval_protection_stage(position, records))
    stages.append(_eval_trailing_stop_stage(position, records))

    if position.status == AlpacaPositionStatus.CLOSED or records.get('trailing_stop') and records['trailing_stop'].is_triggered:
        stages.append(_eval_exit_stage(position, records))
        stages.append(_eval_close_stage(position, records))
    elif position.status == AlpacaPositionStatus.OPEN:
        exit_trackers = [t for t in records.get('trackers', []) if t.role in (
            AlpacaOrderRole.EXIT_SIGNAL, AlpacaOrderRole.EXIT_TRAILING
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

    executed = [s for s in signal_logs if s.status == 'executed']
    failed = [s for s in signal_logs if s.status in ('error', 'rejected')]

    details = []
    for s in signal_logs:
        icon = '✅' if s.status == 'executed' else '❌'
        details.append(f'{icon} #{s.id} {s.action} [{s.status}] {_fmt_time(s.created_at)}')
        if s.error_message:
            details.append(f'   原因: {s.error_message}')

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

    entry_trades = [t for t in trades if t.side and t.side.value in ('buy', 'sell')]
    details = []
    errors = []

    for t in trades:
        side_str = t.side.value.upper() if t.side else '?'
        status_str = t.status.value.upper() if t.status else '?'
        icon = '✅' if status_str == 'FILLED' else ('❌' if status_str == 'REJECTED' else '⏳')
        line = f'{icon} Trade #{t.id} {side_str} {_fmt_qty(t.quantity)} @ {_fmt_price(t.price)} [{status_str}]'
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

    filled_trades = [t for t in trades if t.status and t.status.value == 'filled']
    rejected_trades = [t for t in trades if t.status and t.status.value == 'rejected']

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


def _eval_entry_order_stage(records: Dict, position: AlpacaPosition) -> Dict:
    trackers = records.get('trackers', [])
    entry_trackers = [t for t in trackers if t.role == AlpacaOrderRole.ENTRY]

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


def _eval_position_stage(position: AlpacaPosition, records: Dict) -> Dict:
    details = []
    legs = records.get('legs', [])
    entry_legs = [l for l in legs if l.leg_type in (AlpacaLegType.ENTRY, AlpacaLegType.ADD)]

    if entry_legs:
        for l in entry_legs:
            label = '首次入场' if l.leg_type == AlpacaLegType.ENTRY else '加仓'
            details.append(f'✅ {label}: {_fmt_qty(l.quantity)} @ {_fmt_price(l.price)} ({_fmt_time(l.filled_at)})')

    details.append(f'持仓方向: {position.side} | 总入场: {_fmt_qty(position.total_entry_quantity)} @ {_fmt_price(position.avg_entry_price)}')

    if position.status == AlpacaPositionStatus.OPEN and position.total_entry_quantity <= 0:
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


def _eval_protection_stage(position: AlpacaPosition, records: Dict) -> Dict:
    oco_groups = records.get('oco_groups', [])
    ts = records.get('trailing_stop')
    trackers = records.get('trackers', [])
    sl_trackers = [t for t in trackers if t.role == AlpacaOrderRole.STOP_LOSS]
    tp_trackers = [t for t in trackers if t.role == AlpacaOrderRole.TAKE_PROFIT]

    details = []

    if not oco_groups and not sl_trackers and not tp_trackers:
        if position.status == AlpacaPositionStatus.OPEN:
            return {
                'stage': STAGE_PROTECTION,
                'name': 'OCO 保护',
                'status': STATUS_ERROR,
                'message': '仓位无任何保护（无 OCO/SL/TP）',
                'time': None,
                'details': ['该仓位没有止损或止盈保护'],
            }
        else:
            return {
                'stage': STAGE_PROTECTION,
                'name': 'OCO 保护',
                'status': STATUS_SKIPPED,
                'message': '未设置 OCO 保护',
                'time': None,
                'details': [],
            }

    for og in oco_groups:
        status_str = og.status.value if og.status else '?'
        icon = '✅' if status_str in ('ACTIVE', 'CANCELLED') else ('⚠️' if status_str in ('TRIGGERED_STOP', 'TRIGGERED_TP') else '❓')
        details.append(f'{icon} OCO #{og.id} [{status_str}] SL={_fmt_price(og.stop_price)} TP={_fmt_price(og.take_profit_price)}')
        if og.modify_count and og.modify_count > 0:
            details.append(f'   修改次数: {og.modify_count}')

    for t in sl_trackers:
        icon = '✅' if t.status == 'FILLED' else ('⏳' if t.status not in TERMINAL_STATUSES else '⊘')
        details.append(f'{icon} SL 单 [{t.status}] price={_fmt_price(t.stop_price)}')

    for t in tp_trackers:
        icon = '✅' if t.status == 'FILLED' else ('⏳' if t.status not in TERMINAL_STATUSES else '⊘')
        details.append(f'{icon} TP 单 [{t.status}] price={_fmt_price(t.limit_price)}')

    stuck_trackers = [t for t in sl_trackers + tp_trackers if t.status not in TERMINAL_STATUSES and t.status != 'HELD']

    if stuck_trackers and position.status == AlpacaPositionStatus.OPEN:
        mins = _minutes_since(stuck_trackers[0].created_at)
        if mins and mins > 5:
            return {
                'stage': STAGE_PROTECTION,
                'name': 'OCO 保护',
                'status': STATUS_WARNING,
                'message': f'保护单状态异常: {stuck_trackers[0].status} 已 {mins:.0f} 分钟',
                'time': _fmt_time(oco_groups[0].created_at) if oco_groups else None,
                'details': details,
            }

    return {
        'stage': STAGE_PROTECTION,
        'name': 'OCO 保护',
        'status': STATUS_OK,
        'message': f'保护已建立 (SL={_fmt_price(oco_groups[0].stop_price) if oco_groups else "?"} TP={_fmt_price(oco_groups[0].take_profit_price) if oco_groups else "?"})',
        'time': _fmt_time(oco_groups[0].created_at) if oco_groups else None,
        'details': details,
    }


def _eval_trailing_stop_stage(position: AlpacaPosition, records: Dict) -> Dict:
    ts = records.get('trailing_stop')
    ts_logs = records.get('ts_logs', [])

    if not ts:
        if position.status == AlpacaPositionStatus.OPEN:
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
    details.append(f'Entry: {_fmt_price(ts.entry_price)} | SL: {_fmt_price(ts.stop_loss_price)} | TS: {_fmt_price(ts.trailing_stop_price)} | TP: {_fmt_price(ts.take_profit_price)}')
    details.append(f'Phase: {ts.phase} | Active: {ts.is_active} | Triggered: {ts.is_triggered}')
    if ts.highest_price:
        details.append(f'Highest: {_fmt_price(ts.highest_price)} | Lowest: {_fmt_price(ts.lowest_price)}')
    if ts_logs:
        details.append(f'调整记录: {len(ts_logs)} 条')
        for log in ts_logs[-5:]:
            details.append(f'  {_fmt_time(log.created_at)} [{log.event_type}] price={_fmt_price(log.current_price)} ts={_fmt_price(log.trailing_stop_price)}')

    if ts.is_triggered:
        status = STATUS_OK
        msg = f'Trailing Stop 已触发 @ {_fmt_price(ts.triggered_price)} ({_fmt_time(ts.triggered_at)})'
        if ts.trigger_reason:
            details.append(f'触发原因: {ts.trigger_reason}')
    elif ts.is_active:
        status = STATUS_OK
        msg = f'Trailing Stop 运行中 (phase={ts.phase})'
    elif not ts.is_active and not ts.is_triggered:
        status = STATUS_WARNING
        msg = f'Trailing Stop 已停用但未触发（异常停用）'
    else:
        status = STATUS_OK
        msg = f'Trailing Stop 状态: active={ts.is_active}, triggered={ts.is_triggered}'

    if ts.is_triggered and position.status == AlpacaPositionStatus.OPEN:
        exit_trackers = [t for t in records.get('trackers', []) if t.role in (
            AlpacaOrderRole.EXIT_TRAILING, AlpacaOrderRole.EXIT_SIGNAL
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


def _eval_exit_stage(position: AlpacaPosition, records: Dict) -> Dict:
    trackers = records.get('trackers', [])
    exit_trackers = [t for t in trackers if t.role in (
        AlpacaOrderRole.EXIT_SIGNAL, AlpacaOrderRole.EXIT_TRAILING,
        AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT,
    ) and t.status == 'FILLED']

    all_exit_trackers = [t for t in trackers if t.role in (
        AlpacaOrderRole.EXIT_SIGNAL, AlpacaOrderRole.EXIT_TRAILING,
        AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT,
    )]

    details = []
    for t in all_exit_trackers:
        icon = '✅' if t.status == 'FILLED' else ('⊘' if t.status in TERMINAL_STATUSES else '⏳')
        line = f'{icon} {t.role.value} {t.side} {_fmt_qty(t.quantity)} [{t.status}]'
        if t.status == 'FILLED':
            line += f' @ {_fmt_price(t.avg_fill_price)}'
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


def _eval_close_stage(position: AlpacaPosition, records: Dict) -> Dict:
    legs = records.get('legs', [])
    exit_legs = [l for l in legs if l.leg_type == AlpacaLegType.EXIT]

    details = []
    for l in exit_legs:
        method = l.exit_method.value if l.exit_method else '未知'
        details.append(f'✅ 出场: {_fmt_qty(l.quantity)} @ {_fmt_price(l.price)} 方式={method} ({_fmt_time(l.filled_at)})')

    if position.status == AlpacaPositionStatus.CLOSED:
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

    if position.status == AlpacaPositionStatus.OPEN:
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


def _build_lifecycle_for_orphan_tracker(tracker: AlpacaOrderTracker) -> Dict:
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
        'details': [f'Order ID: {tracker.alpaca_order_id}', f'Role: {tracker.role.value}'],
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


def _build_summary(position: AlpacaPosition, records: Dict) -> Dict:
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
        'entry': f'{_fmt_qty(position.total_entry_quantity)} @ {_fmt_price(position.avg_entry_price)}',
        'exit': f'{_fmt_qty(position.total_exit_quantity)} @ {_fmt_price(position.avg_exit_price)}' if position.total_exit_quantity else '',
        'pnl': pnl_str,
        'hold_time': hold_str,
        'opened_at': _fmt_time(position.opened_at),
        'closed_at': _fmt_time(position.closed_at),
    }


def get_global_health() -> Dict:
    open_positions = AlpacaPosition.query.filter_by(status=AlpacaPositionStatus.OPEN).all()

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

    stuck_orders = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.status.notin_(list(TERMINAL_STATUSES)),
        AlpacaOrderTracker.created_at < datetime.utcnow() - timedelta(minutes=5),
    ).all()
    for order in stuck_orders:
        mins = _minutes_since(order.created_at)
        in_open_pos = any(pos.symbol == order.symbol for pos in open_positions)
        if not in_open_pos:
            issues.append({
                'severity': 'warning',
                'symbol': order.symbol,
                'message': f'订单 {order.role.value} 卡在 {order.status} 已 {mins:.0f} 分钟',
                'position_id': None,
                'detail': f'Order ID: {order.alpaca_order_id[:12]}...',
            })

    issues.sort(key=lambda x: (0 if x['severity'] == 'error' else 1 if x['severity'] == 'warning' else 2))

    return {
        'total_open': len(open_positions),
        'ok_count': ok_count,
        'warning_count': warning_count,
        'error_count': error_count,
        'issues': issues,
    }


def _check_position_health(position: AlpacaPosition) -> List[Dict]:
    issues = []

    if not position.trailing_stop_id:
        issues.append({
            'severity': 'error',
            'symbol': position.symbol,
            'message': '仓位无 Trailing Stop',
            'position_id': position.id,
            'detail': f'{position.side} {_fmt_qty(position.total_entry_quantity)} @ {_fmt_price(position.avg_entry_price)}',
        })
        return issues

    ts = AlpacaTrailingStopPosition.query.get(position.trailing_stop_id)
    if not ts:
        issues.append({
            'severity': 'error',
            'symbol': position.symbol,
            'message': f'Trailing Stop #{position.trailing_stop_id} 记录不存在',
            'position_id': position.id,
            'detail': '',
        })
        return issues

    if ts.is_triggered and position.status == AlpacaPositionStatus.OPEN:
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
        })

    if not ts.is_active and not ts.is_triggered:
        issues.append({
            'severity': 'warning',
            'symbol': position.symbol,
            'message': 'Trailing Stop 异常停用（未触发）',
            'position_id': position.id,
            'detail': f'TS #{ts.id}, phase={ts.phase}',
        })

    trackers = AlpacaOrderTracker.query.filter(
        AlpacaOrderTracker.symbol == position.symbol,
        AlpacaOrderTracker.trailing_stop_id == ts.id,
        AlpacaOrderTracker.role.in_([AlpacaOrderRole.STOP_LOSS, AlpacaOrderRole.TAKE_PROFIT]),
        AlpacaOrderTracker.status.notin_(list(TERMINAL_STATUSES)),
    ).all()
    return issues


def get_recent_closed_lifecycles(limit: int = 20) -> List[Dict]:
    positions = AlpacaPosition.query.filter_by(
        status=AlpacaPositionStatus.CLOSED
    ).order_by(AlpacaPosition.closed_at.desc()).limit(limit).all()

    results = []
    for pos in positions:
        try:
            lc = _build_lifecycle_for_position(pos)
            if lc:
                results.append(lc)
        except Exception as e:
            logger.warning(f"Failed to build lifecycle for closed position {pos.id}: {e}")
    return results
