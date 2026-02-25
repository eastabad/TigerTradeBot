import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class MatchResult:
    def __init__(self):
        self.matched_groups: List[Dict] = []
        self.unmatched_signals: List[Dict] = []
        self.unmatched_trackers: List[Dict] = []
        self.unmatched_api_fills: List[Dict] = []
        self.anomalies: List[Dict] = []
        self.stats: Dict = {}

    def to_dict(self) -> Dict:
        return {
            'matched_groups': self.matched_groups,
            'unmatched_signals': [_strip_raw(s) for s in self.unmatched_signals],
            'unmatched_trackers': [_strip_raw(t) for t in self.unmatched_trackers],
            'unmatched_api_fills': [_strip_raw(f) for f in self.unmatched_api_fills],
            'anomalies': self.anomalies,
            'stats': self.stats,
        }


_STRIP_FIELDS = {'raw_record', 'raw_signal', 'signal_data', 'raw_data'}

def _strip_raw(record: Dict) -> Dict:
    return {k: v for k, v in record.items() if k not in _STRIP_FIELDS}


def _time_close(t1, t2, threshold_seconds=300) -> bool:
    if t1 is None or t2 is None:
        return False
    if isinstance(t1, str):
        try:
            t1 = datetime.fromisoformat(t1)
        except Exception:
            return False
    if isinstance(t2, str):
        try:
            t2 = datetime.fromisoformat(t2)
        except Exception:
            return False
    return abs((t1 - t2).total_seconds()) <= threshold_seconds


def match_broker_data(broker_data: Dict[str, List[Dict]], broker_name: str) -> MatchResult:
    result = MatchResult()

    signals = broker_data.get('signals', [])
    trades = broker_data.get('trades', [])
    trackers = broker_data.get('trackers', [])
    closed_positions = broker_data.get('closed_positions', [])
    api_fills = broker_data.get('api_fills', [])

    used_tracker_ids = set()
    used_api_fill_ids = set()
    used_signal_ids = set()
    used_trade_ids = set()

    for cp in closed_positions:
        group = {
            'broker': broker_name,
            'match_type': 'closed_position',
            'symbol': cp.get('symbol'),
            'exit_method': cp.get('exit_method'),
            'total_pnl': cp.get('total_pnl'),
            'total_pnl_pct': cp.get('total_pnl_pct'),
            'avg_entry_price': cp.get('avg_entry_price'),
            'exit_price': cp.get('price'),
            'exit_time': str(cp.get('time')) if cp.get('time') else None,
            'account_type': cp.get('account_type'),
            'closed_position': _strip_raw(cp),
            'entry_trackers': [],
            'exit_trackers': [],
            'entry_signals': [],
            'api_fills': [],
            'anomalies': [],
        }

        symbol = cp.get('symbol', '').upper()
        exit_order_id = cp.get('order_id')

        cp_source_id = cp.get('source_id')
        cp_time = cp.get('time')

        for ot in trackers:
            if ot.get('source_id') in used_tracker_ids:
                continue
            ot_symbol = (ot.get('symbol') or '').upper()
            if ot_symbol != symbol:
                continue

            if exit_order_id and str(ot.get('order_id')) == str(exit_order_id):
                group['exit_trackers'].append(_strip_raw(ot))
                used_tracker_ids.add(ot.get('source_id'))
                continue

            if ot.get('closed_position_id') and ot.get('closed_position_id') == cp_source_id:
                if ot.get('role') in ('EXIT', 'exit', 'CLOSE', 'close',
                                      'EXIT_TRAILING', 'exit_trailing',
                                      'EXIT_SIGNAL', 'exit_signal',
                                      'STOP_LOSS', 'stop_loss',
                                      'TAKE_PROFIT', 'take_profit'):
                    group['exit_trackers'].append(_strip_raw(ot))
                else:
                    group['entry_trackers'].append(_strip_raw(ot))
                used_tracker_ids.add(ot.get('source_id'))
                continue

            if ot.get('trade_id') and cp_source_id:
                if ot.get('role') in ('ENTRY', 'entry', 'ADD', 'add', 'OPEN', 'open'):
                    if _time_close(ot.get('time'), cp_time, 86400 * 7):
                        group['entry_trackers'].append(_strip_raw(ot))
                        used_tracker_ids.add(ot.get('source_id'))
                        continue

        if not group['exit_trackers']:
            for ot in trackers:
                if ot.get('source_id') in used_tracker_ids:
                    continue
                ot_symbol = (ot.get('symbol') or '').upper()
                if ot_symbol != symbol:
                    continue
                ot_role = (ot.get('role') or '').upper()
                if ot_role in ('EXIT', 'CLOSE', 'EXIT_TRAILING', 'EXIT_SIGNAL',
                               'STOP_LOSS', 'TAKE_PROFIT'):
                    if _time_close(ot.get('time'), cp_time, 300):
                        group['exit_trackers'].append(_strip_raw(ot))
                        used_tracker_ids.add(ot.get('source_id'))

        for af in api_fills:
            af_key = f"{af.get('order_id')}_{af.get('symbol')}_{af.get('time')}"
            if af_key in used_api_fill_ids:
                continue
            af_symbol = (af.get('symbol') or '').upper()
            if af_symbol != symbol:
                continue

            if exit_order_id and str(af.get('order_id')) == str(exit_order_id):
                group['api_fills'].append(_strip_raw(af))
                used_api_fill_ids.add(af_key)
                continue

            matched_by_tracker = False
            for et in group['entry_trackers'] + group['exit_trackers']:
                if str(af.get('order_id')) == str(et.get('order_id')):
                    group['api_fills'].append(_strip_raw(af))
                    used_api_fill_ids.add(af_key)
                    matched_by_tracker = True
                    break

        for sig in signals:
            if sig.get('source_id') in used_signal_ids:
                continue
            if sig.get('trade_id') and cp.get('source_id'):
                sig_symbol = (sig.get('symbol') or '').upper()
                if sig_symbol == symbol or not sig_symbol:
                    group['entry_signals'].append(_strip_raw(sig))
                    used_signal_ids.add(sig.get('source_id'))

        _check_group_anomalies(group)
        result.matched_groups.append(group)

    remaining_trackers_by_order = {}
    for ot in trackers:
        if ot.get('source_id') in used_tracker_ids:
            continue
        order_id = str(ot.get('order_id', ''))
        if order_id not in remaining_trackers_by_order:
            remaining_trackers_by_order[order_id] = []
        remaining_trackers_by_order[order_id].append(ot)

    processed_order_groups = set()
    for ot in trackers:
        if ot.get('source_id') in used_tracker_ids:
            continue
        order_id = str(ot.get('order_id', ''))
        if order_id in processed_order_groups:
            continue

        same_order_trackers = remaining_trackers_by_order.get(order_id, [ot])
        processed_order_groups.add(order_id)

        group = {
            'broker': broker_name,
            'match_type': 'tracker_only',
            'symbol': ot.get('symbol'),
            'account_type': ot.get('account_type'),
            'entry_trackers': [],
            'exit_trackers': [],
            'entry_signals': [],
            'api_fills': [],
            'anomalies': [],
        }

        for tracker in same_order_trackers:
            used_tracker_ids.add(tracker.get('source_id'))
            role = tracker.get('role', '')
            if role in ('EXIT', 'exit', 'CLOSE', 'close'):
                group['exit_trackers'].append(_strip_raw(tracker))
            else:
                group['entry_trackers'].append(_strip_raw(tracker))

        symbol = (ot.get('symbol') or '').upper()
        for af in api_fills:
            af_key = f"{af.get('order_id')}_{af.get('symbol')}_{af.get('time')}"
            if af_key in used_api_fill_ids:
                continue
            if str(af.get('order_id')) == order_id:
                group['api_fills'].append(_strip_raw(af))
                used_api_fill_ids.add(af_key)

        _check_group_anomalies(group)
        result.matched_groups.append(group)

    for sig in signals:
        if sig.get('source_id') not in used_signal_ids:
            if sig.get('parsed_successfully') == False:
                continue
            result.unmatched_signals.append(sig)

    for ot in trackers:
        if ot.get('source_id') not in used_tracker_ids:
            result.unmatched_trackers.append(ot)

    for af in api_fills:
        af_key = f"{af.get('order_id')}_{af.get('symbol')}_{af.get('time')}"
        if af_key not in used_api_fill_ids:
            result.unmatched_api_fills.append(af)

    total_matched = len(result.matched_groups)
    total_anomalies = sum(len(g.get('anomalies', [])) for g in result.matched_groups)
    result.stats = {
        'broker': broker_name,
        'total_matched_groups': total_matched,
        'total_anomalies': total_anomalies,
        'unmatched_signals': len(result.unmatched_signals),
        'unmatched_trackers': len(result.unmatched_trackers),
        'unmatched_api_fills': len(result.unmatched_api_fills),
        'closed_positions_count': len(closed_positions),
        'total_signals': len(signals),
        'total_trackers': len(trackers),
        'total_api_fills': len(api_fills),
    }

    return result


def _check_group_anomalies(group: Dict):
    anomalies = group.get('anomalies', [])

    cp = group.get('closed_position')
    if cp:
        if not group.get('exit_trackers') and not group.get('api_fills'):
            anomalies.append({
                'type': 'missing_exit_tracker',
                'severity': 'warning',
                'description': f"Closed position for {group.get('symbol')} has no matching exit tracker or API fill",
            })

        if not group.get('entry_trackers'):
            anomalies.append({
                'type': 'missing_entry_tracker',
                'severity': 'info',
                'description': f"Closed position for {group.get('symbol')} has no entry trackers matched (may have entered on a different day)",
            })

        for et in group.get('exit_trackers', []):
            et_price = et.get('price')
            cp_price = cp.get('price')
            if et_price and cp_price and et_price > 0 and cp_price > 0:
                diff_pct = abs(et_price - cp_price) / cp_price * 100
                if diff_pct > 1.0:
                    anomalies.append({
                        'type': 'price_mismatch',
                        'severity': 'warning',
                        'description': f"Exit price mismatch for {group.get('symbol')}: tracker={et_price}, closed_pos={cp_price} (diff={diff_pct:.2f}%)",
                    })

    for et in group.get('entry_trackers', []):
        matched_api = False
        for af in group.get('api_fills', []):
            if str(af.get('order_id')) == str(et.get('order_id')):
                matched_api = True
                qty_diff = abs((af.get('quantity') or 0) - (et.get('quantity') or 0))
                if qty_diff > 0:
                    anomalies.append({
                        'type': 'quantity_mismatch',
                        'severity': 'warning',
                        'description': f"Quantity mismatch for {group.get('symbol')} order {et.get('order_id')}: tracker={et.get('quantity')}, api={af.get('quantity')}",
                    })
                break

    group['anomalies'] = anomalies


def run_matching(all_data: Dict[str, Dict]) -> Dict[str, MatchResult]:
    results = {}
    for broker_name, broker_data in all_data.items():
        logger.info(f"Running rule-based matching for {broker_name}")
        results[broker_name] = match_broker_data(broker_data, broker_name)
        logger.info(f"Matching results for {broker_name}: {results[broker_name].stats}")
    return results
