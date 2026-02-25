"""Error Analyzer - Aggregate ERROR/WARNING logs by pattern to identify recurring issues.

Scans SystemLog and AlpacaSystemLog tables, normalizes error messages to extract
patterns (removing specific values like prices, order IDs, timestamps), groups by
pattern, and provides frequency analysis.
"""
import re
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

NORMALIZE_PATTERNS = [
    (re.compile(r'\$[\d,]+\.?\d*'), '$X.XX'),
    (re.compile(r'#\d+'), '#NNN'),
    (re.compile(r'\b\d{10,}\b'), 'ORDERID'),
    (re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE), 'UUID'),
    (re.compile(r'\b\d+\.\d{2,}\b'), 'N.NN'),
    (re.compile(r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}'), 'TIMESTAMP'),
    (re.compile(r'\b\d{4}-\d{2}-\d{2}\b'), 'DATE'),
    (re.compile(r'qty=\d+\.?\d*'), 'qty=N'),
    (re.compile(r'price=\d+\.?\d*'), 'price=N'),
    (re.compile(r'pnl=\-?\d+\.?\d*'), 'pnl=N'),
    (re.compile(r'P&L=\$?\-?[\d,]+\.?\d*'), 'P&L=$N'),
    (re.compile(r'\b[A-Z]{2,5}\b(?=/|_|\s|$)'), lambda m: m.group() if m.group() in _KEEP_WORDS else 'SYM'),
]

_KEEP_WORDS = {
    'ERROR', 'WARNING', 'INFO', 'DEBUG', 'CRITICAL',
    'FILLED', 'CANCELLED', 'REJECTED', 'PENDING', 'OPEN', 'CLOSED',
    'BUY', 'SELL', 'LONG', 'SHORT', 'ENTRY', 'EXIT', 'ADD',
    'OCO', 'OCA', 'STOP', 'LIMIT', 'MARKET',
    'PAPER', 'REAL', 'ACTIVE', 'TRIGGERED',
    'API', 'DB', 'WS', 'HTTP', 'JSON', 'SQL',
    'NOT', 'AND', 'FOR', 'THE', 'NO', 'NULL', 'NONE',
    'TRAILING', 'WEBHOOK', 'SIGNAL', 'POSITION', 'ORDER',
    'STP', 'LMT', 'MKT', 'GTC', 'DAY',
    'TIGER', 'ALPACA',
}


def normalize_message(message: str) -> str:
    normalized = message
    for pattern, replacement in NORMALIZE_PATTERNS:
        if callable(replacement):
            normalized = pattern.sub(replacement, normalized)
        else:
            normalized = pattern.sub(replacement, normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    if len(normalized) > 200:
        normalized = normalized[:200] + '...'
    return normalized


def analyze_errors(
    hours: int = 24,
    min_count: int = 2,
    levels: List[str] = None,
    source_filter: str = None,
    system: str = 'both',
) -> Dict:
    from app import db
    from models import SystemLog

    if levels is None:
        levels = ['ERROR', 'WARNING', 'CRITICAL']

    cutoff = datetime.utcnow() - timedelta(hours=hours)
    results = {
        'timeframe_hours': hours,
        'analyzed_at': datetime.utcnow().isoformat(),
        'patterns': [],
        'total_errors': 0,
        'unique_patterns': 0,
        'by_source': {},
        'by_category': {},
        'timeline': [],
    }

    all_logs = []

    if system in ('tiger', 'both'):
        query = SystemLog.query.filter(
            SystemLog.timestamp >= cutoff,
            SystemLog.level.in_(levels),
        )
        if source_filter:
            query = query.filter(SystemLog.source == source_filter)
        tiger_logs = query.order_by(SystemLog.timestamp.desc()).all()
        for log in tiger_logs:
            all_logs.append({
                'id': log.id,
                'timestamp': log.timestamp,
                'level': log.level,
                'source': log.source or 'unknown',
                'category': log.category or 'general',
                'message': log.message or '',
                'symbol': log.symbol,
                'system': 'tiger',
            })

    if system in ('alpaca', 'both'):
        try:
            from alpaca.models import AlpacaSystemLog
            query = AlpacaSystemLog.query.filter(
                AlpacaSystemLog.timestamp >= cutoff,
                AlpacaSystemLog.level.in_(levels),
            )
            if source_filter:
                query = query.filter(AlpacaSystemLog.source == source_filter)
            alpaca_logs = query.order_by(AlpacaSystemLog.timestamp.desc()).all()
            for log in alpaca_logs:
                all_logs.append({
                    'id': log.id,
                    'timestamp': log.timestamp,
                    'level': log.level,
                    'source': log.source or 'unknown',
                    'category': log.category or 'general',
                    'message': log.message or '',
                    'symbol': log.symbol,
                    'system': 'alpaca',
                })
        except Exception:
            pass

    results['total_errors'] = len(all_logs)

    pattern_groups = defaultdict(lambda: {
        'pattern': '',
        'count': 0,
        'level': '',
        'sources': set(),
        'categories': set(),
        'symbols': set(),
        'systems': set(),
        'first_seen': None,
        'last_seen': None,
        'sample_messages': [],
        'sample_ids': [],
    })

    source_counts = defaultdict(int)
    category_counts = defaultdict(int)

    for log in all_logs:
        pattern = normalize_message(log['message'])
        group = pattern_groups[pattern]
        group['pattern'] = pattern
        group['count'] += 1
        group['level'] = log['level']
        group['sources'].add(log['source'])
        group['categories'].add(log['category'])
        group['systems'].add(log['system'])
        if log['symbol']:
            group['symbols'].add(log['symbol'])

        if group['first_seen'] is None or log['timestamp'] < group['first_seen']:
            group['first_seen'] = log['timestamp']
        if group['last_seen'] is None or log['timestamp'] > group['last_seen']:
            group['last_seen'] = log['timestamp']

        if len(group['sample_messages']) < 3:
            group['sample_messages'].append(log['message'][:500])
            group['sample_ids'].append(log['id'])

        source_counts[log['source']] += 1
        category_counts[log['category']] += 1

    sorted_patterns = sorted(pattern_groups.values(), key=lambda x: x['count'], reverse=True)

    for group in sorted_patterns:
        if group['count'] >= min_count:
            results['patterns'].append({
                'pattern': group['pattern'],
                'count': group['count'],
                'level': group['level'],
                'sources': sorted(group['sources']),
                'categories': sorted(group['categories']),
                'symbols': sorted(group['symbols']),
                'systems': sorted(group['systems']),
                'first_seen': group['first_seen'].isoformat() if group['first_seen'] else None,
                'last_seen': group['last_seen'].isoformat() if group['last_seen'] else None,
                'sample_messages': group['sample_messages'],
                'sample_ids': group['sample_ids'],
                'frequency_per_hour': round(group['count'] / max(hours, 1), 2),
            })

    results['unique_patterns'] = len(sorted_patterns)
    results['recurring_patterns'] = len(results['patterns'])
    results['by_source'] = dict(sorted(source_counts.items(), key=lambda x: x[1], reverse=True))
    results['by_category'] = dict(sorted(category_counts.items(), key=lambda x: x[1], reverse=True))

    hour_buckets = defaultdict(int)
    for log in all_logs:
        bucket = log['timestamp'].replace(minute=0, second=0, microsecond=0)
        hour_buckets[bucket] += 1

    for bucket in sorted(hour_buckets.keys()):
        results['timeline'].append({
            'hour': bucket.isoformat(),
            'count': hour_buckets[bucket],
        })

    return results


def format_discord_digest(analysis: Dict) -> str:
    lines = []
    lines.append(f"**Error Digest ({analysis['timeframe_hours']}h)**")
    lines.append(f"Total: {analysis['total_errors']} | Unique: {analysis['unique_patterns']} | Recurring: {analysis['recurring_patterns']}")
    lines.append("")

    if not analysis['patterns']:
        lines.append("No recurring error patterns detected.")
        return '\n'.join(lines)

    for i, p in enumerate(analysis['patterns'][:10], 1):
        freq = f" ({p['frequency_per_hour']}/hr)" if p['frequency_per_hour'] >= 1 else ""
        symbols = f" [{', '.join(p['symbols'][:3])}]" if p['symbols'] else ""
        sources = ', '.join(p['sources'][:2])
        lines.append(f"**{i}. [{p['level']}] x{p['count']}{freq}** `{sources}`{symbols}")
        lines.append(f"   {p['pattern'][:150]}")
        lines.append("")

    if analysis['by_source']:
        top_sources = list(analysis['by_source'].items())[:5]
        source_str = ' | '.join(f"{s}: {c}" for s, c in top_sources)
        lines.append(f"**By Source:** {source_str}")

    return '\n'.join(lines)


def send_error_digest(hours: int = 4):
    try:
        analysis = analyze_errors(hours=hours, min_count=2)

        if analysis['total_errors'] == 0:
            return

        message = format_discord_digest(analysis)

        try:
            from discord_notifier import DiscordNotifier
            notifier = DiscordNotifier()
            notifier.send_system_notification(
                title=f"Error Digest ({hours}h)",
                message=message[:1900],
                level='warning' if analysis['recurring_patterns'] > 0 else 'info'
            )
        except Exception:
            pass

        try:
            from alpaca.discord_notifier import alpaca_discord
            alpaca_discord.send_system_notification(
                title=f"Error Digest ({hours}h)",
                message=message[:1900],
            )
        except Exception:
            pass

        logger.info(f"Error digest sent: {analysis['total_errors']} errors, {analysis['recurring_patterns']} recurring patterns")
    except Exception as e:
        logger.error(f"Failed to send error digest: {e}")
