import json
import logging
import os
import requests
from datetime import datetime, date
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


def _make_json_safe(obj):
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items() if k != 'raw_record'}
    if isinstance(obj, (list, tuple)):
        return [_make_json_safe(item) for item in obj]
    if hasattr(obj, '__dict__'):
        return str(obj)
    return str(obj)


def generate_report(target_date, match_results: Dict, ai_analysis: Dict) -> Dict:
    report = {
        'date': str(target_date),
        'generated_at': datetime.now().isoformat(),
        'brokers': {},
        'ai_analysis': ai_analysis,
        'summary': {},
    }

    total_trades = 0
    total_pnl = 0.0
    total_anomalies = 0
    total_unmatched = 0

    for broker_name, result in match_results.items():
        result_dict = result.to_dict() if hasattr(result, 'to_dict') else result
        stats = result_dict.get('stats', {})

        winners = 0
        losers = 0
        broker_pnl = 0.0
        for group in result_dict.get('matched_groups', []):
            pnl = group.get('total_pnl')
            if pnl is not None:
                if pnl > 0:
                    winners += 1
                elif pnl < 0:
                    losers += 1
                broker_pnl += pnl

        broker_report = {
            'stats': stats,
            'matched_groups': result_dict.get('matched_groups', []),
            'unmatched_signals': result_dict.get('unmatched_signals', []),
            'unmatched_trackers': result_dict.get('unmatched_trackers', []),
            'unmatched_api_fills': result_dict.get('unmatched_api_fills', []),
            'performance': {
                'winners': winners,
                'losers': losers,
                'win_rate': round(winners / (winners + losers) * 100, 1) if (winners + losers) > 0 else 0,
                'total_pnl': round(broker_pnl, 2),
            },
        }

        report['brokers'][broker_name] = broker_report
        total_trades += stats.get('closed_positions_count', 0)
        total_pnl += broker_pnl
        total_anomalies += stats.get('total_anomalies', 0)
        total_unmatched += stats.get('unmatched_signals', 0) + stats.get('unmatched_trackers', 0) + stats.get('unmatched_api_fills', 0)

    report['summary'] = {
        'total_trades': total_trades,
        'total_pnl': round(total_pnl, 2),
        'total_anomalies': total_anomalies,
        'total_unmatched': total_unmatched,
        'health_score': _calculate_health_score(total_anomalies, total_unmatched, total_trades),
    }

    return report


def _calculate_health_score(anomalies: int, unmatched: int, total: int) -> int:
    if total == 0:
        return 100
    score = 100
    score -= min(anomalies * 5, 30)
    score -= min(unmatched * 10, 40)
    return max(score, 0)


def save_report_to_db(report: Dict) -> int:
    from app import db
    from trade_analysis.models import AnalysisReport

    target_date = datetime.strptime(report['date'], '%Y-%m-%d').date() if isinstance(report['date'], str) else report['date']

    safe_report = _make_json_safe(report)
    safe_summary = _make_json_safe(report.get('summary', {}))
    safe_ai = _make_json_safe(report.get('ai_analysis', {}))

    existing = AnalysisReport.query.filter_by(report_date=target_date).first()
    if existing:
        existing.report_data = safe_report
        existing.summary_data = safe_summary
        existing.ai_analysis_data = safe_ai
        existing.health_score = report.get('summary', {}).get('health_score', 0)
        existing.total_trades = report.get('summary', {}).get('total_trades', 0)
        existing.total_pnl = report.get('summary', {}).get('total_pnl', 0)
        existing.updated_at = datetime.now()
        db.session.commit()
        logger.info(f"Updated existing report for {target_date}, id={existing.id}")
        return existing.id
    else:
        new_report = AnalysisReport(
            report_date=target_date,
            report_data=safe_report,
            summary_data=safe_summary,
            ai_analysis_data=safe_ai,
            health_score=report.get('summary', {}).get('health_score', 0),
            total_trades=report.get('summary', {}).get('total_trades', 0),
            total_pnl=report.get('summary', {}).get('total_pnl', 0),
        )
        db.session.add(new_report)
        db.session.commit()
        logger.info(f"Saved new report for {target_date}, id={new_report.id}")
        return new_report.id


def format_discord_message(report: Dict) -> str:
    summary = report.get('summary', {})
    date = report.get('date', 'Unknown')
    health = summary.get('health_score', 0)
    total_pnl = summary.get('total_pnl', 0)
    total_trades = summary.get('total_trades', 0)
    total_anomalies = summary.get('total_anomalies', 0)
    total_unmatched = summary.get('total_unmatched', 0)

    if health >= 90:
        health_emoji = '🟢'
    elif health >= 70:
        health_emoji = '🟡'
    else:
        health_emoji = '🔴'

    pnl_emoji = '📈' if total_pnl >= 0 else '📉'

    lines = [
        f"**📊 交易分析报告 - {date}**",
        f"",
        f"{health_emoji} 健康评分：**{health}/100**",
        f"{pnl_emoji} 总盈亏：**${total_pnl:+.2f}**",
        f"📋 总交易数：**{total_trades}**",
        f"⚠️ 异常：**{total_anomalies}** | 未匹配：**{total_unmatched}**",
    ]

    for broker_name, broker_data in report.get('brokers', {}).items():
        perf = broker_data.get('performance', {})
        lines.append(f"")
        lines.append(f"**{broker_name.upper()}**")
        lines.append(f"  胜/负：{perf.get('winners', 0)}/{perf.get('losers', 0)}（{perf.get('win_rate', 0)}%）")
        lines.append(f"  盈亏：${perf.get('total_pnl', 0):+.2f}")

    ai = report.get('ai_analysis', {})
    signal_quality = ai.get('signal_quality', {})
    if signal_quality.get('summary'):
        lines.append(f"")
        lines.append(f"**🤖 AI 分析洞察**")
        lines.append(signal_quality['summary'][:300])

    recommendations = signal_quality.get('recommendations', [])
    if recommendations:
        lines.append(f"")
        lines.append(f"**💡 重点建议**")
        for rec in recommendations[:3]:
            priority = rec.get('priority', '').upper()
            lines.append(f"  [{priority}] {rec.get('recommendation', '')[:100]}")

    return '\n'.join(lines)


def send_discord_notification(report: Dict, webhook_type: str = 'default') -> bool:
    if webhook_type == 'tts':
        webhook_url = os.environ.get('DISCORD_TTS_WEBHOOK_URL')
    else:
        webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')

    if not webhook_url:
        logger.warning(f"Discord webhook URL not configured for type: {webhook_type}")
        return False

    message = format_discord_message(report)

    try:
        payload = {'content': message}
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code in (200, 204):
            logger.info("Discord notification sent successfully")
            return True
        else:
            logger.error(f"Discord notification failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Discord notification error: {e}")
        return False
