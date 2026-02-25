import os
import re
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from anthropic import Anthropic
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

logger = logging.getLogger(__name__)

AI_INTEGRATIONS_ANTHROPIC_API_KEY = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_API_KEY")
AI_INTEGRATIONS_ANTHROPIC_BASE_URL = os.environ.get("AI_INTEGRATIONS_ANTHROPIC_BASE_URL")

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = Anthropic(
            api_key=AI_INTEGRATIONS_ANTHROPIC_API_KEY,
            base_url=AI_INTEGRATIONS_ANTHROPIC_BASE_URL,
        )
    return _client


def _is_rate_limit_error(exception: BaseException) -> bool:
    error_msg = str(exception)
    return (
        "429" in error_msg
        or "RATELIMIT_EXCEEDED" in error_msg
        or "quota" in error_msg.lower()
        or "rate limit" in error_msg.lower()
        or (hasattr(exception, "status_code") and exception.status_code == 429)
    )


def _serialize_for_json(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, 'value'):
        return obj.value
    if isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    return str(obj)


_LARGE_FIELDS = {'raw_record', 'raw_signal', 'signal_data', 'raw_data'}


def _extract_json(response: str) -> dict:
    response = response.strip()
    if response.startswith('```'):
        lines = response.split('\n', 1)
        response = lines[1] if len(lines) > 1 else response[3:]
        if response.endswith('```'):
            response = response[:-3]
        response = response.strip()
        if response.startswith('json'):
            response = response[4:].strip()

    try:
        return json.loads(response)
    except json.JSONDecodeError:
        pass

    match = re.search(r'\{[\s\S]*\}', response)
    if match:
        candidate = match.group(0)
        depth = 0
        end_pos = 0
        for i, ch in enumerate(candidate):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    end_pos = i + 1
                    break
        if end_pos > 0:
            try:
                return json.loads(candidate[:end_pos])
            except json.JSONDecodeError:
                pass

    raise json.JSONDecodeError("No valid JSON found in response", response, 0)


def _clean_for_prompt(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: _clean_for_prompt(v) for k, v in data.items() if k not in _LARGE_FIELDS}
    if isinstance(data, list):
        return [_clean_for_prompt(item) for item in data]
    return _serialize_for_json(data)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception(_is_rate_limit_error),
    reraise=True,
)
def _call_claude(system_prompt: str, user_prompt: str) -> str:
    client = _get_client()
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return message.content[0].text


def analyze_unmatched_records(match_results: Dict, all_data: Dict) -> Dict:
    all_unmatched = []
    for broker_name, result in match_results.items():
        result_dict = result.to_dict() if hasattr(result, 'to_dict') else result
        for sig in result_dict.get('unmatched_signals', []):
            sig['_broker'] = broker_name
            all_unmatched.append(sig)
        for tracker in result_dict.get('unmatched_trackers', []):
            tracker['_broker'] = broker_name
            all_unmatched.append(tracker)
        for fill in result_dict.get('unmatched_api_fills', []):
            fill['_broker'] = broker_name
            all_unmatched.append(fill)

    if not all_unmatched:
        return {'ai_matches': [], 'explanations': [], 'summary': 'No unmatched records to analyze.'}

    all_matched_groups = []
    for broker_name, result in match_results.items():
        result_dict = result.to_dict() if hasattr(result, 'to_dict') else result
        for group in result_dict.get('matched_groups', []):
            all_matched_groups.append(group)

    system_prompt = """你是一个交易系统数据分析师。你的工作是分析未匹配的交易记录，请用中文回复：
1. 根据标的、时间、数量和价格，尝试在未匹配记录之间找到逻辑匹配
2. 解释某些记录未匹配的原因（如：订单被拒、部分成交、不同日期的入场单等）
3. 标记任何可能表明数据完整性问题的可疑模式

请以有效的JSON格式回复，结构如下：
{
    "ai_matches": [
        {
            "records": [<匹配的未匹配记录索引>],
            "confidence": 0.0-1.0,
            "reason": "匹配原因说明"
        }
    ],
    "explanations": [
        {
            "record_index": <索引>,
            "explanation": "该记录未匹配的原因",
            "severity": "info|warning|error",
            "suggested_action": "建议的处理方式"
        }
    ],
    "data_issues": [
        {
            "type": "问题类型",
            "description": "问题描述",
            "severity": "info|warning|error"
        }
    ],
    "summary": "简要总结"
}"""

    unmatched_clean = _clean_for_prompt(all_unmatched)
    matched_summary = []
    for g in all_matched_groups[:20]:
        matched_summary.append({
            'broker': g.get('broker'),
            'symbol': g.get('symbol'),
            'match_type': g.get('match_type'),
            'exit_method': g.get('exit_method'),
            'total_pnl': g.get('total_pnl'),
            'exit_time': g.get('exit_time'),
        })

    user_prompt = f"""分析以下未匹配的交易记录，尝试找到匹配关系或解释未匹配的原因。

未匹配记录（共{len(all_unmatched)}条）：
{json.dumps(unmatched_clean, default=_serialize_for_json, indent=2)}

已匹配交易组（摘要，共{len(matched_summary)}组）：
{json.dumps(matched_summary, default=_serialize_for_json, indent=2)}

请用中文分析并以指定的JSON格式回复。"""

    try:
        response = _call_claude(system_prompt, user_prompt)
        return _extract_json(response)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse AI response as JSON: {e}\nRaw response: {response[:1000]}")
        return {
            'ai_matches': [],
            'explanations': [],
            'data_issues': [],
            'summary': f'AI analysis completed but response was not valid JSON. Raw: {response[:500]}',
        }
    except Exception as e:
        logger.error(f"AI analysis failed: {e}")
        return {
            'ai_matches': [],
            'explanations': [],
            'data_issues': [],
            'summary': f'AI analysis failed: {str(e)}',
        }


def analyze_signal_quality(match_results: Dict, all_data: Dict) -> Dict:
    all_groups = []
    for broker_name, result in match_results.items():
        result_dict = result.to_dict() if hasattr(result, 'to_dict') else result
        for group in result_dict.get('matched_groups', []):
            if group.get('total_pnl') is not None:
                all_groups.append(group)

    if not all_groups:
        return {'signal_stats': {}, 'recommendations': [], 'summary': 'No completed trades to analyze.'}

    system_prompt = """你是一个交易策略分析师。分析已完成的交易并提供以下内容，请用中文回复：
1. 按标的和出场方式的盈亏统计
2. 信号质量评估（哪些信号导致盈利或亏损）
3. 模式识别（交易时段、仓位大小等）
4. 改善交易表现的可操作建议

请以有效的JSON格式回复，结构如下：
{
    "signal_stats": {
        "total_trades": <int>,
        "winners": <int>,
        "losers": <int>,
        "win_rate": <float>,
        "total_pnl": <float>,
        "avg_win": <float>,
        "avg_loss": <float>,
        "by_symbol": {
            "SYMBOL": {"trades": <int>, "pnl": <float>, "win_rate": <float>}
        },
        "by_exit_method": {
            "出场方式": {"trades": <int>, "pnl": <float>}
        }
    },
    "patterns": [
        {
            "pattern": "模式描述",
            "impact": "positive|negative|neutral",
            "confidence": 0.0-1.0
        }
    ],
    "recommendations": [
        {
            "priority": "high|medium|low",
            "recommendation": "可操作的建议",
            "expected_impact": "预期影响"
        }
    ],
    "summary": "简要总体评估"
}"""

    groups_clean = _clean_for_prompt(all_groups[:50])
    user_prompt = f"""分析以下{len(all_groups)}笔已完成交易的信号质量和模式：

{json.dumps(groups_clean, default=_serialize_for_json, indent=2)}

请用中文提供全面的分析，并以指定的JSON格式回复。"""

    try:
        response = _call_claude(system_prompt, user_prompt)
        return _extract_json(response)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse signal quality AI response: {e}\nRaw response: {response[:1000]}")
        return {
            'signal_stats': {},
            'patterns': [],
            'recommendations': [],
            'summary': f'Signal quality analysis completed but response was not valid JSON.',
        }
    except Exception as e:
        logger.error(f"Signal quality analysis failed: {e}")
        return {
            'signal_stats': {},
            'patterns': [],
            'recommendations': [],
            'summary': f'Signal quality analysis failed: {str(e)}',
        }


def analyze_anomalies(match_results: Dict) -> Dict:
    all_anomalies = []
    for broker_name, result in match_results.items():
        result_dict = result.to_dict() if hasattr(result, 'to_dict') else result
        for group in result_dict.get('matched_groups', []):
            for anomaly in group.get('anomalies', []):
                anomaly['broker'] = broker_name
                anomaly['symbol'] = group.get('symbol')
                all_anomalies.append(anomaly)

    if not all_anomalies:
        return {'critical_issues': [], 'warnings': [], 'summary': 'No anomalies detected.'}

    if len(all_anomalies) <= 3:
        return {
            'critical_issues': [a for a in all_anomalies if a.get('severity') == 'error'],
            'warnings': [a for a in all_anomalies if a.get('severity') in ('warning', 'info')],
            'summary': f'{len(all_anomalies)} anomalies detected by rule-based matching.',
        }

    system_prompt = """你是一个交易系统数据完整性分析师。分析交易数据中发现的异常，请用中文回复：
1. 优先排列需要立即关注的异常
2. 分析根本原因
3. 推荐修复方案

请以有效的JSON格式回复：
{
    "critical_issues": [{"description": "问题描述", "root_cause": "根本原因", "fix": "修复建议"}],
    "warnings": [{"description": "问题描述", "root_cause": "根本原因", "fix": "修复建议"}],
    "summary": "简要总结"
}"""

    user_prompt = f"""分析交易数据中发现的以下{len(all_anomalies)}个异常：

{json.dumps(all_anomalies, default=_serialize_for_json, indent=2)}

请用中文提供按优先级排列的分析。"""

    try:
        response = _call_claude(system_prompt, user_prompt)
        return _extract_json(response)
    except Exception as e:
        logger.error(f"Anomaly analysis failed: {e}")
        return {
            'critical_issues': [a for a in all_anomalies if a.get('severity') == 'error'],
            'warnings': [a for a in all_anomalies if a.get('severity') in ('warning', 'info')],
            'summary': f'Anomaly analysis fallback (AI failed): {len(all_anomalies)} anomalies found.',
        }


def run_ai_analysis(match_results: Dict, all_data: Dict, skip_signal_quality: bool = False) -> Dict:
    logger.info("Starting AI analysis...")

    unmatched_analysis = analyze_unmatched_records(match_results, all_data)
    logger.info("Unmatched records analysis complete")

    anomaly_analysis = analyze_anomalies(match_results)
    logger.info("Anomaly analysis complete")

    signal_quality = {}
    if not skip_signal_quality:
        signal_quality = analyze_signal_quality(match_results, all_data)
        logger.info("Signal quality analysis complete")

    return {
        'unmatched_analysis': unmatched_analysis,
        'anomaly_analysis': anomaly_analysis,
        'signal_quality': signal_quality,
    }
