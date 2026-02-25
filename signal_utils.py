"""Shared signal parsing utilities.

Used by both Tiger and Alpaca code paths. This module has no external dependencies
beyond the Python standard library, so it can be imported safely on any deployment
(including VPS without the alpaca/ directory).
"""
import json
import re
import logging

logger = logging.getLogger(__name__)


def parse_signal_fields(signal_data_str: str) -> dict:
    result = {
        'signal_content': None,
        'signal_grade': None,
        'signal_score': None,
        'signal_timeframe': None,
        'signal_indicator': None,
    }

    if not signal_data_str:
        return result

    result['signal_content'] = signal_data_str

    try:
        sig = json.loads(signal_data_str)
    except (json.JSONDecodeError, TypeError):
        return result

    extras = sig.get('extras', {})
    if not isinstance(extras, dict):
        extras = {}

    result['signal_indicator'] = extras.get('indicator')
    result['signal_timeframe'] = extras.get('timeframe')

    result['signal_grade'] = extras.get('grade')
    result['signal_score'] = extras.get('score')

    if not result['signal_grade']:
        filter_result = sig.get('filter_result', {})
        if isinstance(filter_result, dict):
            rating = filter_result.get('rating', '')
            if rating:
                rating_str = str(rating).strip().strip('"')
                grade, score = _parse_rating_string(rating_str)
                if grade:
                    result['signal_grade'] = grade
                    if score is not None:
                        result['signal_score'] = score

    if not result['signal_grade'] and result['signal_indicator']:
        grade, score = _parse_grade_from_indicator(result['signal_indicator'])
        if grade:
            result['signal_grade'] = grade
            if score is not None:
                result['signal_score'] = score

    if not result['signal_grade']:
        osc = extras.get('oscrating')
        trend = extras.get('trendrating')
        if osc is not None and trend is not None:
            try:
                avg_rating = (float(osc) + float(trend)) / 2
                if avg_rating >= 70:
                    result['signal_grade'] = 'A'
                elif avg_rating >= 50:
                    result['signal_grade'] = 'B'
                else:
                    result['signal_grade'] = 'C'
                result['signal_score'] = int(avg_rating)
            except (ValueError, TypeError):
                pass

    if not result['signal_grade'] and result['signal_indicator']:
        indicator_lower = result['signal_indicator'].lower().strip()
        default_grade = _get_default_grade_for_indicator(indicator_lower)
        if default_grade:
            result['signal_grade'] = default_grade

    return result


def _parse_rating_string(rating: str):
    match = re.match(r'^([ABC])(\+{0,2}|-{0,2})(\d+)$', rating)
    if match:
        grade = match.group(1)
        sign = match.group(2)
        num = int(match.group(3))
        if '-' in sign:
            num = -num
        return grade, num
    return None, None


def _parse_grade_from_indicator(indicator: str):
    match = re.search(r'Signal:\s*([ABC])(\+{0,2}|-{0,2})(\d+)', indicator)
    if match:
        grade = match.group(1)
        sign = match.group(2)
        num = int(match.group(3))
        if '-' in sign:
            num = -num
        return grade, num
    return None, None


INDICATOR_DEFAULT_GRADES = {
    'wavematrix bottom': 'B',
    'wavematrix top': 'B',
    'wavematrix longstrongsignal': 'A',
    'wavematrix shortstrongsignal': 'A',
    'trend continuation': 'B',
    'rsx visual bottom buy': 'B',
    'rsx visual top sell': 'B',
    'rsx bullish reversal': 'B',
    'rsx bearish reversal': 'B',
    'momo strong entry': 'B',
    'sniperbuy': 'A',
    'snipersell': 'A',
}


def _get_default_grade_for_indicator(indicator_lower: str) -> str:
    for pattern, grade in INDICATOR_DEFAULT_GRADES.items():
        if pattern in indicator_lower:
            return grade
    return None
