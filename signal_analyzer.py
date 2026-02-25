import re
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def parse_signal_grades(signal_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse signal grading information from webhook signal data.
    
    Extracts:
    - signal_type: WaveMatrix, TDindicator, Momo, RSX, etc.
    - signal_grade: A, B, C (from "Signal: B-4")
    - signal_score: -4, +0, +3 (from "Signal: B-4")
    - htf_grade: A, B, C (from "HTF: B-4")
    - htf_score: -4, +0, +3 (from "HTF: B-4")
    - htf_pass_status: strongpass, pass, reject
    - trend_strength: 70.6 (percentage)
    - signal_timeframe: 15, 60, etc.
    
    Returns dict with parsed values (None if not found)
    """
    result = {
        'signal_indicator': None,
        'signal_type': None,
        'signal_grade': None,
        'signal_score': None,
        'htf_grade': None,
        'htf_score': None,
        'htf_pass_status': None,
        'trend_strength': None,
        'signal_timeframe': None
    }
    
    try:
        extras = signal_data.get('extras', {})
        indicator = extras.get('indicator', '')
        
        if not indicator:
            logger.debug(f"No indicator field in signal extras, skipping grade parsing")
            return result
        
        result['signal_indicator'] = indicator
        logger.info(f"📊 Parsing signal indicator: {indicator[:100]}")
        
        # Extract timeframe
        timeframe = extras.get('timeframe', '')
        if timeframe:
            result['signal_timeframe'] = str(timeframe).replace('m', '').strip()
        
        # Determine signal type
        result['signal_type'] = _determine_signal_type(indicator)
        
        # Parse signal grade and score: "Signal: B-4" or "Signal: A+2"
        signal_match = re.search(r'Signal:\s*([ABC])([+-]?\d+)?', indicator)
        if signal_match:
            result['signal_grade'] = signal_match.group(1)
            score_str = signal_match.group(2)
            if score_str:
                result['signal_score'] = int(score_str)
        
        # Parse HTF grade, score and pass status: "HTF: B-4,strongpass" or "HTF: C+0,pass"
        htf_match = re.search(r'HTF:\s*([ABC])([+-]?\d+)?[,\s]*(strongpass|pass|reject)?', indicator)
        if htf_match:
            result['htf_grade'] = htf_match.group(1)
            htf_score_str = htf_match.group(2)
            if htf_score_str:
                result['htf_score'] = int(htf_score_str)
            if htf_match.group(3):
                result['htf_pass_status'] = htf_match.group(3)
        
        # Parse trend strength: "趋势强度: 70.6%"
        trend_match = re.search(r'趋势强度:\s*([\d.]+)%', indicator)
        if trend_match:
            result['trend_strength'] = float(trend_match.group(1))
        
        logger.info(f"📊 Signal grades parsed: type={result['signal_type']}, "
                   f"grade={result['signal_grade']}{result['signal_score'] or ''}, "
                   f"HTF={result['htf_grade']}{result['htf_score'] or ''}({result['htf_pass_status']}), "
                   f"trend={result['trend_strength']}%, tf={result['signal_timeframe']}")
        
    except Exception as e:
        logger.error(f"❌ Error parsing signal grades: {e}")
    
    return result


def _determine_signal_type(indicator: str) -> str:
    """Determine signal type from indicator string"""
    indicator_lower = indicator.lower()
    
    if 'wavematrix' in indicator_lower or 'signal:' in indicator_lower:
        signal_type = 'WaveMatrix'
    elif 'tdindicator' in indicator_lower or 'aimonitor' in indicator_lower:
        signal_type = 'TDindicator'
    elif 'momo' in indicator_lower:
        signal_type = 'Momo'
    elif 'rsx' in indicator_lower:
        signal_type = 'RSX'
    elif 'sniper' in indicator_lower:
        signal_type = 'Sniper'
    else:
        signal_type = 'Other'
        logger.debug(f"Unknown signal type in indicator: {indicator[:80]}")
    
    return signal_type


def get_signal_summary(grades: Dict[str, Any]) -> str:
    """Generate a human-readable signal summary for display"""
    parts = []
    
    if grades.get('signal_type'):
        parts.append(grades['signal_type'])
    
    if grades.get('signal_grade'):
        grade_str = grades['signal_grade']
        if grades.get('signal_score') is not None:
            score = grades['signal_score']
            grade_str += f"{'+' if score >= 0 else ''}{score}"
        parts.append(f"信号:{grade_str}")
    
    if grades.get('htf_grade'):
        htf_str = grades['htf_grade']
        if grades.get('htf_score') is not None:
            score = grades['htf_score']
            htf_str += f"{'+' if score >= 0 else ''}{score}"
        if grades.get('htf_pass_status'):
            htf_str += f"({grades['htf_pass_status']})"
        parts.append(f"HTF:{htf_str}")
    
    if grades.get('trend_strength') is not None:
        parts.append(f"趋势:{grades['trend_strength']}%")
    
    return ' | '.join(parts) if parts else 'N/A'


def parse_signal_from_raw(raw_signal: str) -> Dict[str, Any]:
    """Parse signal grades from raw signal string (stored in SignalLog)"""
    try:
        clean_signal = raw_signal
        if raw_signal.startswith('[PAPER]'):
            clean_signal = raw_signal[7:].strip()
            logger.debug(f"Stripped [PAPER] prefix from raw signal")
        
        signal_data = json.loads(clean_signal)
        return parse_signal_grades(signal_data)
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON in raw signal: {e}, raw={raw_signal[:100]}")
        return {
            'signal_indicator': None, 'signal_type': None, 'signal_grade': None,
            'signal_score': None, 'htf_grade': None, 'htf_score': None,
            'htf_pass_status': None, 'trend_strength': None, 'signal_timeframe': None
        }
    except Exception as e:
        logger.error(f"❌ Error parsing raw signal: {e}")
        return {
            'signal_indicator': None,
            'signal_type': None,
            'signal_grade': None,
            'signal_score': None,
            'htf_grade': None,
            'htf_score': None,
            'htf_pass_status': None,
            'trend_strength': None,
            'signal_timeframe': None
        }
