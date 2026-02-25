"""
TBUS Utility Functions - Account detection and shared helpers.

Provides centralized TBUS account detection used by routing logic
in existing files (tiger_client.py, oca_service.py, trailing_stop_engine.py).
"""

import os
import logging
import threading

logger = logging.getLogger(__name__)

_tbus_detection_cache = {}
_tbus_cache_lock = threading.Lock()


def _read_tiger_config() -> dict:
    config_data = {}
    config_path = './tiger_openapi_config.properties'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    config_data[key] = value
    return config_data


def is_tbus_real_account() -> bool:
    """Check if the current real account is a TBUS (US Standard) account.
    
    Reads from tiger_openapi_config.properties and caches the result.
    This is used by routing logic in existing files to determine
    whether to use TBUS-specific order handling.
    
    Returns:
        True if the real account license is TBUS, False otherwise (TBSG etc.)
    """
    with _tbus_cache_lock:
        if 'is_tbus' in _tbus_detection_cache:
            return _tbus_detection_cache['is_tbus']
    
    try:
        config_data = _read_tiger_config()
        license_type = config_data.get('license', '')
        result = license_type == 'TBUS'
        
        with _tbus_cache_lock:
            _tbus_detection_cache['is_tbus'] = result
        
        if result:
            logger.info("TBUS account detected - will use TBUS protection order logic")
        
        return result
    except Exception as e:
        logger.error(f"Error detecting TBUS account: {e}")
        return False


def is_tbus_position(account_type: str) -> bool:
    """Check if a position with given account_type should use TBUS logic.
    
    Only real accounts can be TBUS. Paper accounts always use standard logic.
    
    Args:
        account_type: 'real' or 'paper'
        
    Returns:
        True if account_type is 'real' AND the real account is TBUS
    """
    if account_type != 'real':
        return False
    return is_tbus_real_account()


def clear_tbus_cache():
    """Clear the TBUS detection cache. Call when config changes."""
    with _tbus_cache_lock:
        _tbus_detection_cache.clear()
    logger.info("TBUS detection cache cleared")
