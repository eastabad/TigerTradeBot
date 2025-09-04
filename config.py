import os
from app import db
from models import TradingConfig

def get_config(key: str, default: str = None):
    """Get configuration value"""
    try:
        # First try environment variable
        env_value = os.getenv(key)
        if env_value:
            return env_value
        
        # Then try database
        config = TradingConfig.query.filter_by(key=key).first()
        if config:
            return config.value
        
        return default
    except Exception:
        return default

def set_config(key: str, value: str, description: str = None):
    """Set configuration value"""
    config = TradingConfig.query.filter_by(key=key).first()
    
    if config:
        config.value = value
        if description:
            config.description = description
    else:
        config = TradingConfig()
        config.key = key
        config.value = value
        config.description = description
        db.session.add(config)
    
    db.session.commit()

def get_all_configs() -> dict:
    """Get all configuration values"""
    configs = {}
    try:
        db_configs = TradingConfig.query.all()
        for config in db_configs:
            configs[config.key] = config.value
    except Exception:
        pass
    
    return configs

# Default configurations
DEFAULT_CONFIGS = {
    'TRADING_ENABLED': 'true',
    'MAX_TRADE_AMOUNT': '10000',
    'LOG_LEVEL': 'INFO',
    'WEBHOOK_TIMEOUT': '30'
}

def initialize_default_configs():
    """Initialize default configurations if they don't exist"""
    for key, value in DEFAULT_CONFIGS.items():
        if not TradingConfig.query.filter_by(key=key).first():
            set_config(key, value)
