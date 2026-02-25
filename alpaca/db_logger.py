import logging
import json
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def log_system(
    level: str,
    source: str,
    message: str,
    category: str = None,
    symbol: str = None,
    extra_data: dict = None,
):
    try:
        from app import db
        from alpaca.models import AlpacaSystemLog

        entry = AlpacaSystemLog(
            timestamp=datetime.utcnow(),
            level=level.upper(),
            source=source,
            category=category,
            message=message,
            symbol=symbol,
            extra_data=json.dumps(extra_data, default=str) if extra_data else None,
        )
        db.session.add(entry)
        db.session.commit()
    except Exception as e:
        logger.debug(f"Failed to write AlpacaSystemLog: {e}")
        try:
            from app import db
            db.session.rollback()
        except Exception:
            pass


def log_info(source: str, message: str, **kwargs):
    log_system('INFO', source, message, **kwargs)


def log_warning(source: str, message: str, **kwargs):
    log_system('WARNING', source, message, **kwargs)


def log_error(source: str, message: str, **kwargs):
    log_system('ERROR', source, message, **kwargs)


def log_critical(source: str, message: str, **kwargs):
    log_system('CRITICAL', source, message, **kwargs)


def cleanup_old_logs(days: int = 7):
    try:
        from app import db
        from alpaca.models import AlpacaSystemLog
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(days=days)
        deleted = AlpacaSystemLog.query.filter(
            AlpacaSystemLog.timestamp < cutoff
        ).delete()
        db.session.commit()
        if deleted:
            logger.info(f"Cleaned up {deleted} old Alpaca system logs")
        return deleted
    except Exception as e:
        logger.error(f"Failed to cleanup old logs: {e}")
        return 0
