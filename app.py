import os
import logging
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix

# Configure logging
logging.basicConfig(level=logging.DEBUG)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

# Create the app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

import json as _json

@app.template_filter('parse_json')
def parse_json_filter(value):
    try:
        return _json.loads(value)
    except Exception:
        return value

@app.template_filter('pretty_json')
def pretty_json_filter(value):
    try:
        parsed = _json.loads(value) if isinstance(value, str) else value
        return _json.dumps(parsed, indent=2, ensure_ascii=False)
    except Exception:
        return value

# Configure the database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "sqlite:///trading.db")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}

# Initialize the app with the extension
db.init_app(app)

def run_migrations():
    """Run database migrations to add missing columns"""
    from sqlalchemy import text, inspect
    
    migrations = [
        # TrailingStopPosition: first_entry_price
        ("trailing_stop_position", "first_entry_price", "ALTER TABLE trailing_stop_position ADD COLUMN first_entry_price FLOAT"),
        # TrailingStopPosition: signal_stop_loss (original signal stop, never changes)
        ("trailing_stop_position", "signal_stop_loss", "ALTER TABLE trailing_stop_position ADD COLUMN signal_stop_loss FLOAT"),
        # TrailingStopConfig: tightening settings
        ("trailing_stop_config", "tighten_threshold", "ALTER TABLE trailing_stop_config ADD COLUMN tighten_threshold FLOAT DEFAULT 0.02"),
        ("trailing_stop_config", "tighten_atr_multiplier", "ALTER TABLE trailing_stop_config ADD COLUMN tighten_atr_multiplier FLOAT DEFAULT 0.6"),
        ("trailing_stop_config", "tighten_trail_pct", "ALTER TABLE trailing_stop_config ADD COLUMN tighten_trail_pct FLOAT DEFAULT 0.005"),
        # CompletedTrade: FIFO tracking fields
        ("completed_trade", "remaining_quantity", "ALTER TABLE completed_trade ADD COLUMN remaining_quantity FLOAT"),
        ("completed_trade", "exited_quantity", "ALTER TABLE completed_trade ADD COLUMN exited_quantity FLOAT DEFAULT 0"),
        ("completed_trade", "avg_exit_price", "ALTER TABLE completed_trade ADD COLUMN avg_exit_price FLOAT"),
        # TrailingStopPosition: trigger retry count for safe close
        ("trailing_stop_position", "trigger_retry_count", "ALTER TABLE trailing_stop_position ADD COLUMN trigger_retry_count INTEGER DEFAULT 0"),
        # TrailingStopPosition: Always-On Soft Stop breach tracking
        ("trailing_stop_position", "breach_detected_at", "ALTER TABLE trailing_stop_position ADD COLUMN breach_detected_at TIMESTAMP"),
        ("trailing_stop_position", "breach_price", "ALTER TABLE trailing_stop_position ADD COLUMN breach_price FLOAT"),
        # EntrySignalRecord: display fields for Trade Analytics
        ("entry_signal_record", "position_id", "ALTER TABLE entry_signal_record ADD COLUMN position_id INTEGER"),
        ("entry_signal_record", "position_key", "ALTER TABLE entry_signal_record ADD COLUMN position_key VARCHAR(100)"),
        ("entry_signal_record", "stop_price", "ALTER TABLE entry_signal_record ADD COLUMN stop_price FLOAT"),
        ("entry_signal_record", "take_profit_price", "ALTER TABLE entry_signal_record ADD COLUMN take_profit_price FLOAT"),
        ("entry_signal_record", "exit_price", "ALTER TABLE entry_signal_record ADD COLUMN exit_price FLOAT"),
        ("entry_signal_record", "exit_time", "ALTER TABLE entry_signal_record ADD COLUMN exit_time TIMESTAMP"),
        ("entry_signal_record", "exit_method", "ALTER TABLE entry_signal_record ADD COLUMN exit_method VARCHAR(50)"),
        ("entry_signal_record", "hold_duration_seconds", "ALTER TABLE entry_signal_record ADD COLUMN hold_duration_seconds FLOAT"),
        # AlpacaEntrySignalRecord: display fields for Trade Analytics
        ("alpaca_entry_signal_record", "position_key", "ALTER TABLE alpaca_entry_signal_record ADD COLUMN position_key VARCHAR(100)"),
        ("alpaca_entry_signal_record", "stop_price", "ALTER TABLE alpaca_entry_signal_record ADD COLUMN stop_price FLOAT"),
        ("alpaca_entry_signal_record", "take_profit_price", "ALTER TABLE alpaca_entry_signal_record ADD COLUMN take_profit_price FLOAT"),
        ("alpaca_entry_signal_record", "exit_price", "ALTER TABLE alpaca_entry_signal_record ADD COLUMN exit_price FLOAT"),
        ("alpaca_entry_signal_record", "exit_time", "ALTER TABLE alpaca_entry_signal_record ADD COLUMN exit_time TIMESTAMP"),
        ("alpaca_entry_signal_record", "exit_method", "ALTER TABLE alpaca_entry_signal_record ADD COLUMN exit_method VARCHAR(50)"),
        ("alpaca_entry_signal_record", "hold_duration_seconds", "ALTER TABLE alpaca_entry_signal_record ADD COLUMN hold_duration_seconds FLOAT"),
    ]
    
    inspector = inspect(db.engine)
    
    for table_name, column_name, sql in migrations:
        try:
            if table_name in inspector.get_table_names():
                columns = [col['name'] for col in inspector.get_columns(table_name)]
                if column_name not in columns:
                    db.session.execute(text(sql))
                    db.session.commit()
                    logging.info(f"✅ Migration: Added {column_name} to {table_name}")
        except Exception as e:
            db.session.rollback()
            logging.warning(f"Migration skipped for {table_name}.{column_name}: {e}")
    
    # Update existing config values (data migration)
    try:
        # Update switch_profit_ratio from 0.90 to 0.85 and switch_profit_ratio_strong from 0.95 to 0.90
        db.session.execute(text("""
            UPDATE trailing_stop_config 
            SET switch_profit_ratio = 0.85, switch_profit_ratio_strong = 0.90 
            WHERE switch_profit_ratio = 0.90 AND switch_profit_ratio_strong = 0.95
        """))
        db.session.commit()
        logging.info("✅ Updated switch_profit_ratio to 0.85 and switch_profit_ratio_strong to 0.90")
    except Exception as e:
        db.session.rollback()
        logging.warning(f"Data migration skipped: {e}")
    
    # Initialize signal_stop_loss from fixed_stop_loss for existing active positions
    try:
        result = db.session.execute(text("""
            UPDATE trailing_stop_position 
            SET signal_stop_loss = fixed_stop_loss 
            WHERE signal_stop_loss IS NULL AND is_active = true
        """))
        db.session.commit()
        if result.rowcount > 0:
            logging.info(f"✅ Initialized signal_stop_loss for {result.rowcount} active positions")
    except Exception as e:
        db.session.rollback()
        logging.warning(f"signal_stop_loss initialization skipped: {e}")
    
    # Initialize remaining_quantity for open CompletedTrade records
    try:
        result = db.session.execute(text("""
            UPDATE completed_trade 
            SET remaining_quantity = entry_quantity, exited_quantity = 0 
            WHERE remaining_quantity IS NULL AND is_open = true
        """))
        db.session.commit()
        if result.rowcount > 0:
            logging.info(f"✅ Initialized remaining_quantity for {result.rowcount} open trades")
    except Exception as e:
        db.session.rollback()
        logging.warning(f"remaining_quantity initialization skipped: {e}")
    
    # Widen trigger_reason column from varchar(100) to varchar(500)
    try:
        from sqlalchemy import text as _text
        result = db.session.execute(_text("""
            SELECT character_maximum_length FROM information_schema.columns 
            WHERE table_name = 'trailing_stop_position' AND column_name = 'trigger_reason'
        """))
        row = result.fetchone()
        if row and row[0] and row[0] < 500:
            db.session.execute(_text("ALTER TABLE trailing_stop_position ALTER COLUMN trigger_reason TYPE varchar(500)"))
            db.session.commit()
            logging.info("✅ Migration: Widened trigger_reason to varchar(500)")
    except Exception as e:
        db.session.rollback()
        logging.warning(f"trigger_reason widen skipped: {e}")

    # Widen exit_indicator column from varchar(100) to varchar(500)
    try:
        from sqlalchemy import text as _text
        result = db.session.execute(_text("""
            SELECT character_maximum_length FROM information_schema.columns 
            WHERE table_name = 'closed_position' AND column_name = 'exit_indicator'
        """))
        row = result.fetchone()
        if row and row[0] and row[0] < 500:
            db.session.execute(_text("ALTER TABLE closed_position ALTER COLUMN exit_indicator TYPE varchar(500)"))
            db.session.commit()
            logging.info("✅ Migration: Widened exit_indicator to varchar(500)")
    except Exception as e:
        db.session.rollback()
        logging.warning(f"exit_indicator widen skipped: {e}")

    # Backfill exit_indicator from exit_signal_content JSON for historical records
    try:
        result = db.session.execute(text("""
            UPDATE closed_position 
            SET exit_indicator = (exit_signal_content::json->'extras'->>'indicator')
            WHERE exit_indicator IS NULL 
            AND exit_signal_content IS NOT NULL 
            AND exit_signal_content LIKE '%"indicator"%'
        """))
        db.session.commit()
        if result.rowcount > 0:
            logging.info(f"✅ Backfilled exit_indicator for {result.rowcount} closed positions")
    except Exception as e:
        db.session.rollback()
        logging.warning(f"exit_indicator backfill skipped: {e}")

with app.app_context():
    import models
    import alpaca.models  # noqa: F401
    import trade_analysis.models  # noqa: F401
    db.create_all()
    
    run_migrations()
    
    from watchlist_service import init_default_watchlist
    init_default_watchlist()

from routes import *
from trade_analysis.routes import *  # noqa: F401

from alpaca import alpaca_bp
from alpaca.routes import *  # noqa: F401
app.register_blueprint(alpaca_bp)

# Setup database logging handler
try:
    from db_log_handler import setup_db_logging
    setup_db_logging(app)
    logging.info("📊 Database log handler initialized")
except Exception as e:
    logging.error(f"Failed to setup database logging: {e}")

# Start trailing stop scheduler (only in main process)
def start_trailing_stop_scheduler():
    try:
        from trailing_stop_scheduler import start_scheduler
        start_scheduler(app, interval_seconds=5)
        logging.info("📊 Trailing stop auto-scheduler initialized")
    except Exception as e:
        logging.error(f"Failed to start trailing stop scheduler: {str(e)}")

# Initialize Tiger WebSocket push client for real-time data
# Only initialize in a single process to avoid duplicate connections
_push_client_initialized = False

def start_push_client():
    global _push_client_initialized
    if _push_client_initialized:
        return
    
    try:
        from tiger_push_client import initialize_push_client
        if initialize_push_client(register_handlers=True):
            logging.info("📊 Tiger WebSocket push client initialized")
            _push_client_initialized = True
        else:
            logging.warning("⚠️ Tiger WebSocket push client failed to connect, using polling fallback")
    except Exception as e:
        logging.error(f"Failed to initialize push client: {str(e)}")

# Start reconciliation scheduler (hourly fetch) - uses lock to prevent duplicate runs
_reconciliation_scheduler_started = False

def start_reconciliation_scheduler():
    global _reconciliation_scheduler_started
    if _reconciliation_scheduler_started:
        return
    _reconciliation_scheduler_started = True
    
    try:
        from reconciliation_service import scheduled_fetch_filled_orders, scheduled_reconciliation
        import threading
        
        def _reconciliation_loop():
            import time as time_module
            time_module.sleep(60)
            while True:
                try:
                    scheduled_fetch_filled_orders(app)
                    scheduled_reconciliation(app)
                except Exception as e:
                    logging.error(f"Reconciliation scheduler error: {str(e)}")
                time_module.sleep(3600)
        
        t = threading.Thread(target=_reconciliation_loop, daemon=True)
        t.start()
        logging.info("📊 Reconciliation scheduler initialized (hourly fetch)")
    except Exception as e:
        logging.error(f"Failed to start reconciliation scheduler: {str(e)}")

# Start scheduler when app initializes
start_trailing_stop_scheduler()

# Start WebSocket push client
start_push_client()

# Start reconciliation scheduler
start_reconciliation_scheduler()

# Start holdings sync scheduler (periodic Tiger position sync to local DB)
_holdings_sync_started = False

def start_holdings_sync_scheduler():
    global _holdings_sync_started
    if _holdings_sync_started:
        return
    _holdings_sync_started = True
    
    try:
        import threading
        
        def _holdings_sync_loop():
            import time as time_module
            time_module.sleep(60)
            while True:
                try:
                    from holdings_sync import sync_all_holdings
                    with app.app_context():
                        sync_all_holdings()
                except Exception as e:
                    logging.error(f"Holdings sync scheduler error: {str(e)}")
                time_module.sleep(300)
        
        t = threading.Thread(target=_holdings_sync_loop, daemon=True)
        t.start()
        logging.info("📊 Holdings sync scheduler initialized (API fallback every 5min, real-time via WebSocket)")
    except Exception as e:
        logging.error(f"Failed to start holdings sync scheduler: {str(e)}")

start_holdings_sync_scheduler()

_alpaca_scheduler_started = False

def start_alpaca_trailing_stop_scheduler():
    global _alpaca_scheduler_started
    if _alpaca_scheduler_started:
        return
    _alpaca_scheduler_started = True

    try:
        from alpaca.trailing_stop_scheduler import start_scheduler as alpaca_start_scheduler
        alpaca_start_scheduler(app)
        logging.info("Alpaca trailing stop scheduler initialized")
    except Exception as e:
        logging.error(f"Failed to start Alpaca trailing stop scheduler: {str(e)}")

start_alpaca_trailing_stop_scheduler()

_trade_analysis_scheduler_started = False

def start_trade_analysis_scheduler():
    global _trade_analysis_scheduler_started
    if _trade_analysis_scheduler_started:
        return
    _trade_analysis_scheduler_started = True

    try:
        import threading
        from datetime import time as dt_time

        def _analysis_loop():
            import time as time_module
            from datetime import datetime, date, timedelta
            import pytz

            time_module.sleep(120)
            et = pytz.timezone('US/Eastern')
            last_run_date = None

            while True:
                try:
                    now_et = datetime.now(et)
                    target_hour = 17
                    today_date = now_et.date()

                    if now_et.weekday() >= 5:
                        time_module.sleep(60)
                        continue

                    if now_et.hour == target_hour and now_et.minute < 5 and last_run_date != today_date:
                        target_date = date.today()
                        last_run_date = today_date

                        logging.info(f"🤖 Running scheduled trade analysis for {target_date}")
                        with app.app_context():
                            from trade_analysis.orchestrator import run_daily_analysis
                            result = run_daily_analysis(target_date=target_date, send_discord=True)
                            logging.info(f"🤖 Scheduled trade analysis result: {result.get('status')}")

                        time_module.sleep(300)
                    else:
                        time_module.sleep(30)
                except Exception as e:
                    logging.error(f"Trade analysis scheduler error: {str(e)}")
                    time_module.sleep(60)

        t = threading.Thread(target=_analysis_loop, daemon=True)
        t.start()
        logging.info("🤖 Trade analysis scheduler initialized (daily at 5:00 PM ET)")
    except Exception as e:
        logging.error(f"Failed to start trade analysis scheduler: {str(e)}")

start_trade_analysis_scheduler()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
