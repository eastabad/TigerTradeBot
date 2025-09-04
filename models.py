from datetime import datetime
from app import db
from sqlalchemy import Enum
import enum

class OrderStatus(enum.Enum):
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    PARTIALLY_FILLED = "partially_filled"

class OrderType(enum.Enum):
    MARKET = "market"
    LIMIT = "limit"

class Side(enum.Enum):
    BUY = "buy"
    SELL = "sell"

class Trade(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    side = db.Column(Enum(Side), nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=True)  # None for market orders
    order_type = db.Column(Enum(OrderType), nullable=False)
    status = db.Column(Enum(OrderStatus), nullable=False, default=OrderStatus.PENDING)
    tiger_order_id = db.Column(db.String(50), nullable=True)
    signal_data = db.Column(db.Text, nullable=True)  # Store original TradingView signal
    error_message = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    filled_price = db.Column(db.Float, nullable=True)
    filled_quantity = db.Column(db.Float, nullable=True)
    
    # Stop loss and take profit prices
    stop_loss_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)
    
    # Child orders (for stop loss and take profit)
    stop_loss_order_id = db.Column(db.String(50), nullable=True)
    take_profit_order_id = db.Column(db.String(50), nullable=True)
    
    # Trading session settings
    trading_session = db.Column(db.String(20), nullable=True, default='regular')  # regular, extended, overnight, full
    outside_rth = db.Column(db.Boolean, nullable=True, default=False)  # Outside regular trading hours
    
    # Close position flag
    is_close_position = db.Column(db.Boolean, nullable=True, default=False)  # True for close/flat signals

class TradingConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.Text, nullable=False)
    description = db.Column(db.String(500), nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SignalLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    raw_signal = db.Column(db.Text, nullable=False)
    parsed_successfully = db.Column(db.Boolean, default=False)
    error_message = db.Column(db.Text, nullable=True)
    ip_address = db.Column(db.String(50), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    trade_id = db.Column(db.Integer, db.ForeignKey('trade.id'), nullable=True)
