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
    tiger_response = db.Column(db.Text, nullable=True)  # Store Tiger API response
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
    
    # Reference price for market order conversion
    reference_price = db.Column(db.Float, nullable=True)  # Used to convert market orders with stop/take profit
    
    # Auto-protection fields for position increases
    needs_auto_protection = db.Column(db.Boolean, nullable=True, default=False)  # Flag for auto-protection
    protection_info = db.Column(db.Text, nullable=True)  # JSON string with protection details
    
    # Account type: real or paper
    account_type = db.Column(db.String(20), nullable=True, default='real')  # 'real' or 'paper'
    
    # Entry average cost - captured at position close for P&L display
    entry_avg_cost = db.Column(db.Float, nullable=True)  # Average cost at time of trade
    
    # Direct link: close order → entry order (established at close-time, not after the fact)
    parent_entry_order_id = db.Column(db.String(50), nullable=True)  # Entry order's tiger_order_id

class PositionCost(db.Model):
    """Track real-time position cost basis for each symbol/account.
    
    This table maintains the current average cost for open positions.
    When a position is fully closed, the data is used to populate Trade.entry_avg_cost
    before the row is reset for the next trading cycle.
    """
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')  # 'real' or 'paper'
    
    # Position tracking
    quantity = db.Column(db.Float, nullable=False, default=0)  # Current position size
    total_cost_basis = db.Column(db.Float, nullable=False, default=0)  # Total cost = qty * avg_price
    average_cost = db.Column(db.Float, nullable=True)  # Computed: total_cost_basis / quantity
    
    # Timestamps
    first_entry_at = db.Column(db.DateTime, nullable=True)  # When position was first opened
    last_fill_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Unique constraint: one record per symbol per account type
    __table_args__ = (
        db.UniqueConstraint('symbol', 'account_type', name='uq_symbol_account'),
    )
    
    def update_on_buy(self, quantity: float, price: float):
        """Update cost basis when buying (opening/adding to position)"""
        new_total_cost = self.total_cost_basis + (quantity * price)
        new_quantity = self.quantity + quantity
        
        self.total_cost_basis = new_total_cost
        self.quantity = new_quantity
        self.average_cost = new_total_cost / new_quantity if new_quantity > 0 else 0
        
        if self.first_entry_at is None:
            self.first_entry_at = datetime.utcnow()
        self.last_fill_at = datetime.utcnow()
    
    def update_on_sell(self, quantity: float):
        """Update cost basis when selling (reducing/closing position).
        Returns the average cost at time of sale for recording in Trade."""
        avg_cost_at_sale = self.average_cost
        
        new_quantity = self.quantity - quantity
        if new_quantity <= 0:
            # Position fully closed - reset for next cycle
            self.quantity = 0
            self.total_cost_basis = 0
            self.average_cost = None
            self.first_entry_at = None
        else:
            # Partial close - reduce proportionally
            self.quantity = new_quantity
            self.total_cost_basis = new_quantity * self.average_cost
        
        self.last_fill_at = datetime.utcnow()
        return avg_cost_at_sale

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
    
    # Relationship to Trade for easy status lookup
    trade = db.relationship('Trade', backref='signal_logs', foreign_keys=[trade_id])
    
    # Extended fields for webhook tracking
    endpoint = db.Column(db.String(50), nullable=True)  # /webhook, /webhook_paper, /webhook_both
    account_type = db.Column(db.String(20), nullable=True)  # real, paper, both
    tiger_status = db.Column(db.String(50), nullable=True)  # success, error, partial
    tiger_order_id = db.Column(db.String(100), nullable=True)  # Order ID(s) from Tiger API
    tiger_response = db.Column(db.Text, nullable=True)  # Full Tiger API response


class TrailingStopMode(enum.Enum):
    CONSERVATIVE = "conservative"  # Fixed take profit only
    BALANCED = "balanced"  # Switch to trailing when near TP
    AGGRESSIVE = "aggressive"  # Switch early, wider trailing


class TrailingStopPosition(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    
    # Position info
    side = db.Column(db.String(10), nullable=False)  # 'long' or 'short'
    entry_price = db.Column(db.Float, nullable=False)  # Average entry price (updated on scaling)
    first_entry_price = db.Column(db.Float, nullable=True)  # First entry price (preserved, never updated on scaling)
    quantity = db.Column(db.Float, nullable=False)
    timeframe = db.Column(db.String(10), nullable=True, default='15')  # Signal timeframe for ATR calculation
    
    # Original order info
    trade_id = db.Column(db.Integer, db.ForeignKey('trade.id'), nullable=True)
    signal_stop_loss = db.Column(db.Float, nullable=True)  # Signal's original stop loss (never changes)
    fixed_stop_loss = db.Column(db.Float, nullable=True)  # Current Tiger stop order price (updated on adjustments)
    fixed_take_profit = db.Column(db.Float, nullable=True)  # Original take profit price
    stop_loss_order_id = db.Column(db.String(50), nullable=True)
    take_profit_order_id = db.Column(db.String(50), nullable=True)
    
    # Trailing stop tracking
    highest_price = db.Column(db.Float, nullable=True)  # For long positions
    lowest_price = db.Column(db.Float, nullable=True)  # For short positions
    current_trailing_stop = db.Column(db.Float, nullable=True)
    previous_trailing_stop = db.Column(db.Float, nullable=True)
    
    # ATR data
    current_atr = db.Column(db.Float, nullable=True)
    volatility_pct = db.Column(db.Float, nullable=True)
    
    # State tracking
    profit_tier = db.Column(db.Integer, default=0)  # 0, 1, 2
    current_profit_pct = db.Column(db.Float, nullable=True)
    mode = db.Column(Enum(TrailingStopMode), default=TrailingStopMode.BALANCED)
    
    # Switch state
    has_switched_to_trailing = db.Column(db.Boolean, default=False)
    switch_triggered_at = db.Column(db.DateTime, nullable=True)
    switch_reason = db.Column(db.String(100), nullable=True)
    
    # Trend strength tracking
    trend_strength = db.Column(db.Float, nullable=True)  # 0-100 综合评分
    atr_convergence = db.Column(db.Float, nullable=True)  # ATR收敛度 (0-1, 越小越收敛)
    momentum_score = db.Column(db.Float, nullable=True)  # 动量评分 (ATR倍数)
    consecutive_highs = db.Column(db.Integer, default=0)  # 连续创新高/低计数
    
    # Lifecycle tracking - HOW was this TS created?
    # Values: 'webhook_immediate', 'entry_fill_handler', 'scheduler_orphan', 'manual'
    creation_source = db.Column(db.String(30), nullable=True)
    
    # Progressive stop tracking (阶梯止损状态)
    progressive_stop_tier = db.Column(db.Integer, default=0)  # 0=original, 1-4=tiers
    last_stop_adjustment_price = db.Column(db.Float, nullable=True)  # Price when stop was last adjusted
    stop_adjustment_count = db.Column(db.Integer, default=0)  # Number of times stop was adjusted
    
    # Status
    is_active = db.Column(db.Boolean, default=True)
    is_triggered = db.Column(db.Boolean, default=False)
    triggered_at = db.Column(db.DateTime, nullable=True)
    triggered_price = db.Column(db.Float, nullable=True)
    trigger_reason = db.Column(db.String(500), nullable=True)
    trigger_retry_count = db.Column(db.Integer, default=0)  # Retry count for close failures
    
    # Always-On Soft Stop: breach tracking for OCA grace window
    breach_detected_at = db.Column(db.DateTime, nullable=True)  # When stop breach first detected
    breach_price = db.Column(db.Float, nullable=True)  # Price at first breach detection
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_check_at = db.Column(db.DateTime, nullable=True)
    
    # Note: Partial unique index (uq_active_trailing_stop_active) is created directly in database
    # to enforce uniqueness only when is_active=true


class TrailingStopConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    
    # ATR settings
    atr_period = db.Column(db.Integer, default=14)
    
    # Profit tier thresholds (as decimal, e.g., 0.01 = 1%)
    tier_0_threshold = db.Column(db.Float, default=0.01)  # < 1%
    tier_1_threshold = db.Column(db.Float, default=0.03)  # < 3%
    
    # Base multipliers for each tier
    tier_0_multiplier = db.Column(db.Float, default=2.5)
    tier_1_multiplier = db.Column(db.Float, default=2.0)
    tier_2_multiplier = db.Column(db.Float, default=1.5)
    
    # Volatility thresholds
    low_volatility_threshold = db.Column(db.Float, default=0.008)  # 0.8%
    high_volatility_threshold = db.Column(db.Float, default=0.015)  # 1.5%
    
    # Volatility factors
    low_volatility_factor = db.Column(db.Float, default=0.8)
    mid_volatility_factor = db.Column(db.Float, default=1.0)
    high_volatility_factor = db.Column(db.Float, default=1.2)
    
    # Dynamic percent stop settings - piecewise linear formula
    # Tier 1: 0% ~ tier1_upper → 0% ~ tier1_percent
    # Tier 2: tier1_upper ~ tier2_upper → tier1_percent ~ tier2_percent
    # Tier 3: > tier2_upper → tier2_percent ~ max_percent_stop (capped)
    dynamic_pct_tier1_upper = db.Column(db.Float, default=0.02)  # 2% profit
    dynamic_pct_tier2_upper = db.Column(db.Float, default=0.05)  # 5% profit
    dynamic_pct_tier1_percent = db.Column(db.Float, default=0.002)  # 0.2% trail
    dynamic_pct_tier2_percent = db.Column(db.Float, default=0.005)  # 0.5% trail
    max_percent_stop = db.Column(db.Float, default=0.008)  # 0.8% max trail
    
    # Switch settings (ratio of planned profit to trigger switch)
    switch_profit_ratio = db.Column(db.Float, default=0.85)  # 85% (降低以提前撤销止盈)
    switch_profit_ratio_strong = db.Column(db.Float, default=0.90)  # 90% for strong trend
    
    # Trend strength settings
    trend_strength_threshold = db.Column(db.Float, default=60.0)  # Score above this = strong trend
    momentum_lookback = db.Column(db.Integer, default=5)  # K-line bars to look back for momentum
    atr_convergence_weight = db.Column(db.Float, default=0.3)  # Weight for ATR convergence (0-1)
    momentum_weight = db.Column(db.Float, default=0.4)  # Weight for momentum score (0-1)
    consecutive_weight = db.Column(db.Float, default=0.3)  # Weight for consecutive highs (0-1)
    
    # Progressive stop loss adjustment settings (阶梯止损上移)
    # Each tier defines: profit threshold → move stop to X% profit level
    # 加密间距版本：每1%利润上移一次止损，锁定约60%利润
    progressive_stop_enabled = db.Column(db.Boolean, default=True)
    prog_tier1_profit = db.Column(db.Float, default=0.01)  # 1% profit triggers
    prog_tier1_stop_at = db.Column(db.Float, default=0.0)  # Move to breakeven (0%)
    prog_tier2_profit = db.Column(db.Float, default=0.02)  # 2% profit triggers  
    prog_tier2_stop_at = db.Column(db.Float, default=0.005)  # Move to 0.5% profit
    prog_tier3_profit = db.Column(db.Float, default=0.03)  # 3% profit triggers
    prog_tier3_stop_at = db.Column(db.Float, default=0.015)  # Move to 1.5% profit
    prog_tier4_profit = db.Column(db.Float, default=0.04)  # 4% profit triggers
    prog_tier4_stop_at = db.Column(db.Float, default=0.025)  # Move to 2.5% profit
    prog_tier5_profit = db.Column(db.Float, default=0.05)  # 5% profit triggers
    prog_tier5_stop_at = db.Column(db.Float, default=0.035)  # Move to 3.5% profit
    prog_tier6_profit = db.Column(db.Float, default=0.06)  # 6% profit triggers
    prog_tier6_stop_at = db.Column(db.Float, default=0.045)  # Move to 4.5% profit
    prog_tier7_profit = db.Column(db.Float, default=0.07)  # 7% profit triggers
    prog_tier7_stop_at = db.Column(db.Float, default=0.055)  # Move to 5.5% profit
    prog_tier8_profit = db.Column(db.Float, default=0.08)  # 8% profit triggers
    prog_tier8_stop_at = db.Column(db.Float, default=0.065)  # Move to 6.5% profit
    
    # 切换条件 (新增条件B和C)
    switch_profit_threshold = db.Column(db.Float, default=0.08)  # 条件B: 利润>=8%且趋势强
    switch_force_profit = db.Column(db.Float, default=0.10)  # 条件C: 利润>=10%强制切换
    
    # Post-switch settings
    post_switch_multiplier = db.Column(db.Float, default=1.2)
    post_switch_trail_pct = db.Column(db.Float, default=0.05)  # 5%
    
    # Tightening settings for position scaling (加仓后收紧trailing)
    # When cost is close to current price, tighten trailing distance
    tighten_threshold = db.Column(db.Float, default=0.02)  # Trigger when cost distance < 2%
    tighten_atr_multiplier = db.Column(db.Float, default=0.6)  # Reduce ATR multiplier from 1.2 to 0.6
    tighten_trail_pct = db.Column(db.Float, default=0.005)  # Fixed 0.5% trail when tightened
    
    # Check interval (5 seconds for real-time Tiger data)
    check_interval_seconds = db.Column(db.Integer, default=5)
    
    # Global enable
    is_enabled = db.Column(db.Boolean, default=True)
    
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TrailingStopLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('trailing_stop_position.id'), nullable=False)
    
    event_type = db.Column(db.String(50), nullable=False)  # 'check', 'update', 'switch', 'trigger'
    
    # Price data at event
    current_price = db.Column(db.Float, nullable=True)
    highest_price = db.Column(db.Float, nullable=True)
    trailing_stop_price = db.Column(db.Float, nullable=True)
    atr_value = db.Column(db.Float, nullable=True)
    profit_pct = db.Column(db.Float, nullable=True)
    
    # Details
    details = db.Column(db.Text, nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class ExitMethod(enum.Enum):
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    TRAILING_STOP = "trailing_stop"
    WEBHOOK_SIGNAL = "webhook_signal"
    MANUAL = "manual"
    EXTERNAL = "external"  # Position closed externally (e.g., via broker platform)


class PositionStatus(enum.Enum):
    OPEN = "open"
    CLOSED = "closed"


class LegType(enum.Enum):
    ENTRY = "entry"
    ADD = "add"
    EXIT = "exit"


class Position(db.Model):
    """Core position entity - groups all orders for one trading cycle.
    
    Identified by position_key = {symbol}_{trade_date}_{sequence_number}
    e.g., AAPL_2025-02-06_1 = first AAPL position opened on Feb 6
    """
    id = db.Column(db.Integer, primary_key=True)
    
    position_key = db.Column(db.String(100), unique=True, nullable=False, index=True)
    symbol = db.Column(db.String(20), nullable=False, index=True)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    trade_date = db.Column(db.Date, nullable=False)
    sequence_number = db.Column(db.Integer, nullable=False, default=1)
    
    side = db.Column(db.String(10), nullable=False)  # 'long' or 'short'
    status = db.Column(Enum(PositionStatus), nullable=False, default=PositionStatus.OPEN)
    
    total_entry_quantity = db.Column(db.Float, nullable=False, default=0)
    total_exit_quantity = db.Column(db.Float, nullable=False, default=0)
    avg_entry_price = db.Column(db.Float, nullable=True)
    avg_exit_price = db.Column(db.Float, nullable=True)
    
    realized_pnl = db.Column(db.Float, nullable=True)
    commission = db.Column(db.Float, nullable=True)
    
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('trailing_stop_position.id'), nullable=True)
    
    # Lifecycle tracking - HOW was position closed?
    # Values: 'websocket_fill', 'polling_fill', 'reconciliation', 'ghost_detection', 'soft_stop', 'manual'
    close_source = db.Column(db.String(30), nullable=True)
    
    opened_at = db.Column(db.DateTime, nullable=True)
    closed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    legs = db.relationship('PositionLeg', backref='position', lazy='dynamic',
                           order_by='PositionLeg.filled_at.asc()')
    
    __table_args__ = (
        db.UniqueConstraint('symbol', 'account_type', 'trade_date', 'sequence_number',
                            name='uq_position_identity'),
    )
    
    @property
    def remaining_quantity(self):
        return self.total_entry_quantity - self.total_exit_quantity
    
    @property
    def entry_legs(self):
        return PositionLeg.query.filter(
            PositionLeg.position_id == self.id,
            PositionLeg.leg_type.in_([LegType.ENTRY, LegType.ADD])
        ).order_by(PositionLeg.filled_at.asc()).all()
    
    @property
    def exit_legs(self):
        return PositionLeg.query.filter_by(
            position_id=self.id, leg_type=LegType.EXIT
        ).order_by(PositionLeg.filled_at.asc()).all()
    
    @property
    def hold_duration_seconds(self):
        if self.opened_at and self.closed_at:
            return int((self.closed_at - self.opened_at).total_seconds())
        return None
    
    @property
    def pnl_percent(self):
        if self.realized_pnl is not None and self.avg_entry_price and self.total_entry_quantity:
            cost = self.avg_entry_price * self.total_entry_quantity
            if cost > 0:
                return (self.realized_pnl / cost) * 100
        return None


class PositionLeg(db.Model):
    """Each order associated with a position - entries, adds, and exits."""
    id = db.Column(db.Integer, primary_key=True)
    
    position_id = db.Column(db.Integer, db.ForeignKey('position.id'), nullable=False, index=True)
    leg_type = db.Column(Enum(LegType), nullable=False)
    
    tiger_order_id = db.Column(db.String(50), nullable=True, index=True)
    price = db.Column(db.Float, nullable=True)
    quantity = db.Column(db.Float, nullable=True)
    filled_at = db.Column(db.DateTime, nullable=True)
    
    trade_id = db.Column(db.Integer, db.ForeignKey('trade.id'), nullable=True)
    
    signal_content = db.Column(db.Text, nullable=True)
    signal_grade = db.Column(db.String(5), nullable=True)
    signal_score = db.Column(db.Integer, nullable=True)
    signal_indicator = db.Column(db.Text, nullable=True)
    signal_timeframe = db.Column(db.String(10), nullable=True)
    
    oca_group_id = db.Column(db.Integer, db.ForeignKey('oca_group.id'), nullable=True)
    stop_order_id = db.Column(db.String(50), nullable=True)
    take_profit_order_id = db.Column(db.String(50), nullable=True)
    stop_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)
    
    exit_method = db.Column(Enum(ExitMethod), nullable=True)
    realized_pnl = db.Column(db.Float, nullable=True)
    commission = db.Column(db.Float, nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    trade = db.relationship('Trade', backref='position_legs', foreign_keys=[trade_id])
    oca_group = db.relationship('OCAGroup', backref='position_legs', foreign_keys=[oca_group_id])


class EntrySignalRecord(db.Model):
    """Record each entry signal (including scaling/加仓) for forensics tracking"""
    id = db.Column(db.Integer, primary_key=True)
    
    # Link to parent ClosedPosition
    closed_position_id = db.Column(db.Integer, db.ForeignKey('closed_position.id'), nullable=True)
    
    # Link to Position
    position_id = db.Column(db.Integer, db.ForeignKey('position.id'), nullable=True, index=True)
    position_key = db.Column(db.String(100), nullable=True)
    
    # Entry Details
    symbol = db.Column(db.String(20), nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    entry_time = db.Column(db.DateTime, nullable=True)
    entry_price = db.Column(db.Float, nullable=True)
    quantity = db.Column(db.Float, nullable=True)
    side = db.Column(db.String(10), nullable=False)  # 'long' or 'short'
    is_scaling = db.Column(db.Boolean, default=False)  # True if this is 加仓
    
    # Order Details
    entry_order_id = db.Column(db.String(50), nullable=True)  # Tiger order ID
    
    # Signal Content
    signal_log_id = db.Column(db.Integer, db.ForeignKey('signal_log.id'), nullable=True)
    raw_json = db.Column(db.Text, nullable=True)  # Original JSON payload
    
    # Extracted Indicators from signal
    indicator_trigger = db.Column(db.Text, nullable=True)  # Which indicator triggered
    signal_grade = db.Column(db.String(5), nullable=True)  # A, B, C
    signal_score = db.Column(db.Integer, nullable=True)
    htf_grade = db.Column(db.String(5), nullable=True)
    htf_score = db.Column(db.Integer, nullable=True)
    timeframe = db.Column(db.String(10), nullable=True)
    
    # Stop/TP from this signal
    signal_stop_loss = db.Column(db.Float, nullable=True)
    signal_take_profit = db.Column(db.Float, nullable=True)
    
    # Actual SL/TP (from trailing stop)
    stop_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)
    
    # Exit Details (filled when position closes)
    exit_price = db.Column(db.Float, nullable=True)
    exit_time = db.Column(db.DateTime, nullable=True)
    exit_method = db.Column(db.String(50), nullable=True)
    hold_duration_seconds = db.Column(db.Float, nullable=True)
    
    # Calculated on exit (filled by close logic)
    contribution_pnl = db.Column(db.Float, nullable=True)  # (exit_price - entry_price) * qty
    contribution_pct = db.Column(db.Float, nullable=True)  # percentage
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class ClosedPosition(db.Model):
    """Each row = one exit order (平仓动作). Links to multiple entry signals."""
    id = db.Column(db.Integer, primary_key=True)
    
    # Exit Order Info
    symbol = db.Column(db.String(20), nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    exit_order_id = db.Column(db.String(50), nullable=True)  # Tiger order ID
    exit_time = db.Column(db.DateTime, nullable=True)
    exit_price = db.Column(db.Float, nullable=True)
    exit_quantity = db.Column(db.Float, nullable=True)
    side = db.Column(db.String(10), nullable=False)  # original position side: 'long' or 'short'
    
    # Exit Method
    exit_method = db.Column(Enum(ExitMethod), nullable=True)
    exit_signal_content = db.Column(db.Text, nullable=True)  # If closed by signal
    exit_indicator = db.Column(db.String(500), nullable=True)  # Signal indicator name, e.g., "RSX cross Exit"
    
    # P&L Summary (from Tiger API)
    total_pnl = db.Column(db.Float, nullable=True)  # realized_pnl from Tiger API
    total_pnl_pct = db.Column(db.Float, nullable=True)
    avg_entry_price = db.Column(db.Float, nullable=True)  # Weighted average of all entries
    commission = db.Column(db.Float, nullable=True)  # commission from Tiger API
    
    # Related IDs
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('trailing_stop_position.id'), nullable=True)
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationship to entry signals
    entry_signals = db.relationship('EntrySignalRecord', backref='closed_position_ref', 
                                     foreign_keys='EntrySignalRecord.closed_position_id',
                                     lazy='dynamic')


class CompletedTrade(db.Model):
    """Track completed trades from entry to exit for signal quality analysis"""
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    
    # Entry Information
    entry_signal_id = db.Column(db.Integer, db.ForeignKey('signal_log.id'), nullable=True)
    entry_signal_content = db.Column(db.Text, nullable=True)
    entry_time = db.Column(db.DateTime, nullable=True)
    entry_price = db.Column(db.Float, nullable=True)
    entry_quantity = db.Column(db.Float, nullable=True)
    side = db.Column(db.String(10), nullable=False)  # 'long' or 'short'
    
    # Exit Information
    exit_method = db.Column(Enum(ExitMethod), nullable=True)
    exit_signal_id = db.Column(db.Integer, db.ForeignKey('signal_log.id'), nullable=True)
    exit_signal_content = db.Column(db.Text, nullable=True)
    exit_time = db.Column(db.DateTime, nullable=True)
    exit_price = db.Column(db.Float, nullable=True)
    exit_quantity = db.Column(db.Float, nullable=True)
    
    # P&L Analysis
    pnl_amount = db.Column(db.Float, nullable=True)
    pnl_percent = db.Column(db.Float, nullable=True)
    highest_price = db.Column(db.Float, nullable=True)
    lowest_price = db.Column(db.Float, nullable=True)
    max_profit_pct = db.Column(db.Float, nullable=True)
    max_drawdown_pct = db.Column(db.Float, nullable=True)
    
    # Holding Statistics
    hold_duration_seconds = db.Column(db.Integer, nullable=True)
    stop_adjustment_count = db.Column(db.Integer, default=0)
    scaled_in = db.Column(db.Boolean, default=False)
    
    # Original Order Info
    original_stop_loss = db.Column(db.Float, nullable=True)
    original_take_profit = db.Column(db.Float, nullable=True)
    final_stop_loss = db.Column(db.Float, nullable=True)
    
    # Signal Analysis Dimensions
    signal_indicator = db.Column(db.Text, nullable=True)
    signal_type = db.Column(db.String(50), nullable=True)  # WaveMatrix, TDindicator, etc.
    signal_grade = db.Column(db.String(5), nullable=True)  # A, B, C
    signal_score = db.Column(db.Integer, nullable=True)  # -4, +0, +3, etc.
    htf_grade = db.Column(db.String(5), nullable=True)  # A, B, C
    htf_score = db.Column(db.Integer, nullable=True)  # -4, +0, +3, etc.
    htf_pass_status = db.Column(db.String(20), nullable=True)  # strongpass, pass, reject
    trend_strength = db.Column(db.Float, nullable=True)  # 70.6%, etc.
    signal_timeframe = db.Column(db.String(10), nullable=True)
    
    # Status and FIFO Tracking
    is_open = db.Column(db.Boolean, default=True)  # True = position still open, False = closed
    remaining_quantity = db.Column(db.Float, nullable=True)  # Remaining open quantity (for FIFO matching)
    exited_quantity = db.Column(db.Float, nullable=True)  # Total quantity exited so far
    avg_exit_price = db.Column(db.Float, nullable=True)  # Weighted average exit price
    
    # Related IDs
    trade_id = db.Column(db.Integer, db.ForeignKey('trade.id'), nullable=True)
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('trailing_stop_position.id'), nullable=True)
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class OCAStatus(enum.Enum):
    """OCA group status"""
    ACTIVE = "active"                  # OCA订单组激活中
    TRIGGERED_STOP = "triggered_stop"  # 止损触发
    TRIGGERED_TP = "triggered_tp"      # 止盈触发
    CANCELLED = "cancelled"            # 已取消（手动/信号平仓）
    EXPIRED = "expired"                # 已过期（DAY订单）
    SOFT_STOP = "soft_stop"            # 软保护触发（Paper盘前盘后）


class OCAGroup(db.Model):
    """Track OCA (One-Cancels-All) order groups for position protection.
    
    Each OCA group contains a stop loss order and a take profit order.
    When one triggers, the other is automatically cancelled by Tiger.
    """
    id = db.Column(db.Integer, primary_key=True)
    
    # Core identification
    oca_group_id = db.Column(db.String(100), unique=True, nullable=False, index=True)  # Tiger OCA group ID
    symbol = db.Column(db.String(20), nullable=False)
    account = db.Column(db.String(50), nullable=False)  # Account number
    account_type = db.Column(db.String(20), nullable=False)  # 'real' or 'paper'
    
    # Position info
    side = db.Column(db.String(10), nullable=False)  # 'long' or 'short' (original position)
    quantity = db.Column(db.Float, nullable=False)
    entry_price = db.Column(db.Float, nullable=True)  # Reference entry price
    
    # Order IDs
    stop_order_id = db.Column(db.String(50), nullable=True)  # Tiger stop loss order ID
    take_profit_order_id = db.Column(db.String(50), nullable=True)  # Tiger take profit order ID
    
    # Prices
    stop_price = db.Column(db.Float, nullable=True)  # Current stop loss price
    stop_limit_price = db.Column(db.Float, nullable=True)  # Limit price for STP_LMT
    take_profit_price = db.Column(db.Float, nullable=True)  # Take profit price
    
    # Order settings
    time_in_force = db.Column(db.String(10), nullable=False, default='GTC')  # GTC or DAY
    outside_rth_stop = db.Column(db.Boolean, default=True)  # Stop order extended hours
    outside_rth_tp = db.Column(db.Boolean, default=True)  # Take profit extended hours
    
    # Status tracking
    status = db.Column(Enum(OCAStatus), nullable=False, default=OCAStatus.ACTIVE)
    triggered_order_id = db.Column(db.String(50), nullable=True)  # Which order triggered
    triggered_price = db.Column(db.Float, nullable=True)  # Price at trigger
    triggered_at = db.Column(db.DateTime, nullable=True)
    
    # Related records
    trade_id = db.Column(db.Integer, db.ForeignKey('trade.id'), nullable=True)
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('trailing_stop_position.id'), nullable=True)
    closed_position_id = db.Column(db.Integer, db.ForeignKey('closed_position.id'), nullable=True)
    
    # Rebuild tracking (for Paper DAY orders)
    rebuild_count = db.Column(db.Integer, default=0)
    last_rebuild_at = db.Column(db.DateTime, nullable=True)
    previous_stop_order_id = db.Column(db.String(50), nullable=True)  # Before rebuild
    previous_tp_order_id = db.Column(db.String(50), nullable=True)  # Before rebuild
    
    # Lifecycle tracking - WHAT triggered OCA creation?
    # Values: 'webhook_immediate', 'ts_creation_auto', 'entry_fill_handler', 'scheduler_orphan', 'oca_rebuild', 'ts_update', 'manual'
    creation_source = db.Column(db.String(30), nullable=True)
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    trade = db.relationship('Trade', backref='oca_groups', foreign_keys=[trade_id])
    trailing_stop = db.relationship('TrailingStopPosition', backref='oca_groups', foreign_keys=[trailing_stop_id])


class OrderRole(enum.Enum):
    """Order role for tracking what type of order this is"""
    ENTRY = "entry"                    # 开仓订单
    EXIT_SIGNAL = "exit_signal"        # 信号平仓订单
    EXIT_TRAILING = "exit_trailing"    # Trailing stop平仓订单
    STOP_LOSS = "stop_loss"            # 止损子订单
    TAKE_PROFIT = "take_profit"        # 止盈子订单


class OrderTracker(db.Model):
    """Track all orders and their expected roles for unified order monitoring.
    
    This table records every order placed through our system, allowing us to:
    1. Match Tiger order fills to their original purpose
    2. Correctly set exit_method in ClosedPosition
    3. Capture accurate realized_pnl and commission from Tiger API
    """
    id = db.Column(db.Integer, primary_key=True)
    
    # Order identification
    tiger_order_id = db.Column(db.String(50), unique=True, nullable=False, index=True)
    parent_order_id = db.Column(db.String(50), nullable=True)  # For attached orders
    
    # Order details
    symbol = db.Column(db.String(20), nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    role = db.Column(Enum(OrderRole), nullable=False)
    
    # Order parameters
    side = db.Column(db.String(10), nullable=True)  # BUY/SELL
    quantity = db.Column(db.Float, nullable=True)
    order_type = db.Column(db.String(20), nullable=True)  # MARKET/LIMIT/STOP/STP_LMT
    limit_price = db.Column(db.Float, nullable=True)
    stop_price = db.Column(db.Float, nullable=True)
    
    # Fill information (updated when order fills)
    status = db.Column(db.String(20), nullable=False, default='PENDING')  # PENDING/FILLED/CANCELLED/REJECTED
    filled_quantity = db.Column(db.Float, nullable=True)
    avg_fill_price = db.Column(db.Float, nullable=True)
    realized_pnl = db.Column(db.Float, nullable=True)  # From Tiger API
    commission = db.Column(db.Float, nullable=True)    # From Tiger API
    fill_time = db.Column(db.DateTime, nullable=True)
    
    # OCA Group tracking
    oca_group_id = db.Column(db.Integer, db.ForeignKey('oca_group.id'), nullable=True)
    leg_role = db.Column(db.String(20), nullable=True)  # 'stop_loss' or 'take_profit' within OCA
    
    # Signal content
    signal_content = db.Column(db.Text, nullable=True)
    
    # Related records
    trade_id = db.Column(db.Integer, db.ForeignKey('trade.id'), nullable=True)
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('trailing_stop_position.id'), nullable=True)
    closed_position_id = db.Column(db.Integer, db.ForeignKey('closed_position.id'), nullable=True)
    
    # Lifecycle tracking - HOW was this fill detected?
    # Values: 'websocket', 'polling', 'reconciliation'
    fill_source = db.Column(db.String(30), nullable=True)
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to OCA Group
    oca_group = db.relationship('OCAGroup', backref='order_trackers', foreign_keys=[oca_group_id])


class TigerFilledOrder(db.Model):
    """Raw filled order data from Tiger API - authoritative source for reconciliation"""
    id = db.Column(db.Integer, primary_key=True)
    
    order_id = db.Column(db.String(50), nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    symbol = db.Column(db.String(20), nullable=False)
    action = db.Column(db.String(10), nullable=False)  # BUY or SELL
    is_open = db.Column(db.Boolean, nullable=False)  # True=开仓, False=平仓
    
    quantity = db.Column(db.Float, nullable=True)
    filled = db.Column(db.Float, nullable=True)
    avg_fill_price = db.Column(db.Float, nullable=True)
    limit_price = db.Column(db.Float, nullable=True)
    
    realized_pnl = db.Column(db.Float, nullable=True)
    commission = db.Column(db.Float, nullable=True)
    
    order_time = db.Column(db.BigInteger, nullable=True)  # ms timestamp from Tiger
    trade_time = db.Column(db.BigInteger, nullable=True)  # ms timestamp from Tiger
    order_time_str = db.Column(db.String(30), nullable=True)  # formatted ET string
    trade_time_str = db.Column(db.String(30), nullable=True)
    
    status = db.Column(db.String(30), nullable=True)
    order_type = db.Column(db.String(30), nullable=True)  # LMT, STP, MKT etc.
    outside_rth = db.Column(db.Boolean, nullable=True)
    parent_id = db.Column(db.String(50), nullable=True)
    
    raw_json = db.Column(db.Text, nullable=True)
    
    reconciled = db.Column(db.Boolean, default=False)
    reconciled_at = db.Column(db.DateTime, nullable=True)
    matched_order_id = db.Column(db.String(50), nullable=True)  # matched opening/closing order
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('order_id', 'account_type', name='uq_tiger_filled_order'),
    )


class ReconciliationRun(db.Model):
    """Track each reconciliation run for audit"""
    id = db.Column(db.Integer, primary_key=True)
    
    run_date = db.Column(db.Date, nullable=False)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    run_type = db.Column(db.String(20), nullable=False, default='scheduled')  # scheduled or manual
    
    status = db.Column(db.String(20), nullable=False, default='running')  # running, completed, failed
    
    total_orders_fetched = db.Column(db.Integer, default=0)
    new_orders_stored = db.Column(db.Integer, default=0)
    positions_matched = db.Column(db.Integer, default=0)
    records_corrected = db.Column(db.Integer, default=0)
    records_created = db.Column(db.Integer, default=0)
    
    details = db.Column(db.Text, nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime, nullable=True)


class TigerHolding(db.Model):
    __tablename__ = 'tiger_holding'
    id = db.Column(db.Integer, primary_key=True)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    symbol = db.Column(db.String(20), nullable=False)
    quantity = db.Column(db.Float, nullable=False, default=0)
    average_cost = db.Column(db.Float, nullable=True)
    market_value = db.Column(db.Float, nullable=True)
    unrealized_pnl = db.Column(db.Float, nullable=True)
    unrealized_pnl_pct = db.Column(db.Float, nullable=True)
    sec_type = db.Column(db.String(10), nullable=True)
    currency = db.Column(db.String(10), nullable=True)
    multiplier = db.Column(db.Float, nullable=True, default=1)
    salable_qty = db.Column(db.Float, nullable=True)
    latest_price = db.Column(db.Float, nullable=True)
    synced_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('account_type', 'symbol', name='uq_tiger_holding_account_symbol'),
    )


class SystemLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    level = db.Column(db.String(10), nullable=False, index=True)
    source = db.Column(db.String(50), nullable=True, index=True)
    category = db.Column(db.String(30), nullable=True, index=True)
    message = db.Column(db.Text, nullable=False)
    symbol = db.Column(db.String(20), nullable=True, index=True)
    account_type = db.Column(db.String(20), nullable=True)
    extra_data = db.Column(db.Text, nullable=True)


class WatchlistSymbol(db.Model):
    __tablename__ = 'watchlist_symbols'
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), unique=True, nullable=False, index=True)
    added_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    last_signal_time = db.Column(db.DateTime, nullable=True)
    last_position_time = db.Column(db.DateTime, nullable=True)
    source = db.Column(db.String(50), nullable=False, default='manual')
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    notes = db.Column(db.String(200), nullable=True)


class TigerAlignmentRun(db.Model):
    __tablename__ = 'tiger_alignment_run'

    id = db.Column(db.Integer, primary_key=True)

    alignment_date = db.Column(db.Date, nullable=False, index=True)
    account_type = db.Column(db.String(20), nullable=False, default='real')
    status = db.Column(db.String(20), nullable=False, default='running')

    broker_position_count = db.Column(db.Integer, default=0)
    system_open_count = db.Column(db.Integer, default=0)

    positions_closed = db.Column(db.Integer, default=0)
    positions_created = db.Column(db.Integer, default=0)
    positions_adjusted = db.Column(db.Integer, default=0)

    details = db.Column(db.Text, nullable=True)
    error_message = db.Column(db.Text, nullable=True)

    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime, nullable=True)


class BarCache(db.Model):
    __tablename__ = 'bar_cache'
    __table_args__ = (
        db.UniqueConstraint('symbol', 'timeframe', 'timestamp', name='uq_bar_cache_symbol_tf_ts'),
        db.Index('ix_bar_cache_symbol_tf', 'symbol', 'timeframe'),
    )
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    timeframe = db.Column(db.String(10), nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False)
    open = db.Column(db.Float, nullable=False)
    high = db.Column(db.Float, nullable=False)
    low = db.Column(db.Float, nullable=False)
    close = db.Column(db.Float, nullable=False)
    volume = db.Column(db.BigInteger, nullable=False, default=0)
