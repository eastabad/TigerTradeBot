from datetime import datetime
from app import db
from sqlalchemy import Enum
import enum


class AlpacaOrderStatus(enum.Enum):
    PENDING = "pending"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    DONE_FOR_DAY = "done_for_day"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REPLACED = "replaced"
    PENDING_CANCEL = "pending_cancel"
    PENDING_REPLACE = "pending_replace"
    ACCEPTED = "accepted"
    PENDING_NEW = "pending_new"
    ACCEPTED_FOR_BIDDING = "accepted_for_bidding"
    STOPPED = "stopped"
    REJECTED = "rejected"
    SUSPENDED = "suspended"
    CALCULATED = "calculated"


class AlpacaOrderType(enum.Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class AlpacaSide(enum.Enum):
    BUY = "buy"
    SELL = "sell"


class AlpacaTimeInForce(enum.Enum):
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    OPG = "opg"
    CLS = "cls"


class AlpacaExitMethod(enum.Enum):
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    TRAILING_STOP = "trailing_stop"
    WEBHOOK_SIGNAL = "webhook_signal"
    MANUAL = "manual"
    EXTERNAL = "external"
    OCO_STOP = "oco_stop"
    OCO_TAKE_PROFIT = "oco_take_profit"


class AlpacaPositionStatus(enum.Enum):
    OPEN = "open"
    CLOSED = "closed"


class AlpacaLegType(enum.Enum):
    ENTRY = "entry"
    ADD = "add"
    EXIT = "exit"


class AlpacaOCOStatus(enum.Enum):
    ACTIVE = "active"
    TRIGGERED_STOP = "triggered_stop"
    TRIGGERED_TP = "triggered_tp"
    CANCELLED = "cancelled"
    REPLACED = "replaced"


class AlpacaOrderRole(enum.Enum):
    ENTRY = "entry"
    EXIT_SIGNAL = "exit_signal"
    EXIT_TRAILING = "exit_trailing"
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"


class AlpacaTrade(db.Model):
    __tablename__ = 'alpaca_trade'

    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False)
    side = db.Column(Enum(AlpacaSide), nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=True)
    order_type = db.Column(Enum(AlpacaOrderType), nullable=False)
    time_in_force = db.Column(Enum(AlpacaTimeInForce), nullable=True, default=AlpacaTimeInForce.DAY)
    status = db.Column(Enum(AlpacaOrderStatus), nullable=False, default=AlpacaOrderStatus.PENDING)

    alpaca_order_id = db.Column(db.String(100), nullable=True, index=True)
    client_order_id = db.Column(db.String(100), nullable=True)
    signal_data = db.Column(db.Text, nullable=True)
    alpaca_response = db.Column(db.Text, nullable=True)
    error_message = db.Column(db.Text, nullable=True)

    filled_price = db.Column(db.Float, nullable=True)
    filled_quantity = db.Column(db.Float, nullable=True)

    stop_loss_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)

    extended_hours = db.Column(db.Boolean, nullable=True, default=False)
    is_close_position = db.Column(db.Boolean, nullable=True, default=False)
    reference_price = db.Column(db.Float, nullable=True)

    needs_auto_protection = db.Column(db.Boolean, nullable=True, default=False)
    protection_info = db.Column(db.Text, nullable=True)

    entry_avg_cost = db.Column(db.Float, nullable=True)
    signal_timeframe = db.Column(db.String(10), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AlpacaPosition(db.Model):
    __tablename__ = 'alpaca_position'

    id = db.Column(db.Integer, primary_key=True)

    position_key = db.Column(db.String(100), unique=True, nullable=False, index=True)
    symbol = db.Column(db.String(20), nullable=False, index=True)
    trade_date = db.Column(db.Date, nullable=False)
    sequence_number = db.Column(db.Integer, nullable=False, default=1)

    side = db.Column(db.String(10), nullable=False)
    status = db.Column(Enum(AlpacaPositionStatus), nullable=False, default=AlpacaPositionStatus.OPEN)

    total_entry_quantity = db.Column(db.Float, nullable=False, default=0)
    total_exit_quantity = db.Column(db.Float, nullable=False, default=0)
    avg_entry_price = db.Column(db.Float, nullable=True)
    avg_exit_price = db.Column(db.Float, nullable=True)

    realized_pnl = db.Column(db.Float, nullable=True)
    commission = db.Column(db.Float, nullable=True)

    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('alpaca_trailing_stop_position.id'), nullable=True)

    opened_at = db.Column(db.DateTime, nullable=True)
    closed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    legs = db.relationship('AlpacaPositionLeg', backref='position', lazy='dynamic',
                           order_by='AlpacaPositionLeg.filled_at.asc()')

    __table_args__ = (
        db.UniqueConstraint('symbol', 'trade_date', 'sequence_number',
                            name='uq_alpaca_position_identity'),
    )

    @property
    def remaining_quantity(self):
        return self.total_entry_quantity - self.total_exit_quantity

    @property
    def entry_legs(self):
        return AlpacaPositionLeg.query.filter(
            AlpacaPositionLeg.position_id == self.id,
            AlpacaPositionLeg.leg_type.in_([AlpacaLegType.ENTRY, AlpacaLegType.ADD])
        ).order_by(AlpacaPositionLeg.filled_at.asc()).all()

    @property
    def exit_legs(self):
        return AlpacaPositionLeg.query.filter_by(
            position_id=self.id, leg_type=AlpacaLegType.EXIT
        ).order_by(AlpacaPositionLeg.filled_at.asc()).all()

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


class AlpacaPositionLeg(db.Model):
    __tablename__ = 'alpaca_position_leg'

    id = db.Column(db.Integer, primary_key=True)

    position_id = db.Column(db.Integer, db.ForeignKey('alpaca_position.id'), nullable=False, index=True)
    leg_type = db.Column(Enum(AlpacaLegType), nullable=False)

    alpaca_order_id = db.Column(db.String(100), nullable=True, index=True)
    price = db.Column(db.Float, nullable=True)
    quantity = db.Column(db.Float, nullable=True)
    filled_at = db.Column(db.DateTime, nullable=True)

    trade_id = db.Column(db.Integer, db.ForeignKey('alpaca_trade.id'), nullable=True)

    signal_content = db.Column(db.Text, nullable=True)
    signal_grade = db.Column(db.String(5), nullable=True)
    signal_score = db.Column(db.Integer, nullable=True)
    signal_indicator = db.Column(db.Text, nullable=True)
    signal_timeframe = db.Column(db.String(10), nullable=True)

    oco_group_id = db.Column(db.Integer, db.ForeignKey('alpaca_oco_group.id'), nullable=True)
    stop_order_id = db.Column(db.String(100), nullable=True)
    take_profit_order_id = db.Column(db.String(100), nullable=True)
    stop_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)

    exit_method = db.Column(Enum(AlpacaExitMethod), nullable=True)
    realized_pnl = db.Column(db.Float, nullable=True)
    commission = db.Column(db.Float, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    trade = db.relationship('AlpacaTrade', backref='position_legs', foreign_keys=[trade_id])
    oco_group = db.relationship('AlpacaOCOGroup', backref='position_legs', foreign_keys=[oco_group_id])


class AlpacaOCOGroup(db.Model):
    __tablename__ = 'alpaca_oco_group'

    id = db.Column(db.Integer, primary_key=True)

    oco_order_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    symbol = db.Column(db.String(20), nullable=False)

    side = db.Column(db.String(10), nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    entry_price = db.Column(db.Float, nullable=True)

    stop_order_id = db.Column(db.String(100), nullable=True)
    take_profit_order_id = db.Column(db.String(100), nullable=True)

    stop_price = db.Column(db.Float, nullable=True)
    stop_limit_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)

    time_in_force = db.Column(db.String(10), nullable=False, default='gtc')
    extended_hours = db.Column(db.Boolean, default=False)

    status = db.Column(Enum(AlpacaOCOStatus), nullable=False, default=AlpacaOCOStatus.ACTIVE)
    triggered_order_id = db.Column(db.String(100), nullable=True)
    triggered_price = db.Column(db.Float, nullable=True)
    triggered_at = db.Column(db.DateTime, nullable=True)

    trade_id = db.Column(db.Integer, db.ForeignKey('alpaca_trade.id'), nullable=True)
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('alpaca_trailing_stop_position.id'), nullable=True)

    modify_count = db.Column(db.Integer, default=0)
    last_modified_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    trade = db.relationship('AlpacaTrade', backref='oco_groups', foreign_keys=[trade_id])
    trailing_stop = db.relationship('AlpacaTrailingStopPosition', backref='oco_groups', foreign_keys=[trailing_stop_id])


class AlpacaOrderTracker(db.Model):
    __tablename__ = 'alpaca_order_tracker'

    id = db.Column(db.Integer, primary_key=True)

    alpaca_order_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    client_order_id = db.Column(db.String(100), nullable=True)
    parent_order_id = db.Column(db.String(100), nullable=True)

    symbol = db.Column(db.String(20), nullable=False)
    role = db.Column(Enum(AlpacaOrderRole), nullable=False)

    side = db.Column(db.String(10), nullable=True)
    quantity = db.Column(db.Float, nullable=True)
    order_type = db.Column(db.String(20), nullable=True)
    limit_price = db.Column(db.Float, nullable=True)
    stop_price = db.Column(db.Float, nullable=True)

    status = db.Column(db.String(20), nullable=False, default='PENDING')
    filled_quantity = db.Column(db.Float, nullable=True)
    avg_fill_price = db.Column(db.Float, nullable=True)
    realized_pnl = db.Column(db.Float, nullable=True)
    commission = db.Column(db.Float, nullable=True)
    fill_time = db.Column(db.DateTime, nullable=True)

    oco_group_id = db.Column(db.Integer, db.ForeignKey('alpaca_oco_group.id'), nullable=True)
    leg_role = db.Column(db.String(20), nullable=True)

    signal_content = db.Column(db.Text, nullable=True)

    trade_id = db.Column(db.Integer, db.ForeignKey('alpaca_trade.id'), nullable=True)
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('alpaca_trailing_stop_position.id'), nullable=True)
    position_id = db.Column(db.Integer, db.ForeignKey('alpaca_position.id'), nullable=True)

    fill_source = db.Column(db.String(30), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    oco_group = db.relationship('AlpacaOCOGroup', backref='order_trackers', foreign_keys=[oco_group_id])


class AlpacaTrailingStopPosition(db.Model):
    __tablename__ = 'alpaca_trailing_stop_position'

    id = db.Column(db.Integer, primary_key=True)

    symbol = db.Column(db.String(20), nullable=False)
    side = db.Column(db.String(10), nullable=False, default='long')

    entry_price = db.Column(db.Float, nullable=False)
    first_entry_price = db.Column(db.Float, nullable=True)
    quantity = db.Column(db.Float, nullable=False)
    current_price = db.Column(db.Float, nullable=True)
    highest_price = db.Column(db.Float, nullable=True)
    lowest_price = db.Column(db.Float, nullable=True)

    stop_loss_price = db.Column(db.Float, nullable=True)
    signal_stop_loss = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)
    trailing_stop_price = db.Column(db.Float, nullable=True)

    is_dynamic = db.Column(db.Boolean, default=False)
    phase = db.Column(db.String(30), default='progressive')
    atr_value = db.Column(db.Float, nullable=True)
    trend_score = db.Column(db.Float, nullable=True)

    is_active = db.Column(db.Boolean, default=True)
    is_triggered = db.Column(db.Boolean, default=False)
    triggered_at = db.Column(db.DateTime, nullable=True)
    triggered_price = db.Column(db.Float, nullable=True)
    trigger_reason = db.Column(db.String(500), nullable=True)
    trigger_retry_count = db.Column(db.Integer, default=0)

    timeframe = db.Column(db.String(10), nullable=True)

    trade_id = db.Column(db.Integer, db.ForeignKey('alpaca_trade.id'), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AlpacaTrailingStopConfig(db.Model):
    __tablename__ = 'alpaca_trailing_stop_config'

    id = db.Column(db.Integer, primary_key=True)

    atr_multiplier = db.Column(db.Float, default=1.2)
    atr_period = db.Column(db.Integer, default=14)
    trail_pct = db.Column(db.Float, default=0.015)
    initial_stop_pct = db.Column(db.Float, default=0.03)
    dynamic_switch_profit_pct = db.Column(db.Float, default=0.05)
    tighten_threshold = db.Column(db.Float, default=0.02)
    tighten_atr_multiplier = db.Column(db.Float, default=0.6)
    tighten_trail_pct = db.Column(db.Float, default=0.005)
    check_interval_seconds = db.Column(db.Integer, default=5)
    is_enabled = db.Column(db.Boolean, default=True)

    trend_strength_threshold = db.Column(db.Float, default=60.0)
    switch_profit_threshold = db.Column(db.Float, default=0.05)
    switch_force_profit = db.Column(db.Float, default=0.10)
    switch_profit_ratio = db.Column(db.Float, default=0.90)
    switch_profit_ratio_strong = db.Column(db.Float, default=0.85)
    momentum_lookback = db.Column(db.Integer, default=10)
    atr_convergence_weight = db.Column(db.Float, default=0.3)
    momentum_weight = db.Column(db.Float, default=0.4)
    consecutive_weight = db.Column(db.Float, default=0.3)

    cost_distance_threshold = db.Column(db.Float, default=0.02)
    cost_tighten_atr_multiplier = db.Column(db.Float, default=0.6)
    cost_tighten_trail_pct = db.Column(db.Float, default=0.005)

    inverse_protection_enabled = db.Column(db.Boolean, default=True)
    inverse_trigger_ratio = db.Column(db.Float, default=0.50)

    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AlpacaTrailingStopLog(db.Model):
    __tablename__ = 'alpaca_trailing_stop_log'

    id = db.Column(db.Integer, primary_key=True)
    trailing_stop_id = db.Column(db.Integer, db.ForeignKey('alpaca_trailing_stop_position.id'), nullable=False)

    event_type = db.Column(db.String(50), nullable=False)
    current_price = db.Column(db.Float, nullable=True)
    highest_price = db.Column(db.Float, nullable=True)
    trailing_stop_price = db.Column(db.Float, nullable=True)
    atr_value = db.Column(db.Float, nullable=True)
    profit_pct = db.Column(db.Float, nullable=True)
    details = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    trailing_stop = db.relationship('AlpacaTrailingStopPosition', backref='logs', foreign_keys=[trailing_stop_id])


class AlpacaSignalLog(db.Model):
    __tablename__ = 'alpaca_signal_log'

    id = db.Column(db.Integer, primary_key=True)

    source = db.Column(db.String(50), nullable=True, default='tradingview')
    raw_data = db.Column(db.Text, nullable=True)
    parsed_data = db.Column(db.Text, nullable=True)
    symbol = db.Column(db.String(20), nullable=True)
    action = db.Column(db.String(20), nullable=True)
    status = db.Column(db.String(20), nullable=True, default='received')
    error_message = db.Column(db.Text, nullable=True)

    trade_id = db.Column(db.Integer, db.ForeignKey('alpaca_trade.id'), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class AlpacaEntrySignalRecord(db.Model):
    __tablename__ = 'alpaca_entry_signal_record'

    id = db.Column(db.Integer, primary_key=True)

    symbol = db.Column(db.String(20), nullable=False)
    position_key = db.Column(db.String(100), nullable=True)
    entry_time = db.Column(db.DateTime, nullable=True)
    entry_price = db.Column(db.Float, nullable=True)
    quantity = db.Column(db.Float, nullable=True)
    side = db.Column(db.String(10), nullable=False)
    is_scaling = db.Column(db.Boolean, default=False)

    entry_order_id = db.Column(db.String(100), nullable=True)
    signal_log_id = db.Column(db.Integer, db.ForeignKey('alpaca_signal_log.id'), nullable=True)
    raw_json = db.Column(db.Text, nullable=True)

    indicator_trigger = db.Column(db.Text, nullable=True)
    signal_grade = db.Column(db.String(5), nullable=True)
    signal_score = db.Column(db.Integer, nullable=True)
    htf_grade = db.Column(db.String(5), nullable=True)
    htf_score = db.Column(db.Integer, nullable=True)
    timeframe = db.Column(db.String(10), nullable=True)

    signal_stop_loss = db.Column(db.Float, nullable=True)
    signal_take_profit = db.Column(db.Float, nullable=True)

    stop_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)

    exit_price = db.Column(db.Float, nullable=True)
    exit_time = db.Column(db.DateTime, nullable=True)
    exit_method = db.Column(db.String(50), nullable=True)
    hold_duration_seconds = db.Column(db.Float, nullable=True)

    contribution_pnl = db.Column(db.Float, nullable=True)
    contribution_pct = db.Column(db.Float, nullable=True)

    position_id = db.Column(db.Integer, db.ForeignKey('alpaca_position.id'), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class AlpacaFilledOrder(db.Model):
    __tablename__ = 'alpaca_filled_order'

    id = db.Column(db.Integer, primary_key=True)

    alpaca_order_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    client_order_id = db.Column(db.String(100), nullable=True)
    symbol = db.Column(db.String(20), nullable=False)
    side = db.Column(db.String(10), nullable=False)

    quantity = db.Column(db.Float, nullable=True)
    filled_qty = db.Column(db.Float, nullable=True)
    filled_avg_price = db.Column(db.Float, nullable=True)
    limit_price = db.Column(db.Float, nullable=True)
    stop_price = db.Column(db.Float, nullable=True)

    order_type = db.Column(db.String(30), nullable=True)
    order_class = db.Column(db.String(30), nullable=True)
    time_in_force = db.Column(db.String(10), nullable=True)
    extended_hours = db.Column(db.Boolean, nullable=True)

    status = db.Column(db.String(30), nullable=True)
    submitted_at = db.Column(db.String(50), nullable=True)
    filled_at = db.Column(db.String(50), nullable=True)

    raw_json = db.Column(db.Text, nullable=True)

    reconciled = db.Column(db.Boolean, default=False)
    reconciled_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class AlpacaHolding(db.Model):
    __tablename__ = 'alpaca_holding'

    id = db.Column(db.Integer, primary_key=True)

    symbol = db.Column(db.String(20), nullable=False, unique=True, index=True)
    quantity = db.Column(db.Float, nullable=False, default=0)
    average_cost = db.Column(db.Float, nullable=True)
    market_value = db.Column(db.Float, nullable=True)
    cost_basis = db.Column(db.Float, nullable=True)
    unrealized_pnl = db.Column(db.Float, nullable=True)
    unrealized_pnl_pct = db.Column(db.Float, nullable=True)
    current_price = db.Column(db.Float, nullable=True)
    lastday_price = db.Column(db.Float, nullable=True)
    change_today = db.Column(db.Float, nullable=True)
    asset_class = db.Column(db.String(20), nullable=True)
    exchange = db.Column(db.String(20), nullable=True)
    side = db.Column(db.String(10), nullable=True)

    synced_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class AlpacaTradingConfig(db.Model):
    __tablename__ = 'alpaca_trading_config'

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False, index=True)
    value = db.Column(db.Text, nullable=True)
    description = db.Column(db.Text, nullable=True)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AlpacaReconciliationRun(db.Model):
    __tablename__ = 'alpaca_reconciliation_run'

    id = db.Column(db.Integer, primary_key=True)

    run_date = db.Column(db.Date, nullable=False)
    run_type = db.Column(db.String(20), nullable=False, default='scheduled')

    status = db.Column(db.String(20), nullable=False, default='running')

    total_activities_fetched = db.Column(db.Integer, default=0)
    new_fills_stored = db.Column(db.Integer, default=0)
    positions_matched = db.Column(db.Integer, default=0)
    records_corrected = db.Column(db.Integer, default=0)
    records_created = db.Column(db.Integer, default=0)

    details = db.Column(db.Text, nullable=True)
    error_message = db.Column(db.Text, nullable=True)

    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime, nullable=True)


class AlpacaAlignmentRun(db.Model):
    __tablename__ = 'alpaca_alignment_run'

    id = db.Column(db.Integer, primary_key=True)

    alignment_date = db.Column(db.Date, nullable=False, index=True)
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


class AlpacaSystemLog(db.Model):
    __tablename__ = 'alpaca_system_log'

    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    level = db.Column(db.String(10), nullable=False, index=True)
    source = db.Column(db.String(50), nullable=True, index=True)
    category = db.Column(db.String(30), nullable=True, index=True)
    message = db.Column(db.Text, nullable=False)
    symbol = db.Column(db.String(20), nullable=True, index=True)
    extra_data = db.Column(db.Text, nullable=True)
