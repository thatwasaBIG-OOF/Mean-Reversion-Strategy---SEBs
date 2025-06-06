"""
Simplified position and order tracking.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
from config.settings import PositionState, OrderState, ExitReason

@dataclass
class OrderInfo:
    """Simplified order tracking"""
    order_id: Optional[str] = None
    side: str = ""
    size: int = 0
    price: float = 0.0
    state: OrderState = OrderState.NONE
    is_stop: bool = False  # True for stop orders, False for limit orders

    def is_pending(self) -> bool:
        return self.state == OrderState.PENDING

    def is_filled(self) -> bool:
        return self.state == OrderState.FILLED

    def reset(self):
        self.order_id = None
        self.side = ""
        self.size = 0
        self.price = 0.0
        self.state = OrderState.NONE
        self.is_stop = False

@dataclass
class TradingState:
    """Unified state management for position, orders, and metrics"""
    
    # Position info
    position_side: PositionState = PositionState.FLAT
    position_size: int = 0
    entry_price: float = 0.0
    entry_time: Optional[datetime] = None
    
    # Risk management
    stop_loss: Optional[float] = None
    trail_stop: Optional[float] = None
    trail_activated: bool = False
    best_price: float = 0.0
    
    # Performance tracking
    max_favorable: float = 0.0
    max_adverse: float = 0.0
    
    # Order tracking
    entry_order: OrderInfo = field(default_factory=OrderInfo)
    partial_exit_order: OrderInfo = field(default_factory=OrderInfo)
    full_exit_order: OrderInfo = field(default_factory=OrderInfo)
    
    # Exit tracking
    last_exit_time: Optional[datetime] = None
    last_exit_reason: Optional[ExitReason] = None
    last_exit_side: Optional[PositionState] = None
    last_exit_price: float = 0.0
    
    # Trade metrics
    total_trades: int = 0
    winning_trades: int = 0
    daily_trades: int = 0
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    consecutive_losses: int = 0
    max_consecutive_losses: int = 0

    def is_flat(self) -> bool:
        return self.position_side == PositionState.FLAT

    def is_long(self) -> bool:
        return self.position_side == PositionState.LONG

    def is_short(self) -> bool:
        return self.position_side == PositionState.SHORT

    def has_pending_orders(self) -> bool:
        """Check if any orders are pending"""
        return (self.entry_order.is_pending() or 
                self.partial_exit_order.is_pending() or 
                self.full_exit_order.is_pending())

    def reset_position(self):
        """Reset position-related state"""
        self.position_side = PositionState.FLAT
        self.position_size = 0
        self.entry_price = 0.0
        self.entry_time = None
        self.stop_loss = None
        self.trail_stop = None
        self.trail_activated = False
        self.best_price = 0.0
        self.max_favorable = 0.0
        self.max_adverse = 0.0
        
    def reset_orders(self):
        """Reset all order states"""
        self.entry_order.reset()
        self.partial_exit_order.reset()
        self.full_exit_order.reset()

    def reset_all(self):
        """Complete state reset"""
        self.reset_position()
        self.reset_orders()

    def update_exit_info(self, reason: ExitReason, side: PositionState, price: float):
        """Update exit tracking info"""
        self.last_exit_time = datetime.now()
        self.last_exit_reason = reason
        self.last_exit_side = side
        self.last_exit_price = price

    def is_in_exit_cooldown(self, cooldown_seconds: int) -> bool:
        """Check if we're in exit cooldown period"""
        if not self.last_exit_time:
            return False
        
        time_since_exit = (datetime.now() - self.last_exit_time).total_seconds()
        return time_since_exit < cooldown_seconds

    def should_block_entry(self, signal: str, cooldown_seconds: int) -> bool:
        """Check if entry should be blocked due to recent exit"""
        if not self.is_in_exit_cooldown(cooldown_seconds):
            return False
        
        # Block opposite signals after target hits
        if self.last_exit_reason == ExitReason.TARGET_HIT:
            if signal == 'SELL' and self.last_exit_side == PositionState.LONG:
                return True
            if signal == 'BUY' and self.last_exit_side == PositionState.SHORT:
                return True
        
        return False

    def update_trade_metrics(self, pnl_points: float):
        """Update trading metrics after a trade"""
        self.total_trades += 1
        self.daily_trades += 1
        self.total_pnl += pnl_points
        self.daily_pnl += pnl_points
        
        if pnl_points > 0:
            self.winning_trades += 1
            self.largest_win = max(self.largest_win, pnl_points)
            self.consecutive_losses = 0
        else:
            self.largest_loss = min(self.largest_loss, pnl_points)
            self.consecutive_losses += 1
            self.max_consecutive_losses = max(
                self.max_consecutive_losses, self.consecutive_losses
            )

    def get_win_rate(self) -> float:
        """Calculate win rate percentage"""
        if self.total_trades == 0:
            return 0.0
        return (self.winning_trades / self.total_trades) * 100

    def reset_daily_metrics(self):
        """Reset daily metrics"""
        self.daily_trades = 0
        self.daily_pnl = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary for status reporting"""
        return {
            'position_side': self.position_side.name,
            'position_size': self.position_size,
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'trail_stop': self.trail_stop,
            'trail_activated': self.trail_activated,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'win_rate': self.get_win_rate(),
            'total_pnl': self.total_pnl,
            'daily_pnl': self.daily_pnl,
            'largest_win': self.largest_win,
            'largest_loss': self.largest_loss,
            'consecutive_losses': self.consecutive_losses,
            'in_cooldown': self.is_in_exit_cooldown(30),
            'has_pending_orders': self.has_pending_orders()
        }