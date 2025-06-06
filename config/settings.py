"""
Configuration settings for the mean reversion trading strategy.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from tsxapipy.api.client import APIClient

class PositionState(Enum):
    """Position states"""
    FLAT = 0
    LONG = 1
    SHORT = -1

class OrderState(Enum):
    """Order status tracking"""
    NONE = "none"
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"

class ExitReason(Enum):
    """Exit reason tracking"""
    TARGET_HIT = "TARGET_HIT"
    STOP_LOSS = "STOP_LOSS"
    TRAIL_STOP = "TRAIL_STOP"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    TIMEOUT = "TIMEOUT"
    MANUAL_FLATTEN = "MANUAL_FLATTEN"

@dataclass
class StrategyConfig:
    """Strategy configuration parameters"""
    # SEB Parameters
    sample_period: int = 21
    standard_errors: float = 2.0
    averaging_periods: int = 3
    band_threshold: float = 0.2

    # Risk Management
    stop_loss_distance: float = 3.5
    trail_stop_distance: float = 2.0
    trail_activation_distance: float = 2.0
    max_position_time_minutes: int = 120

    # Position Sizing
    initial_position_size: int = 3
    partial_exit_size: int = 2
    partial_exit_at_mean: bool = True

    # Order Management
    entry_order_cooldown_seconds: int = 30
    order_update_threshold: float = 0.25
    exit_order_update_threshold: float = 0.25

    # Daily Limits
    max_daily_trades: int = 10
    max_daily_loss: float = 1000.0

    # Market Data
    tick_size: float = 0.25
    tick_value: float = 12.50
    candle_interval_minutes: int = 2

    # Logging
    log_trades: bool = True
    log_file: str = 'logs/trades.csv'

    def validate(self):
        """Validate configuration parameters"""
        if self.sample_period < 5:
            raise ValueError("Window size must be at least 5")
        if self.standard_errors <= 0:
            raise ValueError("Deviation multiplier must be positive")
        if self.tick_size <= 0:
            raise ValueError("Tick size must be positive")
        if self.tick_value <= 0:
            raise ValueError("Tick value must be positive")

    @classmethod
    def create_futures_config(cls, contract_type: str = 'ES') -> 'StrategyConfig':
        """Create configuration for futures contracts"""
        configs = {
            'ES': {
                'band_threshold': 0.5,
                'stop_loss_distance': 3.5,
                'trail_stop_distance': 2.0,
                'initial_position_size': 3,
                'tick_value': 12.50
            },
            'NQ': {
                'band_threshold': 1.0,
                'stop_loss_distance': 7.0,
                'trail_stop_distance': 4.0,
                'initial_position_size': 2,
                'tick_value': 5.00
            }
        }
        
        config = configs.get(contract_type.upper(), configs['ES'])
        
        return cls(
            sample_period=21,
            standard_errors=2.0,
            averaging_periods=3,
            band_threshold=config['band_threshold'],
            stop_loss_distance=config['stop_loss_distance'],
            trail_stop_distance=config['trail_stop_distance'],
            initial_position_size=config['initial_position_size'],
            tick_size=0.25,
            tick_value=config['tick_value'],
            candle_interval_minutes=2
        )

@dataclass
class TradingConfig:
    """Trading-specific configuration"""
    api_client: Optional['APIClient'] = None
    account_id: Optional[str] = None
    contract_id: Optional[str] = None
    live_trading: bool = False
    environment: str = "DEMO"

    def validate(self):
        """Validate trading configuration"""
        if self.live_trading:
            if not self.api_client:
                raise ValueError("API client required for live trading")
            if not self.account_id:
                raise ValueError("Account ID required for live trading")
            if not self.contract_id:
                raise ValueError("Contract ID required for live trading")