"""
Trade logging utility for CSV output.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from config.settings import StrategyConfig
from core.position import TradingState
from utils.price_utils import PriceUtils


class TradeLogger:
    """Trade-specific logging for CSV output"""

    def __init__(self, log_file: str = 'logs/trades.csv'):
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self._initialize_csv_headers()

    def _initialize_csv_headers(self):
        """Initialize CSV file with headers if it doesn't exist"""
        if not self.log_file.exists():
            headers = [
                'exit_time', 'side', 'entry_price', 'exit_price', 'size',
                'duration_minutes', 'pnl_points', 'pnl_dollars', 'exit_reason',
                'max_favorable', 'max_adverse', 'stop_loss', 'trail_activated',
                'win_rate', 'daily_trades', 'daily_pnl'
            ]

            with open(self.log_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)

    def log_trade(self, state: TradingState, exit_price: float, 
                  exit_reason: str, config: StrategyConfig):
        """Log a completed trade to CSV"""
        try:
            if not state.entry_time:
                self.logger.warning("Cannot log trade - no entry time")
                return

            exit_time = datetime.now()
            duration = (exit_time - state.entry_time).total_seconds() / 60

            # Calculate P&L
            pnl_points = PriceUtils.calculate_pnl_points(
                entry_price=state.entry_price,
                exit_price=exit_price,
                size=state.position_size,
                is_long=state.is_long()
            )

            pnl_dollars = PriceUtils.points_to_dollars(
                pnl_points, config.tick_size, config.tick_value
            )

            # Prepare row data
            row = [
                exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                state.position_side.name,
                f"{state.entry_price:.2f}",
                f"{exit_price:.2f}",
                state.position_size,
                f"{duration:.1f}",
                f"{pnl_points:.2f}",
                f"{pnl_dollars:.2f}",
                exit_reason,
                f"{state.max_favorable:.2f}",
                f"{state.max_adverse:.2f}",
                f"{state.stop_loss:.2f}" if state.stop_loss else "0",
                "Yes" if state.trail_activated else "No",
                f"{state.get_win_rate():.1f}",
                state.daily_trades,
                f"{state.daily_pnl:.2f}"
            ]

            # Write to CSV
            with open(self.log_file, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(row)

            self.logger.debug(f"Trade logged: {exit_reason}, P&L: {pnl_points:.2f}pts")

        except Exception as e:
            self.logger.error(f"Failed to log trade: {e}")

    def get_log_file_path(self) -> str:
        """Get the full path to the log file"""
        return str(self.log_file.absolute())