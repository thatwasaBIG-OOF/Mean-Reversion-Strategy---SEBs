"""
Mean reversion trading strategy with Standard Error Bands (SEB).
Refactored for simplicity and performance.
"""

import logging
import pytz
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from config.settings import StrategyConfig, TradingConfig, PositionState, OrderState, ExitReason
from core.position import TradingState, OrderInfo
from core.indicators import SEBCalculator
from trading.order_manager import OrderManager
from utils.price_utils import PriceUtils
from utils.trade_logger import TradeLogger

# Import TSX API constants
try:
    from tsxapipy.trading import (
        ORDER_STATUS_FILLED,
        ORDER_STATUS_CANCELLED,
        ORDER_STATUS_REJECTED,
        ORDER_STATUS_WORKING,
        ORDER_STATUS_TO_STRING_MAP
    )
except ImportError:
    # Define fallback constants
    ORDER_STATUS_FILLED = 1
    ORDER_STATUS_CANCELLED = 2
    ORDER_STATUS_REJECTED = 3
    ORDER_STATUS_WORKING = 4
    ORDER_STATUS_TO_STRING_MAP = {
        1: "FILLED",
        2: "CANCELLED",
        3: "REJECTED",
        4: "WORKING"
    }


class MeanReversionStrategy:
    """Simplified mean reversion trading strategy"""

    def __init__(self, strategy_config: StrategyConfig, trading_config: TradingConfig):
        # Validate configurations
        strategy_config.validate()
        trading_config.validate()

        self.config = strategy_config
        self.trading_config = trading_config
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.seb_calculator = SEBCalculator(
            strategy_config.sample_period,
            strategy_config.standard_errors,
            strategy_config.averaging_periods
        )

        self.order_manager = OrderManager(
            api_client=trading_config.api_client,
            account_id=trading_config.account_id,
            contract_id=trading_config.contract_id,
            live_trading=trading_config.live_trading
        )

        self.trade_logger = TradeLogger(strategy_config.log_file)

        # State management - simplified
        self.state = TradingState()
        self.candle_history: List[float] = []
        self.current_candle: Optional[Dict[str, Any]] = None
        self.seb_bands: Dict[str, float] = {}
        self.current_price: float = 0.0

        # Order status tracking for UserHubStream
        self._order_status_cache: Dict[str, int] = {}

        # Strategy control
        self._last_signal: Optional[str] = None
        self._tick_count = 0
        self.eastern = pytz.timezone('US/Eastern')

        # Initialize with historical data
        self._initialize_historical_data()

        mode = "LIVE" if trading_config.live_trading else "SIMULATION"
        self.logger.info(f"Mean Reversion Strategy initialized in {mode} mode")

    def _initialize_historical_data(self):
        """Load historical data to initialize SEB"""
        if not self.trading_config.api_client or not self.trading_config.contract_id:
            self.logger.info("No API client/contract - using simulation mode")
            return

        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=3)

            bars = self.trading_config.api_client.get_historical_bars(
                contract_id=self.trading_config.contract_id,
                unit=2,  # Minutes
                unit_number=self.config.candle_interval_minutes,
                start_time_iso=start_time.isoformat(),
                end_time_iso=end_time.isoformat(),
                limit=self.config.sample_period + 10
            )

            if bars and bars.bars:
                closes = [bar.c for bar in bars.bars if bar.c is not None]
                if len(closes) >= self.config.sample_period:
                    self.candle_history = closes[-self.config.sample_period:]
                    self.seb_bands = self.seb_calculator.calculate(self.candle_history) or {}
                    if closes:
                        self.current_price = closes[-1]
                    self.logger.info("SEB initialized with historical data")

        except Exception as e:
            self.logger.error(f"Error initializing historical data: {e}")

    def process_tick(self, price_data: Dict[str, Any]):
        """Process incoming market tick"""
        try:
            # Extract and validate price
            price = float(price_data.get('price', 0))
            if not PriceUtils.validate_price(price):
                return

            self.current_price = price
            self._tick_count += 1

            # Update candle data
            self._update_candle_data(price)

            # Check for fills first (most important)
            if not self.trading_config.live_trading:
                self._check_for_fills()

            # Process strategy logic
            if self.seb_bands:
                if self.state.is_flat() and not self.state.has_pending_orders():
                    self._check_entry_signals(price)
                elif not self.state.is_flat():
                    self._update_position_tracking(price)
                    self._check_exit_conditions(price)

        except Exception as e:
            self.logger.error(f"Error processing tick: {e}")

    def _update_candle_data(self, price: float):
        """Update candle data and recalculate SEB"""
        now = datetime.now(tz=self.eastern)
        candle_time = now.replace(second=0, microsecond=0)
        interval = self.config.candle_interval_minutes
        minute = candle_time.minute - (candle_time.minute % interval)
        start_time = candle_time.replace(minute=minute)

        # Update or create candle
        if self.current_candle and self.current_candle['start_time'] == start_time:
            # Update existing candle
            self.current_candle['high'] = max(self.current_candle['high'], price)
            self.current_candle['low'] = min(self.current_candle['low'], price)
            self.current_candle['close'] = price
        else:
            # Complete previous candle
            if self.current_candle:
                self.candle_history.append(self.current_candle['close'])
                if len(self.candle_history) > self.config.sample_period:
                    self.candle_history.pop(0)

                # Recalculate SEB
                new_bands = self.seb_calculator.calculate(self.candle_history)
                if new_bands:
                    self.seb_bands = new_bands
                    self._check_order_updates()

            # Start new candle
            self.current_candle = {
                'start_time': start_time,
                'open': price,
                'high': price,
                'low': price,
                'close': price
            }

    def _check_entry_signals(self, price: float):
        """Check for entry signals"""
        if not self.seb_bands:
            return

        # Check if in exit cooldown
        if self.state.should_block_entry(self._last_signal, self.config.entry_order_cooldown_seconds):
            return

        # Get signal
        signal = self.seb_calculator.get_signal(price, self.seb_bands, self.config.band_threshold)

        if signal and signal != self._last_signal:
            if signal == 'BUY':
                stop_price = PriceUtils.round_to_tick(
                    self.seb_bands['lower_band'], self.config.tick_size
                )
                self._place_entry_order('BUY', stop_price)
            elif signal == 'SELL':
                stop_price = PriceUtils.round_to_tick(
                    self.seb_bands['upper_band'], self.config.tick_size
                )
                self._place_entry_order('SELL', stop_price)

            self._last_signal = signal
        elif not signal:
            self._last_signal = None

    def _place_entry_order(self, side: str, price: float):
        """Place entry order"""
        order_id = self.order_manager.place_stop_market_order(
            side=side,
            size=self.config.initial_position_size,
            price=price
        )

        if order_id:
            self.state.entry_order = self.order_manager.create_order_info(
                side=side,
                size=self.config.initial_position_size,
                price=price,
                order_id=order_id,
                is_stop=True
            )
            self.logger.info(f"Entry order placed: {side} STOP Market @ {price:.2f}")

            # Simulate immediate fill in sim mode
            if not self.trading_config.live_trading:
                self._handle_entry_fill(side, price)

    def _handle_entry_fill(self, side: str, fill_price: float):
        """Handle entry order fill"""
        # Set position
        self.state.position_side = PositionState.LONG if side == 'BUY' else PositionState.SHORT
        self.state.position_size = self.config.initial_position_size
        self.state.entry_price = fill_price
        self.state.entry_time = datetime.now(tz=self.eastern)
        self.state.best_price = fill_price

        # Set stop loss
        self.state.stop_loss = PriceUtils.calculate_stop_price(
            entry_price=fill_price,
            distance=self.config.stop_loss_distance,
            is_long=self.state.is_long(),
            tick_size=self.config.tick_size
        )

        # Mark entry order as filled
        self.state.entry_order.state = OrderState.FILLED

        # Place exit orders
        self._place_exit_orders()

        self.logger.info(
            f"Position opened: {side} {self.config.initial_position_size} @ {fill_price:.2f}, "
            f"Stop: {self.state.stop_loss:.2f}"
        )

    def _place_exit_orders(self):
        """Place exit orders"""
        if not self.seb_bands or self.state.is_flat():
            return

        exit_side = 'SELL' if self.state.is_long() else 'BUY'

        # Calculate target prices
        mean_price = PriceUtils.round_to_tick(self.seb_bands['mean'], self.config.tick_size)
        target_price = PriceUtils.round_to_tick(
            self.seb_bands['upper_band'] if self.state.is_long() else self.seb_bands['lower_band'],
            self.config.tick_size
        )

        # Place partial exit at mean
        if self.config.partial_exit_at_mean:
            partial_id = self.order_manager.place_limit_order(
                side=exit_side,
                size=self.config.partial_exit_size,
                price=mean_price
            )

            if partial_id:
                self.state.partial_exit_order = self.order_manager.create_order_info(
                    side=exit_side,
                    size=self.config.partial_exit_size,
                    price=mean_price,
                    order_id=partial_id,
                    is_stop=False
                )

        # Place full exit at target
        remaining_size = self.state.position_size
        if self.config.partial_exit_at_mean:
            remaining_size -= self.config.partial_exit_size

        full_id = self.order_manager.place_limit_order(
            side=exit_side,
            size=remaining_size,
            price=target_price
        )

        if full_id:
            self.state.full_exit_order = self.order_manager.create_order_info(
                side=exit_side,
                size=remaining_size,
                price=target_price,
                order_id=full_id,
                is_stop=False
            )

    def _update_position_tracking(self, price: float):
        """Update position tracking"""
        if self.state.is_flat():
            return

        # Update best price
        if self.state.is_long() and price > self.state.best_price:
            self.state.best_price = price
        elif self.state.is_short() and price < self.state.best_price:
            self.state.best_price = price

        # Calculate excursion
        excursion = PriceUtils.calculate_excursion(
            self.state.entry_price, price, self.state.is_long()
        )

        # Update max favorable/adverse
        if excursion > self.state.max_favorable:
            self.state.max_favorable = excursion
        elif excursion < 0 and abs(excursion) > self.state.max_adverse:
            self.state.max_adverse = abs(excursion)

        # Check trail stop activation
        if not self.state.trail_activated and excursion >= self.config.trail_activation_distance:
            self.state.trail_activated = True
            self.logger.info("Trailing stop activated")

        # Update trailing stop
        if self.state.trail_activated:
            new_trail = PriceUtils.calculate_trail_stop(
                best_price=self.state.best_price,
                trail_distance=self.config.trail_stop_distance,
                is_long=self.state.is_long(),
                tick_size=self.config.tick_size
            )

            if self.state.is_long():
                self.state.trail_stop = max(self.state.trail_stop or 0, new_trail)
            else:
                self.state.trail_stop = min(self.state.trail_stop or float('inf'), new_trail)

    def _check_exit_conditions(self, price: float):
        """Check stop loss and trailing stop conditions"""
        if self.state.is_flat():
            return

        # Check position timeout
        if self.state.entry_time:
            time_in_position = (datetime.now(tz=self.eastern) - self.state.entry_time).total_seconds()
            if time_in_position > self.config.max_position_time_minutes * 60:
                self._emergency_exit(price, ExitReason.TIMEOUT)
                return

        # Check stop loss
        if self.state.stop_loss:
            if (self.state.is_long() and price <= self.state.stop_loss) or \
               (self.state.is_short() and price >= self.state.stop_loss):
                self._emergency_exit(price, ExitReason.STOP_LOSS)
                return

        # Check trailing stop
        if self.state.trail_stop:
            if (self.state.is_long() and price <= self.state.trail_stop) or \
               (self.state.is_short() and price >= self.state.trail_stop):
                self._emergency_exit(price, ExitReason.TRAIL_STOP)
                return

    def _emergency_exit(self, price: float, reason: ExitReason):
        """Emergency exit position"""
        # Cancel pending exit orders
        self._cancel_exit_orders()

        # Place market order
        exit_side = 'SELL' if self.state.is_long() else 'BUY'
        order_id = self.order_manager.place_market_order(
            side=exit_side,
            size=self.state.position_size
        )

        if order_id:
            # Calculate and record P&L
            pnl_points = PriceUtils.calculate_pnl_points(
                entry_price=self.state.entry_price,
                exit_price=price,
                size=self.state.position_size,
                is_long=self.state.is_long()
            )

            self.state.update_trade_metrics(pnl_points)
            self.state.update_exit_info(reason, self.state.position_side, price)

            # Log trade
            if self.config.log_trades:
                self.trade_logger.log_trade(self.state, price, reason.value, self.config)

            # Display result
            self._display_trade_result(price, reason.value, pnl_points)

            # Reset state
            self.state.reset_all()

            self.logger.info(f"Emergency exit: {reason.value} @ {price:.2f}")

    def _check_order_updates(self):
        """Check if orders need price updates due to band changes"""
        if not self.seb_bands:
            return

        # Update entry order
        if self.state.entry_order.is_pending():
            new_price = None
            if self.state.entry_order.side == 'BUY':
                new_price = PriceUtils.round_to_tick(self.seb_bands['lower_band'], self.config.tick_size)
            elif self.state.entry_order.side == 'SELL':
                new_price = PriceUtils.round_to_tick(self.seb_bands['upper_band'], self.config.tick_size)

            if new_price and abs(new_price - self.state.entry_order.price) >= self.config.order_update_threshold:
                self.order_manager.modify_order_price(self.state.entry_order, new_price)

        # Update exit orders
        if not self.state.is_flat():
            # Update partial exit
            if self.state.partial_exit_order.is_pending():
                mean_price = PriceUtils.round_to_tick(self.seb_bands['mean'], self.config.tick_size)
                if abs(mean_price - self.state.partial_exit_order.price) >= self.config.exit_order_update_threshold:
                    self.order_manager.modify_order_price(self.state.partial_exit_order, mean_price)

            # Update full exit
            if self.state.full_exit_order.is_pending():
                target_price = PriceUtils.round_to_tick(
                    self.seb_bands['upper_band'] if self.state.is_long() else self.seb_bands['lower_band'],
                    self.config.tick_size
                )
                if abs(target_price - self.state.full_exit_order.price) >= self.config.exit_order_update_threshold:
                    self.order_manager.modify_order_price(self.state.full_exit_order, target_price)

    def _check_for_fills(self):
        """Check for order fills in simulation mode"""
        if self.trading_config.live_trading:
            return  # Live fills handled by UserHubStream

        # Check entry order
        if self.state.entry_order.is_pending():
            if (self.state.entry_order.side == 'BUY' and self.current_price <= self.state.entry_order.price) or \
               (self.state.entry_order.side == 'SELL' and self.current_price >= self.state.entry_order.price):
                self._handle_entry_fill(self.state.entry_order.side, self.state.entry_order.price)

        # Check exit orders
        if self.state.partial_exit_order.is_pending():
            if (self.state.partial_exit_order.side == 'SELL' and self.current_price >= self.state.partial_exit_order.price) or \
               (self.state.partial_exit_order.side == 'BUY' and self.current_price <= self.state.partial_exit_order.price):
                self._handle_partial_exit_fill()

        if self.state.full_exit_order.is_pending():
            if (self.state.full_exit_order.side == 'SELL' and self.current_price >= self.state.full_exit_order.price) or \
               (self.state.full_exit_order.side == 'BUY' and self.current_price <= self.state.full_exit_order.price):
                self._handle_full_exit_fill()

    def handle_order_update(self, order_update: Dict[str, Any]):
        """Handle order updates from UserHubStream"""
        try:
            order_id = str(order_update.get('id', ''))
            status = order_update.get('status', -1)

            # Update cache
            self._order_status_cache[order_id] = status

            # Check if it's one of our orders
            if order_id == self.state.entry_order.order_id:
                if status == ORDER_STATUS_FILLED:
                    fill_price = order_update.get('avgPx', self.state.entry_order.price)
                    self._handle_entry_fill(self.state.entry_order.side, fill_price)
                elif status in [ORDER_STATUS_CANCELLED, ORDER_STATUS_REJECTED]:
                    self.state.entry_order.reset()

            elif order_id == self.state.partial_exit_order.order_id:
                if status == ORDER_STATUS_FILLED:
                    self._handle_partial_exit_fill()

            elif order_id == self.state.full_exit_order.order_id:
                if status == ORDER_STATUS_FILLED:
                    self._handle_full_exit_fill()

        except Exception as e:
            self.logger.error(f"Error handling order update: {e}")

    def handle_position_update(self, position_update: Dict[str, Any]):
        """Handle position updates from UserHubStream"""
        try:
            contract_id = position_update.get('contractId', '')
            quantity = position_update.get('quantity', 0)

            if str(self.trading_config.contract_id) in str(contract_id):
                # Sync position if needed
                if quantity == 0 and not self.state.is_flat():
                    self.logger.warning("Position sync: Account is flat but strategy has position - resetting")
                    self.state.reset_all()

        except Exception as e:
            self.logger.error(f"Error handling position update: {e}")

    def _handle_partial_exit_fill(self):
        """Handle partial exit fill"""
        self.state.position_size -= self.config.partial_exit_size
        self.state.partial_exit_order.state = OrderState.FILLED
        self.state.partial_exit_order.reset()
        self.logger.info(f"Partial exit filled - {self.state.position_size} contracts remaining")

    def _handle_full_exit_fill(self):
        """Handle full exit fill"""
        # Calculate P&L
        pnl_points = PriceUtils.calculate_pnl_points(
            entry_price=self.state.entry_price,
            exit_price=self.state.full_exit_order.price,
            size=self.state.position_size,
            is_long=self.state.is_long()
        )

        self.state.update_trade_metrics(pnl_points)
        self.state.update_exit_info(ExitReason.TARGET_HIT, self.state.position_side, self.state.full_exit_order.price)

        # Log trade
        if self.config.log_trades:
            self.trade_logger.log_trade(self.state, self.state.full_exit_order.price, ExitReason.TARGET_HIT.value, self.config)

        # Display result
        self._display_trade_result(self.state.full_exit_order.price, ExitReason.TARGET_HIT.value, pnl_points)

        # Reset state
        self.state.reset_all()

    def _cancel_exit_orders(self):
        """Cancel all pending exit orders"""
        if self.state.partial_exit_order.is_pending() and self.state.partial_exit_order.order_id:
            self.order_manager.cancel_order(self.state.partial_exit_order.order_id)
            self.state.partial_exit_order.reset()

        if self.state.full_exit_order.is_pending() and self.state.full_exit_order.order_id:
            self.order_manager.cancel_order(self.state.full_exit_order.order_id)
            self.state.full_exit_order.reset()

    def _display_trade_result(self, exit_price: float, reason: str, pnl_points: float):
        """Display trade result"""
        pnl_dollars = PriceUtils.points_to_dollars(pnl_points, self.config.tick_size, self.config.tick_value)
        emoji = "💰" if pnl_points >= 0 else "💸"

        duration = 0
        if self.state.entry_time:
            duration = (datetime.now(tz=self.eastern) - self.state.entry_time).total_seconds() / 60

        self.logger.info(
            f"{emoji} Exit @ {exit_price:.2f} | {reason} | "
            f"P&L: {pnl_points:+.2f}pts (${pnl_dollars:+.0f}) | "
            f"Duration: {duration:.1f}m | "
            f"Daily: {self.state.daily_pnl:+.2f}pts"
        )

    def flatten_all_positions(self) -> bool:
        """Emergency flatten all positions"""
        try:
            # Cancel all orders
            if self.state.entry_order.is_pending() and self.state.entry_order.order_id:
                self.order_manager.cancel_order(self.state.entry_order.order_id)

            self._cancel_exit_orders()

            # Close position if any
            if not self.state.is_flat():
                self._emergency_exit(self.current_price, ExitReason.MANUAL_FLATTEN)

            # Reset state
            self.state.reset_all()
            return True

        except Exception as e:
            self.logger.error(f"Error flattening positions: {e}")
            self.state.reset_all()
            return False

    def cancel_all_pending_orders(self) -> bool:
        """Cancel all pending orders"""
        success = True

        if self.state.entry_order.is_pending() and self.state.entry_order.order_id:
            if not self.order_manager.cancel_order(self.state.entry_order.order_id):
                success = False
            self.state.entry_order.reset()

        if self.state.partial_exit_order.is_pending() and self.state.partial_exit_order.order_id:
            if not self.order_manager.cancel_order(self.state.partial_exit_order.order_id):
                success = False
            self.state.partial_exit_order.reset()

        if self.state.full_exit_order.is_pending() and self.state.full_exit_order.order_id:
            if not self.order_manager.cancel_order(self.state.full_exit_order.order_id):
                success = False
            self.state.full_exit_order.reset()

        return success

    def get_status(self) -> Dict[str, Any]:
        """Get strategy status"""
        live_pnl_points, live_pnl_dollars = 0.0, 0.0

        if not self.state.is_flat() and self.current_price > 0:
            live_pnl_points, live_pnl_dollars = PriceUtils.get_live_pnl(
                entry_price=self.state.entry_price,
                current_price=self.current_price,
                size=self.state.position_size,
                is_long=self.state.is_long(),
                tick_size=self.config.tick_size,
                tick_value=self.config.tick_value
            )

        status = self.state.to_dict()
        status.update({
            'live_pnl_points': live_pnl_points,
            'live_pnl_dollars': live_pnl_dollars,
            'current_price': self.current_price,
            'seb_bands': self.seb_bands,
            'candle_count': len(self.candle_history),
            'ticks_processed': self._tick_count,
            'live_trading': self.trading_config.live_trading
        })

        return status

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        return {
            'total_trades': self.state.total_trades,
            'winning_trades': self.state.winning_trades,
            'win_rate': self.state.get_win_rate(),
            'total_pnl_points': self.state.total_pnl,
            'total_pnl_dollars': PriceUtils.points_to_dollars(
                self.state.total_pnl, self.config.tick_size, self.config.tick_value
            ),
            'daily_pnl_points': self.state.daily_pnl,
            'daily_pnl_dollars': PriceUtils.points_to_dollars(
                self.state.daily_pnl, self.config.tick_size, self.config.tick_value
            ),
            'largest_win': self.state.largest_win,
            'largest_loss': self.state.largest_loss,
            'consecutive_losses': self.state.consecutive_losses,
            'max_consecutive_losses': self.state.max_consecutive_losses,
            'ticks_processed': self._tick_count
        }