"""
Price calculation and utility functions.
"""

from typing import Tuple


class PriceUtils:
    """Price calculation utilities"""

    @staticmethod
    def round_to_tick(price: float, tick_size: float = 0.25) -> float:
        """
        Round price to nearest valid tick size

        Args:
            price: Price to round
            tick_size: Minimum price increment

        Returns:
            Price rounded to nearest tick
        """
        if tick_size <= 0:
            raise ValueError("Tick size must be positive")

        return round(price / tick_size) * tick_size

    @staticmethod
    def calculate_pnl_points(entry_price: float, exit_price: float, 
                            size: int, is_long: bool) -> float:
        """
        Calculate P&L in points

        Args:
            entry_price: Entry price
            exit_price: Exit price
            size: Position size (always positive)
            is_long: True for long position, False for short

        Returns:
            P&L in points
        """
        if is_long:
            return (exit_price - entry_price) * size
        else:
            return (entry_price - exit_price) * size

    @staticmethod
    def points_to_dollars(points: float, tick_size: float = 0.25, 
                         tick_value: float = 12.50) -> float:
        """
        Convert points to dollar value

        Args:
            points: P&L in points
            tick_size: Minimum price increment
            tick_value: Dollar value per tick

        Returns:
            Dollar value
        """
        if tick_size <= 0:
            raise ValueError("Tick size must be positive")

        return (points / tick_size) * tick_value

    @staticmethod
    def calculate_stop_price(entry_price: float, distance: float, 
                           is_long: bool, tick_size: float = 0.25) -> float:
        """
        Calculate stop loss price

        Args:
            entry_price: Entry price
            distance: Stop distance in points
            is_long: True for long position, False for short
            tick_size: Minimum price increment

        Returns:
            Stop loss price rounded to tick
        """
        if is_long:
            stop_price = entry_price - distance
        else:
            stop_price = entry_price + distance

        return PriceUtils.round_to_tick(stop_price, tick_size)

    @staticmethod
    def calculate_trail_stop(best_price: float, trail_distance: float, 
                           is_long: bool, tick_size: float = 0.25) -> float:
        """
        Calculate trailing stop price

        Args:
            best_price: Best price achieved
            trail_distance: Trailing distance in points
            is_long: True for long position, False for short
            tick_size: Minimum price increment

        Returns:
            Trailing stop price rounded to tick
        """
        if is_long:
            trail_price = best_price - trail_distance
        else:
            trail_price = best_price + trail_distance

        return PriceUtils.round_to_tick(trail_price, tick_size)

    @staticmethod
    def get_live_pnl(entry_price: float, current_price: float, 
                     size: int, is_long: bool, 
                     tick_size: float = 0.25, 
                     tick_value: float = 12.50) -> Tuple[float, float]:
        """
        Calculate live P&L in both points and dollars

        Args:
            entry_price: Entry price
            current_price: Current market price
            size: Position size
            is_long: True for long position, False for short
            tick_size: Minimum price increment
            tick_value: Dollar value per tick

        Returns:
            Tuple of (pnl_points, pnl_dollars)
        """
        pnl_points = PriceUtils.calculate_pnl_points(
            entry_price, current_price, size, is_long
        )
        pnl_dollars = PriceUtils.points_to_dollars(
            pnl_points, tick_size, tick_value
        )

        return pnl_points, pnl_dollars

    @staticmethod
    def validate_price(price: float) -> bool:
        """Validate that price is a positive number"""
        return isinstance(price, (int, float)) and price > 0

    @staticmethod
    def calculate_excursion(entry_price: float, current_price: float, 
                          is_long: bool) -> float:
        """
        Calculate excursion (favorable or adverse movement)

        Args:
            entry_price: Entry price
            current_price: Current price
            is_long: True for long position, False for short

        Returns:
            Excursion in points (positive = favorable, negative = adverse)
        """
        if is_long:
            return current_price - entry_price
        else:
            return entry_price - current_price