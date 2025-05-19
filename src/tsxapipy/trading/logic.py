# topstep_data_suite/src/tsxapipy/trading/logic.py
import logging
from typing import Optional, Union

logger = logging.getLogger(__name__)

Numeric = Union[int, float]
"""Type alias for values that can be either an integer or a float."""

def decide_trade(latest_price: Numeric, moving_avg: Optional[Numeric]) -> Optional[str]:
    """Decides a trade based on a simple crossover of latest price and a moving average.

    If `latest_price` is above `moving_avg`, it signals a "BUY".
    If `latest_price` is below `moving_avg`, it signals a "SELL".
    If they are equal, or if `moving_avg` is None, or if inputs are invalid,
    it returns None (no trade signal).

    Args:
        latest_price (Numeric): The current market price or a recent closing price.
        moving_avg (Optional[Numeric]): The calculated moving average against which
            to compare the `latest_price`. If None, no decision can be made.

    Returns:
        Optional[str]: "BUY" if price > moving average,
                       "SELL" if price < moving average,
                       None otherwise (no signal or invalid input).
    
    Raises:
        Does not directly raise exceptions for invalid numeric inputs but logs
        an error and returns None. Callers should handle potential None return.
    """
    if moving_avg is None:
        logger.debug("Cannot decide trade: moving average is None.")
        return None

    try:
        # Ensure inputs are valid numbers for comparison
        price_val = float(latest_price)
        ma_val = float(moving_avg)
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid numeric inputs for decide_trade. Price: '{latest_price}' (type: {type(latest_price)}), "
                     f"MA: '{moving_avg}' (type: {type(moving_avg)}). Error: {e}")
        return None

    if price_val > ma_val:
        logger.debug(f"Trade decision: BUY (Price: {price_val:.4f} > MA: {ma_val:.4f})")
        return "BUY"
    elif price_val < ma_val:
        logger.debug(f"Trade decision: SELL (Price: {price_val:.4f} < MA: {ma_val:.4f})")
        return "SELL"
    else: # price_val == ma_val
        logger.debug(f"No trade decision: Price ({price_val:.4f}) is equal to MA ({ma_val:.4f}).")
        return None