# topstep_data_suite/src/tsxapipy/trading/indicators.py
import logging
from typing import Union, Optional, Sequence, List # Added List for internal use with pandas
import pandas as pd # Moved import to top as it's used by EMA

logger = logging.getLogger(__name__)

Numeric = Union[int, float]
"""Type alias for values that can be either an integer or a float."""

def simple_moving_average(data: Sequence[Numeric], period: int) -> Optional[float]:
    """Calculates the Simple Moving Average (SMA) for a given period.

    The SMA is the unweighted mean of the previous `period` data points.

    Args:
        data (Sequence[Numeric]): A sequence (list, tuple, deque, etc.) of numerical data points
            (e.g., closing prices).
        period (int): The number of data points to include in the average.
            Must be a positive integer.

    Returns:
        Optional[float]: The calculated Simple Moving Average as a float if enough
        data points are available and inputs are valid. Returns None if:
        - `data` does not contain enough elements for the specified `period`.
        - `period` is not a positive integer.
        - `data` contains non-numeric values that cannot be converted to float.
        - Any other unexpected error occurs during calculation.
    """
    if not isinstance(data, Sequence): # Check if it's a sequence type
        logger.warning(f"Invalid input for SMA: data is not a sequence (type: {type(data)}).")
        return None
    if not isinstance(period, int) or period <= 0:
        logger.warning(f"Invalid input for SMA: period ('{period}') must be a positive integer.")
        return None
    if len(data) < period:
        logger.debug(f"Not enough data for SMA period {period}. Have {len(data)} points, need {period}.")
        return None

    # Consider only the most recent 'period' data points
    relevant_data_segment = data[-period:]

    try:
        # Ensure all data points in the segment are numeric and sum them
        numeric_sum = sum(float(x) for x in relevant_data_segment)
        return numeric_sum / period
    except (ValueError, TypeError) as e:
        logger.error(f"Error calculating SMA: Non-numeric data or type error in segment {relevant_data_segment}. Error: {e}",
                     exc_info=True) # Log traceback for debugging
        return None
    except Exception as e: # Catch any other unexpected error during calculation
        logger.error(f"Unexpected error calculating SMA for data segment {relevant_data_segment}: {e}",
                     exc_info=True)
        return None

def exponential_moving_average(data: Sequence[Numeric], period: int) -> Optional[float]:
    """Calculates the Exponential Moving Average (EMA) using pandas.

    The EMA gives more weight to recent prices, making it more responsive to new information.
    This implementation uses `pandas.Series.ewm()` with `adjust=False` for a standard EMA calculation.

    Args:
        data (Sequence[Numeric]): A sequence of numerical data points (e.g., closing prices).
        period (int): The period for the EMA. This is used as the `span` in `ewm()`.
            Must be a positive integer.

    Returns:
        Optional[float]: The calculated Exponential Moving Average as a float for the
        last data point if successful. Returns None if:
        - `pandas` library is not installed.
        - `data` does not contain enough elements for the specified `period`.
        - `period` is not a positive integer.
        - `data` contains non-numeric values.
        - Any other unexpected error occurs during calculation.
    """
    if not isinstance(data, Sequence):
        logger.warning(f"Invalid input for EMA: data is not a sequence (type: {type(data)}).")
        return None
    if not isinstance(period, int) or period <= 0:
        logger.warning(f"Invalid input for EMA: period ('{period}') must be a positive integer.")
        return None
    # ewm().mean() can produce results with fewer than `period` data points, but typically
    # an EMA is considered more stable once at least `period` points are available.
    # Depending on the strictness, you might still want len(data) >= period.
    # Pandas ewm handles short series by giving more weight to initial points.
    if not data: # Pandas will error on empty series
        logger.debug("Not enough data for EMA: input data is empty.")
        return None


    try:
        # Convert sequence to a list of floats for pandas Series
        numeric_data: List[float] = [float(x) for x in data]
        series = pd.Series(numeric_data)
        
        # Calculate EMA. span=period is common. adjust=False uses recursive formula for EMA.
        ema_series = series.ewm(span=period, adjust=False).mean()
        
        if ema_series.empty:
            return None
        return float(ema_series.iloc[-1]) # Return the last EMA value
    except ImportError: # Should not happen if pandas is a dependency, but good practice
        logger.error("Pandas library is not installed. Cannot calculate EMA with this method.")
        return None
    except (ValueError, TypeError) as e: # Errors from float() conversion or pandas operations
        logger.error(f"Error calculating EMA: Data conversion or type error. Data: {data[:10]}... Error: {e}",
                     exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error calculating EMA for data: {data[:10]}... Error: {e}",
                     exc_info=True)
        return None

# TODO: Consider adding other common indicators if needed by strategies:
# - ATR (Average True Range) - requires high, low, close
# - Bollinger Bands - requires SMA and standard deviation
# - RSI (Relative Strength Index)
# - MACD (Moving Average Convergence Divergence)