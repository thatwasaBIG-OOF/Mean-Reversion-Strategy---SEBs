"""
Standard Error Bands (SEB) indicator implementation.
"""

import numpy as np
import logging
from typing import List, Optional, Dict
from collections import deque


class SEBCalculator:
    """Standard Error Bands calculation engine"""

    def __init__(self, sample_period: int, standard_errors: float, averaging_periods: int):
        self.sample_period = sample_period
        self.standard_errors = standard_errors
        self.averaging_periods = averaging_periods
        self.logger = logging.getLogger(__name__)
        
        # Store recent calculations for smoothing
        self.recent_means = deque(maxlen=averaging_periods)
        self.recent_upper_bands = deque(maxlen=averaging_periods)
        self.recent_lower_bands = deque(maxlen=averaging_periods)
        
        # Cache for optimization
        self._last_calculation: Optional[Dict[str, float]] = None
        self._last_prices_hash: Optional[int] = None

        self.validate_parameters()

    def validate_parameters(self):
        """Validate SEB parameters"""
        if self.sample_period < 3:
            raise ValueError("Sampling Periods must be at least 3")
        if self.standard_errors <= 0:
            raise ValueError("Standard Errors must be positive")
        if self.averaging_periods < 1:
            raise ValueError("Averaging periods must be at least 1")

    def calculate(self, prices: List[float]) -> Optional[Dict[str, float]]:
        """
        Calculate Standard Error Bands with smoothing.
        
        Returns:
            Dictionary with keys: mean, upper_band, lower_band, std_error, slope
        """
        if len(prices) < self.sample_period:
            return None
        
        # Check cache to avoid redundant calculations
        prices_hash = hash(tuple(prices[-self.sample_period:]))
        if prices_hash == self._last_prices_hash and self._last_calculation:
            return self._last_calculation

        try:
            # Use only the required number of prices
            y = np.array(prices[-self.sample_period:], dtype=float)
            x = np.arange(len(y))

            # Linear regression
            n = len(y)
            sum_x = np.sum(x)
            sum_y = np.sum(y)
            sum_xy = np.sum(x * y)
            sum_x2 = np.sum(x * x)

            # Calculate slope and intercept
            denominator = n * sum_x2 - sum_x * sum_x
            if abs(denominator) < 1e-10:  # Avoid division by zero
                return None
                
            slope = (n * sum_xy - sum_x * sum_y) / denominator
            intercept = (sum_y - slope * sum_x) / n

            # Calculate standard error
            fitted = intercept + slope * x
            residuals = y - fitted
            
            if n > 2:
                std_error = np.sqrt(np.sum(residuals**2) / (n - 2))
            else:
                std_error = 0

            # Current regression value
            current_reg = intercept + slope * (n - 1)

            # Calculate bands
            raw_values = {
                'mean': current_reg,
                'upper_band': current_reg + self.standard_errors * std_error,
                'lower_band': current_reg - self.standard_errors * std_error,
                'std_error': std_error,
                'slope': slope
            }

            # Apply smoothing if needed
            if self.averaging_periods > 1:
                result = self._apply_smoothing(raw_values)
            else:
                result = raw_values

            # Update cache
            self._last_calculation = result
            self._last_prices_hash = prices_hash

            return result

        except Exception as e:
            self.logger.error(f"SEB calculation failed: {e}")
            return None

    def _apply_smoothing(self, raw_values: Dict[str, float]) -> Dict[str, float]:
        """Apply moving average smoothing to band values"""
        # Update deques
        self.recent_means.append(raw_values['mean'])
        self.recent_upper_bands.append(raw_values['upper_band'])
        self.recent_lower_bands.append(raw_values['lower_band'])

        # Calculate smoothed values
        return {
            'mean': sum(self.recent_means) / len(self.recent_means),
            'upper_band': sum(self.recent_upper_bands) / len(self.recent_upper_bands),
            'lower_band': sum(self.recent_lower_bands) / len(self.recent_lower_bands),
            'std_error': raw_values['std_error'],
            'slope': raw_values['slope']
        }

    def get_signal(self, current_price: float, bands: Dict[str, float], 
                   threshold: float) -> Optional[str]:
        """
        Generate trading signal based on price position relative to bands.
        
        Returns:
            'BUY' if price is below lower band - threshold
            'SELL' if price is above upper band + threshold
            None otherwise
        """
        if not bands:
            return None

        if current_price <= bands['lower_band'] - threshold:
            return 'BUY'
        elif current_price >= bands['upper_band'] + threshold:
            return 'SELL'

        return None

    def reset(self):
        """Reset calculator state and cache"""
        self.recent_means.clear()
        self.recent_upper_bands.clear()
        self.recent_lower_bands.clear()
        self._last_calculation = None
        self._last_prices_hash = None