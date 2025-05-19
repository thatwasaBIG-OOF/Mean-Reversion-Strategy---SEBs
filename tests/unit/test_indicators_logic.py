# topstep_data_suite/tests/unit/test_indicators_logic.py
import logging
import pytest # Assuming you might use pytest features later

# Updated imports
from tsxapipy.trading.indicators import simple_moving_average
from tsxapipy.trading.logic import decide_trade

# Configure logging for tests if desired, or rely on pytest's capture
# Ensure this doesn't interfere if tests are run with other logging setups.
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Indicator Tests (using pytest style) ---
def test_sma_basic():
    assert simple_moving_average([1, 2, 3, 4, 5], 3) == (3 + 4 + 5) / 3 # 4.0
    assert simple_moving_average([10, 20, 30], 3) == 20.0

def test_sma_not_enough_data():
    assert simple_moving_average([1, 2], 3) is None

def test_sma_exact_period_data():
    assert simple_moving_average([1, 2, 3], 3) == 2.0

def test_sma_empty_data():
    assert simple_moving_average([], 3) is None

def test_sma_invalid_period():
    assert simple_moving_average([1, 2, 3, 4, 5], 0) is None
    assert simple_moving_average([1, 2, 3, 4, 5], -1) is None

def test_sma_non_numeric_data():
    assert simple_moving_average([1, 2, 'a', 4, 5], 3) is None
    assert simple_moving_average([1, 2, None, 4, 5], 3) is None # type: ignore

def test_sma_float_data():
    assert simple_moving_average([1.0, 2.5, 3.0, 4.5, 5.0], 3) == pytest.approx((3.0 + 4.5 + 5.0) / 3)


# --- Trade Logic Tests (using pytest style) ---
def test_decide_trade_buy():
    assert decide_trade(105, 100) == "BUY"
    assert decide_trade(100.1, 100.0) == "BUY"

def test_decide_trade_sell():
    assert decide_trade(95, 100) == "SELL"
    assert decide_trade(99.9, 100.0) == "SELL"

def test_decide_trade_no_signal_equal():
    assert decide_trade(100, 100) is None
    assert decide_trade(100.0, 100.0) is None

def test_decide_trade_ma_none():
    assert decide_trade(100, None) is None

def test_decide_trade_invalid_input():
    assert decide_trade("abc", 100) is None
    assert decide_trade(100, "xyz") is None
    assert decide_trade("abc", "xyz") is None
    assert decide_trade(None, 100) is None # type: ignore
    assert decide_trade(100, None) is None


# --- Main test execution (if not using pytest runner directly) ---
def run_all_tests_manually():
    """Helper to run tests if not using pytest CLI."""
    logger.info("Running all unit tests manually...")
    # You would call each test_function() here if not using pytest
    # For pytest, just defining functions starting with "test_" is enough.
    # This function is more for demonstration if you were to run it as `python test_indicators_logic.py`
    # and wanted explicit calls.
    test_sma_basic()
    test_sma_not_enough_data()
    # ... and so on for all test functions
    logger.info("Manual test calls completed (if implemented here).")
    print("If using pytest, these tests are discovered automatically.")


if __name__ == "__main__":
    # Setup basic logging if running this file directly for manual inspection
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Running tests from __main__ (e.g., for direct execution or debugging)...")

    # If you want to use pytest discovery and execution even when running the file directly:
    # pytest.main([__file__]) # This would run all tests in this file using pytest

    # Or, call a manual runner if you created one:
    # run_all_tests_manually()
    # For simplicity with pytest, usually you'd run `pytest` from the terminal in the project root.

    print("To run these tests, use 'pytest' from the 'topstep_data_suite' directory.")
    print("Example test output (manual assertions, not pytest runner):")
    # Example of manually checking assertions for direct run feedback
    try:
        test_sma_basic()
        test_decide_trade_buy()
        # Add more direct calls if you want to see their pass/fail here
        print("Basic manual checks passed (this is not full pytest output).")
    except AssertionError as e:
        print(f"Manual check FAILED: {e}")