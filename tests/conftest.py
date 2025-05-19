# topstep_data_suite/tests/conftest.py
import pytest
import os
import sys

@pytest.fixture(scope="session", autouse=True)
def configure_pythonpath_for_tests():
    """
    Ensures that the 'src' directory is on sys.path when pytest runs,
    so that `from tsxapipy import ...` works in tests.
    This is especially useful if tests are not directly in the project root or if IDE runners behave differently.
    """
    # Calculate path to '.../topstep_data_suite/src'
    # __file__ is '.../topstep_data_suite/tests/conftest.py'
    # os.path.dirname(__file__) is '.../topstep_data_suite/tests'
    # os.path.join(os.path.dirname(__file__), '..') is '.../topstep_data_suite'
    # os.path.join(os.path.dirname(__file__), '..', 'src') is '.../topstep_data_suite/src'
    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    # print(f"Pytest conftest: Added to sys.path: {src_dir}") # For debugging if needed

# Example of another fixture you might use:
# @pytest.fixture
# def mock_api_token():
#     return "mock_test_token_12345"

# @pytest.fixture(scope="session", autouse=True)
# def setup_test_logging():
#     """
#     Configures basic logging for test runs if desired.
#     Pytest captures stdout/stderr by default, including log messages.
#     You can customize pytest logging via pytest.ini or command-line flags.
#     """
#     import logging
#     logging.basicConfig(level=logging.DEBUG, # Or INFO
#                         format='%(asctime)s - %(name)s [%(levelname)s]: %(message)s',
#                         force=True) # Force to override any other basicConfig
#     logging.getLogger("urllib3").setLevel(logging.WARNING) # Quieten noisy libraries
#     logging.getLogger("signalrcore").setLevel(logging.INFO)