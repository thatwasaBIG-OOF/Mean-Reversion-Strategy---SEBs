# tsxapipy/__init__.py
"""
tsxapipy: A Python client library for the TopStep API.

This library provides tools for authentication, API interaction,
historical data management, real-time data streaming, and basic trading utilities.
It now incorporates Pydantic models for enhanced data validation and clarity in API interactions.
"""
import logging

# Standard library imports first

# Then third-party imports (none in this specific __init__ directly)

# Then first-party/local imports
from .auth import authenticate
from .api.client import (
    APIClient,
    MAX_BARS_PER_REQUEST
)
from .api.contract_utils import get_futures_contract_details
from .api.exceptions import (
    LibraryError,
    ConfigurationError,
    APIError,
    AuthenticationError,
    APITimeoutError,
    APIHttpError,
    APIResponseError,
    APIResponseParsingError, # Added new exception
    ContractNotFoundError,
    InvalidParameterError,
    RateLimitExceededError,
    InsufficientFundsError,
    OrderNotFoundError,
    OrderRejectedError,
    MaxPositionLimitError,
    MarketClosedError
)
# Import Pydantic schemas to make key models available at top level
from .api import schemas as api_schemas
from .common.time_utils import UTC_TZ
from .config import (
    API_URL,
    MARKET_HUB_URL,
    USER_HUB_URL,
    TRADING_ENV_SETTING,
    CONTRACT_ID as DEFAULT_CONFIG_CONTRACT_ID,
    ACCOUNT_ID_TO_WATCH as DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH,
    TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES,
    DEFAULT_TOKEN_LIFETIME_HOURS
)
from .historical.gap_detector import find_missing_trading_days
from .historical.parquet_handler import (
    get_last_timestamp_from_parquet,
    append_bars_to_parquet,
    ParquetHandlerError,
    ParquetReadError,
    ParquetWriteError
)
from .historical.updater import (
    HistoricalDataUpdater,
    calculate_next_interval_start
)
from .real_time import DataStream, UserHubStream, StreamConnectionState # Added StreamConnectionState
from .trading.indicators import (
    simple_moving_average,
    exponential_moving_average
)
from .trading.logic import decide_trade
from .trading.order_handler import (
    OrderPlacer,
    place_order_simulated,
    ORDER_TYPES_MAP,
    ORDER_SIDES_MAP,
    ORDER_STATUS_TO_STRING_MAP,
    ORDER_STATUS_PENDING_NEW,
    ORDER_STATUS_NEW,
    ORDER_STATUS_WORKING,
    ORDER_STATUS_PARTIALLY_FILLED,
    ORDER_STATUS_FILLED,
    ORDER_STATUS_PENDING_CANCEL,
    ORDER_STATUS_CANCELLED,
    ORDER_STATUS_REJECTED,
    ORDER_STATUS_EXPIRED,
    ORDER_STATUS_UNKNOWN
)
from .pipeline import LiveCandleAggregator, DataManager


# Configure a NullHandler for the library's root logger.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Version of the library - Increment for Pydantic integration
__version__ = "0.5.0" # Incremented version for significant Pydantic refactor

# Selective exposure of key components for easier top-level imports.
__all__ = [
    # Version
    "__version__",

    # from auth.py
    "authenticate",

    # from api.exceptions.py
    "LibraryError", "ConfigurationError", "APIError", "AuthenticationError",
    "APITimeoutError", "APIHttpError", "APIResponseError", "APIResponseParsingError", # Added
    "ContractNotFoundError", "InvalidParameterError", "RateLimitExceededError",
    "InsufficientFundsError", "OrderNotFoundError", "OrderRejectedError",
    "MaxPositionLimitError", "MarketClosedError",

    # from api.client.py
    "APIClient", "MAX_BARS_PER_REQUEST",

    # from api.contract_utils.py
    "get_futures_contract_details",

    # Pydantic Schemas (key models for convenience)
    "api_schemas", # Expose the whole module as api_schemas
      # Or specific models:
      # "Account", "Contract", "BarData", "OrderDetails", "Position", "Trade",
      # "AuthResponse", "HistoricalBarsResponse", "OrderPlacementResponse", etc.
      # For now, exposing the module is simpler and allows access to all schemas via tsxapipy.api_schemas.ModelName

    # from historical (...)
    "get_last_timestamp_from_parquet", "append_bars_to_parquet",
    "ParquetHandlerError", "ParquetReadError", "ParquetWriteError",
    "find_missing_trading_days", "HistoricalDataUpdater", "calculate_next_interval_start",

    # from real_time (...)
    "DataStream", "UserHubStream", "StreamConnectionState", # Added StreamConnectionState

    # from trading.indicators.py
    "simple_moving_average", "exponential_moving_average",

    # from trading.logic.py
    "decide_trade",

    # from trading.order_handler.py
    "OrderPlacer", "place_order_simulated",
    "ORDER_TYPES_MAP", "ORDER_SIDES_MAP", "ORDER_STATUS_TO_STRING_MAP",
    "ORDER_STATUS_PENDING_NEW", "ORDER_STATUS_NEW", "ORDER_STATUS_WORKING",
    "ORDER_STATUS_PARTIALLY_FILLED", "ORDER_STATUS_FILLED",
    "ORDER_STATUS_PENDING_CANCEL", "ORDER_STATUS_CANCELLED",
    "ORDER_STATUS_REJECTED", "ORDER_STATUS_EXPIRED", "ORDER_STATUS_UNKNOWN",

    # from common.time_utils.py
    "UTC_TZ",

    # from config.py
    "API_URL", "MARKET_HUB_URL", "USER_HUB_URL", "TRADING_ENV_SETTING",
    "DEFAULT_CONFIG_CONTRACT_ID", "DEFAULT_CONFIG_ACCOUNT_ID_TO_WATCH",
    "TOKEN_EXPIRY_SAFETY_MARGIN_MINUTES", "DEFAULT_TOKEN_LIFETIME_HOURS",

    # from pipeline (...)
    "LiveCandleAggregator", "DataManager",
]

# To allow specific model imports like `from tsxapipy import Account`,
# you would uncomment the specific model lines above in __all__ and add:
# from .api.schemas import (
#     Account, Contract, BarData, OrderDetails, Position, Trade,
#     AuthResponse, HistoricalBarsResponse, OrderPlacementResponse # etc.
# )
# For now, using `api_schemas` namespace is chosen for simplicity.