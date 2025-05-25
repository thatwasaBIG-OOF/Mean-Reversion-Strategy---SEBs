# tsxapipy/api/__init__.py
"""
The tsxapipy.api subpackage provides core components for interacting with the
TopStep API, including the API client, contract utilities, and API-specific exceptions.
"""
from .client import (
    APIClient,
    MAX_BARS_PER_REQUEST
)
from .contract_utils import get_futures_contract_details

from .exceptions import (
    APIError,
    AuthenticationError,
    APITimeoutError,
    APIHttpError,
    APIResponseError,
    APIResponseParsingError, # Added APIResponseParsingError
    ContractNotFoundError,
    InvalidParameterError,
    RateLimitExceededError,
    InsufficientFundsError,
    OrderNotFoundError,
    OrderRejectedError,
    MaxPositionLimitError,
    MarketClosedError
)

__all__ = [
    # From client.py
    "APIClient",
    "MAX_BARS_PER_REQUEST",

    # From contract_utils.py
    "get_futures_contract_details",

    # Exceptions
    "APIError",
    "AuthenticationError",
    "APITimeoutError",
    "APIHttpError",
    "APIResponseError",
    "APIResponseParsingError", # Added APIResponseParsingError
    "ContractNotFoundError",
    "InvalidParameterError",
    "RateLimitExceededError",
    "InsufficientFundsError",
    "OrderNotFoundError",
    "OrderRejectedError",
    "MaxPositionLimitError",
    "MarketClosedError",
]