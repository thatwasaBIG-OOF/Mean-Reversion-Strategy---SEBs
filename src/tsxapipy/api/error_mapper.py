# tsxapipy/api/error_mapper.py
"""
Maps API error responses (where success=false) to specific custom APIError exceptions.
"""
import logging
from typing import Dict, Any, Optional

from .exceptions import (
    APIResponseError, ContractNotFoundError, InvalidParameterError, MarketClosedError,
    OrderNotFoundError, OrderRejectedError,
    # These are currently unused but kept for future potential mappings
    # InsufficientFundsError, MaxPositionLimitError, RateLimitExceededError,
    APIError
)

logger = logging.getLogger(__name__)

# pylint: disable=too-many-return-statements, too-many-branches
def map_api_error_response(endpoint: str, response_data: Dict[str, Any]) -> Optional[APIError]:
    """
    Maps a failed API JSON response to a specific custom APIError.

    Args:
        endpoint (str): The API endpoint called (e.g., "/api/Order/place").
        response_data (Dict[str, Any]): Parsed JSON response from API.

    Returns:
        Optional[APIError]: Specific APIError subclass instance or None.
    """
    err_msg = response_data.get('errorMessage')
    err_code = response_data.get('errorCode')
    s_err_code = str(err_code)

    if s_err_code == "8":
        return ContractNotFoundError(
            err_msg or f"Contract operation failed (API errorCode {s_err_code})",
            err_code, response_data
        )
    if s_err_code == "2":
        if endpoint == "/api/Order/place":
            if err_msg and "invalid order type" in err_msg.lower():
                return InvalidParameterError(err_msg, err_code, response_data)
            if err_msg and "invalid order size" in err_msg.lower():
                return InvalidParameterError(err_msg, err_code, response_data)
            if err_msg and "outside of trading hours" in err_msg.lower():
                return MarketClosedError(err_msg, err_code, response_data)
            return InvalidParameterError(
                    err_msg or f"Invalid parameter for place order (API errorCode {s_err_code})",
                    err_code, response_data
                )
        if endpoint == "/api/Order/cancel":
            if err_msg is None:
                return OrderNotFoundError(
                    f"Order not found or cannot be cancelled "
                    f"(API errorCode {s_err_code}, no message)",
                    err_code, response_data
                )
            return OrderRejectedError(err_msg, err_code, response_data)
        if endpoint == "/api/Order/modify":
            if err_msg is None:
                return OrderNotFoundError(
                    f"Order not found or cannot be modified "
                    f"(API errorCode {s_err_code}, no message)",
                    err_code, response_data
                )
            # TODO: Add specific err_msg checks for modify if errorCode 2 is reused
            return OrderRejectedError(err_msg, err_code, response_data)

        logger.warning(
            "ErrorMapper: Unhandled context for errorCode 2 on endpoint '%s'. Message: '%s'.",
            endpoint, err_msg
        )
        return APIResponseError(
            err_msg or f"API operation failed (API errorCode {s_err_code}, endpoint {endpoint})",
            err_code, response_data
        )

    if s_err_code == "1":
        return InvalidParameterError(
            err_msg or f"Invalid account parameter or access issue (API errorCode {s_err_code})",
            err_code, response_data
        )

    logger.debug(
        "ErrorMapper: No specific mapping for endpoint '%s', code '%s', msg: '%s'.",
        endpoint, s_err_code, err_msg
    )
    return None