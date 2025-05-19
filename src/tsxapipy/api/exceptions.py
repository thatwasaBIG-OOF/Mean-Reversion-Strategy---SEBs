# tsxapipy/api/exceptions.py
from typing import Optional, Any, Dict

class LibraryError(Exception):
    """Base class for all custom errors from the tsxapipy library."""
    pass

class ConfigurationError(LibraryError):
    """Raised for errors related to library configuration,
    such as missing or invalid settings in the .env file or environment variables.
    """
    pass

class APIError(LibraryError):
    """Base class for all custom API related errors from this library,
    typically involving an interaction with the remote API.
    """
    pass

class AuthenticationError(APIError):
    """Raised for authentication failures.

    This can occur during initial login (e.g., invalid credentials, API key issues)
    or during token revalidation/re-authentication processes.
    """
    pass

class APITimeoutError(APIError):
    """Raised when an API request to a specific endpoint times out."""
    pass

class APIHttpError(APIError):
    """Raised for HTTP errors received from the API (e.g., 4xx client errors, 5xx server errors).

    Attributes:
        status_code (int): The HTTP status code.
        response_text (Optional[str]): The raw response text, if available.
        headers (Dict[str, str]): The HTTP response headers.
    """
    def __init__(self, status_code: int, message: str,
                 response_text: Optional[str] = None,
                 headers: Optional[Dict[str, str]] = None):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.response_text = response_text
        self.headers = headers if headers is not None else {}

    def __str__(self):
        header_info = f" | Headers: {self.headers}" if self.headers else ""
        resp_text_preview = f" | Response: {self.response_text[:200]}..." if self.response_text and len(self.response_text) > 200 else (f" | Response: {self.response_text}" if self.response_text else "")
        return f"{super().__str__}{resp_text_preview}{header_info}"

class APIResponseError(APIError):
    """
    Raised when an API request is successful HTTP-wise (e.g., 200 OK),
    but the response body indicates a business logic failure
    (e.g., {"success": false, "errorCode": ..., "errorMessage": ...}).

    Attributes:
        error_code (Optional[Any]): The specific error code from the API response body.
        raw_response (Optional[Dict[str, Any]]): The full parsed JSON response body.
    """
    def __init__(self, message: str, error_code: Optional[Any] = None,
                 raw_response: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.error_code = error_code
        self.raw_response = raw_response

    def __str__(self):
        return f"{super().__str__()} (API Error Code: {self.error_code})"

# --- Specific API Response Error Subclasses (ensure they inherit from APIResponseError or APIError) ---

class ContractNotFoundError(APIResponseError): # Or APIError if it can come from HTTP 404
    """API indicates the specified contract could not be found or determined."""
    pass

class InvalidParameterError(APIResponseError): # Or APIError if it can come from HTTP 400
    """API indicates one or more parameters in the request were invalid."""
    pass

class RateLimitExceededError(APIError): # Could be HTTP 429 or JSON error
    """API indicates rate limits have been exceeded.

    Attributes:
        error_code (Optional[Any]): The HTTP status code (e.g., 429) or API specific error code.
        raw_response_body (Optional[Dict[str, Any]]): The JSON response body if error from JSON.
        response_headers (Dict[str, str]): The HTTP response headers, may contain 'Retry-After'.
        retry_after_seconds (Optional[int]): Suggested seconds to wait before retrying, if available.
    """
    def __init__(self, message: str,
                 error_code: Optional[Any] = None,
                 raw_response_body: Optional[Dict[str, Any]] = None,
                 response_headers: Optional[Dict[str, str]] = None,
                 retry_after_seconds: Optional[int] = None):
        super().__init__(message)
        self.error_code = error_code
        self.raw_response_body = raw_response_body
        self.response_headers = response_headers if response_headers is not None else {}
        self.retry_after_seconds = retry_after_seconds

    def __str__(self):
        base_str = super().__str__()
        if self.error_code:
            base_str += f" (Code: {self.error_code})"
        if self.retry_after_seconds is not None:
            return f"{base_str} | Suggested retry after: {self.retry_after_seconds} seconds."
        return base_str

class InsufficientFundsError(APIResponseError):
    """API indicates insufficient funds for the requested trading operation."""
    pass

class OrderNotFoundError(APIResponseError):
    """API indicates a specified order ID was not found (e.g., for cancel/modify)."""
    pass

class OrderRejectedError(APIResponseError):
    """API indicates an order was rejected for a specific reason."""
    pass

class MaxPositionLimitError(APIResponseError):
    """API indicates a maximum position limit would be exceeded by the order."""
    pass

class MarketClosedError(APIResponseError):
    """API indicates the market for the instrument is currently closed for trading."""
    pass