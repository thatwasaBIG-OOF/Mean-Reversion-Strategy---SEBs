# tsxapipy/api/exceptions.py
"""
Exceptions module for tsxapipy.

This module provides custom exceptions for the tsxapipy package.
"""

from typing import Any, Optional 

class LibraryError(Exception):
    """Base exception class for all tsxapipy errors."""
    pass

class ConfigurationError(LibraryError):
    """Raised when there is a configuration error."""
    pass

class AuthenticationError(LibraryError):
    """Raised when authentication fails."""
    pass

class APIError(LibraryError):
    """Base class for API-related errors.
    
    Attributes:
        message (str): The error message.
        error_code (Optional[Any]): The API-specific error code, if available.
        raw_response (Optional[Dict[str, Any]]): The raw JSON response from the API, if available.
        http_status_code (Optional[int]): The HTTP status code, if applicable.
        headers (Optional[Dict[str, str]]): HTTP response headers, if applicable.
        retry_after_seconds (Optional[int]): Seconds to wait before retrying, if applicable.
    """
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None, 
                 http_status_code: int = None, headers: Any = None, retry_after_seconds: int = None):
        super().__init__(message)
        self.message = message 
        self.error_code = error_code
        self.raw_response = raw_response 
        self.http_status_code = http_status_code
        self.headers = headers
        self.retry_after_seconds = retry_after_seconds

    def __str__(self):
        # Provide a more informative string representation
        parts = [self.message]
        if self.error_code is not None:
            parts.append(f"API ErrorCode: {self.error_code}")
        if self.http_status_code is not None:
            parts.append(f"HTTP Status: {self.http_status_code}")
        # Avoid printing large raw_response directly in str(exception)
        # if self.raw_response is not None:
        #     parts.append(f"RawResponse: {str(self.raw_response)[:100]}...") 
        return " | ".join(parts)


class APITimeoutError(APIError):
    """Raised when an API request times out."""
    def __init__(self, message: str = "API request timed out.", error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class APIHttpError(APIError):
    """Raised when an API request returns an HTTP error."""
    def __init__(self, http_status_code: int, message: Optional[str] = None, 
                 response_text: Optional[str] = None, headers: Any = None, 
                 error_code: Any = None, raw_response: Any = None): # Added raw_response for consistency
        self.status_code = http_status_code # Specific alias often used
        self.response_text = response_text  
        
        msg = message or f"HTTP error {http_status_code}"
        # Optionally append response_text preview if no detailed message is provided
        if response_text and not message and len(response_text) < 200: # Only if short
             msg += f" - Server Response: {response_text}"
        elif response_text and not message:
            msg += f" - Server Response Preview: {response_text[:100]}..."

        super().__init__(msg, error_code=error_code, raw_response=raw_response, 
                         http_status_code=http_status_code, headers=headers)

class APIResponseError(APIError):
    """Raised when an API response contains an error (e.g., success=false)."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class APIResponseParsingError(APIError):
    """Raised when there is an error parsing an API response."""
    def __init__(self, message: str, raw_response_text: Optional[str] = None, 
                 error_code: Any = None, raw_response: Any = None): # Added raw_response for consistency
        super().__init__(message, error_code=error_code, raw_response=raw_response)
        self.raw_response_text = raw_response_text


class InvalidParameterError(APIError):
    """Raised when an invalid parameter is provided to an API call."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class RateLimitExceededError(APIError):
    """Raised when the API rate limit is exceeded."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None, 
                 headers: Any = None, retry_after_seconds: Optional[int] = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response, 
                         headers=headers, retry_after_seconds=retry_after_seconds)

class ContractNotFoundError(APIError):
    """Raised when a contract is not found."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class OrderNotFoundError(APIError):
    """Raised when an order is not found."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class OrderRejectedError(APIError):
    """Raised when an order is rejected."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class InsufficientFundsError(APIError):
    """Raised when there are insufficient funds for an operation."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class MaxPositionLimitError(APIError):
    """Raised when a position limit is exceeded."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class MarketClosedError(APIError):
    """Raised when an operation is attempted while the market is closed."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)

class ValueError(APIError): # This is your custom tsxapipy.api.exceptions.ValueError
    """Raised when a value error occurs specific to the API's logic or data expectations."""
    def __init__(self, message: str, error_code: Any = None, raw_response: Any = None):
        super().__init__(message, error_code=error_code, raw_response=raw_response)