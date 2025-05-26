# tsxapipy/auth.py
import logging
from datetime import datetime
from typing import Tuple, Optional

import requests # For making the actual authentication request
from pydantic import ValidationError # For validating the response if using Pydantic models

from tsxapipy.config import API_URL, USERNAME as CONFIG_USERNAME, API_KEY as CONFIG_API_KEY
# DO NOT import from tsxapipy.api.client here for initial authentication logic.
from tsxapipy.api.exceptions import AuthenticationError, ConfigurationError
from tsxapipy.common.time_utils import UTC_TZ
# Import schemas directly from where they are defined
from tsxapipy.api import schemas # For AuthLoginKeyRequest and AuthResponse Pydantic models

logger = logging.getLogger(__name__)

def _perform_initial_authentication_http_call(username: str, api_key: str) -> schemas.AuthResponse:
    """
    Performs the actual HTTP POST request for initial key-based authentication.
    This is a helper for the main authenticate() function.

    Args:
        username (str): Username for authentication.
        api_key (str): API key for authentication.

    Returns:
        schemas.AuthResponse: A Pydantic model of the authentication response.

    Raises:
        AuthenticationError: If authentication fails for any reason.
        ConfigurationError: If API_URL is not set.
    """
    if not API_URL:
        raise ConfigurationError("API_URL is not configured.")

    request_url = f"{API_URL}/api/Auth/loginKey" # Assuming this is the correct endpoint for key auth
    action_desc = "Initial Key Authentication HTTP Call"
    
    try:
        request_model = schemas.AuthLoginKeyRequest(userName=username, apiKey=api_key)
        payload_dict = request_model.model_dump(by_alias=True)
    except ValidationError as e_req_val:
        logger.error(f"{action_desc}: Pydantic validation error creating request payload: {e_req_val}")
        raise AuthenticationError(f"Invalid parameters for key authentication request: {e_req_val}") from e_req_val

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    logger.debug(f"{action_desc}: URL='{request_url}', PayloadKeys='{list(payload_dict.keys())}'")
    
    response: Optional[requests.Response] = None
    try:
        # Using a direct requests.post call for initial auth
        # Timeouts: connect_timeout=10, read_timeout=15 (example values)
        response = requests.post(request_url, headers=headers, json=payload_dict, timeout=(10, 15))
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        response_data_dict = response.json()
        
        auth_response_model = schemas.AuthResponse.model_validate(response_data_dict)

        if auth_response_model.success and auth_response_model.token:
            logger.info(f"{action_desc} successful.")
            return auth_response_model
        
        default_msg = (f"Key login failed (API errorCode: {auth_response_model.error_code}) "
                       "or token missing in response.")
        final_err_msg = auth_response_model.error_message or default_msg
        logger.error(f"{action_desc} failed: {final_err_msg}. Response: {auth_response_model}")
        raise AuthenticationError(final_err_msg)

    except ValidationError as e_resp_val: # Pydantic validation error for the response
        raw_text_for_error = response.text[:500] if response and response.text else "No response text available"
        logger.error(f"{action_desc}: Failed to validate API response: {e_resp_val}. Raw: {raw_text_for_error}")
        raise AuthenticationError(f"Invalid key authentication response structure: {e_resp_val}") from e_resp_val
    except requests.exceptions.HTTPError as e_http:
        err_text = e_http.response.text if e_http.response is not None else "No response body"
        status_code_str = str(e_http.response.status_code) if e_http.response is not None else 'N/A'
        http_err_msg = f"HTTP error during {action_desc}: {status_code_str}"
        if err_text: http_err_msg += f" - {err_text}"
        logger.error(http_err_msg, exc_info=True) 
        raise AuthenticationError(http_err_msg) from e_http
    except requests.exceptions.RequestException as e_req: 
        logger.error(f"{action_desc}: Request failed: {e_req}", exc_info=True)
        raise AuthenticationError(f"Network or request error during key authentication: {e_req}") from e_req
    except Exception as e_gen: 
        logger.error(f"{action_desc}: Unexpected error: {e_gen}", exc_info=True)
        raise AuthenticationError(f"Unexpected error during key authentication: {e_gen}") from e_gen


def authenticate(username: Optional[str] = None, 
                 api_key: Optional[str] = None) -> Tuple[Optional[str], Optional[datetime]]: # Return types are Optional
    """
    Authenticates with the TopStep API using username and API key to get a session token.

    This function prioritizes provided credentials, then falls back to environment
    variables defined in `tsxapipy.config`.

    Args:
        username (Optional[str]): The username for authentication.
                                   If None, uses USERNAME from config.
        api_key (Optional[str]): The API key for authentication.
                                 If None, uses API_KEY from config.

    Returns:
        Tuple[Optional[str], Optional[datetime]]:
            - The session token string if authentication is successful, else None.
            - The UTC datetime when the token was acquired if successful, else None.

    Raises:
        ConfigurationError: If required credentials (username or API key) are
                            missing after checking args and config.
        AuthenticationError: If the authentication attempt fails at the API level
                             or due to other issues during the process.
    """
    logger.info("MODIFIED AUTHENTICATE FUNCTION CALLED - VERSION 2.0 (Corrected)") # Version marker

    effective_username = username if username is not None else CONFIG_USERNAME
    effective_api_key = api_key if api_key is not None else CONFIG_API_KEY

    if not effective_username:
        msg = "Username is required for authentication but not found (checked args and config)."
        logger.error(msg)
        raise ConfigurationError(msg)
    if not effective_api_key:
        msg = "API Key is required for authentication but not found (checked args and config)."
        logger.error(msg)
        raise ConfigurationError(msg)

    logger.info(f"Attempting authentication for user: {effective_username}...")
    
    try:
        auth_response_model = _perform_initial_authentication_http_call(
            username=effective_username,
            api_key=effective_api_key
        )
        
        if auth_response_model.success and auth_response_model.token:
            token_acquired_time = datetime.now(UTC_TZ)
            logger.info(f"Authentication successful. Token acquired at: {token_acquired_time.isoformat()}")
            return auth_response_model.token, token_acquired_time
        else:
            # This path should ideally be unreachable if _perform_initial_authentication_http_call
            # raises an AuthenticationError on failure. This is a defensive fallback.
            logger.error("Authentication failed: _perform_initial_authentication_http_call did not return a success/token or raise an error appropriately.")
            raise AuthenticationError("Authentication failed without a specific API error indication after HTTP call.")

    except AuthenticationError: 
        # Logged by _perform_initial_authentication_http_call or this function's fallback
        logger.error("Authentication attempt failed.")
        raise # Re-raise the caught AuthenticationError
    except ConfigurationError:
        logger.error("Configuration error during authentication.")
        raise # Re-raise
    except Exception as e: 
        msg = f"An unexpected error occurred during the authentication process: {e}"
        logger.error(msg, exc_info=True)
        raise AuthenticationError(msg) from e