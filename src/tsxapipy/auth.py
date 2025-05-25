# tsxapipy/auth.py
import logging
from datetime import datetime
from typing import Tuple, Optional # Added Optional

from tsxapipy.config import API_URL, USERNAME as CONFIG_USERNAME, API_KEY as CONFIG_API_KEY # Aliased config imports
from tsxapipy.api.client import _perform_initial_authentication # This returns schemas.AuthResponse
from tsxapipy.api.exceptions import AuthenticationError, ConfigurationError
from tsxapipy.common.time_utils import UTC_TZ

logger = logging.getLogger(__name__)

def authenticate(username: Optional[str] = None, 
                 api_key: Optional[str] = None) -> Tuple[str, datetime]:
    """
    Authenticates with the TopStep API using provided or configured credentials.

    This function attempts to obtain a session token. It prioritizes credentials
    passed as arguments. If they are not provided, it falls back to using
    USERNAME and API_KEY from the `tsxapipy.config` module (loaded from
    .env or environment variables).

    Args:
        username (Optional[str], optional): The username for authentication.
            If None, `config.USERNAME` is used. Defaults to None.
        api_key (Optional[str], optional): The API key for authentication.
            If None, `config.API_KEY` is used. Defaults to None.

    Returns:
        Tuple[str, datetime]: A tuple containing:
            - The session token (str) upon successful authentication.
            - The UTC datetime (datetime) when the token was acquired.

    Raises:
        ConfigurationError: If necessary credentials (either passed or from config)
                            are missing.
        AuthenticationError: If the authentication attempt fails for other reasons,
                             including issues with the API response structure or
                             a missing token in a successful-looking response.
    """
    auth_username = username if username is not None else CONFIG_USERNAME
    auth_api_key = api_key if api_key is not None else CONFIG_API_KEY

    if not auth_username:
        msg = (
            "Username not provided and not found in configuration. Cannot authenticate. "
            "Pass as argument or set USERNAME in .env/environment variables."
        )
        logger.error(msg)
        raise ConfigurationError(msg)
    if not auth_api_key:
        msg = (
            "API key not provided and not found in configuration. Cannot authenticate. "
            "Pass as argument or set API_KEY in .env/environment variables."
        )
        logger.error(msg)
        raise ConfigurationError(msg)

    logger.info("Attempting authentication for user: %s...", auth_username)
    try:
        # _perform_initial_authentication returns a schemas.AuthResponse Pydantic model
        auth_response_model = _perform_initial_authentication(
            username=auth_username,
            api_key=auth_api_key,
            api_url_override=API_URL  # API_URL is from config, assumed to be correctly set
        )

        # Pydantic model validation within _perform_initial_authentication
        # (and its own call to schemas.AuthResponse.model_validate)
        # should ensure 'success' is true and 'token' is present if it returns normally.
        if not auth_response_model.token: # Defensive check
            # This case implies success=True from API, but token was unexpectedly None after Pydantic validation.
            logger.error("Authentication reported success by API, but token is missing in the validated response model.")
            raise AuthenticationError(
                "Token not found in validated authentication response model despite API success."
            )

        token_str = auth_response_model.token
        acquired_at_utc = datetime.now(UTC_TZ)
        logger.info(
            "Authentication successful. Token acquired at: %s",
            acquired_at_utc.isoformat()
        )
        return token_str, acquired_at_utc
        
    except AuthenticationError: # Re-raise if _perform_initial_authentication itself raises it
        # Error already logged by _perform_initial_authentication or APIClient
        logger.error("Authentication attempt failed (as indicated by an underlying call).")
        raise
    except Exception as e: # pylint: disable=broad-except
        # Catch any other unexpected exceptions during this wrapper's execution
        msg = f"An unexpected error occurred during the authentication process: {e}"
        logger.error(msg, exc_info=True)
        raise AuthenticationError(msg) from e