# tsxapipy/auth.py
import logging
from datetime import datetime
from typing import Tuple

from tsxapipy.config import API_URL, USERNAME, API_KEY
from tsxapipy.api.client import _perform_initial_authentication
from tsxapipy.api.exceptions import AuthenticationError, ConfigurationError
from tsxapipy.common.time_utils import UTC_TZ

logger = logging.getLogger(__name__)

def authenticate() -> Tuple[str, datetime]:
    """Authenticates with the TopStep API using credentials from the environment.

    This function retrieves the USERNAME and API_KEY from the configuration
    and uses them to obtain a session token.

    Returns:
        Tuple[str, datetime]: A tuple containing:
            - The session token (str) upon successful authentication.
            - The UTC datetime (datetime) when the token was acquired.

    Raises:
        ConfigurationError: If USERNAME or API_KEY are not found in the configuration.
        AuthenticationError: If the authentication attempt fails for other reasons.
    """
    if not USERNAME:
        msg = (
            "USERNAME not configured. Cannot authenticate. "
            "Check your .env file or environment variables."
        )
        logger.error(msg)
        raise ConfigurationError(msg)
    if not API_KEY:
        msg = (
            "API_KEY not configured. Cannot authenticate. "
            "Check your .env file or environment variables."
        )
        logger.error(msg)
        raise ConfigurationError(msg)

    logger.info("Attempting authentication for user: %s...", USERNAME)
    try:
        response_data = _perform_initial_authentication(
            username=USERNAME,
            api_key=API_KEY,
            api_url_override=API_URL
        )
        token = response_data.get("token")

        if not token:
            raise AuthenticationError(
                "Token not found in authentication response, "
                "though API call seemed successful."
            )

        acquired_at_utc = datetime.now(UTC_TZ)
        logger.info(
            "Authentication successful. Token acquired at: %s",
            acquired_at_utc.isoformat()
        )
        return token, acquired_at_utc
    except AuthenticationError: # Re-raise if _perform_initial_authentication raises it
        logger.error("Authentication attempt failed (error raised from API client call).")
        raise
    except Exception as e: # pylint: disable=broad-except
        msg = f"An unexpected error occurred during the authentication wrapper process: {e}"
        logger.error(msg, exc_info=True)
        raise AuthenticationError(msg) from e