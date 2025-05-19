# topstep_data_suite/src/tsxapipy/common/time_utils.py
"""
Common time and timezone related utilities for the library.
"""
import pytz

UTC_TZ = pytz.utc
"""A pytz timezone object representing Coordinated Universal Time (UTC).

This should be used whenever a UTC-aware datetime object is needed to ensure
consistency across the library.
Example: `datetime.now(UTC_TZ)`
"""