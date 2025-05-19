# tsxapipy/historical/gap_detector.py
"""
Module for detecting missing trading days in historical data.
"""
import logging
import os
from datetime import date, timedelta
from typing import List, Set

import pandas as pd  # pylint: disable=import-error


logger = logging.getLogger(__name__)

def find_missing_trading_days(
    parquet_file_to_check: str,
    overall_start_date: date,
    overall_end_date: date
) -> List[date]:
    """
    Identifies missing trading days (Mon-Fri) in a Parquet file within a date range.

    Compares dates in 't' column against all possible trading days in the range.

    Args:
        parquet_file_to_check (str): Path to the Parquet file (must have 't' column).
        overall_start_date (date): Start of the date range (inclusive).
        overall_end_date (date): End of the date range (inclusive).

    Returns:
        List[date]: Sorted list of unique `date` objects for missing trading days.
                    Returns all trading days in range if file is bad/empty.
                    Returns fallback (all trading days in range) on unexpected error.
    """
    all_possible_trading_days: Set[date] = set()
    current_scan_date = overall_start_date
    while current_scan_date <= overall_end_date:
        if current_scan_date.weekday() < 5:  # Monday == 0, Friday == 4
            all_possible_trading_days.add(current_scan_date)
        current_scan_date += timedelta(days=1)

    if not all_possible_trading_days:
        logger.info("No trading days found in the specified range: %s to %s.",
                    overall_start_date.isoformat(), overall_end_date.isoformat())
        return []

    try:
        if not os.path.exists(parquet_file_to_check) or \
           os.path.getsize(parquet_file_to_check) == 0:
            logger.info("Parquet file '%s' not found or empty. All trading days in range "
                        "considered missing.", parquet_file_to_check)
            return sorted(list(all_possible_trading_days))

        df = pd.read_parquet(parquet_file_to_check, columns=['t'], engine='pyarrow')

        if df.empty or 't' not in df.columns or df['t'].isnull().all():
            logger.info("Parquet file '%s' empty, lacks 't' column, or 't' has no valid data. "
                        "All trading days in range considered missing.", parquet_file_to_check)
            return sorted(list(all_possible_trading_days))

        # Ensure 't' column is properly converted to datetime and then to date
        df['t_datetime'] = pd.to_datetime(df['t'], errors='coerce', utc=True)
        df.dropna(subset=['t_datetime'], inplace=True)

        if df.empty: # If all timestamps were invalid after coercion
            logger.info("No valid timestamps in '%s' after conversion. All trading days in "
                        "range considered missing.", parquet_file_to_check)
            return sorted(list(all_possible_trading_days))

        present_dates_in_file: Set[date] = set(df['t_datetime'].dt.date)
        missing_days_list: List[date] = sorted(list(all_possible_trading_days -
                                                    present_dates_in_file))

        if missing_days_list:
            logger.info("Identified %d potential missing trading day(s) for gap filling.",
                        len(missing_days_list))
        else:
            logger.info("No missing trading days detected in range based on file content.")
        return missing_days_list

    except Exception as e: # pylint: disable=broad-except
        logger.error("Error during missing day detection in '%s': %s",
                     parquet_file_to_check, e, exc_info=True)
        logger.warning("Fallback: considering all %d trading days in range as potentially missing.",
                       len(all_possible_trading_days))
        return sorted(list(all_possible_trading_days))