# tsxapipy/historical/parquet_handler.py
"""
Module for handling Parquet file operations for historical bar data.
"""
import os
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple

import pandas as pd  # pylint: disable=import-error

from tsxapipy.common.time_utils import UTC_TZ

logger = logging.getLogger(__name__)

class ParquetHandlerError(IOError):
    """Base error for Parquet handling issues within this module."""
    pass

class ParquetReadError(ParquetHandlerError):
    """Error encountered during Parquet file reading operations."""
    pass

class ParquetWriteError(ParquetHandlerError):
    """Error encountered during Parquet file writing or appending operations."""
    pass


def get_last_timestamp_from_parquet(parquet_filepath: str) -> Optional[datetime]:
    """
    Reads a Parquet file and returns the latest timestamp from the 't' column.

    The timestamp is assumed to be UTC or converted to UTC if naive.

    Args:
        parquet_filepath (str): Path to the Parquet file.

    Returns:
        Optional[datetime]: The latest UTC datetime object found in the 't' column,
                            or None if the file doesn't exist, is empty,
                            lacks a 't' column, or contains no valid timestamps.
    """
    try:
        if not os.path.exists(parquet_filepath) or os.path.getsize(parquet_filepath) == 0:
            logger.debug("Parquet file '%s' not found or empty for last timestamp retrieval.",
                         parquet_filepath)
            return None

        df = pd.read_parquet(parquet_filepath, engine='pyarrow', columns=['t'])
        if df.empty or 't' not in df.columns:
            logger.debug("Parquet file '%s' is empty after read or no 't' column.",
                         parquet_filepath)
            return None

        df_valid_t = df.dropna(subset=['t'])
        if df_valid_t.empty:
            logger.debug("No valid (non-NaT) timestamps in 't' column of '%s'.",
                         parquet_filepath)
            return None

        last_timestamp_pandas = df_valid_t['t'].max()
        if pd.isna(last_timestamp_pandas): # Should be caught by dropna, but defensive check
            logger.debug("Max timestamp is NaT in '%s' after dropna.", parquet_filepath)
            return None

        # Convert to Python datetime and ensure UTC awareness
        last_timestamp_datetime = last_timestamp_pandas.to_pydatetime()
        if last_timestamp_datetime.tzinfo is None:
            last_timestamp_utc = UTC_TZ.localize(last_timestamp_datetime)
        else:
            last_timestamp_utc = last_timestamp_datetime.astimezone(UTC_TZ)

        logger.debug("Last timestamp successfully retrieved from '%s': %s",
                     parquet_filepath, last_timestamp_utc.isoformat())
        return last_timestamp_utc
    except FileNotFoundError: # Should be caught by os.path.exists, but handles race conditions
        logger.info("Parquet file '%s' not found (likely race condition during check).", parquet_filepath)
        return None
    except Exception as e: # pylint: disable=broad-except
        logger.error("Error reading last timestamp from Parquet file '%s': %s",
                     parquet_filepath, e, exc_info=True)
        # Consider raising ParquetReadError(f"Failed to get last timestamp from {parquet_filepath}: {e}") from e
        return None

def _prepare_new_bars_df(new_bars_data: List[Dict[str, Any]],
                         expected_cols: List[str],
                         parquet_filepath: str) -> pd.DataFrame:
    """
    Internal helper to create and prepare a DataFrame from new bar data.

    Ensures 't' is UTC datetime, 'contract_id_source' is string, and OHLCV are numeric.
    Reindexes to `expected_cols`, filling missing ones with NA.

    Args:
        new_bars_data (List[Dict[str, Any]]): List of raw bar data dictionaries.
        expected_cols (List[str]): List of column names expected in the output DataFrame.
        parquet_filepath (str): Path to the target Parquet file (for logging context).

    Returns:
        pd.DataFrame: A prepared DataFrame, or an empty DataFrame if input is empty
                      or no valid bars after processing.

    Raises:
        ValueError: If 't' column is missing or timestamp conversion critically fails.
    """
    if not new_bars_data:
        logger.debug("_prepare_new_bars_df: No new bars to prepare (input list is empty).")
        return pd.DataFrame()

    new_df = pd.DataFrame(new_bars_data)
    if new_df.empty:
        logger.debug("_prepare_new_bars_df: DataFrame from new_bars_data is empty.")
        return pd.DataFrame()

    if 't' not in new_df.columns:
        raise ValueError(f"Timestamp column 't' is missing in new_bars_data for {parquet_filepath}.")
    
    # Ensure 'contract_id_source' exists, default to 'UNKNOWN' if missing
    if 'contract_id_source' not in new_df.columns:
        logger.warning("Column 'contract_id_source' missing in new_bars_data for %s. Appending with 'UNKNOWN'.",
                       parquet_filepath)
        new_df['contract_id_source'] = 'UNKNOWN'

    try:
        new_df['t'] = pd.to_datetime(new_df['t'], errors='coerce', utc=True)
    except Exception as e_ts_convert:
        logger.error("Critical error converting 't' column to datetime for file %s: %s",
                     parquet_filepath, e_ts_convert, exc_info=True)
        raise ValueError(f"Timestamp conversion error for {parquet_filepath}: {e_ts_convert}") from e_ts_convert

    new_df.dropna(subset=['t'], inplace=True) # Remove rows where timestamp conversion failed
    if new_df.empty:
        logger.info("No valid bars with convertible timestamps found in new_df for %s.",
                    parquet_filepath)
        return pd.DataFrame()

    # Ensure other expected columns exist and attempt type conversion
    for col in expected_cols:
        if col == 't': # Already handled
            continue
        if col == 'contract_id_source': # Already handled (existence, will be str later)
             if col not in new_df.columns: new_df[col] = pd.NA # Should be present from above
             new_df[col] = new_df[col].astype(str) # Ensure it's string type
        elif col in new_df.columns: # o, h, l, c, v
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
        else: # Column is expected but missing in new_bars_data
            logger.warning("Column '%s' missing in new_bars_data for %s. It will be added with NA values.",
                         col, parquet_filepath)
            new_df[col] = pd.NA # Add as NA, dtype will be handled by _finalize_and_write_df
            
    return new_df.reindex(columns=expected_cols) # Ensure final DataFrame has all expected columns in order


def _read_and_combine_dfs(parquet_filepath: str,
                            bars_to_process_df: pd.DataFrame
                           ) -> Tuple[pd.DataFrame, int]:
    """
    Internal helper to read existing Parquet data, combine with new bars,
    and filter out new bars older than existing data.

    Args:
        parquet_filepath (str): Path to the Parquet file.
        bars_to_process_df (pd.DataFrame): DataFrame of new bars to potentially add.

    Returns:
        Tuple[pd.DataFrame, int]:
            - combined_df: DataFrame containing existing data plus new, valid bars.
            - len_original_existing_df: Number of rows in the original existing DataFrame.
    """
    len_original_existing_df = 0
    if os.path.exists(parquet_filepath) and os.path.getsize(parquet_filepath) > 0:
        try:
            logger.debug("Reading existing data from %s for merge.", parquet_filepath)
            existing_df = pd.read_parquet(parquet_filepath, engine='pyarrow')
            len_original_existing_df = len(existing_df)

            if not existing_df.empty and 't' in existing_df.columns:
                # Ensure existing 't' is UTC datetime for proper comparison
                if not pd.api.types.is_datetime64_any_dtype(existing_df['t']):
                    existing_df['t'] = pd.to_datetime(existing_df['t'], errors='coerce', utc=True)
                elif existing_df['t'].dt.tz is None: # Naive datetime
                    existing_df['t'] = existing_df['t'].dt.tz_localize(UTC_TZ)
                else: # Already tz-aware, ensure it's UTC
                    existing_df['t'] = existing_df['t'].dt.tz_convert(UTC_TZ)
                existing_df.dropna(subset=['t'], inplace=True) # Remove rows if 't' became NaT

                if not existing_df.empty:
                    last_existing_ts = existing_df['t'].max()
                    if pd.notna(last_existing_ts): # Filter new bars that are older or same
                        bars_to_process_df = \
                            bars_to_process_df[bars_to_process_df['t'] > last_existing_ts]

                if bars_to_process_df.empty:
                    logger.info("All new bars are older than or same as existing data in %s. No append needed.",
                                parquet_filepath)
                    return existing_df if not existing_df.empty else pd.DataFrame(columns=bars_to_process_df.columns), \
                           len_original_existing_df # Return existing or empty schema'd DF

                combined_df = pd.concat([existing_df, bars_to_process_df], ignore_index=True)
            else: # existing_df is empty or lacks 't' column
                combined_df = bars_to_process_df
        except Exception as read_exc: # pylint: disable=broad-except
            logger.error("Could not read or process existing Parquet file %s: %s. "
                         "Will attempt to write new data as a new file.",
                         parquet_filepath, read_exc, exc_info=True)
            combined_df = bars_to_process_df
            len_original_existing_df = 0 # Treat as if writing to a new file
    else:
        logger.info("Parquet file %s is new or empty. Data will be written as new.", parquet_filepath)
        combined_df = bars_to_process_df
    return combined_df, len_original_existing_df

def _finalize_and_write_df(combined_df: pd.DataFrame, parquet_filepath: str,
                             expected_dtypes: Dict[str, str],
                             len_original_existing_df: int) -> int:
    """
    Internal helper to finalize dtypes, sort, deduplicate, and write DataFrame to Parquet.

    Args:
        combined_df (pd.DataFrame): The DataFrame to be finalized and written.
        parquet_filepath (str): Path to the target Parquet file.
        expected_dtypes (Dict[str, str]): Dictionary mapping column names to target dtype strings.
        len_original_existing_df (int): Number of rows in the original existing DataFrame,
                                        used to calculate net new rows.

    Returns:
        int: Net number of new, unique bars effectively added to the file.
    """
    if combined_df.empty:
        logger.info("No data to write to %s after processing and filtering.", parquet_filepath)
        return 0

    # Apply schema / dtypes before writing
    for col, target_dtype_str in expected_dtypes.items():
        if col not in combined_df.columns: continue # Should be handled by reindex in _prepare_new_bars_df

        current_dtype = combined_df[col].dtype
        try:
            if col == 't': # Ensure 't' is 'datetime64[ns, UTC]'
                if not pd.api.types.is_datetime64_ns_dtype(current_dtype) or \
                   getattr(current_dtype, 'tz', None) != UTC_TZ:
                    combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce', utc=True)
            elif target_dtype_str == 'object' and pd.api.types.is_string_dtype(current_dtype):
                pass # Already string-like, pandas might use 'string[pyarrow]' etc.
            elif str(current_dtype) != target_dtype_str: # Avoid unnecessary astype if already correct
                if target_dtype_str == 'float64': # For OHLCV
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
                elif target_dtype_str == 'object': # For contract_id_source
                    combined_df[col] = combined_df[col].astype(str) # Explicitly string
                else: # General case, not expected for current schema
                    combined_df[col] = combined_df[col].astype(target_dtype_str, errors='ignore')
        except Exception as e_astype: # pylint: disable=broad-except
            logger.warning("Could not coerce column '%s' to '%s' (current: '%s'). Error: %s. Data may be inconsistent.",
                           col, target_dtype_str, current_dtype, e_astype)

    combined_df.sort_values(by='t', inplace=True)
    num_rows_before_dedup = len(combined_df)
    combined_df.drop_duplicates(subset=['t'], keep='last', inplace=True) # Keep most recent for a given timestamp
    if num_rows_before_dedup > len(combined_df):
        logger.info("Dropped %d duplicate bar(s) by timestamp before writing to %s.",
                    num_rows_before_dedup - len(combined_df), parquet_filepath)

    output_dir = os.path.dirname(parquet_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info("Created output directory for Parquet file: %s", output_dir)

    # Final check for 't' column timezone awareness and type before writing
    if 't' in combined_df.columns and pd.api.types.is_datetime64_any_dtype(combined_df['t']):
        if combined_df['t'].dt.tz is None:
            logger.warning("Column 't' is naive datetime before writing to %s. Localizing to UTC.", parquet_filepath)
            combined_df['t'] = combined_df['t'].dt.tz_localize(UTC_TZ)
        elif str(combined_df['t'].dt.tz) != str(UTC_TZ): # Check if it's already UTC
            logger.warning("Column 't' is not UTC (%s) before writing to %s. Converting to UTC.",
                           combined_df['t'].dt.tz, parquet_filepath)
            combined_df['t'] = combined_df['t'].dt.tz_convert(UTC_TZ)
    elif 't' in combined_df.columns: # 't' column exists but is not datetime
        logger.error("Column 't' is not of datetime type (is %s) before writing to %s. Data might be incorrect.",
                     combined_df['t'].dtype, parquet_filepath)
    else: # 't' column missing
        logger.error("Timestamp column 't' is missing entirely before writing to %s. Cannot write valid bar data.", parquet_filepath)
        raise ParquetWriteError(f"Timestamp column 't' missing, cannot write to {parquet_filepath}.")


    combined_df.to_parquet(parquet_filepath, engine='pyarrow', index=False)
    final_appended_count = len(combined_df) - len_original_existing_df
    logger.info("Successfully wrote data to Parquet file %s. Net change: %d rows. Total rows in file: %d",
                parquet_filepath, final_appended_count, len(combined_df))
    return max(0, final_appended_count) # Ensure non-negative return


def append_bars_to_parquet(new_bars_data: List[Dict[str, Any]], parquet_filepath: str) -> int:
    """
    Appends new bar data to a Parquet file, handling schema and avoiding duplicates.

    Data is expected to include 't' (timestamp), 'o', 'h', 'l', 'c', 'v',
    and 'contract_id_source'. Timestamps ('t') are converted to UTC.
    The file is created if it doesn't exist.

    Args:
        new_bars_data (List[Dict[str, Any]]): A list of dictionaries, where each
            dictionary represents a bar and contains at least 't', 'o', 'h', 'l', 'c', 'v',
            and 'contract_id_source'.
        parquet_filepath (str): The full path to the Parquet file.

    Returns:
        int: The net number of new, unique bars that were actually added to the Parquet file.
             This can be less than `len(new_bars_data)` if some bars were duplicates
             or older than existing data. Returns 0 if no new bars were added.

    Raises:
        ValueError: If essential data like 't' (timestamp) is missing from `new_bars_data`
                    or if timestamp conversion critically fails.
        ParquetWriteError: For critical errors during Parquet file writing operations
                           that prevent successful completion.
    """
    # pylint: disable=too-many-locals
    expected_columns = ['t', 'o', 'h', 'l', 'c', 'v', 'contract_id_source']
    # These dtypes are targets for the final Parquet file.
    expected_dtypes = {
        't': 'datetime64[ns, UTC]',
        'o': 'float64', 'h': 'float64', 'l': 'float64', 'c': 'float64', 'v': 'float64',
        'contract_id_source': 'object' # Pandas uses 'object' for strings often, pyarrow handles as string
    }

    try:
        prepared_new_df = _prepare_new_bars_df(new_bars_data, expected_columns, parquet_filepath)
        if prepared_new_df.empty and not new_bars_data: # No input data was provided
            return 0
        if prepared_new_df.empty and new_bars_data: # Input data was provided, but became empty after prep (e.g., all invalid timestamps)
             logger.info("No processable bars found after preparation for file %s.", parquet_filepath)
             return 0


        combined_df, len_original_existing_df = _read_and_combine_dfs(
            parquet_filepath, prepared_new_df.copy() # Pass a copy to avoid modification issues if df is reused
        )

        return _finalize_and_write_df(combined_df, parquet_filepath,
                                      expected_dtypes, len_original_existing_df)

    except ValueError as ve: # Catch ValueErrors from _prepare_new_bars_df or elsewhere
        logger.error("ValueError during Parquet operation for file %s: %s",
                     parquet_filepath, ve, exc_info=True)
        raise # Re-raise to signal failure to the caller
    except Exception as e: # pylint: disable=broad-except # Catch other unexpected errors
        logger.error("Critical error updating Parquet file %s: %s",
                     parquet_filepath, e, exc_info=True)
        raise ParquetWriteError(f"Failed to write or append to Parquet file {parquet_filepath}: {e}") from e