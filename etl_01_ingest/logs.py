
"""
Helpers for loading and persisting import and session logs.
"""

import pandas as pd
from etl_01_ingest.schemas import SESSION_LOG_COLUMNS, IMPORT_LOG_COLUMNS
from etl_01_ingest.paths import IMPORT_LOG_FILE, SESSION_LOG_FILE



def load_import_log() -> pd.DataFrame:
    if IMPORT_LOG_FILE.exists():
        df = pd.read_csv(IMPORT_LOG_FILE)
    else:
        df = pd.DataFrame()

    # Ensure all expected columns exist
    for col in IMPORT_LOG_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Keep only expected columns (avoids legacy junk)
    return df[IMPORT_LOG_COLUMNS]



def load_session_log() -> pd.DataFrame:
    if SESSION_LOG_FILE.exists():
        return pd.read_csv(SESSION_LOG_FILE)
    return pd.DataFrame(columns=SESSION_LOG_COLUMNS)


def write_import_log(df_import_log: pd.DataFrame) -> None:
    df_import_log.to_csv(IMPORT_LOG_FILE, index=False)


def write_session_log(df_session_log: pd.DataFrame) -> None:
    df_session_log.to_csv(SESSION_LOG_FILE, index=False)
