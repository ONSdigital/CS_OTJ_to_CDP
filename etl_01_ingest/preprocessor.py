
import hashlib
import pandas as pd
from datetime import datetime, timezone

from etl_01_ingest.paths import (
    IMPORT_DIR,
    DUP_OUTPUT_DIR,
    ISSUE_OUTPUT_DIR,
    OUTPUT_DIR,
)
from etl_01_ingest.logs import load_import_log
from etl_01_ingest.schemas import IMPORT_LOG_COLUMNS


def run_preprocessing(session_id: str) -> tuple[pd.DataFrame, dict]:
    """
    Main preprocessing routine.

    Returns:
        updated_import_log_df
        session_summary (dict)
    """

    # 1 - set ups
    
    # 1.1 - import log's csv as df, so can update
    df_import_log = load_import_log()

    # 1.2 - set up counts
    check_count = 0
    duplicate_count = 0
    issue_count = 0
    pre_processed_count = 0

    # 2 - loop through each file in the import directory, and action accordingly

    for csv_file in IMPORT_DIR.glob("*.csv"):

        # 2.1 - inital set ups
        print(csv_file.name)
        check_count += 1

        file_name = csv_file.name
        checksum = hashlib.md5(csv_file.read_bytes()).hexdigest()
        is_duplicate = checksum in df_import_log["file_checksum"].values

        # 2.2 - is it a duplicate?
        if is_duplicate:
            duplicate_count += 1
            csv_file.rename(DUP_OUTPUT_DIR / file_name)
            outcome = "duplicate"


        else:
            #2.3 - if not a duplicate, is it broken?
            try:
                pd.read_csv(csv_file)
            except Exception as e:
                issue_count += 1
                csv_file.rename(ISSUE_OUTPUT_DIR / file_name)
                outcome = f"read_error: {e}"

            else:
                #2.4 - if not duplicate or broken consider ok to process
                pre_processed_count += 1
                csv_file.rename(OUTPUT_DIR / file_name)
                outcome = "preprocessed_ok"

        #2.5 - add a line to df for this check
        df_import_log.loc[len(df_import_log)] = {
            "file_name": file_name,
            "file_checksum": checksum,
            "file_unique": not is_duplicate,
            "preprocess_outcome": outcome,
            "processed_timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": session_id,
        }

    # 3 - once all checks complete, make a dict ready to add to session-log
    session_summary = {
        "files_processed": check_count,
        "duplicate_files": duplicate_count,
        "issue_files": issue_count,
        "processed_files": pre_processed_count,
    }

    # 4 - outputs of this function when it is called
    return df_import_log, session_summary
