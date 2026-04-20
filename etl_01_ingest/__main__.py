#doing this as a __main__ so that each stage of pipeline can be run indipendently

from datetime import datetime, timezone


from etl_01_ingest.logs import (
    load_session_log,
    write_import_log,
    write_session_log,
)

from etl_01_ingest.preprocessor import run_preprocessing


def main() -> None:
    # 1 - set ups
    now_utc = datetime.now(timezone.utc)
    session_id = now_utc.isoformat()

    # 2 - if 

    # 2 - call main functionality, loop through each file
    df_import_log, summary = run_preprocessing(session_id)

    # 3 - once finished looping, prepare data about this whole session
    df_session_log = load_session_log()
    df_session_log.loc[len(df_session_log)] = {
        "session_id": session_id,
        "files_processed": summary["files_processed"],
        "duplicate_files": summary["duplicate_files"],
        "issue_files": summary["issue_files"],
        "processed_files": summary["processed_files"],
        "start_time_date": session_id,
        "end_time_date": datetime.now(timezone.utc).isoformat(),
    }

    # 4 - save each of the log dataframes back to csv's
    write_import_log(df_import_log)
    write_session_log(df_session_log)

    # 5 - final notifications for user
    print("Pre-Processing Complete")
    print(f"{summary['files_processed']} csv files checked")
    print(f"{summary['duplicate_files']} are duplicates")
    print(f"{summary['issue_files']} have issues")
    print(f"{summary['processed_files']} accepted for next stage")
    print("2x log files updated (session log and import sub-log)")

# 0 - Run this section only if this file is executed directly, not imported.
if __name__ == "__main__":
    main()
