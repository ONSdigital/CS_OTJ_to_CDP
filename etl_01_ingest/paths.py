
"""
Filesystem paths used by the ingestion pipeline.
"""

from pathlib import Path

# Project root
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd()



# Data directories
IMPORT_DIR = BASE_DIR / "data" / "data_01-otj_originals"
DUP_OUTPUT_DIR = BASE_DIR / "data" / "data_02-otj_duplicates_to_discard"
ISSUE_OUTPUT_DIR = BASE_DIR / "data" / "data_03-otj_issues_to_check"
OUTPUT_DIR = BASE_DIR / "data" / "data_04-otj_processed"

# Log directories
IMPORT_LOG_DIR = BASE_DIR / "logs" / "sub_logs"
SESSION_LOG_DIR = BASE_DIR / "logs"

# Log files
IMPORT_LOG_FILE = IMPORT_LOG_DIR / "sub_log_01-import_log.csv"
SESSION_LOG_FILE = SESSION_LOG_DIR / "log_01-session_log.csv"

# Ensure required directories exist
for d in [
    IMPORT_DIR,
    DUP_OUTPUT_DIR,
    ISSUE_OUTPUT_DIR,
    OUTPUT_DIR,
    IMPORT_LOG_DIR,
    SESSION_LOG_DIR,
]:
    d.mkdir(parents=True, exist_ok=True)
