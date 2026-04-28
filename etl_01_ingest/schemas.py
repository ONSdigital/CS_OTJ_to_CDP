
"""
Schemas and structural expectations.

This module defines:
- expected OTJ file structure (versioned)
- import log schema
- session log schema

These are treated as contracts between pipeline stages.
"""

IMPORT_LOG_COLUMNS = [
    "file_name",
    "file_checksum",
    "file_unique",
    "preprocess_outcome",
    "processed_timestamp",
    "session_id",
]

SESSION_LOG_COLUMNS = [
    "session_id",
    "files_processed",
    "duplicate_files",
    "issue_files",
    "processed_files",
    "start_time_date",
    "end_time_date",
]
