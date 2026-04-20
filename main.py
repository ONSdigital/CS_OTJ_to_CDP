# This to be main 'engine' of this system
# To bring functionality from other areas / sub folders

import pandas as pd
import hashlib
from pathlib import Path
from datetime import datetime, timezone


#0.1
now_utc = datetime.now(timezone.utc)

START_TIME_DATE = now_utc.isoformat()


#------------------------
# 0.2 - Config

EXPECTED_OTJ_COLUMNS_V1 = [
    'Date',
    'Start',
    'End',
    'Duration',
    'Status',
    'Activity',
    'Description',
    'Competencies',
    'Edit',
    'Coach Notes',
    'Delete',
    'Submission History'
]

IMPORT_LOG_COLUMNS = [
    "file_name",
    "file_checksum",
    "file_unique",
    "preprocess_outcome",
    "processed_timestamp",
    "session_id"
]

SESSION_LOG_COLUMNS = [
    "session_id",
    "files_processed",
    "duplicate_files",
    "issue_files",
    "processed_files",
    "start_time_date",
    "end_time_date"
]

#--------
# how this works
# - sets directories: 'base' that everything can work from, then various places in 'data'
# - creates / updates the import log


#--------------------------------
# to move this to another file when done

# 0.3 - session ID, so that multiple imports can be grouped for any future troubleshooting analysis
IMPORT_SESSION_ID = START_TIME_DATE

# 1.1 - Project Directory
# What is the path of this project within the OS's file system
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

# 1.2 - OTJ Import Directory
# Where OTJ files are to be added by users, so they can be processed
IMPORT_DIR = BASE_DIR / "data" / "data_01-otj_originals"

# 1.3 - Duplicate OTJ Output Directory
# Where OTJ files are moved to, if it is determined they have already been pre-processed (this check is via checksum)
DUP_OUTPUT_DIR = BASE_DIR / "data" / "data_02-otj_duplicates_to_discard"
DUP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True) #prevents crashes if the folder already exists

# 1.4 - Issue OTJ Output Directory
# Where OTJ files are moved to, if there is found to be an issue during pre-processing
ISSUE_OUTPUT_DIR = BASE_DIR / "data" / "data_03-otj_issues_to_check"
ISSUE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True) #prevents crashes if the folder already exists

# 1.5 - Pre-Processed OTJ Output Directory
# Where OTJ files are moved to, once pre-processing is completed, to store them ready for further processing
OUTPUT_DIR = BASE_DIR / "data" / "data_04-otj_processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True) #prevents crashes if the folder already exists

# 1.6 - OTJ Import Log Directory
# Every time a file is pre-processed detailed of the action are stored, so we can check future imports for duplication
IMPORT_LOG_DIR = BASE_DIR / "logs" / "sub_logs"
IMPORT_LOG_DIR.mkdir(parents=True, exist_ok=True) #prevents crashes if the folder already exists
IMPORT_LOG_FILE = IMPORT_LOG_DIR / "sub_log_01-import_log.csv"

# 1.7 - Session Log Directory
# Every time this scrip is run a log will be added

SESSION_LOG_DIR = BASE_DIR / "logs" 
SESSION_LOG_DIR.mkdir(parents=True, exist_ok=True) #prevents crashes if the folder already exists
SESSION_LOG_FILE = SESSION_LOG_DIR / "log_01-session_log.csv"

#-------------------------------
#2 - Load / Create Import Log as Dataframe, ready to be added to
# (can't add lines to csv, we add to dataframe, then update whole csv at end)

if IMPORT_LOG_FILE.exists():
    df_import_log = pd.read_csv(IMPORT_LOG_FILE)
else:
    df_import_log = pd.DataFrame(columns=IMPORT_LOG_COLUMNS)



if SESSION_LOG_FILE.exists():
    df_session_log = pd.read_csv(SESSION_LOG_FILE)
else:
    df_session_log = pd.DataFrame(columns=SESSION_LOG_COLUMNS)


#---------
#test ideas
# - does log csv exsist, are it's columns as expected
# - are there any files in import directory that aren't csv?
# - odd characters in file name
# - 2 files with identical contents, but different file names, does checksum cover this?
# - non-csv, transformed to csv, then uploaded
# doesn't have .csv in name, but still csv?



#------------------------------
# move this to another file when done as well

#Pre-Processing

# get a list of all files in the import directory
# for each file, check if it's been imported before
# if it has, move to the duplicate folder
# else, run some checks, then move to processed folder
# log details of all files pre-processed

# 0 - set up for all check

check_count = 0
issue_count = 0
duplicate_count = 0
pre_processed_count = 0

otj_import_file_list = IMPORT_DIR.glob("*.csv")

for csv_file in otj_import_file_list:
    print(csv_file.name)
 
    # 1 - set up for this check

    check_count += 1
    file_name = csv_file.name

    # 2 - check if duplicate from earlier import
    new_file_checksum = hashlib.md5(csv_file.read_bytes()).hexdigest()

    is_duplicate_file = new_file_checksum in df_import_log['file_checksum'].values
    
    # 3.1 - if duplicate
    if is_duplicate_file:
        # 3.1 - move file
        duplicate_count += 1
        csv_file.rename(DUP_OUTPUT_DIR / file_name)
        preprocess_outcome = "duplicate"

    #if not duplicate...    
    else:
        # 3.2 - if issue with file
        try:
            df_otj_raw_data = pd.read_csv(csv_file)
        except Exception as e:
            issue_count += 1
            csv_file.rename(ISSUE_OUTPUT_DIR / file_name)
            preprocess_outcome = f"read_error: {e}"
            
        
        # 3.3 - ok, not duplicate or issue-file
        else:
            pre_processed_count += 1
            csv_file.rename(OUTPUT_DIR / file_name)
            preprocess_outcome = "preprocessed_ok"
        
    
    # 4 - log import df update
    df_import_log.loc[len(df_import_log)] = {
        "file_name": file_name,
        "file_checksum": new_file_checksum,
        "file_unique": not is_diplicate_file,
        "preprocess_outcome": preprocess_outcome,
        "processed_timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": IMPORT_SESSION_ID
    }
    


# 6 - update session log

END_TIME_DATE = datetime.now(timezone.utc).isoformat()

df_session_log.loc[len(df_session_log)] = {
        "session_id": IMPORT_SESSION_ID,
        "files_processed": check_count,
        "duplicate_files": duplicate_count,
        "issue_files": issue_count,
        "processed_files": pre_processed_count,
        "start_time_date": START_TIME_DATE,
        "end_time_date": END_TIME_DATE
    }

# update the csvs
df_session_log.to_csv(SESSION_LOG_FILE, index=False)
df_import_log.to_csv(IMPORT_LOG_FILE, index=False)


# 7 - advise outcome of pre-processing

print("Pre-Processing Complete")
print(f"{check_count} csv files checked")
print(f"{duplicate_count} are duplicates")
print(f"{issue_count} have issues")
print(f"{pre_processed_count} accepted for next stage")
print("2x log files updated (session log and import sub-log)")