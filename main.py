# This to be main 'engine' of this system
# To bring functionality from other areas / sub folders

import pandas as pd
import hashlib
from pathlib import Path


#--------------------------------
# to move this to another file when done

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
df_import_log = pd.read_csv(IMPORT_LOG_DIR, sep=',', skiprows=0, header=0)


#---------
#test ideas
# - does log csv exsist, are it's columns as expected
# - are there any files in import directory that aren't csv?



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

    file_checksum = hashlib.md5(csv_file.read_bytes()).hexdigest()

    unique_file_check = file_checksum in df_import_log['file_checksum'].values
      
    file_name = csv_file.name

    df_otj_raw_data = pd.read_csv(csv_file, sep=',', skiprows=0, header=0)
    
    # 3 - run checks against file
    #todo decide what these will be, without overlapping too much on full-import processing
        # maybe check columns are named as expected
        # and data in them the data type / roughly formatted as expected
    
    # 4 - log details to csv
      # add a row, add details from above into the following columns: 
      # file_name
      # file_checksum
      # file_unique
      # preprocess_outcome

    # 5 - move file to correct folder
    # 5.1 duplicate file
      #move file script to add
    duplicate_count += 1

    # 5.2 issue with file
      #move file script to add
    issue_count += 1

    # 5.3 ready for next step
      #move file script to add
    pre_processed_count += 1


# advise outcome of pre-processing
print('Pre-Processing Complete')
print(check_count + ' csv files have been pre-processed')
print(issue_count + ' have issues')
print(duplicate_count + ' are duplicates from previous pre-processing runs')
print(pre_processed_count + ' have been accepted, and passed to the next stage for full processing')