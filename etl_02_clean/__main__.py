# This file handles the clean stage of the ETL pipeline.
# It takes the ingested OTJ data and makes it easier to work with.

# The aim of this stage is to:
# - keep only the useful OTJ columns
# - clean text fields
# - standardise dates
# - prepare the data for the later transform stage
# - convert duration into a numeric format

# 1 - imports etc:

# will wait for etl_01_ingest to be done before adding any imports here, but likely will need:
import csv
import pandas as pd
from otj_schema import OTJ_COLUMNS

# This should be the output from the previous ETL stage


# 2 - add your code to this function (replace 'pass')
def main():
    # Load the OTJ file
    df = pd.read_csv("HJ OTJ.csv") # this is a test at the moment / needs to be changed eventually as it will be the ingest file from 01

    # Check the file contains all expected OTJ columns
    missing_columns = [col for col in OTJ_COLUMNS if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing expected OTJ columns: {missing_columns}")
    
    # clean text columns 
    text_columns = ["Status", "Activity", "Description", "Competencies", "Coach Notes"]

    for col in text_columns:
    df[col] = df[col].fillna("").astype(str).str.strip()


    
    print("OTJ file loaded successfully")
    print("Schema check passed")
    print("Text columns cleaned")
