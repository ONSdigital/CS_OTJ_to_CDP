# A tool for converting Cambridge Spark off-the-job hours log to Continuous Personal Development log 

Made by cohort 3 data architecture/engineering apprentices. 

# Work in Progress:

> This repo implements file‑level ingestion and preparation only (stage 1); validation of file contents occurs in later stages, once those stages have been created.

# Contents

- Overview
- Background
- Pipeline Stages
- How to set up
- Where to add OTJ .csv
- How to run
- Methodology
- Logs Locations

# Overview

An ETL Pipeline, split into several stages.

This is to take OTJ Logs, and translate them into CPD logs.

- **OTJ: 'Off the job'**

  Logged hours where studying has taken place during working hours. These logs are exported from the Cambridge Spark apprenticeship platform (Edukate).
- **CPD: 'Continuous Professional Development'**
  
  Logged learning activities required for ONS employees in digital and data roles.

# Background

There are 2 practical factors driving this project: 

1. CPD logging is compulsory for all Government Digital and Data roles.

2. Apprentices must record all learning activity as part of their apprenticeship requirements.

In an attempt to reduce duplication and manual effort, this project aims to automate the conversion of OTJ logs into CPD‑compatible formats, while giving apprentices experience building a real‑world data pipeline.

# Pipeline Stages

- 01 - Ingest (in progress)
- 02 - Clean 
- 03 - Store Cleaned Ingest
- 04 - Mapping Logic
- 05 - Schema for Export
- 06 - Create Export

Each stage is designed to be runnable independently, with its own package‑level entry point.

# How to set up

from the project root:

terminal:

> - python -m venv .venv
>
> - .venv\Scripts\activate
>
> - pip install -r requirements.txt


# Where to add OTJ .csv

Place exported OTJ CSV files into:

> data > data_01-otj_originals

Multiple files can be added at once.

**Note:** CSV and Excel files are ignored by Git and will not be shared.


# How to run

Each stage can be run independently - each stage is implemented as a standalone Python package with its own entry point.

## Example: stage 1

terminal:

> (ensure virtual environment is active)
>
> - python -m etl_01_ingest
>

(this runs the package entry point defined in etl_01_ingest/\_\_main\_\_.py)

You must provide your own OTJ CSV files, even when testing.


# Methodology

## 01 - Ingest

  - inputs: One or more OTJ CSV exports from Edukate
  - process: 
    - Calculates a checksum for each file
    - Identifies previously ingested duplicates
    - Attempts to read each file as a valid CSV
    - Categorises files as:
      - duplicate
      - unreadable (cannot be parsed as a valid CSV)
      - ready for processing
  - outputs: 
    - Files are moved into outcome‑specific folders
    - Import‑level and session‑level logs are updated

This stage validates file‑level integrity only (existence, readability, and duplication).
Validation of file contents and business logic occurs in later stages.

tbc:

- 02 - Clean
- 03 - Store Cleaned Ingest
- 04 - Mapping Logic
- 05 - Schema for Export
- 06 - Create Export

# Logs Locations

## Ingest Logs:

> - import session log: etl_01_ingest > logs > log_01-session_log.csv
> - itemised file import log: etl_01_ingest > logs > sub_logs > sub_log_01-import_log.csv
