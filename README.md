# S&P 500 Components Intraday Pricing

This project reconstructs intraday (5 minute) S&P 500 index constituent stock prices and weights from 1994-2024. The codebase processes data from multiple sources (WRDS TAQ, CRSP, S&P/Dow Jones indices) to create a comprehensive dataset for financial research.

## Project Structure

### a01_create_SP500_weights/
**Purpose**: Construct the *daily S&P 500 constituent set* at the PERMCO level. This stage:
    (1) Determines membership for each calendar day 
    (2) Builds an effective-dated crosswalk linking CUSIP, symbol, and PERMCO, 
    (3) Enforces issuer-level membership to permit composite treatment of dual-class or tracking shares
    (4) Reconciles cross-source inconsistencies (identifier changes, vendor discrepancies) under explicit precedence rules. 

#### Code Files:
- **a01_downloadData.py**: Downloads S&P 500 constituent data from S&P databases
    - Pulls: 
    (1) Daily constituent data from 1994-2024
    (2) S&P 500 index, market cap, and divisor
    (3) Seperately pulls S&P 500 from 1994-1995 (TRTH does not carry intraday values for S&P 500 in those years)
    - Exports to respective Stata and Python formats for downstream use

- **a02_mergeTAQMaster.do**: Merges annual TAQ master files into unified dataset
    - Download TAQ master files from WRDS
    - Seperately process TAQ master files from 1994-2010
        - Instead of listing all stocks for each day, pre-2011 TAQ master files use "Effective Date", labeled as *FDATE* or *DATEF*.
          We expand the master file into a total daily listing here
    - Merge all yearly files into one main file

- **a03_createDailyDataset.do**: Validates source datasets against each other and generates initial daily dataset
    - TAQ Master edits:
    (1) TAQ Master's trading symbols occasionally change inconsistently with TAQ Trades dataset. Change Master's symbols to Trades' symbols
    (2) Some entries are present in S&P's Constituent dataset and TAQ Trades but missing in TAQ Master. Add these entries to TAQ Master
    (3) Within a date_daily+CUSIP there can be multiple trading symbols. TAQ Master is reshaped to store all syms within one entry.
    - Start with S&P Constituent dataset:
    (1) One specific CUSIP (748356102) ends with a "o". This does not match TAQ and is removed.
    (2) Some S&P CUSIP's change on different days than TAQ master, leading to mismatches. This results in errant drops when S&P+TAQMaster 
        are merged on CUSIP. These are realigned by changing S&P CUSIPs to match TAQ Master.
    - S&P Const is then merged with TAQ master

- **a04_create_pre2003_sym_list.py**: Generates symbol lists for pre-2003 TAQ data retrieval
    - .txt files of all symbols are generated for ease of pulling data from TAQ.

- **a05_createCusipListWide.do**: Creates reference document of all entries with entry/exit information
    - TBC

- **a06_permcolinking.do**: Links CUSIPs to PERMCO/PERMNO identifiers
    - Links CUSIP-PERMNO-PERMCO and attaches it to reference document

### a02_download_TAQ_data/
**Purpose**: Download intraday price data at a 5-minute frequency for the respective stocks.

#### Code Files:
- **a01_pull_TAQ_intraday.py**: Query used in WRDS Jupyter lab to pull pricing data from TAQMSEC (2004-2024)
- **a02_pull_pre2004_TAQ_intraday.sas**: Query used in WRDS SAS Studio to pull pricing data from TAQ (1994-2003)

### a03_intraday_SP500_replication/
**Purpose**: Create the final dataset

#### Code Files:
- **a01_createSP500_5min.do**: Processes S&P 500 benchmark data from TRTH
    - Imports and cleans Thomson Reuters Tick History (TRTH) S&P 500 data
    - Creates 5-minute interval benchmark for validation