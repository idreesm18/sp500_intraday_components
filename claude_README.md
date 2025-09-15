# S&P 500 Intraday Components Replication Project

This project reconstructs intraday S&P 500 index values using constituent stock prices and weights from 1994-2024. The codebase processes data from multiple sources (WRDS TAQ, CRSP, S&P/Dow Jones indices) to create a comprehensive dataset for financial research.

## Project Structure

### a01_create_SP500_weights/
**Purpose:** Creates S&P 500 constituent weights and manages symbol-CUSIP-PERMCO linkages

#### Code Files:
- **a01_downloadData.py**: Downloads S&P 500 constituent data from Bloomberg BDP and FAME databases
  - Pulls daily constituent data from 1994-2024
  - Exports to Stata format for downstream processing
  - Handles dual-class shares and CUSIP duplicates

- **a02_mergeTAQMaster.do**: Merges annual TAQ master files into unified dataset
  - Processes pre-2011 and post-2011 TAQ master files separately
  - Handles symbol/CUSIP changes and date format standardization
  - Creates complete daily symbol-CUSIP mapping

- **a03_createDailyDataset.do**: Creates final daily S&P 500 dataset with weights
  - Merges S&P constituent data with TAQ master symbols
  - Calculates price weights (pw_) for index reconstruction
  - Handles CUSIP/symbol mismatches through correction files
  - Validates market cap calculations against FAME data

- **a04_create_pre2003_sym_list.py**: Generates symbol lists for pre-2003 TAQ data retrieval
  - Extracts unique symbols by year from daily processed files
  - Creates text files for TAQ data filtering

- **a05_createCusipListWide.do**: Creates wide format CUSIP entry/exit periods
  - Identifies S&P 500 entry and exit dates for each CUSIP
  - Links CUSIPs to PERMCO identifiers using multiple datasets
  - Handles corporate actions and identifier changes

- **a06_permcolinking.do**: Links CUSIPs to PERMCO/PERMNO identifiers
  - Uses TAQ linking files (1994-2024) with quality scores
  - Resolves multiple PERMNO matches to single PERMCO
  - Creates final PERMCO-CUSIP mapping for the entire period

### a02_download_TAQ_data/
**Purpose:** Downloads and preprocesses intraday TAQ price data

#### Code Files:
- **a01_pull_TAQ_intraday.py**: Downloads post-2004 TAQ data from WRDS
  - Pulls 5-minute aggregated prices using SQL queries
  - Filters by S&P 500 constituent symbols
  - Handles symbol root/suffix combinations
  - **Note:** Requires WRDS JupyterLab environment

- **a02_pull_pre2004_TAQ_intraday.sas**: Downloads pre-2004 TAQ data using SAS
  - Processes daily TAQ files for 1994-2003
  - Creates 5-minute price buckets from tick data
  - **Note:** Requires WRDS SAS Studio environment

- **a03_compress_TAQ_intraday.do**: Standardizes and compresses TAQ data format
  - Combines symbol root/suffix into single symbol field
  - Creates datetime variables in 5-minute intervals
  - Standardizes actual close time formatting across all years

### a03_intraday_SP500_replication/
**Purpose:** Reconstructs intraday S&P 500 index values

#### Code Files:
- **a01_createSP500_5min.do**: Processes S&P 500 benchmark data from TRTH
  - Imports and cleans Thomson Reuters Tick History (TRTH) S&P 500 data
  - Creates 5-minute interval benchmark for validation
  - Handles timezone adjustments and data formatting

- **a02_cleanTaqData5min.do**: Cleans and standardizes TAQ intraday data
  - Processes 2011-2024 TAQ data with corrections
  - Appends patch files for data quality issues
  - Standardizes datetime formatting across all years

- **a03_createIntradayPrices_5min.do**: Merges daily weights with intraday prices
  - Combines S&P 500 weights with TAQ price data
  - Forward-fills missing prices within trading sessions
  - Calculates intraday S&P 500 index values
  - Validates against TRTH benchmark data
  - **Key Issue:** Early years (1994-1995) use different TRTH data source

- **a04_fix_TRTH_data.do**: Handles missing TRTH benchmark data
  - Identifies early close trading days (holidays)
  - Forward-fills missing TRTH values
  - Recalculates percentage differences after adjustments

- **a05_create_composite_prices.do**: Creates composite prices for dual-class shares
  - Handles companies with multiple share classes (GOOG/GOOGL, etc.)
  - Weight-averages prices across share classes
  - Covers major corporate restructurings (AT&T, US West, Sprint)
  - **Critical for accuracy:** Prevents double-counting of market cap

- **a06_createWideFormatIntraday.do**: Converts to wide format and adds metadata
  - Reshapes data for time-series analysis (dates × stocks)
  - Links PERMCO identifiers to id_ticker symbols
  - Adds descriptive labels for variables
  - Creates final analytical dataset

- **a0Y_fix_missing_TAQ_data.do**: Debugging script for missing TAQ data
  - Identifies specific cases of missing price data
  - Used for data quality validation and corrections

### a04_preliminary_analysis/
**Purpose:** Data quality analysis and preliminary research

#### Code Files:
- **a01_idrees_analysis.do**: Comprehensive data quality assessment
  - Calculates daily stock counts and coverage statistics
  - Analyzes percentage differences between calculated and benchmark S&P 500
  - Identifies missing TAQ data vs. S&P constituent data
  - Creates FOMC meeting analysis framework
  - **Key Findings:** Documents data quality issues by time period

## Data Flow

1. **Download constituent data** (a01) → **Merge TAQ master** (a02) → **Create daily dataset** (a03)
2. **Download TAQ intraday data** (a02 folder) → **Clean and standardize** (a03 folder)
3. **Merge weights with prices** → **Handle dual-class shares** → **Create final dataset**
4. **Quality assessment** and **analysis-ready formats**

## Key Data Quality Issues

### Missing TAQ Data
- Some S&P 500 constituents lack corresponding TAQ price data
- Tracking stocks and certain corporate structures cause gaps
- Pre-2004 data has different availability patterns

### Dual-Class Share Handling
- Companies like Google (GOOG/GOOGL) require composite pricing
- Corporate actions (AT&T, Sprint) need special treatment
- Weight adjustments prevent market cap double-counting

### Benchmark Validation
- TRTH S&P 500 data missing for certain periods (1994-1995)
- Early close days require special handling
- Percentage differences calculated for quality control

## Important Files and Directories

### Source Data:
- `source_data/SP_DJ_Indices/`: S&P 500 constituent and index data
- `source_data/WRDS/`: TAQ master files and linking datasets
- `source_data/CRSP/`: CRSP index data for validation

### Cleaned Files:
- `cleaned_files/daily_processed_1994_2024.dta`: Final daily dataset with weights
- `cleaned_files/final_intraday_SP500_5_min.dta`: Complete intraday dataset
- `cleaned_files/wide_intraday_SP500_5_min.dta`: Wide format for analysis

### Correction Files:
- Multiple CSV files handling symbol/CUSIP mismatches
- Manual corrections for data quality issues
- Composite price definitions for dual-class shares

## Technical Notes

### Software Requirements:
- Python 3.x with pandas, pyodbc, pyfame
- Stata (version supporting large datasets)
- SAS (for pre-2004 data processing)
- WRDS access for TAQ data download

### Data Dependencies:
- Bloomberg BDP access for constituent data
- WRDS TAQ database subscription
- FAME database access
- Thomson Reuters Tick History (TRTH) for benchmark

### Path Configuration:
All scripts handle multiple user environments with hostname-based path switching for Windows/Unix systems.

## Usage Notes

1. **Sequential Processing Required:** Scripts must be run in numerical order within each folder
2. **External Data Sources:** Many scripts require institutional database access
3. **Memory Requirements:** Large datasets require substantial RAM for processing
4. **Validation Recommended:** Always check percentage differences against benchmarks

This project represents a comprehensive effort to reconstruct high-quality intraday S&P 500 data suitable for academic and institutional research applications.