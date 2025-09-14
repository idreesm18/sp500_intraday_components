set more off
clear all

*-------------------------------------------------------------------------------
*** Preliminaries
*-------------------------------------------------------------------------------
display "`c(hostname)'"
display "`c(pwd)'"

** Niklas' path
if "`c(hostname)'" == "IFHTNK00W10" | "`c(hostname)'" == "IFHTNK00W10A" {
            cd "Z:\research-gmsm\Kroner\SP500_intraday_replication\a01_create_SP500_weights"
            }
** Idrees' Path
else {
 cd "/if/research-gmsm/Kroner/SP500_intraday_replication/a01_create_SP500_weights"
 }

*-------------------------------------------------------------------------------
*** Prepare Daily S&P 500 
*-------------------------------------------------------------------------------		
use "source_data/SP_DJ_Indices/FAME_SP500_daily_1994_2024", clear

replace mrkt_cap_tot = mrkt_cap_tot * 1000000
replace divisor = divisor * 1000000
replace date_daily = dofc(date_daily)
format mrkt_cap_tot %17.0g
format divisor %17.0g
format date_daily %td
rename mrkt_cap_tot mrkt_cap_tot_hue
rename SP500 SP500_HUE

save "temp_files/SP_DJ_Indices/FAME_SP500_daily_1994_2024", replace

*-------------------------------------------------------------------------------
*** Prepare Daily S&P 500 1994 to 1995 (TRTH Replacement)
*-------------------------------------------------------------------------------		
use "source_data/SP_DJ_Indices/daily_sp_1994_1995", clear

replace date_daily = dofc(date_daily)
format date_daily %td

save "temp_files/SP_DJ_Indices/daily_sp_1994_1995", replace

*-------------------------------------------------------------------------------
*** Prepare CRSP Daily S&P 500
*-------------------------------------------------------------------------------		
use "source_data/CRSP/SP500_daily_1994_2024", clear

rename caldt date_daily
rename totval mrkt_cap_tot_crsp
rename spindx SP500_CRSP

save "temp_files/CRSP/SP500_daily_1994_2024", replace

*-------------------------------------------------------------------------------
*** Prepare TAQ Master 
*-------------------------------------------------------------------------------		
use "source_data/WRDS/TAQ_master", clear

rename date date_daily
rename symbol_15 sym
drop if missing(cusip)
drop if cusip == ""
drop if cusip == "000000000"
drop if cusip == "           ."
drop if cusip == "0"

*Handling CUSIP-Symbol matching. Check end of file for details (1)
duplicates drop

* Preserve the name columns
preserve
keep date_daily cusip name
duplicates drop
bys cusip date_daily: keep if _n == _N
save "temp_files/temp_cusip_name_match.dta", replace
restore
***

drop name
duplicates drop
bys date_daily cusip (sym): gen sym_id = _n
reshape wide sym, i(date_daily cusip) j(sym_id)
merge 1:1 date_daily cusip using "temp_files/temp_cusip_name_match.dta"
drop _merge

***Replace Master sym's which do not match CT sym's on specific days***
preserve
import delimited using "temp_files/WRDS/incorrect_taq/taq_master_ct_mismatches.csv", clear varnames(1) stringcols(2)
gen double date_stata = daily(date, "MDY")
format date_stata %td
drop date
rename date_stata date_daily
order date_daily

keep date cusip correct_sym1
tempfile corr
save `corr'
restore

merge 1:1 date cusip using `corr', nogen keep(master match)
replace sym1 = correct_sym1 if !missing(correct_sym1)
drop correct_sym1

***Add entries missing in TAQ master which are in hue master and TAQ CT***
preserve
import delimited using "temp_files/WRDS/incorrect_taq/missing_taq_mast_ct.csv", clear varnames(1) stringcols(2)
gen double date_stata = daily(date, "MDY")
format date_stata %td
drop date
rename date_stata date_daily
order date_daily
tempfile corr
save `corr'
restore
append using `corr'
sort date_daily cusip

preserve
import delimited using "temp_files/WRDS/incorrect_taq/taq_master_early_delist.csv", clear varnames(1) stringcols(2)
gen double date_stata = daily(date, "MDY")
format date_stata %td
drop date
rename date_stata date_daily
order date_daily
tempfile corr
save `corr'
restore
append using `corr'
sort date_daily cusip

save "temp_files/WRDS/TAQ_master", replace

*-------------------------------------------------------------------------------
*** Compare FAME S&P 500 MC to CRSP
*-------------------------------------------------------------------------------
// use "temp_files/SP_DJ_Indices/FAME_SP500_daily_2003_2024", clear
//
// merge 1:1 date_daily using "temp_files/CRSP/SP500_daily_2003_2024"
//
// drop if _merge != 3
// drop _merge
//
// replace mrkt_cap_tot_crsp = mrkt_cap_tot_crsp * 1000
// format mrkt_cap_tot_crsp %17.0g
//
// gen mrkt_cap_diff = abs(mrkt_cap_tot_hue - mrkt_cap_tot_crsp)
// format mrkt_cap_diff %17.0g

*-------------------------------------------------------------------------------
**#Prepare Daily S&P 500 Const.
*-------------------------------------------------------------------------------
use "source_data/SP_DJ_Indices/HUE_daily_1994_2024", clear

*Questar has an o at the end of its cusip - treating as typo for now
replace cusip = "748356102" if cusip == "748356102o"

rename effective_date date_daily
rename local_price p_
rename index_shares shrout_hue
rename index_market_cap mrkt_cap
rename index_weight w_hue
replace date_daily = dofc(date_daily)
format date_daily %td

merge m:1 date_daily using "temp_files/SP_DJ_Indices/FAME_SP500_daily_1994_2024"

drop if _merge != 3
drop _merge
format mrkt_cap %14.0g

bys date: egen double mrkt_cap_tot_calc = sum(mrkt_cap)
format mrkt_cap_tot_calc %17.0g
gen double SP500_Calc = mrkt_cap_tot_calc / divisor
gen double pw_ = shrout_hue/divisor

****** Tests to double given market cap vs calculated market cap ******
//gen double mrkt_cap_diff = abs(mrkt_cap_tot_hue - mrkt_cap_tot_calc)
//format mrkt_cap_diff %17.0g
******************************
* Test 1: p_ * shrout_hue = mrkt_cap
//gen double mrkt_cap_test = p_ * shrout_hue
//format mrkt_cap_test %14.0g
//bys date: egen double mrkt_cap_tot_test = sum(mrkt_cap_test)
//format mrkt_cap_tot_test %17.0g
* Test 2: p*w_price = S&P 500
//gen double aux = p_ * pw_
//bys date: egen double SP500_aux = sum(aux)
***********************************************************************
drop company ticker shrout_hue mrkt_cap mrkt_cap_tot_hue divisor market_cap
drop SP500_HUE mrkt_cap_tot_calc SP500_Calc w_hue shares_outstanding
//drop mrkt_cap_diff mrkt_cap_test mrkt_cap_tot_test aux SP500_aux

drop if cusip == ""
drop if cusip == "000000000"
drop if cusip == "           ."
drop if cusip == "0"
*Sanity check, should be 0
duplicates list date_daily cusip

sort cusip date_daily
*reshape to wide for viewing
//reshape wide p_ pw_, i(date_daily) j(cusip) string

***Replace Hue cusip's which do not match TAQ Master cusips's on specific days***
**** There are 4 entries that didn't match, will return and check these if they are still
**** missing afterwards
preserve
import delimited using "temp_files/WRDS/incorrect_taq/hue_taq_master_mismatches.csv", clear varnames(1) stringcols(2)
gen double date_stata = daily(date, "MDY")
format date_stata %td
drop date
rename date_stata date_daily
order date_daily

rename incorrect_cusip cusip
tempfile corr
save `corr'
restore

merge 1:1 date_daily cusip using `corr'
drop if _merge == 2
drop _merge
replace cusip = correct_cusip if !missing(correct_cusip)
drop correct_cusip
***********************************************************************

merge m:1 date_daily cusip using "temp_files/WRDS/TAQ_master"
sort date_daily cusip

*Show cases of sp500 const not appearing in TAQ master:
* In 2024, there is a case of mismatched dates of consts
//drop if _merge != 1
drop if _merge != 3
drop _merge

ds sym*
local sym_vars `r(varlist)'

foreach var in `sym_vars' {
    quietly count if !missing(`var')
    if r(N) == 0 drop `var'
}

sort date_daily cusip

*******export to csv to get intraday price*******
compress
export delimited using "cleaned_files/daily_processed_1994_2024.csv", replace
save "cleaned_files/daily_processed_1994_2024", replace
****************************************************
forvalues year = 1994/2024 {
    use "cleaned_files/daily_processed_1994_2024", clear  // Load the original dataset
    keep if year(date_daily) == `year'    // Keep only rows for the current year
    compress
    export delimited using "cleaned_files/daily_processed_yearly_csv/daily_processed_`year'.csv", replace
    save "cleaned_files/daily_processed_yearly_dta/daily_processed_`year'", replace 
}

*****Fixing coding_error from missing_taq data tests*****
// keep if cusip == "7591EP100" | cusip == "67622P101" | cusip == "608554101" | cusip == "94973V107" | cusip == "35687M206" | cusip == "65248E104"  
// sort sym1 date_daily
// keep if date_daily < mdy(1, 1, 2005) & date_daily > mdy(1, 1, 2004)
// keep if sym1 == "RF" | sym2 == "RF"
// keep if symbol_15 == "RF"
