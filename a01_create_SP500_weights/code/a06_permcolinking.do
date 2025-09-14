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
***Import assumption: We can link a cusip to any of its PERMNOs
*   because we will eventually link it to the PERMCO and all PERMNOs
*   should link to the correct PERMCO

*PERMNO-PERMCO Link
use "source_data/WRDS/cusip_permco_linking/permno_permco.dta", clear
drop PrimaryExch
format PERMNO %8.0g
format PERMCO %8.0g
rename (PERMNO PERMCO) (permno permco)
duplicates drop
save "temp_files/WRDS/cusip_permco_linking/permno_permco.dta", replace

*TAQ (1994-2009) Linking Dataset
*  Score: 0 = CUSIP+Name
*         1 = CUSIP
*         2 = Ticker+Name
*         3 = Ticker
use "source_data/WRDS/cusip_permco_linking/m_taq_link.dta", clear
drop if FDATE >= mdy(1, 1, 2010)
drop FDATE
duplicates drop
sort SYMBOL CUSIP_FULL PERMNO SCORE
rename (PERMNO SYMBOL CUSIP_FULL SCORE) (permno symbol cusip12 score)
gen cusip = substr(cusip12, 1, 9)
drop cusip12 symbol
duplicates drop
drop if score > 1
order permno cusip score
sort cusip score
by cusip: keep if _n == 1
******No cusip8 matches to multiple permnos (keep FDATE if this code is run)
//egen permno_count = tag(cusip8 permno)
//egen unique_permno_count = total(permno_count), by(cusip8)
//keep if unique_permno_count > 1
******
drop score
save "temp_files/WRDS/cusip_permco_linking/m_taq_link.dta", replace

*Daily TAQ (2010-2024) Linking Dataset
*  Linked by CUSIP
use "source_data/WRDS/cusip_permco_linking/d_taq_link.dta", clear
drop symbol date1 date2
order permno cusip
duplicates drop
append using "temp_files/WRDS/cusip_permco_linking/m_taq_link.dta"
duplicates drop
******Two cusip8 which appear in sp500 with multiple permnos
*       - 54414710: PERMNO 89303 removed, 17279 added on 11jun2008
* 	   - This enters sp500 on 11jun2008, so we use 17279
*       - G491BT10: Both PERMNOs (19583, 81910) map to PERMCO 30929
*          - Use 19583 but doesn't matter for PERMCO purposes
//egen permno_count = tag(cusip8 permno)
//egen unique_permno_count = total(permno_count), by(cusip8)
//keep if unique_permno_count > 1
******
sort cusip permno 
by cusip: keep if _n == 1
merge m:1 permno using "temp_files/WRDS/cusip_permco_linking/permno_permco.dta"
*******Four rows do not have a linking PERMCO, but none of these show up
*        in the sp500
drop if _merge != 3
drop _merge
save "temp_files/WRDS/cusip_permco_linking/full_taq_link.dta", replace

*Merge PERMNOs into CUSIP_wide 
use "cleaned_files/cusip_entry_exit_periods_1994_2024.dta", clear
//drop if SP500_start_1 >= mdy(1, 1, 2015)
merge m:1 cusip using "temp_files/WRDS/cusip_permco_linking/full_taq_link.dta"
drop if _merge != 3
drop _merge permno
order permco cusip
sort permco name SP500_start_1
save "cleaned_files/0826_permco_entry_exit_periods_1994_2024.dta", replace
export excel using "cleaned_files/08_26_permco_entry_exit_periods_1994_2024.xlsx", sheet("Main") firstrow(variables)
//export excel using "cleaned_files/permco_entry_exit_periods_1994_2024.xlsx", sheet("Main") firstrow(variables)
********************************************************************************

