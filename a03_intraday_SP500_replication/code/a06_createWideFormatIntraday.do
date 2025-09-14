set more off
clear all

*-------------------------------------------------------------------------------
*** Preliminaries
*-------------------------------------------------------------------------------
display "`c(hostname)'"
display "`c(pwd)'"

** Niklas' path
if "`c(hostname)'" == "IFHTNK00W10" | "`c(hostname)'" == "IFHTNK00W10A" {
            cd "Z:\research-gmsm\Kroner\SP500_intraday_replication\a03_intraday_SP500_replication"
            }
** Idrees' Path
else {
 cd "/if/research-gmsm/Kroner/SP500_intraday_replication/a03_intraday_SP500_replication"
 }
*-------------------------------------------------------------------------------
*** Add PERMCO
*-------------------------------------------------------------------------------
use "../a01_create_SP500_weights/cleaned_files/0826_permco_entry_exit_periods_1994_2024.dta", clear
keep permco cusip

tempfile tempf
save `tempf'

use "cleaned_files/final_intraday_SP500_5_min", clear
//drop permco
merge m:1 cusip using `tempf'
drop if _merge != 3
drop _merge
order Date_ET permco
sort Date_ET sym
save "cleaned_files/final_intraday_SP500_5_min", replace

*-------------------------------------------------------------------------------
*** Add id_ticker
*-------------------------------------------------------------------------------
import delimited using "temp_files/corrections/manual_id_ticker_assignments.csv", clear
tempfile corr
save `corr'

use "cleaned_files/final_intraday_SP500_5_min", clear
drop id_ticker
sort permco Date_ET
by permco: gen id_ticker = sym[_N]
merge m:1 permco using `corr', nogen
replace id_ticker = new_id_ticker if !missing(new_id_ticker)
drop new_id_ticker
order Date_ET id_ticker
save "cleaned_files/final_intraday_SP500_5_min", replace

*-------------------------------------------------------------------------------
*** Save id_ticker-permco linking
*-------------------------------------------------------------------------------
use "cleaned_files/final_intraday_SP500_5_min", clear
keep permco id_ticker
duplicates drop
tempfile temp
save `temp'

use "../a01_create_SP500_weights/cleaned_files/0826_permco_entry_exit_periods_1994_2024.dta", clear
merge m:1 permco using `temp', nogen
order id_ticker
save "../a01_create_SP500_weights/cleaned_files/0826_permco_entry_exit_periods_1994_2024.dta", replace
export excel using "../a01_create_SP500_weights/cleaned_files/0826_permco_entry_exit_periods_1994_2024.xlsx", sheet("Main") firstrow(variables)

*-------------------------------------------------------------------------------
*** Convert intraday prices to wide
*-------------------------------------------------------------------------------
use "cleaned_files/final_intraday_SP500_5_min", clear
rename price p_
rename pw_ w_
keep Date_ET id_ticker p_ w_
sort Date_ET id_ticker
reshape wide p_ w_, i(Date_ET) j(id_ticker) string
save "cleaned_files/wide_intraday_SP500_5_min.dta", replace

*Add SP500 Totals
use "cleaned_files/final_intraday_SP500_5_min", clear
keep Date_ET SP500_intraday SP500_TRTH
duplicates drop
tempfile temp
save `temp'
use "cleaned_files/wide_intraday_SP500_5_min.dta", clear
merge 1:1 Date_ET using `temp', nogen
save "cleaned_files/wide_intraday_SP500_5_min.dta", replace

*Add labels to columns
import delimited "temp_files/corrections/ticker_labels.csv", varnames(1) stringcols(_all) clear

tempfile labelfile
file open fh using `labelfile', write text replace

forvalues i = 1/`=_N' {
    local v = name[`i']
    local l = label[`i']
    * Escape any quotes inside the label text
    local l = subinstr("`l'", `"""', `"""""', .)

    file write fh `"capture confirm variable `v'"' _n
    file write fh `"if !_rc label variable `v' "`l'""' _n
}

file close fh

di "`labelfile'"
type "`labelfile'"

use "cleaned_files/wide_intraday_SP500_5_min.dta", clear

* Apply the labels
do `labelfile'
save "cleaned_files/wide_intraday_SP500_5_min.dta", replace


// duplicates tag Date_ET id_ticker, gen(_dup)
// list if _dup > 0
// keep if _dup > 0
// keep permco
// duplicates drop



***Check which id_ticker-permco obs are 
// preserve
// * Collapse down to unique permco?id_ticker pairs
// bysort permco (id_ticker): keep if _n == 1
//
// * Count how many permcos share each id_ticker
// bysort id_ticker: gen n_permcos = _N
//
// * List only the id_tickers that are shared
// list id_ticker permco if n_permcos > 1, sepby(id_ticker)
// restore

