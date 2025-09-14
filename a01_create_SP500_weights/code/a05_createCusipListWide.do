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
**#Create Wide Format CUSIPs
*-------------------------------------------------------------------------------
use "cleaned_files/daily_processed_1994_2024", clear
sort cusip date_daily
drop p_ pw_

* Preserve the symbol columns
preserve
keep cusip sym1 sym2 sym3
duplicates drop
gen num_syms = !missing(sym1) + !missing(sym2) + !missing(sym3)
sort cusip num_syms sym1
by cusip: keep if _n == _N
save "temp_files/temp_syms.dta", replace
restore

preserve
keep cusip sym1 sym2 sym3
duplicates drop
gen long id = _n
reshape long sym, i(id) j(sym_col)
drop if missing(sym)
bysort cusip sym (id): keep if _n == 1
bysort cusip (sym): gen sym_index = _n
drop sym_col
drop id
reshape wide sym, i(cusip) j(sym_index)
rename sym* sym_*
save "temp_files/temp_syms.dta", replace
restore

* Preserve the name columns
preserve
keep cusip name
duplicates drop
sort cusip name
*within a cusip+name group, the row with empty name
* will always be first so taking last will get us the
* non-empty one
by cusip: keep if _n == _N
save "temp_files/temp_names.dta", replace
restore

* Drop sym/name for now
drop sym*
drop name

* Sort by CUSIP and date
sort cusip date_daily

* Identify starts
gen is_start = date_daily if cusip != cusip[_n-1] | date_daily > date_daily[_n-1] + 7
format is_start %td

* Identify ends
gen is_end = date_daily if (cusip != cusip[_n]) | (cusip == cusip[_n] & date_daily + 7 < date_daily[_n+1])
replace is_end = date_daily if cusip != cusip[_n+1]
format is_end %td

* Remove rows without start or end
keep if !missing(is_start) | !missing(is_end)

* Generate entry/exit periods
by cusip: gen sp_period = sum(!missing(is_start))

* Align starts and ends
gen is_start_final = is_start if !missing(is_start)
format is_start_final %td
bys cusip sp_period: replace is_start_final = is_start_final[_n-1] if missing(is_start_final)
drop is_start
order date_daily cusip is_start_final is_end sp_period
rename is_start_final is_start

* Drop unnecessary rows
keep if !missing(is_start) & !missing(is_end)

* Drop daily date
drop date_daily

* Reshape wide
reshape wide is_start is_end, i(cusip) j(sp_period)

* Rename for clarity
rename is_start* SP500_start_*
rename is_end* SP500_end_*

* Merge Sym
merge 1:1 cusip using "temp_files/temp_syms.dta"
drop _merge
merge 1:1 cusip using "temp_files/temp_names.dta"
drop _merge

order cusip name sym* SP500_start_1 SP500_end_1 SP500_start_2 SP500_end_2
save "cleaned_files/cusip_entry_exit_periods_1994_2024.dta", replace
*****************
*Fix anomalies in data
//export excel using "cleaned_files/cusip_entry_exit_periods_1994_2024.xlsx", sheet("Main") firstrow(variables)
use "source_data/WRDS/cusip_permco_linking.dta", clear
drop PrimaryExch PERMNO
rename CUSIP9 cusip9
format PERMCO %8.0g
drop if missing(cusip)
gen cusip6 = substr(cusip9, 1, 6)
duplicates drop
sort cusip9
order cusip9 cusip6 PERMCO
save "temp_files/WRDS/cusip_permco_linking.dta", replace

use "cleaned_files/cusip_entry_exit_periods_1994_2024.dta", clear
gen cusip6 = substr(cusip, 1, 6)
rename cusip cusip9
order cusip cusip6
merge m:m cusip9 using "temp_files/WRDS/cusip_permco_linking.dta"
drop if _merge == 2
keep if _merge == 1
sort SP500_start_1
*****************
use "source_data/WRDS/a_hdrc_cusip_permno_link.dta", clear
drop PrimaryExch PERMNO
rename HdrCUSIP9 cusip9
format PERMCO %8.0g
drop if missing(cusip)
gen cusip6 = substr(cusip9, 1, 6)
//drop cusip9
gen cusip8 = substr(cusip9, 1, 8)
duplicates drop
sort cusip9
order cusip9 cusip8 cusip6 PERMCO
save "temp_files/WRDS/a_hdrc_cusip_permno_link.dta", replace

use "cleaned_files/cusip_entry_exit_periods_1994_2024.dta", clear
gen cusip6 = substr(cusip, 1, 6)
gen cusip8 = substr(cusip, 1, 8)
rename cusip cusip9
order cusip9 cusip8 cusip6
merge m:1 cusip9 using "temp_files/WRDS/a_hdrc_cusip_permno_link.dta"
drop if _merge == 2
keep if _merge == 1
sort SP500_start_1
*****************
use "source_data/WRDS/a_ccm_cusip_permco_link.dta", clear
drop GVKEY LINKTYPE LPERMNO LINKDT LINKENDDT
rename LPERMCO permco
rename cusip cusip9
format PERMCO %8.0g
drop if missing(cusip)
gen cusip6 = substr(cusip, 1, 6)
//drop cusip9
gen cusip8 = substr(cusip, 1, 8)
duplicates drop
sort cusip9
order cusip9 cusip8 cusip6 permco
save "temp_files/WRDS/a_ccm_cusip_permco_link.dta", replace

use "cleaned_files/cusip_entry_exit_periods_1994_2024.dta", clear
gen cusip6 = substr(cusip, 1, 6)
gen cusip8 = substr(cusip, 1, 8)
rename cusip cusip9
order cusip9 cusip8 cusip6
merge m:1 cusip9 using "temp_files/WRDS/a_hdrc_cusip_permno_link.dta"
drop if _merge == 2
keep if _merge == 1
sort SP500_start_1
*****************
use "source_data/WRDS/crsp_taq_link.dta", clear
drop DATE match_lvl CUSIP
duplicates drop 
rename NCUSIP cusip8
drop if missing(cusip8)
//gen cusip6 = substr(cusip8, 1, 6)
duplicates drop
sort cusip8
save "temp_files/WRDS/crsp_taq_link.dta", replace

use "source_data/WRDS/m_crsp_taq_link.dta", clear
rename CUSIP cusip8
sort cusip8
//gen cusip6 = substr(cusip8, 1, 6)
duplicates drop
append using "temp_files/WRDS/crsp_taq_link.dta"
sort cusip8
drop if cusip8 == "0" | cusip8 == "00000000"
duplicates drop
sort cusip8 PERMNO
by cusip8: keep if _n == _N
save "temp_files/WRDS/full_crsp_taq_link.dta", replace


use "cleaned_files/cusip_entry_exit_periods_1994_2024.dta", clear
gen cusip6 = substr(cusip, 1, 6)
gen cusip8 = substr(cusip, 1, 8)
rename cusip cusip9
order cusip9 cusip8 cusip6
merge m:1 cusip8 using "temp_files/WRDS/full_crsp_taq_link.dta"
drop if _merge == 2
keep if _merge == 1
sort SP500_start_1
