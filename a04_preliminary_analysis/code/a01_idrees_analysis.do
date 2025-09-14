set more off
clear all

*-------------------------------------------------------------------------------
*** Preliminaries
*-------------------------------------------------------------------------------
display "`c(hostname)'"
display "`c(pwd)'"

** Niklas' path
if "`c(hostname)'" == "IFHTNK00W10" | "`c(hostname)'" == "IFHTNK00W10A" {
            cd "Z:\research-gmsm\Kroner\SP500_intraday_replication\a04_preliminary_analysis"
            }
** Idrees' Path
else {
 cd "/if/research-gmsm/Kroner/SP500_intraday_replication/a04_preliminary_analysis"
 }
*-------------------------------------------------------------------------------
*** Basic Summary Stats
*------------------------------------------------------------------------------- 
use "../a03_intraday_SP500_replication/cleaned_files/final_intraday_SP500_5_min.dta", clear

bysort Date_ET: gen stocks_per_full = _N
egen permco_count = nvals(permco), by(Date_ET)

//gen date = dofc(Date_ET)
//keep if date == mdy(11, 1, 2001)


*Max daily stocks per
gen date = dofc(Date_ET)
format date %td
order Date_ET date

bysort date (Date_ET): egen max_daily_permcos = max(permco_count)
//bysort date (Date_ET): egen max_daily_stocks_per_full = max(stocks_per_full)
bysort date (Date_ET): egen max_daily_stocks_per = max(stocks_per)
bysort date (Date_ET): egen avg_daily_percent_diff = mean(percent_diff)
bysort date (Date_ET): egen median_daily_percent_diff = median(percent_diff)

preserve
drop if hh(Date_ET) == 9 & mm(Date_ET) == 30 & ss(Date_ET) == 0
collapse (mean) avg_daily_n93_percent_diff = percent_diff ///
         (median) median_daily_n93_percent_diff = percent_diff, by(date)
tempfile stats
save `stats'
restore
merge m:1 date using `stats', keepusing(avg_daily_n93_percent_diff median_daily_n93_percent_diff) nogenerate

keep date max_daily_permcos max_daily_stocks_per avg_daily_percent_diff median_daily_percent_diff avg_daily_n93_percent_diff median_daily_n93_percent_diff 

gen stocks_per_diff = max_daily_permcos - max_daily_stocks_per

duplicates drop

save "cleaned_files/0925_data_quality_stats.dta", replace

*-------------------------------------------------------------------------------
**# Filter on FOMC
*-------------------------------------------------------------------------------
*Get FOMC times, filter on those
*Use PW to differentiate top 10 vs bottom 490
*Somehow adjust it to be general time + general top 10
*Get movements, plot caverage movements

use "../a03_intraday_SP500_replication/cleaned_files/wide_intraday_SP500_5_min.dta", clear

*One meeting test
gen double fomc_window_start = clock("19mar2024 14:00:00", "DMYhms")
gen double fomc_window_end   = clock("21mar2024 14:00:00", "DMYhms")
keep if inrange(Date_ET, fomc_window_start, fomc_window_end)
drop fomc_window_start fomc_window_end

*Drop missing columns
ds, has(type numeric) 
foreach var of varlist `r(varlist)' {
    count if !missing(`var')
    if r(N) == 0 {
        drop `var'
    }
}

*Get the top 10 weighted components
ds pw_*
local pwvars `r(varlist)'
ds price_*
local pricevars `r(varlist)'
reshape long pw_ price_, i(Date_ET) j(permco)

bysort Date_ET (pw_): gen rank = _n
bysort Date_ET (pw_): replace rank = _N - _n + 1
gen top10 = rank <= 10

*Adjust dataset for plotting
sort permco Date_ET
bysort permco (Date_ET): gen base_price = price_[1]
collapse (mean) indexed_price, by(Date_ET top10)
bysort Date_ET: gen byte first_obs = _n == 1
gen tick = sum(first_obs)



twoway (line indexed_price tick if top10 == 1, lcolor(blue) lwidth(medium) ///
        ) (line indexed_price tick if top10 == 0, lcolor(red) lwidth(medium)), ///
        legend(label(1 "Top 10") label(2 "Rest")) ///
        ytitle("Indexed Price (Base=100)") xtitle("Datetime") ///
        title("Price Movement: Top 10 vs Rest")

************
*-------------------------------------------------------------------------------
*** Daily Average Stocks - Greater than 500
*------------------------------------------------------------------------------- 
use "cleaned_files/data_quality_stats.dta", clear
keep date max_daily_stocks_per_full

sort date
gsort -max_daily date


use "../a01_create_SP500_weights/cleaned_files/permco_entry_exit_periods_1994_2024.dta", clear
gen date_test = mdy(4,3,2020)
keep if SP500_start_1 == date_test | SP500_end_1 == date_test
sort SP500_start_1
*-------------------------------------------------------------------------------
*** Daily Average Stocks - Less than 500
*------------------------------------------------------------------------------- 
use "cleaned_files/HUE_daily_comp_20041208.dta", clear
rename effective_date date
replace date = dofc(date)
format date %td
keep date company cusip 
save "cleaned_files/HUE_daily_comp_20041208_clean.dta", replace

*Seeing which stocks we don't have current prices for
use "../a03_intraday_SP500_replication/cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_1994.dta", clear
gen date = dofc(Date_ET)
format date %td
order date Date_ET
keep if Date_ET == clock("15jun1994 16:00:00", "DMYhms")
sort stocks_per

*Seeing which stocks are missing for an extended period of time - tracking stocks
use "../a03_intraday_SP500_replication/cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_2001.dta", clear
gen date = dofc(Date_ET)
format date %td
order date Date_ET
keep if Date_ET == clock("01nov2001 16:00:00", "DMYhms")
sort stocks_per
merge 1:1 cusip using "cleaned_files/HUE_daily_comp_20011101_clean.dta"
save "../a01_create_SP500_weights/documentation/data_quality_test_files/tracking_stock_example.dta"

*Seeing which stocks are missing for an extended period of time - missing TAQ data
use "../a03_intraday_SP500_replication/cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_2004.dta", clear
gen date = dofc(Date_ET)
format date %td
order date Date_ET
keep if date == mdy(12, 8, 2004)
keep if Date_ET == clock("08dec2004 16:00:00", "DMYhms")
sort stocks_per
merge 1:1 cusip using "cleaned_files/HUE_daily_comp_20041208_clean.dta"

*-------------------------------------------------------------------------------
*** LEGACY: DO NOT USE: Find missing data from TAQ vs HUE
*------------------------------------------------------------------------------- 
use "../a01_create_SP500_weights/source_data/SP_DJ_Indices/HUE_daily_1994_2024.dta", clear
gen date = dofc(effective_date)
format date %td
order date effective_date
drop effective_date
keep date company cusip
drop if missing(cusip)
save "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/HUE_daily_1994_2024.dta", replace

use "../a03_intraday_SP500_replication/cleaned_files/final_intraday_SP500_5_min.dta", clear
gen date = dofc(Date_ET)
format date %td
order Date_ET date
keep date permco cusip
duplicates drop
duplicates report date cusip

//egen permco_count = nvals(permco), by(Date_ET)

*Max daily permcos per
// gen date = dofc(Date_ET)
// format date %td
// order Date_ET date
// bysort date (Date_ET): egen max_daily_permcos = max(permco_count)

merge m:1 date using "cleaned_files/0925_data_quality_stats.dta"
keep date permco cusip max_daily_permcos

merge 1:1 date cusip using "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/HUE_daily_1994_2024.dta"
bysort date (max_daily_permcos): replace max_daily_permcos = max_daily_permcos[_n-1] if missing(max_daily_permcos)
keep if date <= mdy(8, 26, 2024)
keep if _merge == 2
drop if company == "Google Inc A" | company == "Alphabet Inc A"
drop if company == "Discovery Communications Inc" | company == "Discovery Communications Inc C" | company == "Discovery, Inc C"
drop if company == "News Corp A" | company == "Twenty-First Century Fox Inc A" | company == "Fox Corp A"
drop if company == "Under Armour Inc-C" | company == "Under Armour Inc A"
drop if company == "Comcast Corp A Spl"
drop if company == "Sprint Corp. PCS" | company == "WorldCom Inc.-WorldCom Group" | company == "US West Inc."

save "cleaned_files/0925_current_missing_taq_data", replace
export excel using "cleaned_files/0925_current_missing_taq_data.xlsx", firstrow(variables)

*Go through missing companies
use "cleaned_files/0925_current_missing_taq_data", clear

*-------------------------------------------------------------------------------
*** Missing Hue vs TAQ - Using finalized daily processed
*------------------------------------------------------------------------------- 
use "../a01_create_SP500_weights/cleaned_files/daily_processed_1994_2024", replace
keep date_daily cusip name sym*
rename date_daily date
rename name company
save "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_processed_1994_2024.dta", replace

use "../a03_intraday_SP500_replication/cleaned_files/final_intraday_SP500_5_min.dta", clear
gen date = dofc(Date_ET)
format date %td
order Date_ET date
keep date permco cusip
duplicates drop
duplicates report date cusip

merge m:1 date using "cleaned_files/0925_data_quality_stats.dta"
keep date permco cusip max_daily_permcos

merge 1:1 date cusip using "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_processed_1994_2024.dta"
bysort date (max_daily_permcos): replace max_daily_permcos = max_daily_permcos[_n-1] if missing(max_daily_permcos)
keep if date <= mdy(8, 26, 2024)
keep if _merge == 2
*Drop dual class or dual holding stock occurances as we coalesce these into one stock, so each entry will be missing one according to daily_processed_1994_2024
drop if company == "GOOGLE, INC. CLASS A" | company == "Alphabet Inc. Class A Common S" | company == "ALPHABET INC. CLASS A COMMON S" | company == "Google Inc A"
drop if company == "Discovery Communications Inc S" | company == "DISCOVERY COMMUNICATIONS INC S" | company == "Discovery, Inc. Series C Commo"
drop if company == "News Corporation Class A Commo" | company == "NEWS CORPORATION CLASS A COMMO" | company == "Twenty-First Century Fox, Inc." | company == "TWENTY-FIRST CENTURY FOX, INC." | company == "Fox Corporation Class A Common"
drop if company == "Under Armour, Inc." | company == "Under Armour Inc" | company == "UNDER ARMOUR, INC." | company == "Under Armour Inc A"
drop if company == "COMCAST CORP" | company == "Comcast Corp A Spl"
drop if company == "SPRINT CORP PCS GRP  WI" |company == "SPRINT CRP  PCS GRP" | company == "SPRINT CORP  PCS GRP  W.I." | company == "WorldCom Inc.-WorldCom Group" | company == "WORLDCOM INC WORLDCOM GR COM" | company == "U.S.WEST COMMUNICATION GRP DE" | company == "U.S.WEST INC" | company == "US WEST COMM GRP DEL  WI" | company == "US WEST INC"

save "cleaned_files/0925_current_missing_taq_data", replace
export excel using "cleaned_files/0925_current_missing_taq_data.xlsx", firstrow(variables)

*-------------------------------------------------------------------------------
*** Compare Daily Processed VS HUE main
*------------------------------------------------------------------------------- 
use "../a01_create_SP500_weights/cleaned_files/daily_processed_1994_2024", clear
keep date_daily cusip name
rename date_daily date
rename name company
save "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_processed_1994_2024.dta", replace

use "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/HUE_daily_1994_2024.dta", clear
keep if date <= mdy(8, 26, 2024)
merge 1:1 date cusip using "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_processed_1994_2024.dta"
keep if _merge != 3

*-------------------------------------------------------------------------------
*** Compare Daily Processed VS HUE TOTALS
*------------------------------------------------------------------------------- 
use "../a01_create_SP500_weights/cleaned_files/daily_processed_1994_2024", clear
keep date_daily cusip name
rename date_daily date
rename name company
gen one = 1
collapse (sum) one, by(date)
rename one n_cusips
save "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_processed_1994_2024_total_counts.dta", replace

use "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/HUE_daily_1994_2024.dta", clear
keep if date <= mdy(8, 26, 2024)
gen one = 1
collapse (sum) one, by(date)
rename one n_cusips_HUE
merge 1:1 date using "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_processed_1994_2024_total_counts.dta"
drop _merge
gen diff = n_cusips_HUE - n_cusips
gsort -diff date
save "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/0925_hue_vs_daily_processed_total_counts.dta", replace


*-------------------------------------------------------------------------------
*** Check entries in HUE missing in Daily Processed not explained by hue_delist_after
*------------------------------------------------------------------------------- 
use "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/HUE_daily_1994_2024.dta", clear
keep if date <= mdy(8, 26, 2024)
merge 1:1 date cusip using "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_processed_1994_2024.dta"
keep if date == mdy(7, 5, 2012) | date == mdy(3, 4, 2015) | date == mdy(10, 2, 2015) | date == mdy(12, 11, 2015) | date == mdy(1, 2, 2018)
keep if _merge != 3
sort date
