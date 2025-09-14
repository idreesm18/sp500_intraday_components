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
*** 
*-------------------------------------------------------------------------------
local month_str = "07"

di `month_str'

use "../a01_create_SP500_weights/cleaned_files/daily_processed_yearly_dta/daily_processed_2004", clear

keep if month(date) == real("`month_str'") & year(date) == 2004
drop p_

reshape long sym, i(date_daily cusip pw_) j(sym_num)
drop if sym == ""
keep if sym1 == "RF"

merge 1:m date_daily sym using "temp_files/WRDS/TAQ_monthly/TAQ_monthly_`year'/`file'",keepusing(datetime time price actual_close actual_close_micro actual_close_total) keep(3)

drop _merge
sort date_daily time sym

*If not zero, we need to make the adjustments below
duplicates list date_daily cusip time  
*In cases where there are repeated tickers per cusip, here we would
* make adjustments for that to select the true close time_m
bysort datetime cusip (actual_close_total): keep if _n == _N
*Now that we only have one obs per datetime+cusip, we don't have a
*  need for sym_num and can set it all to 1
replace sym_num = 1
***
*This should be zero now
duplicates list date_daily cusip time 

*Get the number of actual stock obs this interval before
*  we forward fill
bysort datetime: gen stocks_per = _N

***Forward fill missing values within a month
sort datetime
encode cusip, gen(cusip_id)
tsset cusip_id datetime, delta(5 minutes)
tsfill
sort datetime

*Additional edits (replacing date and time, forward filling
* other columns) related to tsfill 
drop date_daily
gen date_daily = dofc(datetime)
format date_daily %td
order date_daily, first
drop if dow(date_daily) == 0 | dow(date_daily) == 6
drop if hh(datetime) < 9 | (hh(datetime) == 9 & mm(datetime) < 30)
drop if hh(datetime) > 16 | (hh(datetime) == 16 & mm(datetime) > 0)

drop time
gen double time_sec = hh(datetime)*3600 + mm(datetime) * 60 + ss(datetime)
gen double time = mdyhms(1,1,1960,0,0,0)*1000 + time_sec*1000
format time %tcHH:MM
order time, after(datetime)
drop time_sec

bysort date_daily cusip_id (time): replace cusip = cusip[_n-1] if missing(cusip)
bysort date_daily cusip_id (time): replace price = price[_n-1] if missing(price)
bysort date_daily cusip_id (time): replace pw_ = pw_[_n-1] if missing(pw_)
bysort date_daily cusip_id (time): replace sym_num = sym_num[_n-1] if missing(sym_num)
bysort date_daily cusip_id (time): replace sym = sym[_n-1] if missing(sym)
bysort date_daily cusip_id (time): replace datetime = datetime[_n-1] + 5 * 60000 if missing(datetime)
bysort date_daily cusip_id (time): replace actual_close = actual_close[_n-1] if missing(actual_close)
bysort date_daily cusip_id (time): replace actual_close_micro = actual_close_micro[_n-1] if missing(actual_close_micro)
bysort date_daily cusip_id (time): replace actual_close_total = actual_close_total[_n-1] if missing(actual_close_total)
drop cusip_id sym_num shares_outstanding market_cap actual_close_micro actual_close_total

compress
save "temp_files/WRDS/partial_data/partial_data_`year'/partial_`year'`month_str'", replace



