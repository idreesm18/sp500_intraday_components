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
*** S&P 500
*-------------------------------------------------------------------------------
*shell "C:\Program Files\7-Zip\7z.exe" e "SPX.csv.gz"

import delimited "source_data/TRTH/SPX.csv", clear

* Create date variable	 
gen aux_date1 = substr(datetime,1,10) 
gen aux_date2 = substr(datetime,12,8)
gen double Date_ET = clock(aux_date1 + " " + aux_date2, "YMD hms")
format Date_ET %tc
replace Date_ET = Date_ET + (gmtoffset+1)*60*60*1000

*Clean
drop ric domain datetime gmtoffset type aux_date1 aux_date2 open aliasunderlyingric volume

*Take the last price in a given minute
rename last SP500_TRTH

order Date_ET
label variable Date_ET "Date & Time (ET)"

* Keep only 5-minute invervals
gen date_minutes = mm(Date_ET)
keep if mod(date_minutes,5) == 0
drop date_minutes

tsset Date_ET, delta(5 minute)

save "temp_files/TRTH/SP500_5min", replace

*-------------------------------------------------------------------------------
*** Test: compare closing with daily data
*-------------------------------------------------------------------------------
use "temp_files/TRTH/SP500_5min", replace

gen date_daily = dofc(Date_ET)
format date_daily %td

collapse (last) Date_ET SP500_TRTH, by(date_daily)

merge 1:1  date_daily using "../a02_create_SP500_weights/temp_files/SP_DJ_Indices/FAME_SP500_daily_2024"  
keep if _merge == 3
drop _merge

gen double diff = round(SP500_TRTH - SP500_HUE,0.01)
