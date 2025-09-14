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
*** Compress pre-2004 data
*-------------------------------------------------------------------------------
local start_year = 1994
local end_year = 2003

use "../a02_download_TAQ_data/cleaned_files/TAQ_monthly_1994/TAQ_five_min_price_199401", clear
compress

forvalues year = `start_year'/`end_year' {
	local files: dir "../a02_download_TAQ_data/cleaned_files/TAQ_monthly_`year'/" files "*.dta"
	
	foreach file of local files {

		use "../a02_download_TAQ_data/cleaned_files/TAQ_monthly_`year'/`file'", clear

		*Combine sym_root/suffix to match master file sym
		gen sym = sym_root + ("" + sym_suffix) * (sym_suffix != "" & sym_suffix != "None")
		order sym, first
		drop sym_root sym_suffix

		*Combine date and time_m into datetime
		* Also adjust time_m to 5min intervals
		quietly describe date
		local fmt : format date 
		if !strpos("`fmt'", "%12.0g") { //Pre-2004 dates are already date-only
		    replace date = dofc(date)
		}
		format date %td
		rename date date_daily
		gen double minutes = real(substr(time_m, 1, 2)) * 60 + real(substr(time_m, 4, 2))
		gen double time = (ceil(minutes / 5) * 5) * 60000
		*Any time before 9:30 is grouped with 9:30
		replace time = tc(09:30) if time < tc(09:30)
		gen double datetime = date_daily * 86400000 + time
		format datetime %tcDDMonCCYY_HH:MM
		format time %tcHH:MM
		drop time_m minutes

		*Fix formatting of actual_close	
		* Some older years do not contain the nanosecond
		capture gen actual_close_time_nano = 0
		rename actual_close_time_nano actual_close_nano
		gen actual_close_str = substr(actual_close_time, 1, 8)
		gen double actual_close = clock(actual_close_str, "hms")
		gen double actual_close_micro = cond(length(actual_close_time) > 8, real(substr(actual_close_time, 10, 6)), 0)
		gen double actual_close_total = actual_close + actual_close_micro / 1e3
		format actual_close_total %15.3f
		format actual_close %tcHH:MM:SS
		drop actual_close_time actual_close_str

		*Reorder
		order sym datetime date_daily time price actual_close actual_close_micro actual_close_nano actual_close_total
		sort datetime sym

		save "temp_files/WRDS/TAQ_monthly/TAQ_monthly_`year'/`file'", replace
	}
}


