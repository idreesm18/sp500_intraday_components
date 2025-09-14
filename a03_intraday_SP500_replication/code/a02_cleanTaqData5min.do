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
*** Prepare TAQ Minute Price
*-------------------------------------------------------------------------------
local start_year = 2011
local end_year = 2024

forvalues year = `start_year'/`end_year' {

    forvalues m = 1/12 {
	* YYYYMM
	local mm : display %02.0f `m'
	local file     "TAQ_five_min_price_`year'`mm'.dta"
	local inpath   "../a02_download_TAQ_data/cleaned_files/TAQ_monthly_`year'/`file'"
	local outdir   "temp_files/WRDS/TAQ_monthly/TAQ_monthly_`year'"
	local outpath  "`outdir'/`file'"

	use "`inpath'", clear
	
	* --------- append "incorrect_taq" for some YYYYMM ----------
	local badpath "../a01_create_SP500_weights/temp_files/WRDS/incorrect_taq/final_pass_dtas/TAQ_five_min_price_`year'`mm'.dta"
	capture confirm file "`badpath'"
	if !_rc {
		di as txt "Appending incorrect_taq patch: `badpath'"
		append using "`badpath'"
	}
	else {
		di as txt "No incorrect_taq patch for `year'`mm'"
	}
	
        compress
        save "`inpath'", replace

        * ---------- Combine sym_root/sym_suffix into sym ----------
        * Use safe conditional concatenation (avoids multiplying strings by booleans)
        capture confirm variable sym_root
        if !_rc {
            capture confirm variable sym_suffix
            if !_rc {
                gen sym = sym_root + cond(sym_suffix != "" & sym_suffix != "None", sym_suffix, "")
                order sym, first
                drop sym_root sym_suffix
            }
        }

        * ---------- Date & time bucketing ----------
        * If date is datetime (%tc as %12.0g), convert to Stata daily date first
        quietly describe date
        local fmt : format date
        if !strpos("`fmt'", "%12.0g") {
            replace date = dofc(date)     // pre-2004 already date-only; %12.0g implies datetime
        }
        format date %td
        rename date date_daily

        * time_m is "HH:MM" -> minutes -> 5-min buckets (ceil)
        gen double minutes = real(substr(time_m, 1, 2)) * 60 + real(substr(time_m, 4, 2))
        gen double time    = ceil(minutes/5)*5*60000    // ms since 01jan1960 00:00

        * Any time before 09:30 groups with 09:30
        replace time = tc(09:30) if time < tc(09:30)

        * Build full datetime in ms
        gen double datetime = date_daily*86400000 + time
        format datetime %tcDDMonCCYY_HH:MM
        format time     %tcHH:MM
        drop time_m minutes

        * ---------- actual_close* fields ----------
        capture confirm variable actual_close_time
        if !_rc {
            capture gen actual_close_time_nano = 0
            rename actual_close_time_nano actual_close_nano

            gen actual_close_str  = substr(actual_close_time, 1, 8)
            gen double actual_close = clock(actual_close_str, "hms")
            gen double actual_close_micro = cond(length(actual_close_time) > 8, real(substr(actual_close_time, 10, 6)), 0)
            gen double actual_close_total = actual_close + actual_close_micro/1e3

            format actual_close_total %15.3f
            format actual_close       %tcHH:MM:SS
            drop actual_close_time actual_close_str
        }

        * ---------- Reorder & sort ----------
        capture order sym datetime date_daily time price actual_close actual_close_micro actual_close_nano actual_close_total
        sort datetime sym

        * Save to output location for this year/month
        save "`outpath'", replace
        di as result "Processed & saved: `outpath'"
    }
}



// forvalues year = `start_year'/`end_year' {
// 	local files: dir "../a02_download_TAQ_data/cleaned_files/TAQ_monthly_`year'/" files "*.dta"
//	
// 	foreach file of local files {
//
// 		use "../a02_download_TAQ_data/cleaned_files/TAQ_monthly_`year'/`file'", clear
//		
// 		compress
// 		save "../a02_download_TAQ_data/cleaned_files/TAQ_monthly_`year'/`file'", replace
//
// 		*Combine sym_root/suffix to match master file sym
// 		gen sym = sym_root + ("" + sym_suffix) * (sym_suffix != "" & sym_suffix != "None")
// 		order sym, first
// 		drop sym_root sym_suffix
//
// 		*Combine date and time_m into datetime
// 		* Also adjust time_m to 5min intervals
// 		quietly describe date
// 		local fmt : format date 
// 		if !strpos("`fmt'", "%12.0g") { //Pre-2004 dates are already date-only
// 		    replace date = dofc(date)
// 		}
// 		format date %td
// 		rename date date_daily
// 		gen double minutes = real(substr(time_m, 1, 2)) * 60 + real(substr(time_m, 4, 2))
// 		gen double time = (ceil(minutes / 5) * 5) * 60000
// 		*Any time before 9:30 is grouped with 9:30
// 		replace time = tc(09:30) if time < tc(09:30)
// 		gen double datetime = date_daily * 86400000 + time
// 		format datetime %tcDDMonCCYY_HH:MM
// 		format time %tcHH:MM
// 		drop time_m minutes
//
// 		*Fix formatting of actual_close	
// 		* Some older years do not contain the nanosecond
// 		capture gen actual_close_time_nano = 0
// 		rename actual_close_time_nano actual_close_nano
// 		gen actual_close_str = substr(actual_close_time, 1, 8)
// 		gen double actual_close = clock(actual_close_str, "hms")
// 		*Pre-2004 does not have microseconds
// 		gen double actual_close_micro = cond(length(actual_close_time) > 8, real(substr(actual_close_time, 10, 6)), 0)
// 		gen double actual_close_total = actual_close + actual_close_micro / 1e3
// 		format actual_close_total %15.3f
// 		format actual_close %tcHH:MM:SS
// 		drop actual_close_time actual_close_str
//
// 		*Reorder
// 		order sym datetime date_daily time price actual_close actual_close_micro actual_close_nano actual_close_total
// 		sort datetime sym
//
// 		save "temp_files/WRDS/TAQ_monthly/TAQ_monthly_`year'/`file'", replace
// 	}
// }
