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
*** Merge Daily with Intraday Prices
*-------------------------------------------------------------------------------
forvalues year = 1994/2024 {
	local files: dir "temp_files/WRDS/TAQ_monthly/TAQ_monthly_`year'/" files "*.dta"
	
	foreach file of local files {
	
		local month_str = substr("`file'", -6, 2)
		
		di `month_str'
		
		use "../a01_create_SP500_weights/cleaned_files/daily_processed_yearly_dta/daily_processed_`year'", clear
		
		keep if month(date) == real("`month_str'") & year(date) == `year'
		drop p_
		
		reshape long sym, i(date_daily cusip pw_) j(sym_num)
		drop if sym == ""
		
		duplicates report date_daily sym
		duplicates drop date_daily sym, force
		
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
		drop cusip_id sym_num actual_close_micro actual_close_total
		
		compress
		save "temp_files/WRDS/partial_data/partial_data_`year'/partial_`year'`month_str'", replace
	}
}

local first_file = 1
forvalues year = 1994/2024 {
	local files: dir "temp_files/WRDS/partial_data/partial_data_`year'/" files "*.dta"
	foreach file of local files {
	    if `first_file' {
		use "temp_files/WRDS/partial_data/partial_data_`year'/`file'", clear
		local first_file = 0
	    }
	    else {
		append using "temp_files/WRDS/partial_data/partial_data_`year'/`file'"
	    }
	}
}


drop if missing(price)
sort date_daily time sym

order datetime date_daily time cusip sym* price pw_ actual_close
rename datetime Date_ET
format Date_ET %tcDDMonCCYY_HH:MM:SS

gen double spx_const_part = price * pw_
bys date_daily time: egen double SP500_intraday = sum(spx_const_part)

merge m:1 Date_ET using "temp_files/TRTH/SP500_5min", keep(1 3)
drop _merge

*Needs to be removed next time the full intraday_SP500 is being made
gen double diff = abs(SP500_intraday - SP500_TRTH)
gen double percent_diff = (diff / SP500_TRTH) * 100
drop diff

compress
save "cleaned_files/intraday_SP500_5_min", replace

*Split up the main file into yearly files
forvalues year = 1994/2024 {
    use "cleaned_files/intraday_SP500_5_min", clear  // Load the original dataset
    keep if year(dofc(Date_ET)) == `year'    // Keep only rows for the current year
    save "cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_`year'", replace  // Save the file
}

*Add TRTH data to 1994/1995
forvalues year = 1994/1995 {
    use "cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_`year'", clear  // Load the original dataset
    merge m:1 date_daily using "../a01_create_SP500_weights/temp_files/SP_DJ_Indices/daily_sp_1994_1995", update keep(4)
    drop _merge
    gen double diff = abs(SP500_intraday - SP500_TRTH)
    replace percent_diff = (diff / SP500_TRTH) * 100
    drop diff
    save "cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_`year'", replace  // Save the file
}

*Recreate the full file with the final 1994-1995 files
local first_file = 1
forvalues year = 1994/2024 {
    if `first_file' {
	use "cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_`year'", clear
	local first_file = 0
    }
    else {
	append using "cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_`year'"
    }
}
//drop if missing(SP500_TRTH) //If this is missing, closed trading day
drop date_daily time actual_close spx_const_part

save "cleaned_files/intraday_SP500_5_min", replace

*Split up the main file into yearly files
forvalues year = 1994/2024 {
    use "cleaned_files/intraday_SP500_5_min", clear  // Load the original dataset
    keep if year(dofc(Date_ET)) == `year'    // Keep only rows for the current year
    save "cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_`year'", replace  // Save the file
}

********************************************************************************
********************************************************************************
********************************************************************************

// ********************
// *Additional Testing*
// use "cleaned_files/intraday_SP500_5_min", clear
// drop cusip sym price pw_ 
// duplicates drop
// save "cleaned_files/intraday_SP500_aggregate_5min", replace
// drop if Date_ET < clock("01Jan2000 00:00:00", "DMYhms")
// summarize percent_diff, detail
*Test from which date we lose a lot of data

// sort percent_diff
// reg SP500_intraday SP500_TRTH

// ********************
// *Testing incorrect results*
*05May2003 - 200-400% off
// use "cleaned_files/intraday_SP500_yearly/intraday_SP500_5_min_2003", clear
// keep if date_daily == mdy(5, 5, 2003)
// replace price = price / 100 if price > 1000
// replace spx_const_part = price * pw_
// drop SP500_intraday percent_diff
// bys date_daily time: egen SP500_intraday = sum(spx_const_part)
// gen double diff = abs(SP500_intraday - SP500_TRTH)
// gen double percent_diff = (diff / SP500_TRTH) * 100
// drop diff
// sort percent_diff

// replace price = 67.89 if Date_ET == tc(05may2003 09:30:00) & sym == "CFC"
// replace price = 21.48 if Date_ET == tc(05may2003 09:30:00) & sym == "CFC"


// // ********************
// // *Additional Testing*
// use "cleaned_files/intraday_SP500_5_min", clear
// gen double diff = abs(SP500_intraday - SP500_TRTH)
// gen double percent_diff = (diff / SP500_TRTH) * 100
// drop //drop everything but percent_diff if wanted
// duplicates drop
// summarize percent_diff, detail
// sort percent_diff
// // sum percent_diff, meanonly
// // display "Average Percent Off: " r(mean)
// //
// //
//
// drop date_daily time cusip sym price pw_ 
// // drop actual_close_total stocks_per
// // duplicates drop
// //
// summarize SP500_intraday
// reg SP500_intraday SP500_TRTH
// //
// // sort Date_ET
// // gen trading_index = _n
// // twoway (line SP500_intraday trading_index, lcolor(blue) lwidth(medium)) ///
// //        (line SP500_TRTH trading_index, lcolor(red) lwidth(medium)), ///
// //        legend(label(1 "SPX Calc") label(2 "SPX TRTH")) ///
// //        title("SPX") ///
// //        xlabel(, angle(45)) ylabel(, angle(0))
