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
*** Fixing missing TRTH data
*-------------------------------------------------------------------------------
use "temp_files/TRTH/missing_TRTH_data.dta", clear

gen date = dofc(Date_ET)
format date %td

bysort date (Date_ET): gen start_time = Date_ET == Date_ET[1]
bysort date (Date_ET): gen end_time   = Date_ET == Date_ET[_N]
bysort date: gen obs_count = _N

collapse (first) start=Date_ET (last) end=Date_ET (count) obs_count= Date_ET, by(date)
format obs_count %4.0g

gen start_time = start - dofc(start)*86400000
format start_time %tcHH:MM:SS

gen end_time = end - dofc(end)*86400000
format end_time %tcHH:MM:SS

keep date start_time end_time obs_count
order date start_time end_time obs_count
export excel using "temp_files/TRTH/missing_TRTH_daily_data.xlsx", firstrow(variables) replace

***Adjust main intraday file
use "cleaned_files/intraday_SP500_5_min", clear  // Load the original dataset
gen double time = clock(string(Date_ET, "%tcHH:MM"), "hm")
format time %tcHH:MM
gen date_daily = dofc(Date_ET)
format date_daily %td

gen byte early_close_day = inlist(date_daily, ///
    td(24dec1996), td(3jul1996), td(24dec1997), td(27nov1998), td(24dec1998), ///
    td(26nov1999), td(3jul2000), td(24nov2000), td(3jul2001), td(23nov2001), ///
    td(24dec2001), td(29nov2002), td(24dec2002), td(3jul2003), td(28nov2003), ///
    td(24dec2003), td(26nov2004), td(25nov2005), td(3jul2006), td(24nov2006), ///
    td(3jul2007), td(23nov2007), td(24dec2007), td(3jul2008), td(28nov2008), ///
    td(24dec2008), td(27nov2009), td(24nov2009), td(26nov2010), td(25nov2011), ///
    td(3jul2012), td(23nov2012), td(24dec2012), td(3jul2013), td(29nov2013), ///
    td(24dec2013), td(3jul2014), td(28nov2014), td(24dec2014), td(27nov2015), ///
    td(24dec2015), td(24dec2015), td(25nov2016), td(3jul2017), td(24nov2017), ///
    td(3jul2018), td(23nov2018), td(24dec2018), td(3jul2019), td(29nov2019), ///
    td(24dec2019), td(27nov2020), td(24dec2020), td(26nov2021), td(25nov2022), ///
    td(3jul2023), td(24nov2023), td(3jul2024))
    
gen double cutoff = clock("13:05", "hm")
format cutoff %tcHH:MM
gen byte drop_obs = early_close_day == 1 & (Date_ET - dofc(Date_ET)*86400000) >= cutoff
drop if drop_obs

*Forward fill missing TRTH values
replace SP500_TRTH = SP500_TRTH[_n-1] if missing(SP500_TRTH)

gen double diff = abs(SP500_intraday - SP500_TRTH)
replace percent_diff = (diff / SP500_TRTH) * 100
drop diff

drop time date_daily early_close_day cutoff drop_obs
save "cleaned_files/intraday_SP500_5_min_TRTH_adjusted", replace




