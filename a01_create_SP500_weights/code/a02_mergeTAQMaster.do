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
*** Merge TAQ Master Files
*-------------------------------------------------------------------------------
*edit files from 2010 previous
local pre_2011_files: dir "source_data/WRDS/pre2011_TAQ_master_yearly" files "*.dta"

foreach file of local pre_2011_files {
	use "source_data/WRDS/pre2011_TAQ_master_yearly/`file'", clear
	
	local year = substr("`file'", -8, 4)
	
	*cusip/symbol changes
	rename SYMBOL symbol_15
	rename CUSIP cusip
	rename NAME name
	drop if cusip == "           ."
	replace cusip = substr(cusip, 1, 9)
	drop if missing(cusip)
	drop if cusip == "000000000"
	
	*adjusting date format to match newer years
	gen fdate = dofc(FDATE)
	format fdate %td
	replace fdate = date("01jan`year'", "DMY") if fdate < date("01jan`year'", "DMY")
	gen end_date = date("31dec`year'", "DMY")
	format end_date %td
	expand end_date - fdate + 1
	bysort cusip symbol fdate (fdate): gen date = fdate + _n - 1
	format date %td
	keep if date <= end_date
	
	*remove weekends and other changes
	gen day_of_week = dow(date)
	drop if day_of_week == 0 | day_of_week == 6
	keep date symbol_15 cusip name
	order date symbol_15 cusip name
	sort date symbol_15
	duplicates drop
	
	save "source_data/WRDS/TAQ_master_yearly/`file'", replace

}

local files: dir "source_data/WRDS/TAQ_master_yearly" files "*.dta"

local first_file = 1
foreach file of local files {
    if `first_file' {
        use "source_data/WRDS/TAQ_master_yearly/`file'", clear
        local first_file = 0
    }
    else {
    	append using "source_data/WRDS/TAQ_master_yearly/`file'"
    }
}

replace symbol_15 = subinstr(symbol_15, " ", "", .)
replace name = cond(!missing(name), name, sec_desc)
drop sec_desc
sort date symbol_15 cusip name
save "source_data/WRDS/TAQ_master", replace
