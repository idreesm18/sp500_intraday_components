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
**# Replace Dual Share Class Prices With Composite Price - Base Version
*-------------------------------------------------------------------------------
use "cleaned_files/intraday_SP500_5_min_TRTH_adjusted", clear
save "cleaned_files/final_intraday_SP500_5_min", replace

local composite_groups `" "GOOG_CP GOOG GOOGL 999 999" "DISC_CP DISCA DISCK 999 999" "FOX_CP FOXA FOX 999 999" "NWS_CP NWSA NWS NWSAWI 999" "UA_CP UA UAC UAA 999" "CMCS_CP CMCSA CMCSK 999 999" "WCOM_CP WCOM MCIT 999 999" "TFCF_CP TFCFA TFCF 999 999" "'

//local composite_groups `" "GOOG_CP GOOG GOOGL 999" "'

foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/intraday_SP500_5_min_TRTH_adjusted", clear
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair
    keep if has_both == 1
    save "temp_files/composite_price/`comp_sym'_composite_price.dta", replace
    gen price_x_weight = price * pw_
    collapse (sum) price_x_weight pw_, by(Date_ET)
    gen composite_price = price_x_weight / pw_
    
    preserve
    use "temp_files/composite_price/`comp_sym'_composite_price.dta", clear
    bysort Date_ET (sym): keep if _n == 1
    tempfile meta
    save `meta'
    restore

    merge 1:1 Date_ET using `meta', nogen
    drop price_x_weight price
    order Date_ET cusip sym composite_price pw_
    rename composite_price price
    replace sym = "`comp_sym'"
    save "temp_files/composite_price/`comp_sym'_composite_price_final.dta", replace
}

local composite_groups `" "GOOG_CP GOOG GOOGL 999 999" "DISC_CP DISCA DISCK 999 999" "FOX_CP FOXA FOX 999 999" "NWS_CP NWSA NWS NWSAWI 999" "UA_CP UA UAC UAA 999" "CMCS_CP CMCSA CMCSK 999 999" "WCOM_CP WCOM MCIT 999 999" "TFCF_CP TFCFA TFCF 999 999" "'

//local composite_groups `" "WCOM_CP WCOM MCIT 999" "'


foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/final_intraday_SP500_5_min", clear
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'", "`sym4'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair
    drop if has_both
    append using "temp_files/composite_price/`comp_sym'_composite_price_final.dta"
    drop is_composite_pair pair_count has_both
    save "cleaned_files/final_intraday_SP500_5_min", replace
}
sort Date_ET sym
save "cleaned_files/final_intraday_SP500_5_min", replace
*-------------------------------------------------------------------------------
**# Replace Dual Share Class Prices With Composite Price - AT&T
*-------------------------------------------------------------------------------
local composite_groups `" "T_CP T AWE TWD AWEWI" "'

foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/intraday_SP500_5_min_TRTH_adjusted", clear
    keep if dofc(Date_ET) == mdy(7, 9, 2001)
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'", "`sym4'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair
    keep if has_both == 1
    save "temp_files/composite_price/`comp_sym'_composite_price.dta", replace
    gen price_x_weight = price * pw_
    collapse (sum) price_x_weight pw_, by(Date_ET)
    gen composite_price = price_x_weight / pw_
    
    preserve
    use "temp_files/composite_price/`comp_sym'_composite_price.dta", clear
    bysort Date_ET (sym): keep if _n == 1
    tempfile meta
    save `meta'
    restore

    merge 1:1 Date_ET using `meta', nogen
    drop price_x_weight price
    order Date_ET cusip sym composite_price pw_
    rename composite_price price
    replace sym = "`comp_sym'"
    save "temp_files/composite_price/`comp_sym'_composite_price_final.dta", replace
}

local composite_groups `" "T_CP T AWE TWD AWEWI" "'

foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/final_intraday_SP500_5_min", clear
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'", "`sym4'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair & dofc(Date_ET) == mdy(7, 9, 2001)
    drop if has_both
    append using "temp_files/composite_price/`comp_sym'_composite_price_final.dta"
    drop is_composite_pair pair_count has_both
}
sort Date_ET sym
save "cleaned_files/final_intraday_SP500_5_min", replace

*-------------------------------------------------------------------------------
**# Replace Dual Share Class Prices With Composite Price - USW
*-------------------------------------------------------------------------------
local composite_groups `" "USW_CP USW UMG USWWI UMGWI" "'

foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/intraday_SP500_5_min_TRTH_adjusted", clear
    keep if dofc(Date_ET) <= mdy(6, 12, 1998)
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'", "`sym4'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair
    keep if has_both == 1
    save "temp_files/composite_price/`comp_sym'_composite_price.dta", replace
    gen price_x_weight = price * pw_
    collapse (sum) price_x_weight pw_, by(Date_ET)
    gen composite_price = price_x_weight / pw_
    
    preserve
    use "temp_files/composite_price/`comp_sym'_composite_price.dta", clear
    bysort Date_ET (sym): keep if _n == 1
    tempfile meta
    save `meta'
    restore

    merge 1:1 Date_ET using `meta', nogen
    drop price_x_weight price
    order Date_ET cusip sym composite_price pw_
    rename composite_price price
    replace sym = "`comp_sym'"
    save "temp_files/composite_price/`comp_sym'_composite_price_final.dta", replace
}

local composite_groups `" "USW_CP USW UMG USWWI UMGWI" "'

foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/final_intraday_SP500_5_min", clear
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'", "`sym4'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair & dofc(Date_ET) <= mdy(6, 12, 1998)
    drop if has_both
    append using "temp_files/composite_price/`comp_sym'_composite_price_final.dta"
    drop is_composite_pair pair_count has_both
}
sort Date_ET sym
save "cleaned_files/final_intraday_SP500_5_min", replace

*-------------------------------------------------------------------------------
**# Replace Dual Share Class Prices With Composite Price - SPRINT
*-------------------------------------------------------------------------------
local composite_groups `" "FON_CP FON FONWI PCS PCSWI" "'

foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/intraday_SP500_5_min_TRTH_adjusted", clear
    keep if dofc(Date_ET) <= mdy(4, 22, 2004)
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'", "`sym4'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair
    keep if has_both == 1
    save "temp_files/composite_price/`comp_sym'_composite_price.dta", replace
    gen price_x_weight = price * pw_
    collapse (sum) price_x_weight pw_, by(Date_ET)
    gen composite_price = price_x_weight / pw_
    
    preserve
    use "temp_files/composite_price/`comp_sym'_composite_price.dta", clear
    bysort Date_ET (sym): keep if _n == 1
    tempfile meta
    save `meta'
    restore

    merge 1:1 Date_ET using `meta', nogen
    drop price_x_weight price
    order Date_ET cusip sym composite_price pw_
    rename composite_price price
    replace sym = "`comp_sym'"
    save "temp_files/composite_price/`comp_sym'_composite_price_final.dta", replace
}

local composite_groups `" "FON_CP FON FONWI PCS PCSWI" "'

foreach group in `composite_groups' {
    tokenize `group'
    local comp_sym = "`1'"
    local sym1 = "`2'"
    local sym2 = "`3'"
    local sym3 = "`4'"
    local sym4 = "`5'"
    
    use "cleaned_files/final_intraday_SP500_5_min", clear
    gen is_composite_pair = inlist(sym, "`sym1'", "`sym2'", "`sym3'", "`sym4'")
    bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
    bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair & dofc(Date_ET) <= mdy(4, 22, 2004)
    drop if has_both
    append using "temp_files/composite_price/`comp_sym'_composite_price_final.dta"
    drop is_composite_pair pair_count has_both
}
sort Date_ET sym
save "cleaned_files/final_intraday_SP500_5_min", replace



// ********************
**See all CP stocks
// keep if sym == "GOOG_CP" | sym == "DISC_CP" | sym == "FOX_CP" | sym == "NWS_CP" | sym == "UA_CP" | sym == "CMCS_CP" | sym == "WCOM_CP" | sym == "TFCF_CP" | sym == "T_CP" | sym == "USW_CP" | sym == "FON_CP"
// sort sym Date_ET
**O
// use "cleaned_files/intraday_SP500_5_min", clear
// gen is_composite_pair = inlist(sym, "GOOG", "GOOGL")
// bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
// bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair
// keep if has_both == 1
// save "temp_files/google_composite_price.dta", replace
// gen price_x_weight = price * pw_
// collapse (sum) price_x_weight pw_, by(Date_ET)
// gen composite_price = price_x_weight / pw_
// preserve
// use "temp_files/google_composite_price.dta", clear
// bysort Date_ET (sym): keep if _n == 1
// tempfile meta
// save `meta'
// restore
//
// merge 1:1 Date_ET using `meta', nogen
// drop price_x_weight price
// order Date_ET cusip sym composite_price pw_
// rename composite_price price
// replace sym = "GOOG_CP" if sym == "GOOG"
// save "temp_files/google_composite_price_final.dta", replace
//
//
// use "cleaned_files/intraday_SP500_5_min", clear
// gen is_composite_pair = inlist(sym, "GOOG", "GOOGL")
// bysort Date_ET (is_composite_pair): gen pair_count = sum(is_composite_pair)
// bysort Date_ET: gen has_both = pair_count[_N] == 2 & is_composite_pair
// drop if has_both
// append using "temp_files/google_composite_price_final.dta"
// sort Date_ET sym
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
