"""
Created on Thu Apr 24 18:28:16 2025

@author: m1ism02
"""
/*
Note: This will NOT run unless run in WRDS' SAS Studio. This is stored
 locally for general reference.
 Additionally, this assumes a csv of the daily from step 1 exists
*/

/* Define the years you want to loop over */
%let start_year = 1994;
%let end_year = 2003;

/* Loop through each year from start_year to end_year */
%macro process_years;
    %do year = &start_year %to &end_year %by 1;

        proc import datafile="/home/frb/idreesm10/daily_processed/daily_processed_&year..csv"
            out=daily_&year
            dbms=csv
            replace;
            guessingrows=MAX;
        run;

        data comp_list;
            set daily_&year(keep=sym1 sym2 sym3);
            length symbol $32.;
            array syms{*} sym1 sym2 sym3;
            do i = 1 to dim(syms);
                symbol = strip(syms{i});
                if symbol ne "" then output;
            end;
            keep symbol;
        run;

        proc sort data=comp_list nodupkey; 
            by symbol; 
        run;

        proc sql noprint;
            select quote(trim(symbol)) 
            into :comp_list_str separated by ',' 
            from comp_list;
        quit;

        %do month = 1 %to 12;
            %let month_str = %sysfunc(putn(&month, z2.));

            data taq_data_&year.&month_str;
                length sym_root $32. sym_suffix $8. date 8. time_m $5. actual_close_time $8. price 8.;
                retain sym_root sym_suffix date time_m actual_close_time price;
                stop;
            run;

            %do day = 1 %to 31;
                %let day_str = %sysfunc(putn(&day, z2.)); 
                %let dataset_full = TAQ.CT_&year&month_str&day_str;

                %if %sysfunc(exist(&dataset_full)) %then %do;
                    %put NOTE: Dataset &dataset_full exists for &year-&month_str-&day_str.;

                    data ranked_&year.&month_str.&day_str;
                        set TAQ.CT_&year&month_str&day_str;
                        where (hour(time)*60 + minute(time)) between 240 and 960
                              and SYMBOL in (&comp_list_str);

                        /* Compute 5-min bucket */
                        if hour(TIME) < 9 or (hour(TIME) = 9 and minute(TIME) <= 30) then bucket = 570;
                        else bucket = hour(TIME) * 60 + (5 * ceil(minute(TIME) / 5));

                        /* Create variables needed */
                        sym_root = SYMBOL;
                        sym_suffix = "";
                        date = DATE;
                        time_m = put(TIME, time5.);
                        actual_close_time = put(TIME, time8.);
                        price = PRICE;
                        format date yymmdd10.;
                    run;

                    /* Sort so the first observation per group is the latest in time */
                    proc sort data=ranked_&year.&month_str.&day_str;
                        by symbol date bucket descending time;
                    run;

                    /* Deduplicate to get the latest record per bucket */
                    data day_data_&year.&month_str.&day_str;
                        set ranked_&year.&month_str.&day_str;
                        by symbol date bucket;
                        if first.bucket;
                        keep sym_root sym_suffix date time_m actual_close_time price;
                    run;

                    proc append base=taq_data_&year.&month_str data=day_data_&year.&month_str.&day_str force;
                    run;

                    proc datasets lib=work nolist;
                        delete day_data_&year.&month_str.&day_str ranked_&year.&month_str.&day_str;
                    quit;
                %end;
            %end;

            %let output_dir = /home/frb/idreesm10/TAQ_raw_ctm/TAQ_dtas_&year.;
            %if %sysfunc(fileexist(&output_dir)) = 0 %then %do;
                %put NOTE: The directory &output_dir does not exist. Please create it manually.;
            %end;

            proc export data=taq_data_&year.&month_str
                outfile="&output_dir/TAQ_five_min_price_&year.&month_str..dta"
                dbms=dta
                replace;
            run;

        %end;

    %end;
%mend process_years;

%process_years;

