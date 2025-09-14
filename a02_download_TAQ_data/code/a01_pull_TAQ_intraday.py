#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 16:41:55 2025

@author: m1ism02
"""
#Note: This will NOT run unless run in WRDS' JupyterLab. This is stored
# locally for general reference.
# Additionally, this assumes a csv of the daily from step 1 exists

import os
import pandas as pd
import wrds
HOME_DIR = os.path.expanduser('~')

db = wrds.Connection()

for year in range(2004, 2025):
    print(f"Reading in daily data for {year}:")
    daily_comp = pd.read_csv(f"daily_processed/daily_processed_{year}.csv",
                             dtype={"sym1": str, "sym2": str, "sym3": str})  # Load yearly file
    comp_list = daily_comp[["sym1", "sym2", "sym3"]].stack().dropna().drop_duplicates().tolist()
    comp_list_str = ", ".join(f"'{comp}'" for comp in comp_list)
    print(f"Successfully read in {year}")
          
    for month in range(1, 13): 
        month_str = f"{month:02d}"  # Ensures two-digit format (01, 02, ..., 12)
        query = f"""
            SELECT
                sym_root,
                sym_suffix,
                date,
                TO_CHAR(time_m, 'HH24:MI') AS time_m,
                time_m AS actual_close_time,
                price
            FROM
                (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY sym_root, sym_suffix, date,
                            CASE
                                WHEN EXTRACT(HOUR FROM time_m) < 9 OR
                                     (EXTRACT(HOUR FROM time_m) = 9 AND EXTRACT(MINUTE FROM time_m) <= 30) 
                                THEN 570
                                ELSE EXTRACT(HOUR FROM time_m) * 60 + (5 * CEIL(EXTRACT(MINUTE FROM time_m) / 5))
                            END
                            ORDER BY time_m DESC
                        ) AS five_min_rows
                    FROM
                        taqm_{year}.ctm_{year}
                    WHERE
                        TO_CHAR(date, 'YYYYMM') = '{year}{month_str}' AND
                        EXTRACT(HOUR FROM time_m) * 60 + EXTRACT(MINUTE FROM time_m) BETWEEN 240 AND 960 
                        AND (
                            (COALESCE(sym_suffix, '') = '' AND sym_root IN ({comp_list_str}))
                            OR
                            (COALESCE(sym_suffix, '') != '' AND sym_root || sym_suffix IN ({comp_list_str}))
                            )
                ) a
            WHERE
                five_min_rows = 1
            ORDER BY
                time_m,
                sym_root
        """
        print(f"Running query for {year}-{month_str}...")
        df = db.raw_sql(query)
        df["date"] = pd.to_datetime(df["date"])
        df['actual_close_time'] = df['actual_close_time'].apply(lambda x: x.strftime('%H:%M:%S.%f'))
        df['sym_suffix'] = df['sym_suffix'].astype(str)

        save_path = f"{HOME_DIR}/TAQ_raw_ctm/TAQ_dtas_{year}/TAQ_five_min_price_{year}{month_str}.dta"
        df.to_stata(save_path, write_index=False)
        print(f"Saved: {save_path}")

db.close()
print("All years processed.")