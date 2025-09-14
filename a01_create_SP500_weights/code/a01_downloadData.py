#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 14:12:22 2025

@author: m1ism02
"""
import os
import sys
import pyodbc
import pandas as pd
sys.path.insert(1, f'/if/appl/python/packages/prod/{str(sys.version_info[0])}.{str(sys.version_info[1])}')
import pyfame as fm
####### Settings #######
os.chdir('/if/research-gmsm/Kroner/SP500_intraday_replication/a01_create_SP500_weights')
# Renew Kerberos principal
os.system('/opt/local/bin/tclkinit')
os.system('sh /if/fame/fm/scripts/refresh_kerberos.sh')
# Hive and Impala do not support transactional processing,
# so you must use autocommit=True
# Impala
cnxn_impala = pyodbc.connect('DSN=bdp-prod', autocommit=True)
######################################################################
query = """SELECT DISTINCT effective_date, company, cusip, ticker, local_price, shares_outstanding, market_cap, index_shares, index_market_cap, index_weight
FROM bdp_sp_dow_jones_indices.vw_equity_constituents_close
WHERE index_name = "S&P 500" and effective_date > "19940101"
ORDER BY effective_date, company
"""

#Pull data
all_sp = pd.read_sql_query(query, cnxn_impala)
fame_sp_mc = fm.get_data('valuation_daily', ['SP500.MCAP.B', 'SP500.DIVISOR.B', 'SP500.INDEX.B'], start = pd.Period("1994-01-01", "B"), end = pd.Period("2024-12-31", "B"))
daily_sp_1994_1995 = fm.get_data('valuation_daily', ['SP500.INDEX.B'], start = pd.Period("1994-01-01", "B"), end = pd.Period("1995-12-31", "B"))

#Export data file to stata
all_sp['effective_date'] = pd.to_datetime(all_sp['effective_date'])
all_sp.to_stata("/if/research-gmsm/Kroner/SP500_intraday_replication/a01_create_SP500_weights/source_data/SP_DJ_Indices/HUE_daily_1994_2024.dta", write_index=False)

fame_sp_mc = fame_sp_mc.reset_index().rename(columns={'index': 'date_daily', 'SP500.MCAP.B': 'mrkt_cap_tot', 'SP500.DIVISOR.B': 'divisor', 'SP500.INDEX.B': 'SP500'}).dropna()
fame_sp_mc['date_daily'] = fame_sp_mc['date_daily'].dt.to_timestamp()
fame_sp_mc.to_stata("/if/research-gmsm/Kroner/SP500_intraday_replication/a01_create_SP500_weights/source_data/SP_DJ_Indices/FAME_SP500_daily_1994_2024.dta", write_index=False)

daily_sp_1994_1995 = daily_sp_1994_1995.reset_index().rename(columns={'index': 'date_daily', 'SP500.INDEX.B': 'SP500_TRTH'}).dropna()
daily_sp_1994_1995['date_daily'] = daily_sp_1994_1995['date_daily'].dt.to_timestamp()
daily_sp_1994_1995.to_stata("/if/research-gmsm/Kroner/SP500_intraday_replication/a01_create_SP500_weights/source_data/SP_DJ_Indices/daily_sp_1994_1995.dta", write_index=False)
###############################################################################
query = """SELECT DISTINCT effective_date, company, cusip, ticker, local_price, shares_outstanding, market_cap, index_shares, index_market_cap, index_weight
FROM bdp_sp_dow_jones_indices.vw_equity_constituents_close
WHERE index_name = "S&P 500" and effective_date > "19940101"
ORDER BY effective_date, company
"""

#Pull data
all_sp = pd.read_sql_query(query, cnxn_impala)
all_sp['effective_date'] = pd.to_datetime(all_sp['effective_date'])
all_sp.to_stata("/if/research-gmsm/Kroner/SP500_intraday_replication/a04_preliminary_analysis/cleaned_files/HUE_daily_comp_20041208.dta", write_index=False)

all_sp['cusip6'] = all_sp['cusip'].str[:6]

# Keep only rows where the first 6 characters are duplicated
all_sp_dups = all_sp[all_sp['cusip6'].duplicated(keep=False)]


cnxn_impala.close()





































