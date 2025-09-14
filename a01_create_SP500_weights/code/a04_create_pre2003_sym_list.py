#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 13:41:36 2025

@author: m1ism02
"""
import os
import pandas as pd

os.chdir('/if/research-gmsm/Kroner/SP500_intraday_replication/a01_create_SP500_weights')

for year in range(1994, 2004, 1):
    print(f"Processing year {year}...")
    
    # Read the CSV
    df = pd.read_csv(f"cleaned_files/daily_processed_yearly_csv/daily_processed_{year}.csv", dtype=str)
    
    # Combine and clean symbols
    symbols = pd.concat([df["sym1"], df["sym2"], df["sym3"]])
    symbols = symbols.dropna().str.strip()
    symbols = symbols[symbols != ""]
    unique_symbols = sorted(symbols.unique())

    # Write to text file
    with open(f"cleaned_files/sym_yearly/sym_yearly_{year}.txt", "w") as f:
        for sym in unique_symbols:
            f.write(f"{sym}\n")

    print(f"Saved sym_yearly/sym_yearly_{year}.txt")