"""
This script controls the processing/data analysis to generate precipitation accumulation
windows. Two dataframes are output: latest_obs and master_db. The former contains the 
latest accumulations and is used by the dash-app to display values on the map, while 
master_db contains all of the time series data 

Currently the merged_df precip data will still be incorrect as precipitation resets
at midnight or 1 am local since the raw tile files are being used to generate it. 
Need to correct it. 

This script probably could use the most work. Lots of repeated calculations and other 
inefficiencies. 
"""

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
from datetime import datetime, timedelta
#from concurrent.futures import ThreadPoolExecutor
#from multiprocessing import Pool
#from functools import partial
import time
import os
#from glob import glob
#from collections import defaultdict

from configs import (MAX_AGE_MINUTES, MAX_DIFF_MINUTES, WUNDER_DIR)
from utils.log import logfile

SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"{datetime.utcnow().strftime('%Y%m%d')}_driver.log")

# Needs to be largest --> smallest
ACCUM_PERIODS = [180, 60, 30, 15]

def data_calc_qc(end_dt, data, counts, window=180):
    """
    Optional parameters:
    window: int - default = 180 minutes or 3 hours
        Time window (minutes) over which to look at precip values.  
    """
    start_dt = end_dt - pd.to_timedelta(window, unit='minutes')
    deltas_start = (start_dt - data.dateutc).abs()
    deltas_end = (end_dt - data.dateutc).abs()

    # Observations falling within the search window, padded by MAX_DIFF_MINUTES.
    if deltas_start.loc[deltas_start.idxmin()] <= pd.to_timedelta(MAX_DIFF_MINUTES, 
                                                                  unit='minutes'):
        filtered_df = data.loc[deltas_start.idxmin():deltas_end.idxmin()]

        # Check if filtered data is monotonically increasing
        dx = np.diff(filtered_df['precip'])
        idx = np.where(dx < 0)
        num_backwards = len(idx[0])
        precip_amount = filtered_df.iloc[-1]['precip'] - filtered_df.iloc[0]['precip']
        precip_amount = round(precip_amount, 2)
        
        # To filter out the [None] entries. 
        if np.isfinite(precip_amount):
        
            # CASE 1: Filtered data is entirely monotonically increasing
            if num_backwards == 0:
                counts['passed'] += 1
                return precip_amount
            
            # CASE 2: A single precip decrease in this window.
            elif num_backwards == 1:

                # This reset happened either at midnight or 1 am local. There also seem
                # to be sites that reset at other hours??
                hours = filtered_df['localhour']
                if (hours.iloc[idx[0][0]] == 23 and hours.iloc[idx[0][0]+1] == 0) or \
                   (hours.iloc[idx[0][0]] == 0 and hours.iloc[idx[0][0]+1] == 1):
                    
                    max_daily_val = filtered_df['precip'].iloc[idx[0][0]]
                    temp = filtered_df['precip'][idx[0][0]+1:] + max_daily_val
                    filtered_df['precip'][idx[0][0]+1:] = temp 
                    
                    counts['passed'] += 1
                    return filtered_df.iloc[-1]['precip'] - filtered_df.iloc[0]['precip']

                # This reset happened at another time. In this case, while the rest of
                # the data may be okay, for now, assume the data is bad at this time. 
                counts['too_many_backwards'] += 1
            
            # CASE 3:
            # We are probably neglecting some good data with sites that temporarily 
            # report a negative dx, but then return to the baseline.
            else:
                counts['too_many_backwards'] += 1
        
        else:
            counts['not_numeric'] += 1
    else:
        counts['too_old'] += 1
    return np.nan 

def data_qc(df):
    precip_amount = np.nan

    # CASE 1: Filtered data is entirely monotonically increasing
    if df['precip'].is_monotonic_increasing:
        precip_amount = df['precip'].iloc[-1] - df['precip'].iloc[0]
        precip_amount = float(round(precip_amount, 2))
    else:
        dx = np.diff(df['precip'])
        idx = np.where(dx < 0)
        num_backwards = len(idx[0])
        
        # CASE 2: A single precip decrease in this window.
        #if num_backwards == 1:
    
    return precip_amount

def process(now): 
    def calc_site_precip(site, output_dict):
        """
        Defining this as an inner function to provide access to the main dataframe, 
        without having to pass it back-and-forth across processes. 
        """
        data = filtered.loc[filtered.siteid==site]
        end_dt = data['dateutc'].iloc[-1]

        output_dict['siteid'].append(site)
        output_dict['lon'].append(data['lon'].iloc[-1])
        output_dict['lat'].append(data['lat'].iloc[-1])
        output_dict['latest_ob_time'].append(end_dt)

        # Start with the longest window and work inwards
        for accum_pd in ACCUM_PERIODS:
            start_dt = end_dt-timedelta(minutes=accum_pd)
            deltas = (start_dt - data.dateutc).abs()
            if deltas.loc[deltas.idxmin()] <= timedelta(minutes=MAX_DIFF_MINUTES):
                window_df = data.loc[deltas.idxmin():]
                precip_amount = data_qc(window_df)
                output_dict[f"{accum_pd}_min"].append(precip_amount)
            else:
                output_dict[f"{accum_pd}_min"].append(np.nan)

    df = pd.read_parquet(f"{WUNDER_DIR}/merged_tiles.parquet")
    df.dropna(subset=['precip'], inplace=True)

    # Find most recent observation for each station. Filter out sites with data older
    # than MAX_AGE_MINUTES
    temp = df.groupby('siteid')['dateutc'].max()
    age_threshold = now - timedelta(minutes=MAX_AGE_MINUTES)
    valid_sites = temp.loc[temp > age_threshold].index
    filtered = df[df['siteid'].isin(valid_sites)]
    filtered.sort_values(by=['siteid', 'dateutc'], inplace=True)
    filtered.reset_index(inplace=True)

    # Initialize storage dictionary for data output
    output_dict = {
        'siteid': [],
        'lon': [],
        'lat': [],
        'latest_ob_time': [],
    }
    for i in ACCUM_PERIODS: 
        output_dict[f"{i}_min"] = []

    # Returns the most recent observation times for all sites in the filtered df
    #most_recent_times = filtered.groupby('siteid')['dateutc'].max().dropna()
    siteids = filtered.siteid.unique()
    for site in siteids:
        calc_site_precip(site, output_dict)
        
    output_df = pd.DataFrame.from_dict(output_dict)
    
    # Drop rows in which data is NaN for all precip time periods.
    cols = output_df.columns[output_df.columns.str.contains('_min')]
    output_df.dropna(subset=cols, how='all', inplace=True)
    filename = f"{WUNDER_DIR}/latest_obs.parquet"
    output_df.to_parquet(filename)

    #with ThreadPoolExecutor(max_workers=50) as executor:
    #    executor.map(calc_site_precip, siteids)
    #    executor.shutdown(wait=True)

    #with Pool(16) as pool:
    #    result = pool.starmap(calc_site_precip, zip(siteids, repeat(filtered)))
    
def main():
    t1 = time.time()
    now = datetime.utcnow()
    process(now)

    print(time.time() - t1)

if __name__ == '__main__':
    main()