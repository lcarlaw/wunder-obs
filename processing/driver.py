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
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from functools import partial
import time
import os, shutil
from glob import glob
from collections import defaultdict

from configs import (MAX_AGE_MINUTES, MAX_DIFF_MINUTES, WUNDER_DIR, 
                     OUTPUT_DIR, ARCHIVE_DIR)
from utils.log import logfile

SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"{datetime.utcnow().strftime('%Y%m%d')}_driver.log")

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

def create_tile_output(tilefile, now):
    df = pd.read_parquet(tilefile)
    siteids = df.siteid.unique()
    df.sort_values(by=['siteid', 'dateutc'], inplace=True)
    output_dict = {
        'siteid': [],
        'lon': [],
        'lat': [],
        'latest_ob_time': [],
        '15_min': [],
        '30_min': [],
        '60_min': [],
        '180_min': []
    }
    for site in siteids:
        
        data = df[df.siteid == site]
        data['precip'].fillna(value=np.nan, inplace=True)
        end_dt = data['dateutc'].iloc[-1] # Latest ob for this site

        counts = {
            'total': 0,
            'not_numeric': 0, 
            'too_many_backwards': 0,
            'too_old': 0,
            'passed': 0
        }

        counts['total'] = len(data)

        # Latest ob for this site is recent enough to continue
        if np.abs(now - end_dt) <= pd.to_timedelta(MAX_AGE_MINUTES, unit='minutes'):
            output_dict['siteid'].append(site)
            output_dict['lon'].append(data['lon'].iloc[-1])
            output_dict['lat'].append(data['lat'].iloc[-1])
            output_dict['latest_ob_time'].append(end_dt)

            for window in [15, 30, 60, 180]:          
                val = data_calc_qc(end_dt, data, counts, window=window)
                output_dict[f"{window}_min"].append(val)

        else:
            counts['too_old'] += 1

        #print(counts)
    return output_dict

def process_data(now):
    """
    """    
    tilefiles = glob(f"{WUNDER_DIR}/*.parquet")

    '''
    def _fetch_mrms():
        from subprocess import Popen, PIPE
        from configs import MRMS_URL 

        CURL_EXE = '/usr/bin/curl'
        url = f"{MRMS_URL}/MRMS_RadarOnly_QPE_01H.latest.grib2.gz"
        
        log.info(f"Downloading {url}")
        args = [CURL_EXE, '-o', f"{MRMS_DIR}/MRMS_QPE_01H.latest.grib2.gz", url]
        p = Popen(args, stderr=PIPE)
        p.wait()
    '''

    def _execute_threadpool():
        with ThreadPoolExecutor(max_workers=100) as executor:
            executor.map(partial(create_tile_output, now=now), tilefiles)
            executor.shutdown(wait=True)

    def _execute_pool():
        with Pool(4) as pool:
            result = pool.map(partial(create_tile_output, now=now), tilefiles)
        return result

    t1 = time.time()
    res = _execute_pool()
    
    # Merge lists (which contain data dictionaries) passed out by Pool together.
    # Generate master data frame
    output = defaultdict(list)
    for d in res: 
        for k, v in d.items():
            output[k].extend(v)
    output_df = pd.DataFrame.from_dict(output)

    # Drop rows in which data is NaN for all precip time periods.
    cols = output_df.columns[output_df.columns.str.contains('_min')]
    output_df.dropna(subset=cols, how='all', inplace=True)
    filename = f"{OUTPUT_DIR}/latest_obs.parquet"
    output_df.to_parquet(filename, engine='pyarrow', index=False)

    #log.info("Saving copy of latest observations to archive folder")
    #shutil.copyfile(filename, 
    #                f"{ARCHIVE_DIR}/{now.strftime('%Y%m%d_%H%M')}.parquet")

    """
    Maybe not great we have to do this again. 

    Merged all of the individual tile files into a master dataframe. This will be used
    by the dash-app when a user clicks a specific point to produce timeseries data.

    Issues when rounding midnight since we are reading from the raw tile files. 

    Also, only need the last few hours of data since we're (currently) only displaying 
    3 hours on the dash app. 
    """
    merged_df = []
    for tile in tilefiles:
        df = pd.read_parquet(tile)
        merged_df.append(df)
    merged_df = pd.concat(merged_df)
    merged_df.drop_duplicates(inplace=True)
    merged_df.sort_values(by=['siteid', 'dateutc'], inplace=True)
    merged_df.dropna(subset='precip', inplace=True)
    filename = f"{OUTPUT_DIR}/master_db.parquet"
    merged_df.to_parquet(filename, engine='pyarrow', index=False)

    log.info(f"# of observations: {len(merged_df):_}")
    log.info(f"Total Time: {round(time.time()-t1, 2)} seconds")

def main():
    now = datetime.utcnow()
    process_data(now)

if __name__ == '__main__':
    main()
