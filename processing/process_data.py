import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None
from datetime import datetime, timedelta
import time
import os
from pathlib import Path 
from glob import glob

from configs import (MAX_AGE_MINUTES, MAX_DIFF_MINUTES, WUNDER_DIR, ACCUM_PERIODS)
from utils.log import logfile

import multiprocessing as mp
from multiprocessing import freeze_support

SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"{datetime.utcnow().strftime('%Y%m%d')}_process_data.log")

def data_qc(df):
    """
    Input df will be sorted, with older data first and newer data at the end. The first
    index is at the start of this particular precip window, while the last index is the 
    most recent observation. 
    """
    precip_amount = np.nan

    # CASE 1: Filtered data is entirely monotonically increasing
    if df['precip'].is_monotonic_increasing:
        precip_amount = df['precip'].iloc[-1] - df['precip'].iloc[0]
        precip_amount = round(precip_amount, 2)
    else:
        dx = np.diff(df['precip'])
        idx = np.where(dx < 0)
        num_backwards = len(idx[0])
        
        # CASE 2: A single precip decrease in this window.
        if num_backwards == 1:
            # This reset happened either at midnight or 1 am local. There also seem
            # to be sites that reset at other hours??
            hours = df['localhour']
            if (hours.iloc[idx[0][0]] == 23 and hours.iloc[idx[0][0]+1] == 0) or \
               (hours.iloc[idx[0][0]] == 0 and hours.iloc[idx[0][0]+1] == 1):
                
                max_daily_val = df['precip'].iloc[idx[0][0]]
                temp = df['precip'][idx[0][0]+1:] + max_daily_val
                df['precip'][idx[0][0]+1:] = temp 
                precip_amount = df.iloc[-1]['precip'] - df.iloc[0]['precip']
                precip_amount = round(precip_amount, 2)

            # This reset happened at another time. In this case, while the rest of
            # the data may be okay, for now, assume the data is bad at this time. 
            #else
        
        # CASE 3:
        # We are probably neglecting some good data with sites that temporarily 
        # report a negative dx, but then return to the baseline.

    return precip_amount

def calc_site_precip(df):
    """
    """
    output_dict = {
        'siteid': [],
        'lon': [],
        'lat': [],
        'latest_ob_time': [],
    }
    for i in ACCUM_PERIODS: 
        output_dict[f"{i}_min"] = []

    sites = df.siteid.unique()
    for site in sites:
        data = df.loc[df.siteid==site]
        end_dt = data['dateutc'].iloc[-1]

        output_dict['siteid'].append(site)
        output_dict['lon'].append(data['lon'].iloc[-1])
        output_dict['lat'].append(data['lat'].iloc[-1])
        output_dict['latest_ob_time'].append(end_dt)

        for accum_pd in ACCUM_PERIODS:
            start_dt = end_dt-timedelta(minutes=accum_pd)
            deltas = (start_dt - data.dateutc).abs()
            if deltas.loc[deltas.idxmin()] <= timedelta(minutes=MAX_DIFF_MINUTES):
                window_df = data.loc[deltas.idxmin():]
                precip_amount = data_qc(window_df)
                output_dict[f"{accum_pd}_min"].append(precip_amount)
            else:
                output_dict[f"{accum_pd}_min"].append(np.nan)

    output_df = pd.DataFrame.from_dict(output_dict)
    return output_df

def chunk_dataframe(df, siteids, n_chunks):
    chunksize = len(siteids) // n_chunks
    for i in range(n_chunks):
        sites = siteids[0:chunksize]
        if i == n_chunks-1:
            sites = siteids[0:]

        out_df = df[df.siteid.isin(sites)]
        yield out_df

        # Remove what we've just chunked from the main dataframe
        df = df[~df.siteid.isin(sites)]
        siteids = list(set(siteids).difference(sites))

def process(now):
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

    # Trim the dataframe to encompass the longest accumulation period window, plus a 
    # small buffer. Saves sending unused data to the QC functions. 
    start_dt = now - timedelta(minutes=max(ACCUM_PERIODS) + 59)
    filtered = filtered[filtered.dateutc.between(start_dt, now)]

    log.info(
        f"Processing {len(filtered):_} observations from "
        f"{len(filtered.siteid.unique()):_} sites.")

    # Chunk the data based on siteid and send to worker processes
    siteids = filtered.siteid.unique()
    n_jobs = 4
    pool = mp.Pool(n_jobs)
    result = pool.imap(calc_site_precip, chunk_dataframe(filtered, siteids, n_jobs))
    pool.close()
    pool.join()

    merged = []
    for chunk in result:
        merged.append(chunk)
    output_df = pd.concat(merged)

    # Drop rows in which data is NaN for all precip time periods.
    cols = output_df.columns[output_df.columns.str.contains('_min')]
    output_df.dropna(subset=cols, how='all', inplace=True)
    filename = f"{WUNDER_DIR}/latest_obs.parquet"
    output_df.to_parquet(filename)

    # Gather the filesizes to keep track of storage on disk
    file_list = glob(f"{WUNDER_DIR}/*")
    filesize = 0.
    for f in file_list:
        filesize += Path(f).stat().st_size
    log.info(f"{WUNDER_DIR} filesize: {round(filesize/1000000., 1)} MB")

def main():
    log.info("=================================================================")
    t1 = time.time()
    now = datetime.utcnow()
    process(now)
    log.info(f"TOTAL time: {round(time.time()-t1, 2)} seconds")

if __name__ == '__main__':
    freeze_support()
    main()
