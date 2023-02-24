"""
Downloads realtime observations from Weather Underground in tiles.  

Code is optimized to handle many simultaneous web server requests and subsequent data 
storage very quickly. The slowest part of this process is IO-related (i.e. reading of 
stored data on disk & the subsequent re-writing to update with the latest observations). 

In testing, acquiring data from 10_000 tile files and writing to disk took about 25-30 
seconds. This takes many minutes to perform synchronously on a single thread and is 
prohibitive for realtime purposes. There seem to be ~60_0000 tiles across the CONUS (each
tile is about 15 x 15 km).

Logic for busted URL calls is needed. Because of asynchronous nature, need to listen for 
specific failed tasks and then send them running within a retry and backoff function 
somehow. Needs more thought. 
"""

import asyncio 
import aiohttp

import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
import requests
import time
from pathlib import Path
from multiprocessing import Pool
from itertools import repeat

from configs import (BASE_URL, PURGE_HOURS, x_start, x_end, y_start, y_end, WUNDER_DIR,
                     MAX_RETRIES)
from utils.log import logfile
import os
SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"{datetime.utcnow().strftime('%Y%m%d')}_download.log")

async def fetch(s, url):
    async with s.get(url) as r:
        if r.status != 200:
            r.raise_for_status()
        return await r.text() 
    
async def fetch_all(s, urls):
    tasks = []
    for url in urls: 
        task = asyncio.create_task(fetch(s, url))
        tasks.append(task)
    res = await asyncio.gather(*tasks, return_exceptions=True)

    '''
    delay = 5
    for _ in range(MAX_RETRIES):
        try:
            res = await asyncio.gather(*tasks)
            msg = f"[GOOD] download status."
            print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} {msg}")
            log.info(msg)
            break

        # This one is ok. Just wait and try again 
        except aiohttp.client_exceptions.ClientConnectorError:
            msg = f"Could not connect to WU. Sleeping {delay} s and trying again."
            print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} {msg}")
            log.warning(msg)
            time.sleep(delay)
            delay *= 2
            continue 

        # This one means there is no data available. 
        except aiohttp.client_exceptions.ClientResponseError:
            msg = f"No data available. Nothing to download."
            print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} {msg}")
            log.error(msg)
            res = '{}'
            break
        else:
            msg = f"Unknown issue downloading WU data. Exiting."
            print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} {msg}")
            log.error(msg)
            res = '{}'
            break
    else:
        res = '{}'
        msg = f"Exceeded {MAX_RETRIES} with failed downloads. Exiting."
        print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} {msg}")
        log.error(msg)
        raise Exception(f"Exceeded {MAX_RETRIES} with failed downloads.")
    '''
    return res

async def download_data(dt, user_datetime=None):
    """
    Main function which oversees downloading of weather underground data files. 

    Initial step is to produce a master list of url requests for every tile and
    requested/latest 15-minute window. Weather Underground seems to store these in
    quarter-hour chunks. Requests are then passed to asyncio helper functions.

    Parameters:
    -----------
    dt: datetime.datetime 
        The current execution time. Will be rounded to the closest quarter-hour. 

    Optional parameters:
    --------------------
    user_datetime: datetime.datetime 
        A requested time in the past. This is used by run_realtime when initially 
        populating the precipitation directory. 

    """
    log.info("=================================================================")
    if user_datetime is not None:
        dt = user_datetime 

    start = pd.to_datetime(dt).floor('15T')
    purge_dt = dt - timedelta(hours=PURGE_HOURS)
    log.info(f"Start of 15-minute window: {start}")

    # Convert to epoch milliseconds
    start = int(start.timestamp()*1000)
    time_string = f"{start}-{start+900000}"                 # Current 15-minute window
    time_string2 = f"{start-900000}-{start}"                # Previous 15-minute window

    urls = []
    xvals = []
    yvals = []
    for x in range(x_start, x_end+1):
        for y in range(y_start, y_end+1):
            urls.append(
                f"{BASE_URL}&x={x}&y={y}&lod=12&tile-size=512&time={time_string}"
                f"&time={time_string2}"
            )
            xvals.append(x)
            yvals.append(y)
            
    # This is where the actual data acquisition from the WU API takes place.
    t1 = time.time()
    async with aiohttp.ClientSession() as session:
        htmls = await fetch_all(session, urls)
    
    log.info(f"TIME TO COMPLETE DATA SCRAPING: {round(time.time()-t1, 2)} seconds")
    
    """
    I/O part of the download process, which is the most CPU-intensive. Pool is much 
    faster than ThreadPool in this case as a result. Lingering improvements are likely
    limited to dataframe I/O. 
    """
    with Pool(8) as pool:
        pool.starmap(parse_info_tiles, zip(htmls, xvals, yvals, repeat(purge_dt)))

def parse_info_tiles(html, xval, yval, purge_dt):
    """
    Work through each tile file (now held within memory) and send to corresponding 
    dataframes (or create them if they don't already exist). 

    Data that is older than PURGE_HOURS is removed from the .json files after each 
    iteration.

    Parameters:
    -----------
    query: dict 
        Dictionary of querystrings being passed as a GET request
    xval, yval: ints
        x and y values of this tile's data.
    purge_dt: datetime.datetime
        Datetime before which older data will be purged
    """
    COLUMNS = ['siteid', 'lon', 'lat','dateutc', 'precip', 'localhour']
    
    # Set the output file. If it already exists, read in the data.
    datafile = f"{WUNDER_DIR}/{xval}_{yval}.parquet"
    df = pd.DataFrame(columns=COLUMNS)
    if Path(datafile).exists():
       df = pd.read_parquet(datafile)

    data_dict = {
        'siteid': [],
        'lon': [],
        'lat': [],
        'dateutc': [],
        'precip': [],
        'localhour': []
    }
    try:
        data = json.loads(html)
        for val in data['features']:
            values = val['properties']
            lon, lat = val['geometry']['coordinates']
            data_dict['siteid'].append(val['id'])
            data_dict['lon'].append(lon)
            data_dict['lat'].append(lat)
            data_dict['dateutc'].append(pd.to_datetime(values['dateutc']))
            data_dict['precip'].append(values['dailyrainin'])
            data_dict['localhour'].append(values['localhour'])

    except json.decoder.JSONDecodeError:
        log.warning(f"No JSON data available for tile {xval}_{yval}")

    except KeyError:
        log.warning(f"Key Errors for tile {xval}_{yval}")

    except requests.exceptions.ConnectTimeout:
        log.warning(f"Connection timed out for tile {xval}_{yval}")
    
    except requests.exceptions.ConnectionError:
        log.warning(f"Max retries for tile {xval}_{yval}")

    # Merge the data and drop any entries older than purge_dt. Save to disk.
    df = pd.concat([df, pd.DataFrame(data_dict)])
    initial_length = len(df)
    df = df.loc[df['dateutc'] >= purge_dt]
    end_length = len(df)
    delta_length = initial_length - end_length

    # Observations within this tile were purged.
    if delta_length > 0:
        log.info(f"Dropped {delta_length} observations from dataframe")

    # Better to sort here after download, or within qc step each time?
    df.to_parquet(datafile, engine='pyarrow', index=False)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-t', '--time-str', dest='time_string', help='Time to attempt &    \
                    fetch past data tiles (these are only available for the last hour).\
                    Format is YYYY-mm-dd/HHMM')
    args = ap.parse_args()
    now = datetime.utcnow()

    user_dt = None
    if args.time_string is not None:
        user_dt = datetime.strptime(args.time_string, '%Y-%m-%d/%H%M')

    t1 = time.time()
    asyncio.run(download_data(now, user_datetime=user_dt))
    log.info(f"TOTAL DOWNLOAD AND I/O TIME: {round(time.time()-t1, 2)} seconds")
