"""
Downloads realtime observations from Weather Underground in tiles.  

Code is optimized to handle many simultaneous web server requests and subsequent data 
storage very quickly. The slowest part of this process is IO-related (i.e. reading of 
stored data on disk & the subsequent re-writing to update with the latest observations). 

In testing, acquiring data from 10_000 tile files and writing to disk took about 25-30 
seconds. This takes many minutes to perform synchronously on a single thread and is 
prohibitive for realtime purposes. There seem to be ~60_0000 tiles across the CONUS 
(each tile is about 15 x 15 km).

For large domains exceeding about 5,000 tiles, separate calls to download_async will 
likely provide better performance than bundling everything into a single execution. 
"""

import asyncio 
import async_timeout
import aiohttp

import re
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

async def fetch(session, url):
    async with session.get(url) as r:
        if r.status != 200:
            r.raise_for_status()
        return await r.text() 
   
def create_tasks(session, urls):
    tasks = []
    xvals = []
    yvals = []
    for url in urls: 
        # Grab the x- and y- tile locations
        loc_str = re.findall('x=\d{0,4}&y=\d{0,4}', url)[0]
        idx = loc_str.find('&')
        xvals.append(int(loc_str[2:idx]))
        yvals.append(int(loc_str[idx+3:]))
        task = asyncio.create_task(fetch(session, url))
        tasks.append(task)
    return tasks, xvals, yvals

async def fetch_all(session, urls):
    tasks, xvals, yvals = create_tasks(session, urls)
    delay = 3
    full_res = []
    return_flag = False
    for _ in range(MAX_RETRIES):
        info_dict = {
        'good': [],
        'bad': [],
        'retry': []
        }
        try:
            async with async_timeout.timeout(20):
                res = await asyncio.gather(*tasks, return_exceptions=True)
                for knt, item in enumerate(res):
                    if type(item) == aiohttp.client_exceptions.ClientConnectorError:
                        info_dict['retry'].append(knt)
                    elif type(item) == aiohttp.client_exceptions.ClientResponseError:
                        info_dict['bad'].append(knt)
                        log.warning(f"[BAD URL]: {str(item.request_info.url)}")   
                    else:
                        info_dict['good'].append(knt)

                indices_to_remove = info_dict['bad'] + info_dict['retry']
                for element in sorted(indices_to_remove, reverse=True):
                    del res[element]
                    del xvals[element]
                    del yvals[element]
                
                full_res.extend(res)

                # 100% of tiles returned successfully. Exit the download loop.  
                if len(info_dict['bad']) == 0 and len(info_dict['retry']) == 0:
                    log.info(f"[SUCCESS] Good download status.")
                    return_flag = True    

                # Some bad data. Remove from the response and exit the download loop. 
                elif len(info_dict['bad']) > 0 and len(info_dict['retry']) == 0:
                    log.info(f"[SUCCESS] Good download status. Some bad URLs")
                    return_flag = True  

                # Need to retry a series of URLs. 
                elif len(info_dict['retry']) > 0:
                    urls = [urls[i] for i in info_dict['retry']]
                    log.info(f"[RETRY] Retrying: {len(urls)} URLs")
                    tasks, xvals, yvals = create_tasks(session, urls)
                    return_flag = False
                    time.sleep(2)
                    continue

                else:
                    log.warning(f"Some other error. Resetting and trying again.")
                    full_res = []
                    return_flag = False 
                    time.sleep(2)
                    continue

                if return_flag: 
                    log.info(f"Sending {len(full_res)} of {len(urls)} tiles to parser.")
                    return full_res, xvals, yvals
                    
        except TimeoutError:
            log.warning(f"Could not connect to WU. Sleeping {delay} seconds.")
            time.sleep(delay)
            delay *= 2
            continue     
        else:
            log.error(f"[FAILURE] No data available to download. Exiting.")
            log.info(f"Sending {len(full_res)} of {len(urls)} tiles to parser.")
            break
    else:
        log.error(f"[FAILURE] Exceeded {MAX_RETRIES} retries. Exiting.")
        log.info(f"Sending {len(full_res)} of {len(urls)} tiles to parser.")

    return full_res, xvals, yvals

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
    for x in range(x_start, x_end+1):
        for y in range(y_start, y_end+1):
            urls.append(
                f"{BASE_URL}&x={x}&y={y}&lod=12&tile-size=512&time={time_string}"
                f"&time={time_string2}"
            )
            
    # This is where the actual data acquisition from the WU API takes place.
    t1 = time.time()
    async with aiohttp.ClientSession() as session:
        htmls, xvals, yvals = await fetch_all(session, urls)
    
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
    df = df.loc[df['dateutc'] >= purge_dt]

    # Better to sort here after download, or within qc step each time?
    df.to_parquet(datafile, engine='pyarrow', index=False)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-t', '--time-str', dest='time_string', help='Time to attempt &    \
                    fetch past data tiles (these are only available for the last hour).\
                    Format is YYYY-mm-dd/HHMM')
    ap.add_argument('-x', '--x-range', dest='x_range', help='Starting and ending x-    \
                    values. Format is: x-start,x-end')
    ap.add_argument('-y', '--y-range', dest='y_range', help='Starting and ending y-    \
                    values. Format is: y-start,y-end')
    args = ap.parse_args()
    now = datetime.utcnow()

    # User has specified tile values in the x-direction. Override configs.py specifications. 
    if args.x_range is not None:
        x_split = args.x_range.split(',')
        x_start, x_end = int(x_split[0]), int(x_split[1])

    # User has specified tile values in the y-direction. Override configs.py specifications. 
    if args.y_range is not None:
        y_split = args.y_range.split(',')
        y_start, y_end = int(y_split[0]), int(y_split[1])

    user_dt = None
    if args.time_string is not None:
        user_dt = datetime.strptime(args.time_string, '%Y-%m-%d/%H%M')

    t1 = time.time()
    asyncio.run(download_data(now, user_datetime=user_dt))
    log.info(f"TOTAL DOWNLOAD AND I/O TIME: {round(time.time()-t1, 2)} seconds")
