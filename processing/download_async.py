"""
Downloads realtime observations from Weather Underground tiles.  

Code is optimized to handle many simultaneous web server requests and subsequent data 
storage very quickly. The slowest part of this process is IO-related (i.e. reading of 
stored data on disk & the subsequent re-writing to update with the latest observations). 

For large domains exceeding about 5,000 tiles, separate calls to download_async will 
likely provide better performance than bundling everything into a single execution. 
"""

import asyncio 
import async_timeout
import aiohttp

import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
import time
from pathlib import Path
from multiprocessing import Pool

from configs import (BASE_URL, PURGE_HOURS, x_start, x_end, y_start, y_end, WUNDER_DIR,
                     MAX_RETRIES)
from utils.log import logfile
import os
SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"{datetime.utcnow().strftime('%Y%m%d')}_download.log")

async def fetch_all(session, urls):
    """Asynchronous GET request functions"""
    total_urls = len(urls)

    async def fetch(session, url):
        # see https://stackoverflow.com/questions/65983012/aiohttp-having-request-body-
        # content-text-when-calling-raise-for-status
        async with session.get(url) as r:
            if r.status != 200:
                r.raise_for_status()
            return await r.text() 
    
    def create_tasks(session, urls):
        tasks = []
        for url in urls: 
            # Create async task group
            task = asyncio.create_task(fetch(session, url))
            tasks.append(task)
        return tasks

    tasks = create_tasks(session, urls)
    delay_seconds = 3
    full_res = []
    return_flag = False
    for _ in range(MAX_RETRIES):
        info_dict = {
        'good': [],
        'bad': [],
        'retry': []
        }
        try:
            async with async_timeout.timeout(5):
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

            log.info(info_dict)
            full_res.extend(res)

            # 100% of tiles returned successfully. Exit the download loop.  
            if len(info_dict['bad']) == 0 and len(info_dict['retry']) == 0:
                log.info(f"[SUCCESS] Good download status.")
                return_flag = True    

            # Some bad data, but no other issues. Bad data previously pruned. Exit.
            elif len(info_dict['bad']) > 0 and len(info_dict['retry']) == 0:
                log.info(f"[SUCCESS] Good download status. Some bad URLs")
                return_flag = True  

            # Need to retry a series of URLs. Will head back through the for loop. 
            elif len(info_dict['retry']) > 0:
                urls = [urls[i] for i in info_dict['retry']]
                log.info(f"[RETRY] Retrying: {len(urls)} URLs")
                tasks, xvals, yvals = create_tasks(session, urls)
                return_flag = False
                time.sleep(delay_seconds)
                continue

            else:
                log.warning(f"Some other error. Resetting and trying again.")
                full_res = []
                return_flag = False 
                time.sleep(delay_seconds)
                continue

            if return_flag: 
                log.info(f"Sending {len(full_res)} of {total_urls} tiles to parser.")
                return full_res
                    
        except (TimeoutError, asyncio.TimeoutError, asyncio.CancelledError):
            full_res = []
            log.warning(f"Could not connect to WU. Sleeping {delay_seconds} seconds.")
            time.sleep(delay_seconds)
            delay_seconds *= 2
            continue     
        else:
            log.error(f"[FAILURE] No data available to download. Exiting.")
            log.info(f"Sending {len(full_res)} of {len(urls)} tiles to parser.")
            break
    else:
        log.error(f"[FAILURE] Exceeded {MAX_RETRIES} retries. Exiting.")
        log.info(f"Sending {len(full_res)} of {len(urls)} tiles to parser.")

    return full_res

async def download_data(dt, user_datetime=None):
    """
    Main function which oversees downloading of weather underground data tile files. 

    Initial step is to produce a master list of url requests for every tile for the 
    current and previous 15-minute rolling window. Weather Underground seems to store
    these in quarter-hour chunks. Requests are then passed to asyncio helper functions.
    Final step involves merging with previous data on disk, pruning old observations,
    and writing the latest dataframe out as a compressed parquet file. Uses default 
    "snappy" compression for better I/O speeds at the expense of larger file sizes. 

    Parameters:
    -----------
    dt: datetime.datetime 
        The current execution time. Will be rounded down to the closest quarter-hour. 

    Optional parameters:
    --------------------
    user_datetime: datetime.datetime 
        A requested time in the past. Note that WU only seems to store about an hour's 
        worth of data within the high-resolution tiles.
    """

    log.info("=================================================================")
    if user_datetime is not None:
        dt = user_datetime 

    start = pd.to_datetime(dt).floor('15T')
    delta = int((dt - start).total_seconds()//60) + 1 
    purge_dt = dt - timedelta(hours=PURGE_HOURS)
    log.info(f"Start of 15-minute window: {start}")

    # Convert to epoch milliseconds
    start = int(start.timestamp()*1000)

    # The :{delta} seems to tell the API how many minutes after the start of the initial
    # reference period we're at now. Without this, repeated calls within a 15-minute 
    # window do not result in additional/new data. 
    time_string = f"{start}-{start+900000}:{delta}"              # current 15-min window      
    time_string2 = f"{start-900000}-{start}:{int(delta+15)}"     # previous 15-min window           

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
        htmls = await fetch_all(session, urls)
    
    log.info(f"TIME TO COMPLETE DATA SCRAPING: {round(time.time()-t1, 2)} seconds")

    """
    I/O part of the download process, which is the most CPU-intensive. Pool is faster  
    than ThreadPool in this case as a result. Lingering improvements are likely limited
    to dataframe I/O. 
    """
    with Pool(4) as pool:
        ret = pool.starmap(parse_info_tiles, zip(htmls))
    
    final = pd.concat(ret)
    datafile = f"{WUNDER_DIR}/merged_tiles.parquet"

    # Read current dataframe on disk, if it exists and merge.
    temp_df = pd.DataFrame(columns=final.columns)
    if Path(datafile).exists():
       temp_df = pd.read_parquet(datafile)
    final = pd.concat([final, temp_df])
    final.drop_duplicates(subset=['siteid', 'dateutc'], inplace=True)

    # Prune entries older than PURGE_HOURS and write to disk.
    final = final.loc[final['dateutc'] >= purge_dt]
    final['siteid'] = final['siteid'].astype('category')
    final[['lon','lat','precip']] = final[['lon','lat','precip']].apply(pd.to_numeric, 
                                                                      downcast='float')
    final['localhour'] = pd.to_numeric(final['localhour'], downcast='unsigned')
    final.to_parquet(datafile)

def parse_info_tiles(html):
    """
    Parse the json data returned from each tile's GET request and return a python dict.

    - Rounding lat/lons to 2 decimal places. 

    Parameters:
    -----------
    html: dict-string 
        Result of GET requests from WU API
    xval, yval: ints
        x and y values of this tile's data
    """
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
            lon = round(lon, 2)
            lat = round(lat, 2)
            data_dict['siteid'].append(val['id'])
            data_dict['lon'].append(lon)
            data_dict['lat'].append(lat)
            data_dict['dateutc'].append(pd.to_datetime(values['dateutc']))
            data_dict['precip'].append(values['dailyrainin'])
            data_dict['localhour'].append(values['localhour'])

    except (json.decoder.JSONDecodeError, KeyError, TypeError):
        log.warning(f"Unable to parse tile")
    
    df = pd.DataFrame.from_dict(data_dict)
    return df

if __name__ == '__main__':
    now = datetime.utcnow()
    ap = argparse.ArgumentParser()
    ap.add_argument('-t', '--time-str', dest='time_string', help='Time to attempt &    \
                    fetch past data tiles (these are only available for the last hour).\
                    Format is YYYY-mm-dd/HHMM')
    ap.add_argument('-x', '--x-range', dest='x_range', help='Starting and ending x-    \
                    values. Format is: x-start,x-end')
    ap.add_argument('-y', '--y-range', dest='y_range', help='Starting and ending y-    \
                    values. Format is: y-start,y-end')
    args = ap.parse_args()

    # User specified tile values in the x-direction. Override configs.py specifications. 
    if args.x_range is not None:
        x_split = args.x_range.split(',')
        x_start, x_end = int(x_split[0]), int(x_split[1])

    # User specified tile values in the y-direction. Override configs.py specifications. 
    if args.y_range is not None:
        y_split = args.y_range.split(',')
        y_start, y_end = int(y_split[0]), int(y_split[1])

    user_dt = None
    if args.time_string is not None:
        user_dt = datetime.strptime(args.time_string, '%Y-%m-%d/%H%M')

    t1 = time.time()
    asyncio.run(download_data(now, user_datetime=user_dt))
    log.info(f"TOTAL DOWNLOAD AND I/O TIME: {round(time.time()-t1, 2)} seconds")
