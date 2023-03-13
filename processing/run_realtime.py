"""
run.py controls the realtime execution of the main download and processing scripts.
"""

import os
import schedule
import time
import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import freeze_support
from pathlib import Path

from configs import PYTHON, OUTPUT_DIR
from utils.log import logfile
from utils.cmd import execute
SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"run-cron.log")

def run():
    log.info(f"Running download script.")
    arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py"
    execute(arg)

    # For large domains, you'll see better performance by splitting into multiple 
    # calls to download_async rather than doing everything all at once. You can specify
    # x and y tile values on the command line with the -x and -y flags. 
    #arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py -x 500,549 -y 700,749"
    #execute(arg)
    #arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py -x 550,599 -y 750,799"
    #execute(arg)

    # Process the tile files: compute running totals and QC erroneous data.
    run_driver()

def run_driver():
    """
    Run the processing script to compute accumulation windows. 
    """
    log.info(f"Running processing script.")
    arg = f"{PYTHON} {SCRIPT_PATH}/driver.py"
    p = execute(arg)
    if p.returncode != 0:
        log.error("[ERROR] status excuting processing script.")   
    return p

def initialize_data():
    """
    Initialize the local db by downloading WU tiles from the past hour. 
    """
    now = datetime.utcnow() 
    latest_quarter_hour = pd.to_datetime(now).floor('15T')
    for back_minutes in range(60, 0, -15):
        dt = latest_quarter_hour - timedelta(minutes=back_minutes)
        datestring = dt.strftime('%Y-%m-%d/%H%M')

        arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py -t {datestring}"
        execute(arg)

    run_driver()

def run_crons():
    task = schedule.Scheduler()
    task.every().hour.at(":02").do(run)
    task.every().hour.at(":10").do(run)
    task.every().hour.at(":17").do(run)
    task.every().hour.at(":25").do(run)
    task.every().hour.at(":32").do(run)
    task.every().hour.at(":40").do(run)
    task.every().hour.at(":47").do(run)
    task.every().hour.at(":55").do(run)
    log.info("Sleeping until crons begin...")
    while True:
        task.run_pending()
        time.sleep(1)

def check_if_dbinit_needed():
    """
    Checks to see if we need to download additional data to build up the local obs
    database. WU only seems to store tiles for about the last hour.

    If most recent db ob is older than 15 minutes, re-init the local db by downloading
    the last hour of data from WU. 
    """
    initialize_db = False
    now = datetime.utcnow()
    latest_db_filename = f"{OUTPUT_DIR}/latest_obs.parquet"
    if Path(latest_db_filename).is_file():
        df = pd.read_parquet(latest_db_filename)
        delta = now - df.latest_ob_time.max()
        if delta.total_seconds() >= 900:
            initialize_db = True
    else:
        initialize_db = True
    return initialize_db

if __name__ == '__main__':
    freeze_support()
    #init_db = check_if_dbinit_needed();
    #if init_db:
    #    initialize_data()
    run_crons()