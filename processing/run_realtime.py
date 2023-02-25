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

def info(text):
    return f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} {text}"

def run():
    msg = info("Running download script.")
    log.info(f"Running download script.")
    print(msg)
    arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py"
    execute(arg)

    run_driver()
    msg = info("Sleeping...")
    print(msg)

def run_driver():
    """
    Run the processing script to compute accumulation windows. 
    """
    msg = info("Running processing script.")
    log.info(f"Running processing script.")
    print(msg)
    arg = f"{PYTHON} {SCRIPT_PATH}/driver.py"
    p = execute(arg)
    if p.returncode != 0:
        msg = info("[ERROR] status excuting processing script.")
        log.error("-->driver.py failed")
        print(msg)
    else:
        msg = info("[GOOD] status excuting processing script.")
        print(msg)
    return p

def initialize_data():
    """
    Initialize the local db by downloading WU tiles from the past hour. 
    """
    now = datetime.utcnow() 
    latest_quarter_hour = pd.to_datetime(now).floor('15T')
    print(info("Initializing precipitation database."))
    for back_minutes in range(60, 0, -15):
        dt = latest_quarter_hour - timedelta(minutes=back_minutes)
        datestring = dt.strftime('%Y-%m-%d/%H%M')

        print(info(f"Downloading data tiles for {dt}-{dt+timedelta(minutes=15)}"))
        arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py -t {datestring}"
        execute(arg)

    run_driver()
    print(info("Sleeping until crons begin..."))

def run_crons():
    task = schedule.Scheduler()
    task.every().hour.at(":02").do(run)
    task.every().hour.at(":17").do(run)
    task.every().hour.at(":32").do(run)
    task.every().hour.at(":47").do(run)
    print(info("Sleeping until crons begin..."))
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

    init_db = check_if_dbinit_needed();
    if init_db:
        initialize_data()
    run_crons()