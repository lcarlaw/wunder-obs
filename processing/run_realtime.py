"""
run.py controls the realtime execution of the main download and processing scripts.
"""

import os
import sys 
import schedule
import time
import pandas as pd
from pathlib import Path 
from glob import glob
from datetime import datetime, timedelta

from configs import PYTHON, CRON_RUN_MINUTES, WUNDER_DIR, LOG_DIR, MAX_DIRECTORY_SIZE
from utils.log import logfile
from utils.cmd import execute
SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"run-cron.log")

def get_directory_sizes():
    """
    Keep track of filesizes. Sources both the logfile and data directories. If these 
    balloon above MAX_DIRECTORY_SIZE (in MB), kill the download script process.
    """
    file_list = glob(f"{WUNDER_DIR}/*")
    file_list.extend(glob(f"{LOG_DIR}/*"))
    directory_size = 0.
    for f in file_list:
        directory_size += Path(f).stat().st_size

    directory_size = directory_size/1000000.
    if directory_size > MAX_DIRECTORY_SIZE:
        log.error(f"Data and logfile directories exceeded {MAX_DIRECTORY_SIZE} MB. Exiting.")
        print(f"Data and logfile directories exceeded {MAX_DIRECTORY_SIZE} MB. Exiting.")
        sys.exit()

def run():
    get_directory_sizes()

    log.info(f"Running download script.")
    arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py"
    p = execute(arg)
    if p.returncode != 0:
        log.error("[ERROR] status excuting download script.")   

    # Process the tile files: compute running totals and QC erroneous data.
    run_processing()

def run_processing():
    """
    Run the processing script to compute accumulation windows. 
    """
    log.info(f"Running processing script.")
    arg = f"{PYTHON} {SCRIPT_PATH}/process_data.py"
    p = execute(arg)
    if p.returncode != 0:
        log.error("[ERROR] status excuting processing script.")   
    else:
        log.info("Done with processing script.")
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

    run()

def run_crons():
    task = schedule.Scheduler()
    task.every(CRON_RUN_MINUTES).minutes.do(run)
    log.info("Sleeping until crons begin...")
    while True:
        task.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    initialize_data()
    run_crons()