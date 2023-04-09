"""
run.py controls the realtime execution of the main download and processing scripts.
"""

import os
import schedule
import time
import pandas as pd
from datetime import datetime, timedelta

from configs import PYTHON, CRON_RUN_MINUTES
from utils.log import logfile
from utils.cmd import execute
SCRIPT_PATH = os.path.dirname(__file__) or "."
log = logfile(f"run-cron.log")

def run():
    log.info(f"Running download script.")
    arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py"
    p = execute(arg)
    if p.returncode != 0:
        log.error("[ERROR] status excuting download script.")   

    # For large domains, you'll see better performance by splitting into multiple 
    # calls to download_async rather than doing everything all at once. You can specify
    # x and y tile values on the command line with the -x and -y flags. 
    #arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py -x 500,549 -y 700,749"
    #execute(arg)
    #arg = f"{PYTHON} {SCRIPT_PATH}/download_async.py -x 550,599 -y 750,799"
    #execute(arg)

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