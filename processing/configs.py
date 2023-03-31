"""
Path to Python exeuctable and path to scripts. Used by run_realtime.py
"""
PYTHON = '/Users/leecarlaw/anaconda3/envs/wunder/bin/python'
SCRIPT_PATH = '/Users/leecarlaw/scripts/wunder-precip/processing'

#PYTHON = r'\\lot-s-filesvr\met-apps\python-virtual-environments\wunder\Scripts\python.exe'
#SCRIPT_PATH = r'\\lot-s-filesvr\met-apps\wunder-precip\processing'

# You should be able to leave these alone. Alter if desired. 
DATA_DIR = f"{SCRIPT_PATH}/data"
OUTPUT_DIR = f"{SCRIPT_PATH}/output"
ARCHIVE_DIR = f"{OUTPUT_DIR}/archive"
LOG_DIR = f"{SCRIPT_PATH}/logs"             # Location of logfiles
WUNDER_DIR = f"{DATA_DIR}/wunder_data"      # Weather Underground data

CRON_RUN_MINUTES = 10                       # How often to run download/process scripts
MAX_RETRIES = 5                             # Max download retries after failure
PURGE_HOURS = 120                           # number of hours to store data locally
MAX_DIFF_MINUTES = 5                        # max differential tolerance for ob age
MAX_AGE_MINUTES = 30                        # older observations won't be displayed

"""
Weather Underground API data stored in tiles. x-values increase west-to-east while y- 
values do so north-to-south. Each tile seems to cover about about 15x15 km area. 

Adding too many tiles into a single call will result in downloads timing out and/or 
diminishing download performance. Keep the total number of tile requests under 5,000 at
a time for best results. 

Alternatively, you can specify the x and y ranges within the download_async call 
directly by using the -x and -y flags:
    python download_async.py -x 500,549 -y 700,745
"""
# WFO LOT
#x_start, x_end = 514, 532
#y_start, y_end = 754, 773

# WFO LOT, ILX, and MKX
x_start, x_end = 500, 540
y_start, y_end = 744, 786

x_start, x_end = 470, 520
y_start, y_end = 780, 830

# Accumulation periods (in minutes)
ACCUM_PERIODS = [180, 60, 30, 15]

# Weather Underground specs. These are all public keys, so no need to hide them.
API_KEY = 'e1f10a1e78da46f5b10a1e78da96f525'
url_base = 'https://api.weather.com/v2/vector-api/products/614/features?'
BASE_URL = f"{url_base}apiKey={API_KEY}"

MRMS_URL = 'https://mrms.ncep.noaa.gov/data/2D/RadarOnly_QPE_01H/'
MAPBOX_ACCESS_TOKEN = "pk.eyJ1IjoibGNhcmxhdyIsImEiOiJja2drMWM5dm0yMGZkMnFsN3NlNHZseGNmIn0.CKrzXaLRaDJ6ggGZT2eWFg"

# Text for dash-application
TOOLTIPS = {
    'display-threshold':
        """Threshold below which data for the accumulation period selected above will
        not be displayed."""
}