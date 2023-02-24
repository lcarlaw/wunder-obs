import os

PYTHON = '/Users/leecarlaw/anaconda3/envs/wunder/bin/python'

# You should be able to leave these alone. Alter if desired. 
SCRIPT_PATH = os.path.dirname(__file__) or "."
DATA_DIR = f"{SCRIPT_PATH}/data"
OUTPUT_DIR = f"{SCRIPT_PATH}/output"
ARCHIVE_DIR = f"{OUTPUT_DIR}/archive"
LOG_DIR = f"{SCRIPT_PATH}/logs"             # Location of logfiles
WUNDER_DIR = f"{DATA_DIR}/wunder_tiles"     # Weather Underground tiles

PURGE_HOURS = 24                            # number of hours to store data locally
MAX_DIFF_MINUTES = 5                        # max differential tolerance for ob age
MAX_AGE_MINUTES = 30                        # older observations won't be displayed
DELTA_TOLERANCE = 20                        # Max 5-min precip rate before flagging
MAX_RETRIES = 5                             # Max download retries after failure

"""
Weather Underground API data stored in tiles. x-values increase west-to-east while y- 
values do so north-to-south. Each tile seems to cover about about 15x15 km area. 

Note that adding tiles will increase both download times and data storage requirements:
    - Disk space: 40 MB/1000 tiles/hour 
    - For 10_000 tiles, GET requests and file storage take about 25 seconds. 
"""

#x_start, x_end = 480, 560
#y_start, y_end = 720, 800

x_start, x_end = 490, 550
y_start, y_end = 730, 790

# Weather Underground specs
API_KEY = 'e1f10a1e78da46f5b10a1e78da96f525'
url_base = 'https://api.weather.com/v2/vector-api/products/614/features?'
BASE_URL = f"{url_base}apiKey={API_KEY}"

MRMS_URL = 'https://mrms.ncep.noaa.gov/data/2D/RadarOnly_QPE_01H/'
MAPBOX_ACCESS_TOKEN = "pk.eyJ1IjoibGNhcmxhdyIsImEiOiJja2drMWM5dm0yMGZkMnFsN3NlNHZseGNmIn0.CKrzXaLRaDJ6ggGZT2eWFg"

# Text for dash-application
infostrings = {
    'general-info': 
        """NOTE: Scripts are set to fetch precipitation data from the Weather Underground
        public-API 4x per hour at :02, :17, :32, and :47. These are parsed into 15-,
        30-, 60-, and 180-minute accumulation intervals.\n   \nNOTE: Unless download 
        scripts are running constantly on a cron, they will need time to accumulate 
        data since only 1 hour of trailing data is available on the Weather 
        Underground API. You will initially notice missing data for the 1- and 3- hour 
        thresholds as a result."""
}

TOOLTIPS = {
    'display-threshold':
        """Threshold below which data for the accumulation period selected above will
        not be displayed."""
}