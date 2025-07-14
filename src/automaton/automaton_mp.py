import os
import re
from datetime import datetime, timedelta
import pytz
import requests
from dotenv import load_dotenv
from vimeo import VimeoClient

# Load environment variables from a .env file
load_dotenv()

# --- Configuration ---
# Reads all necessary credentials from your .env file.
VIMEO_ACCESS_TOKEN = os.environ.get('VIMEO_ACCESS_TOKEN')
VIMEO_CLIENT_ID = os.environ.get('VIMEO_CLIENT_ID')
VIMEO_CLIENT_SECRET = os.environ.get('VIMEO_CLIENT_SECRET')

# --- Ministry Platform API Integration ---
MP_API_ENDPOINT = os.environ.get('MP_API_ENDPOINT')
MP_CLIENT_ID = os.environ.get('MP_CLIENT_ID')
MP_CLIENT_SECRET = os.environ.get('MP_CLIENT_SECRET')

# Timezone for upload date calculations (e.g., 'America/Chicago' for CDT)
TIMEZONE = 'America/Chicago'
LOOKBACK_HOURS = 48
MAX_TIME_DIFFERENCE = 60

# Set these IDs for Ministry Platform Instance
# A list of location_ID values for rooms that have streaming capability.
STREAMING_LOCATION_IDS = []

# --- Folder Configuration ---
# List of folder IDs to EXCLUDE from processing. This rule is absolute.
EXCLUDED_FOLDER_IDS = ['11103430', '182762', '8219992']
DESTINATION_FOLDERS = {
    "Worship Services": '15749517',
    "Weddings and Memorials": '2478125',
    "Scott's Classes": '15680946',
}

# --- Ministry Platform Mapping ---
MP_CATEGORY_TO_VIMEO_FOLDER_NAME = {
    "Worship": "Worship Services",
    "Memorial": "Weddings and Memorials",
    "Wedding": "Weddings and Memorials",
    "Class": "Scott's Classes",
}

def get_vimeo_client(token, key, secret):
    """Initializes and returns the Vimeo client using token, key, and secret."""
    client = VimeoClient(token=token, key=key, secret=secret)
    return client

def get_mp_token():
    """Authenticates with Ministry Platform and returns an Access Token"""
    if not all ([MP_API_ENDPOINT, MP_CLIENT_ID, MP_CLIENT_SECRET]):
        print("INFO: Ministry Platform credentials are not configured. Skipping MP integration.")
        return None
    try:
        print("Attempting to get MP Token...")
        token_url = f"{MP_API_ENDPOINT}/oauth/connect/token"
        payload = {
            'grant_type': 'client_credentials',
            'scope': 'http://www.thinkministry.com/dataplatform/scopes/all',
            'client_id': MP_CLIENT_ID,
            'client_secret': MP_CLIENT_SECRET
        }
        response = requests.post(token_url, data=payload, timeout=10)
        response.raise_for_status()
        print("Successfully authenticated with Ministry Platform.")
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to get Ministry Platform token: {e}")
        return None

def get_mp_events_in_range(token, lookback_hours):
    """Fetches all streamable events from Ministry Platform in a given time window."""
    if not token:
        return []

    print(f"Fetching streamable MP events from the last {lookback_hours} hours...")
    start_time_utc = datetime.now(pytz.utc) - timedelta(hours=lookback_hours)

    # Build the OData filter for locations
    if not STREAMING_LOCATION_IDS:
        print("WARNING: No STREAMING_LOCATION_IDS configured. Cannot fetch MP events.")
        return []
    locations_filter = " or ".join([f"Location_ID eq {loc_id}" for loc_id in STREAMING_LOCATION_IDS])

    odata_filter = (
        f"Event_Start_date ge {start_time_utc.strftime('%Y-%m-%dT%H:%M:SZ')} and "
        f"Cancelled eq false and ({locations_filter})"
    )

    events_endpoint = f"{MP_API_ENDPOINT}/tables/Events"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    params = {'$filter': odata_filter, '$select': 'Event_Title,Event_Start_Date'}

    try:
        response = requests.get(events_endpoint, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        events = response.json()
        print(f"Found {len(events)} potential events in Ministry Platform.")
        # Convert date strings to datetime objects for easier comparison later
        for event in events:
            event['Event_Start_Date_dt'] = datetime.fromisoformat(event['Event_Start_Date'] + 'Z')
        return events
    except requests.exceptions.RequestException as e:
        print(f"ERROR: an error occurred querying Ministry Platform events: {e}")
        return []

# --- Core Logic ---
