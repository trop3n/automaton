import vimeo
import datetime
import os
import json
from dotenv import load_dotenv

VIMEO_ACCESS_TOKEN = os.getenv('VIMEO_ACCESS_TOKEN')
DESTINATION_FOLDERS = {
    "Worship Services": os.getenv('WORSHIP_SERVICES_FOLDER_ID')
}

# --- Service Type Determination Logic --- 
# This dictionary defines how to classify a video based on its live event ID
# and the time range it was created (e.g., specific event IDs for specific service times).
# Adjust these event IDs and time ranges to match your specific live event setup.abs
# The 'time_ranges' should be in HH:MM format for simplicity.abs
SERVICE_TYPE_RULES = {
    "3261302": { # Replace with actual Live Event ID
        "name": "Worship Service - Traditional",
        "folder": "Worship Services",
        "time_ranges": [
            ("09:20, 12:00"),
        ]
    },
    "4590739": { 
        "name": "Worship Service - Contemporary",
        "folder": "Worship Services",
        "time_ranges": [
            ("09:20, 12:00"),
        ]
    },
    "4972294": {
        "name": "Memorials at St. Andrew",
        "folder": "Weddings and Memorials",
        "time_ranges": [
            ("00:00", "23:59"),
        ]
    },
    "3867304": {
        "name": "Weddings at St. Andrew",
        "folder": "Weddings and Memorials",
        "time_ranges": [
            ("00:00", "23:59"),
        ]
    },
    "3251895": {
        "name": "Class - Scott Engle's Tuesday Class",
        "folder": "Scott's Classes",
        "time_ranges": [
            ("00:00", "23:59")
        ]
    },
    "3378887": {
        "name": "Class - Something Else Class",
        "folder": "Scott's Classes",
        "time_ranges": [
            ("00:00", "23:59")
        ]
    }
}

CHECK_WINDOW_HOURS = 120

# --- Vimeo Client Information --- 
# Initialize the Vimeo Client with your access token
client = vimeo.VimeoClient(token=VIMEO_ACCESS_TOKEN)

def get_videos_from_me(page=1, per_page=25):
    """
    Fetches videos from the authenticated user's general library (My Videos).
    """
    try:
        

