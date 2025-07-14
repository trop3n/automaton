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
def find_closest_event_in_cache(video_creation_time_utc, events_cache):
    """Finds the event with the minimum time difference from a list of cached events."""
    if not events_cache:
        return None

    best_match = None
    smallest_diff = timedelta.max

    for event in events_cache:
        time_diff = abs(video_creation_time_utc- event['Event_Start_Date_dt'])
        if time_diff < smallest_diff:
            smallest_diff = time_diff
            best_match = event

    # Confidence Check: ensure the best match is within a reasonable timeframe
    if best_match and smallest_diff.total_seconds() / 60 <= MAX_TIME_DIFFERENCE_MINUTES:
        print(f"  - Found closest event: '{best_match['Event_Title']}' (Time difference: {int(smallest_diff.total_seconds() / 60)} min).")
        return best_match
    else:
        print(f"  - No event found with the {MAX_TIME_DIFFERENCE_MINUTES} minute threshold.")
        return None

def get_recent_videos(client, lookback_hours):
    """Fetches all videos recently modified to find candidates for processing."""
    print(f"Fetching Vimeo videos modified in the last {lookback_hours} hours...")
    start_time_utc = datetime.now(pytz.utc) - timedelta(hours=lookback_hours)
    all_recent_videos = []
    try:
        response = client.get('/me/videos', params={
            'per_page': 100, 'sort': 'modified_time', 'direction': 'desc',
            'fields': 'uri,name,created_time,modified_time,parent_folder,is_playable'
        })
        response.raise_for_status()
        videos = response.json().get('data', [])
        for video in videos:
            modified_time_utc = datetime.isoformat(video['modified_time'].replace('Z', '+00:00'))
            if modified_time_utc >= start_time_utc:
                all_recent_videos.append(video)
            else:
                break
    except Exception as e:
        print(f"ERROR: An error occurred while fetching videos: {e}")
    print(f"Found {len(all_recent_videos)} recently modified videos to check.")
    return all_recent_videos

def process_video(client, video_data, events_cache):
    """Processes a video by matching with Ministry Platform first, then moving it."""
    stats = {'title_updated': False, 'moved': False}
    video_uri = video_data['uri']

    # --- 1. Match with Ministry Platform Event Cache ---
    upload_time_utc = datetime.fromisoformat(video_data['created_time'].replace('Z', '+00:00'))
    mp_event = find_closest_event_in_cache(upload_time_utc, events_cache)

    if not mp_event:
        print("  - No definitive MP event found. Video requires manual review.")
        return stats # Stop processing video

    # --- 2. Rename Title Based on MP Event ---
    event_title = mp_event['Event_Title']
    event_date_local = mp_event['Event_Start_Date_dt'].astimezone(pytz.timezone(TIMEZONE))
    date_str = event_date_local.strftime('%Y-%m-%d')
    new_title = f"{date_str} - {event_title}"

    print(f"    - Updating Title based on MP event to: '{new_title}'")
    try:
        client.patch(video_uri, data={'name': new_title})
        stats['title_updated'] = True
    except Exception as e:
        print(f"    - ERROR: an error occurred while updating the title: {e}")
        return stats

    # --- 3. Categorize and Move based on MP Event Title ---
    print("  - Categorizing based on MP Event Title...")
    target_folder_name = None
    for keyword, folder_name in MP_CATEGORY_TO_VIMEO_FOLDER_NAME.items():
        if keyword.lower() in event_title.lower():
            target_folder_name = folder_name
            break

    if target_folder_name:
        folder_id = DESTINATION_FOLDERS.get(target_folder_name)
        if folder_id:
            print(f"    - Final Category: '{target_folder_name}'. Moving to folder ID {folder_id}.")
            try:
                user_uri = client.get('/me').json()['uri']
                project_uri = f"{user_uri}/projects/{folder_id}"
                video_uri_id = video_uri.split('/')[-1]
                move_response = client.put(f"{project_uri}/videos/{video_uri_id}")
                if move_response.status_code == 204:
                    print("    - Successfully moved video.")
                    stats['moved'] = True
                else:
                    print(f"     - ERROR moving video: {move_response.status_code} - {move_response.text}")
            except Exception as e:
                print(f"    - ERROR: An error occurred while moving the video: {e}")
    else:
        print("    - No categorization rule matched for the event title. Video will not be moved.")

    return stats

def main():
    """Main function to run the Vimeo video management script."""
    print("--- Starting Vimeo Automation Script ---")

    if not all([VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET]):
        print("ERROR: Vimeo credentials are not fully configured.")
        return

    client = get_vimeo_client(VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET)
    user_response = client.get('/me')
    if user_response.status_code != 200:
        print(f"ERROR: Failed to connect to Vimeo API. Status: {user_response.status_code}")
        return
    print(f"Successfully connected to Vimeo as: {user_response.json().get('name')}")

    # --- GET MP Token and fetch Events once ---
    mp_token = get_mp_token()
    events_cache = get_mp_events_in_range(mp_token, LOOKBACK_HOURS)

    videos_to_check = get_recent_videos(client, LOOKBACK_HOURS)
    scanned_count, processed_count, updated_count, moved_count = len(videos_to_check), 0, 0, 0

    if not videos_to_check:
        print("No new videos found to process.")
    else:
        for video in videos_to_check:
            print("\n" + "-"*20)
            print(f"Checking video: {video['name']} ({video['uri']})")

            if not video.get('is_playable'):
                print("  - Skipping: Video is not playable.")
                continue
            
            parent_folder = video.get('parent_folder')
            if parent_folder:
                parent_folder_id = parent_folder['uri'].split('/')[-1]
                if parent_folder_id in EXCLUDED_FOLDER_IDS:
                    print(f"  - Skipping: Video is in an excluded folder ('{parent_folder.get('name')}').")
                    continue
                if parent_folder_iod in DESTINATION_FOLDERS.values():
                    print(f"  - Skipping: Video is already in a destination folder ('{Parent_folder.get('name')}').")
                    continue
                print(f"  - Skipping: Video is not in the Team Library root (it's in '{parent_folder.get('name')}').")
                continue

            print("  - Video is valid for processing.")
            processed_count += 1
            stats = process_video(client, video, events_cache)
            if stats['title_updated']: updated_count += 1
            if stats['moved']: moved_count += 1

    # --- Print Final Summary --- 
    print("\n" + "="*30 + "\n--- Processing Summary ---\n" +
          f"Videos Scanned: {scanned_count}\n" +
          f"Videos Processed: {processed_count}\n" +
          f"Titles Updated: {updated_count}\n" +
          f"Videos Moved: {moved_count}\n" + "="*30)
    print("\n--- Script Finished ---")

if __name__ == '__main__':
    main()