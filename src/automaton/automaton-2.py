import os
import requests
import re
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

try:
    from vimeo import VimeoClient
except ImportError:
    print("The 'vimeo' Python library is not installed")
    print("Please install it using: pip install vimeo")
    print("Exiting script.")
    exit()

# --- Configuration ---
VIMEO_ACCESS_TOKEN = os.getenv('VIMEO_ACCESS_TOKEN')
VIMEO_CLIENT_SECRET = os.getenv('VIMEO_CLIENT_SECRET')
VIMEO_CLIENT_ID = os.getenv('VIMEO_CLIENT_ID')
DATE_FORMAT = "%Y-%m-%d"
LOOKBACK_HOURS = 48

# List of folder IDs to EXCLUDE from the scan.
# Videos within these folders will NOT be checked or updated.
EXCLUDED_FOLDER_IDS = ['11103430', '182762', '8219992']

# --- Destination Folder Mapping ---
# Maps the folder names used in SERVICE_TYPE_RULES to their actual Vimeo folder IDs.
DESTINATION_FOLDERS = {
    "Worship Services": '15749517',
    "Weddings and Memorials": '2478125',
    "Scott's Classes": '15680946',
}

# --- Service Type Determination Logic ---
# This dictionary defines how to classify a video based on its live event ID
# and the time range it was created.
# FIX: Time ranges must be a tuple of two strings: ("HH:MM", "HH:MM")
SERVICE_TYPE_RULES = {
    "3261302": {
        "name": "Worship Service - Traditional",
        "folder": "Worship Services",
        "time_ranges": [
            ("09:20", "12:00"),
        ]
    },
    "4590739": {
        "name": "Worship Service - Contemporary",
        "folder": "Worship Services",
        "time_ranges": [
            ("09:20", "12:00"),
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

# Initialize the Vimeo client
client = VimeoClient(
    token=VIMEO_ACCESS_TOKEN,
    key=VIMEO_CLIENT_ID,
    secret=VIMEO_CLIENT_SECRET
)

def get_authenticated_user_id() -> str | None:
    """Fetches the authenticated user's ID from the Vimeo API."""
    try:
        response = client.get('/me')
        response.raise_for_status()
        user_data = response.json()
        user_uri = user_data.get('uri')
        if user_uri:
            return user_uri.split('/')[-1]
        print("Could not find 'uri' in '/me' response.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching authenticated user ID: {e}")
        if e.response:
            print(f"Response content: {e.response.text}")
    return None

def get_recent_videos_with_folder_and_live_event_info(since_datetime: datetime) -> list[dict]:
    """
    Fetches videos created after a specific datetime, including folder and live event info.
    """
    all_recent_videos = []
    page = 1
    per_page = 100
    print(f"Fetching all videos uploaded since {since_datetime.isoformat()}.")
    while True:
        try:
            response = client.get(
                '/me/videos',
                params={
                    'page': page,
                    'per_page': per_page,
                    'sort': 'date',
                    'direction': 'desc',
                    'fields': 'name,created_time,uri,parent_folder.uri,live_event.uri'
                }
            )
            response.raise_for_status()
            data = response.json()
            videos_on_page = data.get('data', [])

            if not videos_on_page:
                break

            for video in videos_on_page:
                created_time_str = video.get('created_time')
                if created_time_str:
                    video_created_dt = datetime.fromisoformat(created_time_str.replace('Z', '+00:00'))
                    if video_created_dt > since_datetime:
                        all_recent_videos.append(video)
                    else:
                        print(f"Encountered video older than threshold. Stopping pagination.")
                        return all_recent_videos
                else:
                    print(f"Warning: Video URI {video.get('uri', 'N/A')} is missing 'created_time'.")

            if data.get('paging', {}).get('next') is None:
                break
            page += 1
            print(f"Fetched page {page-1}, total recent videos so far: {len(all_recent_videos)}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching videos: {e}")
            if e.response:
                print(f"Response content: {e.response.text}")
            break
    print(f"Finished fetching. Total recent videos found: {len(all_recent_videos)}")
    return all_recent_videos

def get_id_from_uri(uri: str | None, prefix: str) -> str | None:
    """Generic helper to extract an ID from a Vimeo URI."""
    if not isinstance(uri, str) or not uri:
        return None
    match = re.search(rf'/{prefix}/(\d+)', uri)
    return match.group(1) if match else None

def get_video_id_from_uri(uri: str | None) -> str | None:
    return get_id_from_uri(uri, 'videos')

def get_folder_id_from_uri(uri: str | None) -> str | None:
    return get_id_from_uri(uri, 'folders')

def get_live_event_id_from_uri(uri: str | None) -> str | None:
    return get_id_from_uri(uri, 'live_events')

def parse_time_string(time_str: str) -> datetime.time:
    """Parses an 'HH:MM' string into a datetime.time object."""
    return datetime.strptime(time_str, "%H:%M").time()

def determine_destination_folder_id(video_info: dict) -> str | None:
    """Determines the correct destination folder ID for a video based on SERVICE_TYPE_RULES."""
    live_event_info = video_info.get('live_event')
    created_time_str = video_info.get('created_time')

    if not live_event_info or not isinstance(live_event_info, dict):
        return None

    live_event_id = get_live_event_id_from_uri(live_event_info.get('uri'))
    if not live_event_id:
        return None

    video_created_dt_utc = None
    if created_time_str:
        try:
            video_created_dt_utc = datetime.fromisoformat(created_time_str.replace('Z', '+00:00'))
        except ValueError:
            print(f"Warning: Could not parse created_time '{created_time_str}'")

    for rule_event_id, rule_details in SERVICE_TYPE_RULES.items():
        if rule_event_id == live_event_id:
            folder_name = rule_details["folder"]
            # If no time rules, we have a match
            if "time_ranges" not in rule_details or not video_created_dt_utc:
                return DESTINATION_FOLDERS.get(folder_name)

            # Check time rules
            video_time_utc = video_created_dt_utc.time()
            for start_str, end_str in rule_details["time_ranges"]:
                start_time = parse_time_string(start_str)
                end_time = parse_time_string(end_str)
                if (start_time <= end_time and start_time <= video_time_utc <= end_time) or \
                   (start_time > end_time and (video_time_utc >= start_time or video_time_utc <= end_time)):
                    return DESTINATION_FOLDERS.get(folder_name)
    return None

def add_video_to_folder(video_id: str, destination_folder_id: str, user_id: str) -> bool:
    """Adds a video to a specified Vimeo folder."""
    print(f"Attempting to add video ID {video_id} to folder ID {destination_folder_id}...")
    try:
        response = client.put(f'/users/{user_id}/folders/{destination_folder_id}/videos/{video_id}')
        response.raise_for_status()
        print(f"Successfully added video ID {video_id} to folder ID {destination_folder_id}.")
        return True
    except requests.exceptions.HTTPError as e:
        if e.response and e.response.status_code == 400: # Bad Request
             # This error can mean the video is already in the folder, which is okay.
             print(f"Info: Video ID {video_id} might already be in folder ID {destination_folder_id}.")
             return True
        print(f"HTTP error adding video to folder: {e}")
        if e.response:
            print(f"Response content: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Connection error adding video to folder: {e}")
    return False

def update_video_title(video_id: str, new_title: str) -> bool:
    """Updates the title of a specific video on Vimeo."""
    try:
        response = client.patch(f'/videos/{video_id}', data={'name': new_title})
        response.raise_for_status()
        print(f"Successfully updated video ID {video_id} title to: '{new_title}'")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error updating video title for ID {video_id}: {e}")
        if e.response:
            print(f"Response content: {e.response.text}")
    return False

def main():
    """Main function to orchestrate scanning and processing Vimeo videos."""
    if not VIMEO_ACCESS_TOKEN or not VIMEO_CLIENT_ID or not VIMEO_CLIENT_SECRET:
        print("ERROR: Authentication credentials missing from .env file.")
        return

    authenticated_user_id = get_authenticated_user_id()
    if not authenticated_user_id:
        print("ERROR: Could not authenticate. Please check your access token.")
        return

    print(f"Authenticated User ID: {authenticated_user_id}")
    print(f"Excluding folders: {', '.join(EXCLUDED_FOLDER_IDS)}")

    since_datetime = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    print(f"\nLooking for videos created since {since_datetime.isoformat()}.")

    # FIX: Corrected function name to fetch video details including parent folder.
    recent_videos_from_api = get_recent_videos_with_folder_and_live_event_info(since_datetime)

    if not recent_videos_from_api:
        print(f"No recent videos found in the last {LOOKBACK_HOURS} hours. Exiting.")
        return

    videos_to_process = []
    print(f"\nFiltering {len(recent_videos_from_api)} recent videos...")
    for video_info in recent_videos_from_api:
        parent_folder_uri = video_info.get('parent_folder', {}).get('uri') if video_info.get('parent_folder') else None
        folder_id = get_folder_id_from_uri(parent_folder_uri)

        if folder_id and folder_id in EXCLUDED_FOLDER_IDS:
            video_id = get_video_id_from_uri(video_info.get('uri'))
            print(f"-> Skipping video ID {video_id} (in excluded folder ID {folder_id}).")
            continue
        
        videos_to_process.append(video_info)

    print(f"\nFinished filtering. {len(videos_to_process)} videos selected for processing.")
    if not videos_to_process:
        return

    processed_count = 0
    skipped_count = 0

    print("\n--- Starting Processing ---")
    for i, video_info in enumerate(videos_to_process):
        video_id = get_video_id_from_uri(video_info.get('uri'))
        current_title = video_info.get('name', 'Untitled')
        upload_date_str = video_info.get('created_time')
        
        print(f"\n[{i+1}/{len(videos_to_process)}] Processing Video ID: {video_id}")
        print(f"  Current Title: '{current_title}'")

        if not upload_date_str:
            print("  - Skipping: Could not retrieve upload date.")
            skipped_count += 1
            continue

        try:
            dt_object = datetime.fromisoformat(upload_date_str.replace('Z', '+00:00'))
            formatted_date = dt_object.strftime(DATE_FORMAT)

            # --- Update Title ---
            if formatted_date in current_title:
                print(f"  - Skipping: Title already contains the date '{formatted_date}'.")
            else:
                new_title = f"{current_title} ({formatted_date})"
                print(f"  + Updating title to: '{new_title}'")
                if update_video_title(video_id, new_title):
                    processed_count += 1
                else:
                    skipped_count += 1 # Count as skipped if update failed
            
            # --- (Optional) Move Video to Folder ---
            # This section determines the correct folder and moves the video.
            # It is safe to run, as it won't move videos that don't match a rule.
            destination_folder_id = determine_destination_folder_id(video_info)
            if destination_folder_id:
                print(f"  + Sorting video into folder ID: {destination_folder_id}")
                add_video_to_folder(video_id, destination_folder_id, authenticated_user_id)
            else:
                print(f"  - Info: No sorting rule matched for this video.")

        except Exception as e:
            print(f"  - An unexpected error occurred: {e}. Skipping.")
            skipped_count += 1

    print("\n--- Processing Summary ---")
    print(f"Videos successfully processed: {processed_count}")
    print(f"Videos skipped or failed: {skipped_count}")
    print("--------------------------")

if __name__ == '__main__':
    main()