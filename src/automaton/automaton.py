import os
import re
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient

# Load environment variables from a .env file
load_dotenv()

# --- Configuration ---
# Reads all necessary credentials from your .env file.
VIMEO_ACCESS_TOKEN = os.environ.get('VIMEO_ACCESS_TOKEN')
VIMEO_CLIENT_ID = os.environ.get('VIMEO_CLIENT_ID')
VIMEO_CLIENT_SECRET = os.environ.get('VIMEO_CLIENT_SECRET')

# Timezone for upload date calculations (e.g., 'America/Chicago' for CDT)
TIMEZONE = 'America/Chicago'

# Time window to check for recent videos (in hours)
LOOKBACK_HOURS = 48

def get_vimeo_client(token, key, secret):
    """Initializes and returns the Vimeo client using token, key, and secret."""
    client = VimeoClient(token=token, key=key, secret=secret)
    return client

def get_recent_videos_from_root(client, lookback_hours):
    """Fetches recent, playable videos and filters for those in the root of the Team Library."""
    print(f"Fetching videos modified in the last {lookback_hours} hours to find those in the Team Library root...")
    
    # Calculate the start time for the lookback window
    now_utc = datetime.now(pytz.utc)
    start_time_utc = now_utc - timedelta(hours=lookback_hours)
    
    root_videos = []
    
    try:
        # --- KEY CHANGE: Sort by modified_time to find recently finished archives ---
        response = client.get('/me/videos', params={
            'per_page': 100,
            'sort': 'modified_time',
            'direction': 'desc',
            'fields': 'uri,name,created_time,modified_time,parent_folder,is_playable'
        })
        response.raise_for_status()
        
        all_recent_videos = response.json().get('data', [])
        
        for video in all_recent_videos:
            # --- KEY CHANGE: Filter by modified_time instead of created_time ---
            modified_time_str = video.get('modified_time')
            if not modified_time_str:
                continue

            modified_time_utc = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
            if modified_time_utc >= start_time_utc:
                # A video is in the root if its parent_folder is null
                if video.get('parent_folder') is None:
                    root_videos.append(video)
            else:
                # Since the list is sorted by modified_time, we can stop once we're outside the window.
                break
                        
    except Exception as e:
        print(f"An error occurred while fetching videos: {e}")

    print(f"Found {len(root_videos)} recently modified videos in the Team Library root.")
    return root_videos

def prepend_date_to_title(client, video_data):
    """Prepends the upload date to the video title."""
    current_title = video_data.get('name', '')
    
    # The date in the title should be the CREATION date of the event, not the modification date.
    upload_time_utc = datetime.fromisoformat(video_data['created_time'].replace('Z', '+00:00'))
    local_tz = pytz.timezone(TIMEZONE)
    upload_time_local = upload_time_utc.astimezone(local_tz)
    date_str = upload_time_local.strftime('%Y-%m-%d')
    
    new_title = f"{date_str} - {current_title}"
    
    print(f"  - Updating title to: '{new_title}'")
    
    try:
        # Make the API call to update the video's name
        client.patch(video_data['uri'], data={'name': new_title})
        print("  - Successfully updated title.")
    except Exception as e:
        print(f"  - An error occurred while updating the title: {e}")

def main():
    """Main function to run the Vimeo video management script."""
    print("--- Starting Vimeo Date Prepend Script ---")
    
    if not all([VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET]):
        print("ERROR: Vimeo credentials are not fully configured.")
        print("Please ensure VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, and VIMEO_CLIENT_SECRET are in your .env file.")
        return

    client = get_vimeo_client(VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET)
    
    user_response = client.get('/me')
    if user_response.status_code != 200:
        print(f"Failed to connect to Vimeo API. Status: {user_response.status_code}, Response: {user_response.json()}")
        return
    print(f"Successfully connected to Vimeo as: {user_response.json().get('name')}")

    videos_to_process = get_recent_videos_from_root(client, LOOKBACK_HOURS)

    if not videos_to_process:
        print("No videos found to process.")
    else:
        for video in videos_to_process:
            print("\n" + "-"*20)
            print(f"Processing video: {video['name']} ({video['uri']})")

            # --- FINALIZED CHECK ---
            # Rule 1: Only process playable videos. This filters out "phantom" live objects.
            if not video.get('is_playable'):
                print("  - Skipping: Video is not playable (likely a phantom live event object).")
                continue

            # Rule 2: If the title already starts with a date, skip this video.
            current_title = video.get('name', '')
            date_pattern = r'^\d{4}-\d{2}-\d{2} - '
            if re.match(date_pattern, current_title):
                print(f"  - Skipping: Title already has a date prepended.")
                continue
            
            # If the video is playable and not already renamed, prepend the date.
            prepend_date_to_title(client, video)

    print("\n--- Script Finished ---")

if __name__ == "__main__":
    main()