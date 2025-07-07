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

# --- Folder Configuration ---
# List of folder IDs to EXCLUDE from processing. This rule is absolute.
EXCLUDED_FOLDER_IDS = ['11103430', '182762', '8219992']

# Destination folders for categorization
DESTINATION_FOLDERS = {
    "Worship Services": '15749517',
    "Weddings and Memorials": '2478125',
    "Scott's Classes": '15680946',
}

def get_vimeo_client(token, key, secret):
    """Initializes and returns the Vimeo client using token, key, and secret."""
    client = VimeoClient(token=token, key=key, secret=secret)
    return client

def get_recent_videos(client, lookback_hours):
    """Fetches all videos recently modified to find candidates for processing."""
    print(f"Fetching all videos modified in the last {lookback_hours} hours...")
    
    # Calculate the start time for the lookback window
    now_utc = datetime.now(pytz.utc)
    start_time_utc = now_utc - timedelta(hours=lookback_hours)
    
    all_recent_videos = []
    
    try:
        # Sort by modified_time to find recently finished archives
        response = client.get('/me/videos', params={
            'per_page': 100,
            'sort': 'modified_time',
            'direction': 'desc',
            'fields': 'uri,name,created_time,modified_time,parent_folder,is_playable'
        })
        response.raise_for_status()
        
        videos = response.json().get('data', [])
        
        for video in videos:
            modified_time_str = video.get('modified_time')
            if not modified_time_str:
                continue

            modified_time_utc = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
            if modified_time_utc >= start_time_utc:
                all_recent_videos.append(video)
            else:
                # Since the list is sorted, we can stop once we're outside the window.
                break
                        
    except Exception as e:
        print(f"An error occurred while fetching videos: {e}")

    print(f"Found {len(all_recent_videos)} recently modified videos to check.")
    return all_recent_videos

def process_video(client, video_data):
    """Prepends date if needed, then categorizes and moves the video."""
    current_title = video_data.get('name', '')
    original_title = current_title # Keep the original title for keyword matching

    # --- 1. Prepend Date (if necessary) ---
    date_pattern = r'^\d{4}-\d{2}-\d{2} - '
    if not re.match(date_pattern, current_title):
        print("  - Prepending date to title...")
        upload_time_utc = datetime.fromisoformat(video_data['created_time'].replace('Z', '+00:00'))
        local_tz = pytz.timezone(TIMEZONE)
        upload_time_local = upload_time_utc.astimezone(local_tz)
        date_str = upload_time_local.strftime('%Y-%m-%d')
        
        new_title = f"{date_str} - {current_title}"
        
        print(f"    - Updating title to: '{new_title}'")
        
        try:
            client.patch(video_data['uri'], data={'name': new_title})
            print("    - Successfully updated title.")
        except Exception as e:
            print(f"    - An error occurred while updating the title: {e}")
            return # Stop processing this video if renaming fails
    else:
        print("  - Skipping rename: Title already has a date prepended.")


    # --- 2. Categorize and Move ---
    print("  - Checking categorization for moving...")
    video_title_lower = original_title.lower()
    category_folder_name = None

    # First, do a tentative categorization based on title keywords.
    if 'worship' in video_title_lower or 'contemporary' in video_title_lower or 'traditional' in video_title_lower:
        category_folder_name = "Worship Services"
    elif 'memorial' in video_title_lower or 'wedding' in video_title_lower:
        category_folder_name = "Weddings and Memorials"
    elif "scott" in video_title_lower or "class" in video_title_lower:
        category_folder_name = "Scott's Classes"

    # --- NEW: Time-based Veto Logic ---
    # If the title makes us think it's a Worship Service, we double-check the time.
    if category_folder_name == "Worship Services":
        print("    - Tentatively categorized as Worship. Verifying time...")
        upload_time_utc = datetime.fromisoformat(video_data['created_time'].replace('Z', '+00:00'))
        local_tz = pytz.timezone(TIMEZONE)
        upload_time_local = upload_time_utc.astimezone(local_tz)
        
        day_of_week = upload_time_local.weekday()  # Monday is 0, Sunday is 6
        hour = upload_time_local.hour

        # Define the valid time windows for a real Worship Service
        is_saturday_worship = (day_of_week == 5 and (hour == 18 and upload_time_local.minute >= 30 or hour == 19))
        is_sunday_worship = (day_of_week == 6 and 10 <= hour < 14)

        # If the time is OUTSIDE the normal worship hours, override the category.
        if not (is_saturday_worship or is_sunday_worship):
            print("      - Time is outside normal worship hours. Overriding category to 'Weddings and Memorials'.")
            category_folder_name = "Weddings and Memorials"
        else:
            print("      - Time is within normal worship hours. Category confirmed.")
    
    # --- End of New Logic ---

    if category_folder_name:
        folder_id = DESTINATION_FOLDERS.get(category_folder_name)
        if folder_id:
            print(f"    - Final Category: '{category_folder_name}'. Moving to folder ID {folder_id}.")
            try:
                user_response = client.get('/me')
                user_uri = user_response.json()['uri']
                project_uri = f"{user_uri}/projects/{folder_id}"
                video_uri_id = video_data['uri'].split('/')[-1]
                
                move_response = client.put(f"{project_uri}/videos/{video_uri_id}")
                if move_response.status_code == 204:
                    print(f"    - Successfully moved video.")
                else:
                    print(f"    - Error moving video: {move_response.status_code} - {move_response.text}")
            except Exception as e:
                print(f"    - An error occurred while moving the video: {e}")
        else:
            print(f"    - Could not find a folder ID for category '{category_folder_name}'.")
    else:
        print("    - No categorization rule matched. Video will not be moved.")


def main():
    """Main function to run the Vimeo video management script."""
    print("--- Starting Vimeo Automation Script ---")
    
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

    # The function now gets all recent videos, not just those in the root.
    videos_to_check = get_recent_videos(client, LOOKBACK_HOURS)

    if not videos_to_check:
        print("No new videos found to process.")
    else:
        for video in videos_to_check:
            print("\n" + "-"*20)
            print(f"Checking video: {video['name']} ({video['uri']})")

            # Rule 1: Only process playable videos.
            if not video.get('is_playable'):
                print("  - Skipping: Video is not playable (likely a phantom live event object).")
                continue

            # Get parent folder info for exclusion checks
            parent_folder = video.get('parent_folder')
            parent_folder_id = None
            if parent_folder:
                parent_folder_id = parent_folder['uri'].split('/')[-1]

            # Rule 2: Skip if the video is in an excluded folder.
            if parent_folder_id and parent_folder_id in EXCLUDED_FOLDER_IDS:
                print(f"  - Skipping: Video is in an excluded folder ('{parent_folder.get('name')}').")
                continue

            # Rule 3: Only process videos in the Team Library (root).
            if parent_folder is not None:
                # Also check if it's already in a destination folder
                if parent_folder_id in DESTINATION_FOLDERS.values():
                     print(f"  - Skipping: Video is already in a destination folder ('{parent_folder.get('name')}').")
                else:
                    print(f"  - Skipping: Video is not in the Team Library root (it's in '{parent_folder.get('name')}').")
                continue
            
            # If the video passes all checks, process it.
            print("  - Video is valid for processing.")
            process_video(client, video)

    print("\n--- Script Finished ---")

if __name__ == "__main__":
    main()