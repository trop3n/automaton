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
    "The Root Class": '10606776',
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
    """
    Determines the correct title and category, then renames and moves the video if necessary.
    Returns a dictionary with the results of the operations.
    """
    stats = {'title_updated': False, 'moved': False}
    current_title = video_data.get('name', '')
    
    # --- 1. Determine Correct Date and Title ---
    # Use modified_time for a more accurate date of the event
    upload_time_utc = datetime.fromisoformat(video_data['modified_time'].replace('Z', '+00:00'))
    local_tz = pytz.timezone(TIMEZONE)
    upload_time_local = upload_time_utc.astimezone(local_tz)
    correct_date_str = upload_time_local.strftime('%Y-%m-%d')
    
    # Strip any old date from the title to get the base for keyword matching
    original_title_for_categorization = re.sub(r'^\d{4}-\d{2}-\d{2} - ', '', current_title)
    video_title_lower = original_title_for_categorization.lower()
    
    category_folder_name = None
    final_title_suffix = None

    # --- 2. Categorization Logic ---
    day_of_week = upload_time_local.weekday() # Monday is 0, Sunday is 6
    hour = upload_time_local.hour
        
    # Check for Worship Service first to apply specific time-based naming
    if 'worship' in video_title_lower or 'contemporary' in video_title_lower or 'traditional' in video_title_lower:
        category_folder_name = "Worship Services"
        service_type = "Contemporary" if 'contemporary' in video_title_lower else "Traditional"

        # Saturday Service
        if day_of_week == 5 and 17 <= hour < 20: # Expanded 5pm-8pm window for 5:30 service
            final_title_suffix = "Worship Service - Traditional 5:30 PM"
        # Sunday Services
        elif day_of_week == 6:
            # --- REVISED LOGIC: Use a simple cutoff time for Sunday services ---
            # This is more robust against video processing delays.
            if hour < 11:
                final_title_suffix = f"Worship Service - {service_type} 9:30 AM"
            else: # Assumes anything processed at or after 11am is the 11am service
                final_title_suffix = f"Worship Service - {service_type} 11:00 AM"
        # Weekday "Worship" title -> likely a Memorial/Wedding
        else:
            print("  - 'Worship' title found on a weekday. Overriding to 'Weddings and Memorials'.")
            category_folder_name = "Weddings and Memorials"
            final_title_suffix = "Memorial or Wedding Service"
    
    # UPDATED RULE for The Root Class (SUNDAY ONLY)
    elif 'capture - piro hall' in video_title_lower and day_of_week == 6:
        category_folder_name = "The Root Class"
        final_title_suffix = "0930 - The Root Class"
    
    # Check other categories if not a worship service
    elif 'memorial' in video_title_lower or 'wedding' in video_title_lower:
        category_folder_name = "Weddings and Memorials"
        final_title_suffix = "Memorial or Wedding Service"
    elif "scott" in video_title_lower or "class" in video_title_lower:
        category_folder_name = "Scott's Classes"
        final_title_suffix = original_title_for_categorization # Use the original title for classes

    # --- 3. Rename and Move ---
    if category_folder_name and final_title_suffix:
        new_title = f"{correct_date_str} - {final_title_suffix}"

        # Rename if the current title is not exactly correct
        if current_title != new_title:
            print(f"  - Updating title to: '{new_title}'")
            try:
                client.patch(video_data['uri'], data={'name': new_title})
                print("    - Successfully updated title.")
                stats['title_updated'] = True
            except Exception as e:
                print(f"    - An error occurred while updating title: {e}")
                return stats
        else:
            print("  - Skipping rename: Title is already correct.")

        # Move to the correct folder
        folder_id = DESTINATION_FOLDERS.get(category_folder_name)
        if folder_id:
            print(f"  - Moving to folder for '{category_folder_name}' (ID: {folder_id}).")
            try:
                user_response = client.get('/me')
                user_uri = user_response.json()['uri']
                project_uri = f"{user_uri}/projects/{folder_id}"
                video_uri_id = video_data['uri'].split('/')[-1]
                
                move_response = client.put(f"{project_uri}/videos/{video_uri_id}")
                if move_response.status_code == 204:
                    print("    - Successfully moved video.")
                    stats['moved'] = True
                else:
                    print(f"    - Error moving video: {move_response.status_code} - {move_response.text}")
            except Exception as e:
                print(f"    - An error occurred while moving video: {e}")
    else:
        print("  - No categorization rule matched. Video will not be moved.")
    
    return stats


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

    videos_to_check = get_recent_videos(client, LOOKBACK_HOURS)

    # --- Initialize Counters ---
    scanned_count = len(videos_to_check)
    processed_count = 0
    updated_count = 0
    moved_count = 0

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
                if parent_folder_id in DESTINATION_FOLDERS.values():
                     print(f"  - Skipping: Video is already in a destination folder ('{parent_folder.get('name')}').")
                else:
                    print(f"  - Skipping: Video is not in the Team Library root (it's in '{parent_folder.get('name')}').")
                continue
            
            # If the video passes all checks, process it.
            print("  - Video is valid for processing.")
            processed_count += 1
            stats = process_video(client, video)
            if stats['title_updated']:
                updated_count += 1
            if stats['moved']:
                moved_count += 1

    # --- Print Final Summary ---
    print("\n" + "="*30)
    print("--- Processing Summary ---")
    print(f"Videos Scanned: {scanned_count}")
    print(f"Videos Processed: {processed_count}")
    print(f"Titles Updated: {updated_count}")
    print(f"Videos Moved: {moved_count}")
    print("="*30)
    print("\n--- Script Finished ---")

if __name__ == "__main__":
    main()