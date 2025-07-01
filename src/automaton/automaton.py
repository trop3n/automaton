import os
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
from vimeo import VimeoClient

# Load environment variables from a .env file
load_dotenv()

# --- Configuration ---
# Reads all necessary credentials from your .env file, similar to your previous script.
VIMEO_ACCESS_TOKEN = os.environ.get('VIMEO_ACCESS_TOKEN')
VIMEO_CLIENT_ID = os.environ.get('VIMEO_CLIENT_ID')
VIMEO_CLIENT_SECRET = os.environ.get('VIMEO_CLIENT_SECRET')


# Timezone for upload date calculations (e.g., 'America/Chicago' for CDT)
TIMEZONE = 'America/Chicago'

# Time window to check for recent videos (in hours)
LOOKBACK_HOURS = 60

# List of folder IDs to exclude from processing
EXCLUDED_FOLDER_IDS = [
    '11103430', '182762', '8219992', '2255002', '6002849', '4159725',
    '1666435', '4222038', '16030496', '6066027', '418919', '4888153',
    '25429201', '6001619', '19254707', '2855777', '1779818'
]

# --- Folder IDs for Categorization ---
FOLDER_IDS = {
    "Worship Services": '15749517',
    "Weddings and Memorials": '2478125',
    "Scott's Classes": '15680946',
    "Something Else Class": '15680946' # Both class types go to the "Scott's Classes" folder
}

def get_vimeo_client(token, key, secret):
    """Initializes and returns the Vimeo client using token, key, and secret."""
    client = VimeoClient(token=token, key=key, secret=secret)
    return client

def get_recent_videos(client, lookback_hours):
    """Fetches videos from the authenticated user's account uploaded within the lookback window."""
    print("Fetching recent videos...")
    try:
        # Calculate the start time for the lookback window
        now = datetime.now(pytz.utc)
        start_time = now - timedelta(hours=lookback_hours)
        
        # Get the authenticated user's information to access their videos
        user_response = client.get('/me')
        if user_response.status_code != 200:
            print(f"Error fetching user data: {user_response.json()}")
            return []
        user_uri = user_response.json()['uri']

        # Fetch videos, filtering by upload date
        videos_uri = f"{user_uri}/videos"
        response = client.get(videos_uri, params={'fields': 'uri,name,created_time,parent_folder', 'sort': 'date', 'direction': 'desc', 'per_page': 100})

        if response.status_code == 200:
            all_videos = response.json().get('data', [])
            
            # Filter videos based on the lookback window
            recent_videos = []
            for video in all_videos:
                created_time_utc = datetime.fromisoformat(video['created_time'].replace('Z', '+00:00'))
                if created_time_utc >= start_time:
                    recent_videos.append(video)
            
            print(f"Found {len(recent_videos)} videos in the last {lookback_hours} hours.")
            return recent_videos
        else:
            print(f"Error fetching videos: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        print(f"An error occurred while fetching videos: {e}")
        return []

def categorize_and_move_video(client, video_data):
    """Categorizes a video based on its upload time and moves it to the appropriate folder."""
    try:
        # Get the upload time and convert to the specified timezone
        upload_time_utc = datetime.fromisoformat(video_data['created_time'].replace('Z', '+00:00'))
        local_tz = pytz.timezone(TIMEZONE)
        upload_time_local = upload_time_utc.astimezone(local_tz)

        day_of_week = upload_time_local.weekday()  # Monday is 0, Sunday is 6
        hour = upload_time_local.hour
        
        category = None
        folder_id = None
        new_title_category = None


        # --- Categorization Logic ---
        # Something Else Class: Sunday, 12:00 PM - 2:00 PM (14:00)
        if day_of_week == 6 and 12 <= hour < 14:
            category = "Something Else Class"
            folder_id = FOLDER_IDS.get(category)
            new_title_category = "Something Else Class"
        # Worship Service: Saturday 6:30 PM (18:30) - 8:00 PM (20:00) OR Sunday 10:00 AM - 2:00 PM (14:00)
        elif (day_of_week == 5 and (hour == 18 and upload_time_local.minute >= 30 or hour == 19)) or \
             (day_of_week == 6 and 10 <= hour < 14):
            category = "Worship Services"
            folder_id = FOLDER_IDS.get(category)
            new_title_category = "Worship Service"
        # Scott's Tuesday Class: Tuesday, 2:00 PM (14:00) - 4:00 PM (16:00)
        elif day_of_week == 1 and 14 <= hour < 16:
            category = "Scott's Classes"
            folder_id = FOLDER_IDS.get(category)
            new_title_category = "Scott's Tuesday Class"
        # Weddings/Memorials: Monday-Saturday, 8:00 AM - 6:00 PM (18:00)
        elif 0 <= day_of_week <= 5 and 8 <= hour < 18:
            category = "Weddings and Memorials"
            folder_id = FOLDER_IDS.get(category)
            new_title_category = "Memorial or Wedding"
        else:
            print(f"Video '{video_data['name']}' could not be categorized based on upload time.")
            return

        print(f"Video '{video_data['name']}' categorized as '{category}'. Moving to folder for '{category}'.")

        # --- Update Video Title ---
        upload_date_str = upload_time_local.strftime('%Y-%m-%d')
        new_title = f"{upload_date_str} - {new_title_category}"

        # Check if the title already has the date prepended
        if not video_data['name'].startswith(upload_date_str):
            print(f"Updating title to: '{new_title}'")
            client.patch(video_data['uri'], data={'name': new_title})
        else:
            print("Title already contains the upload date. Skipping title update.")


        # --- Move Video to Folder ---
        if folder_id:
            # Get user URI to construct the project URI
            user_response = client.get('/me')
            if user_response.status_code != 200:
                print(f"Error fetching user data for moving video: {user_response.json()}")
                return
            user_uri = user_response.json()['uri']
            
            project_uri = f"{user_uri}/projects/{folder_id}"
            video_uri_id = video_data['uri'].split('/')[-1]
            
            move_response = client.put(f"{project_uri}/videos/{video_uri_id}")
            if move_response.status_code == 204:
                print(f"Successfully moved video to folder ID {folder_id}.")
            else:
                print(f"Error moving video: {move_response.status_code} - {move_response.text}")
        else:
            print(f"No folder ID configured for category: '{category}'.")

    except Exception as e:
        print(f"An error occurred during categorization or moving of video '{video_data.get('name', 'N/A')}': {e}")


def main():
    """Main function to run the Vimeo video management script."""
    print("--- Starting Vimeo Video Management Script ---")
    
    # Check that all necessary credentials are provided
    if not all([VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET]):
        print("ERROR: Vimeo credentials are not fully configured.")
        print("Please ensure VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, and VIMEO_CLIENT_SECRET are in your .env file.")
        return

    client = get_vimeo_client(VIMEO_ACCESS_TOKEN, VIMEO_CLIENT_ID, VIMEO_CLIENT_SECRET)
    
    # Verify connection by fetching user info
    user_response = client.get('/me')
    if user_response.status_code != 200:
        print(f"Failed to connect to Vimeo API. Status: {user_response.status_code}, Response: {user_response.json()}")
        return
    print(f"Successfully connected to Vimeo as: {user_response.json().get('name')}")

    videos = get_recent_videos(client, LOOKBACK_HOURS)

    if not videos:
        print("No recent videos found to process.")
        return

    for video in videos:
        print("\n" + "-"*20)
        print(f"Processing video: {video['name']} ({video['uri']})")
        
        # Check if the video is in an excluded folder
        parent_folder = video.get('parent_folder')
        if parent_folder and parent_folder['uri'].split('/')[-1] in EXCLUDED_FOLDER_IDS:
            print(f"Skipping video because it is in an excluded folder: {parent_folder.get('name')}")
            continue

        # Check if the video is at the root (no parent folder)
        if parent_folder is None:
             categorize_and_move_video(client, video)
        else:
            print("Skipping video as it is already in a folder.")


    print("\n--- Script Finished ---")

if __name__ == "__main__":
    main()