import os
import requests
import re
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv # import load_dotenv

load_dotenv()

try:
    from vimeo import VimeoClient
except ImportError:
    print("The 'vimeo' Python library is not installed")
    print("Please install it using: pip install vimeo")
    print("Exiting script.")
    exit()

VIMEO_ACCESS_TOKEN = os.getenv('VIMEO_ACCESS_TOKEN')
VIMEO_CLIENT_SECRET = os.getenv('VIMEO_CLIENT_SECRET')
VIMEO_CLIENT_ID = os.getenv('VIMEO_CLIENT_ID')
DATE_FORMAT = "%Y-%m-%d"
LOOKBACK_HOURS = 48

# List of folder IDs to EXCLUDE from the scan.
# Videos within these folders will NOT be checked or updated.
# IMPORTANT: Use string format for IDs as they are often treated as strings by APIs.
EXCLUDED_FOLDER_IDS = ['11103430', '8219992', '6002849']

client = VimeoClient(
    token=VIMEO_ACCESS_TOKEN,
    key=VIMEO_CLIENT_ID,
    secret=VIMEO_CLIENT_SECRET
)

def get_authenticated_user_id() -> str | None:
    """
    Fetches the authenticated user's ID from the Vimeo API.
    This is used to confirm authentication and might be needed for certain API calls.
    """
    try:
        response = client.get('/me')
        response.raise_for_status()
        user_data = response.json()
        # The user URI Looks like /users/12345678
        user_uri = user_data.get('uri')
        if user_uri:
            return user_uri.split('/')[-1]
        print("Could not find 'uri' in '/me' response.")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error fetching authenticated user ID: {e}")
        print(f"Response content: {e.response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error fetching authenticated user ID: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while fetching authenticated user ID: {e}")
    return None

def get_recent_videos_with_folder_info(since_datetime: datetime) -> list[dict]:
    """
    Fetches videos from the authenticated user's entire library that were
    uploaded after the specified datetime. It optimizes by stopping pagination
    once videos older than 'since_datetime' are encountered.
    Includes parent_folder.uri to enable folder exclusion later.

    Args:
        since_datetime (datetime): Only videos uploaded after this time will be considered.

    Returns:
        list[dict]: A list of dictionaries, each containing recent video information
                    including parent_folder.uri. Returns an empty list if an error occurs or no videos are found.
    """
    all_recent_videos = []
    page = 1
    per_page = 100 # Maximum allowed by Vimeo

    print(f"Fetching all videos from your Team Library uploaded since {since_datetime.isoformat()} (optimizing by date).")
    while True:
        try:
            # using /me/videos to get all videos for the authenticated user
            # Sorting by 'date' in 'desc' order to enable early exit
            # Requesting 'name', 'created_time', 'uri', and 'parent_folder.uri' fields
            response = client.get(
                '/me/videos',
                params={
                    'page': page,
                    'per_page': per_page,
                    'sort': 'date', # sort by creation date
                    'direction': 'desc', # Newest first
                    'fields': 'name,created_time,uri,parent_folder.uri' # added parent_folder.uri
                }
            )
            response.raise_for_status()

            data = None
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError as json_e:
                print(f" Error decoding JSON response for videos: {json_e}")
                if response is not None:
                    print(f" Raw response content (first 500 chars): {response.text[:500]}...")
                break # Exit loop if JSON is malformed

            if not data: # Check if data is None or empty dict/list after parsing
                print(" Warning: No data or invalid data received in JSON response for videos.")
                break

            videos_on_page = data.get('data', [])
            if not isinstance(videos_on_page, list):
                print(f" Warning: Expected 'data field to be a list, but got {type(videos_on_page)}. Skipping.")
                break # Break if 'data' field isn't a list of videos

            if not videos_on_page:
                break # No more videos

            found_recent_on_page = False
            for video in videos_on_page:
                created_time_str = video.get('created_time')
                if created_time_str:
                    try:
                        video_created_dt = datetime.fromisoformat(created_time_str.replace('Z', '+00:00'))
                        if video_created_dt.tzinfo is None:
                            video_created_dt = video_created_dt.replace(tzinfo=timezone.utc) # Make it timezone-aware

                        if video_created_dt > since_datetime:
                            all_recent_videos.append(video)
                            found_recent_on_page = True
                        else:
                            # If we encounter a video older than our threshold (since Vimeo returns newest first),
                            # we can stop fetching more pages.
                            print(f" Encountered Video ID {get_video_id_from_uri(video.get('uri'))} older than {since_datetime.isoformat()}. Stopping pagination.")
                            return all_recent_videos # Early exit!
                    except ValueError as e:
                        print(f" Warning: Could not parse created_time '{created_time_str}' for video {video.get('uri', 'N/A')}. Error: {e}")
                else:
                    print(f" Warning: Video URI {video.get('uri', 'N/A')} is missing 'created_time'.")

            # Check if there are more pages based on Vimeo's 'next' link
            # If we didn't find any recent videos on this page, and it's not the first page,
            # it implies we've already processed all recent ones or there are none left.
            if data.get('paging', {}).get('next') is None or not found_recent_on_page:
                break 

            page += 1
            print(f" Fetched page {page-1}, total videos so far: {len(all_recent_videos)}")

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error fetching videos: {e}")
            if e.response is not None:
                print(f"Response content: {e.response.text}")
            break
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error fetching videos: {e}")
            break
        except Exception as e:
            print(f"An unexpected error occured while fetching videos: {e}")
            break
    print(f"Finished fetching videos. Total videos found in Team Library: {len(all_recent_videos)}")
    return all_recent_videos

def get_video_id_from_uri(uri: str) -> str | None:
    """
    Extracts the Vimeo video ID from its URI (e.g., "/videos/123456789").
    """
    if not isinstance(uri, str) or not uri: # Explicit check if it's a non-empty string
        return None
    match = re.search(r'/videos/(\d+)', uri)
    if match:
        return match.group(1)
    return None

def get_folder_id_from_uri(folder_uri: str | None) -> str | None:
    """
    Extracts the Vimeo folder ID from its URI (e.g. "/folders/123456789")
    """
    if not isinstance(folder_uri, str) or not folder_uri:
        return None
    match = re.search(r'/folders/(\d+)', folder_uri)
    if match:
        return match.group(1)
    return None
    
def update_video_title(video_id: str, new_title: str) -> bool:
    """
    Updates the title of a specific video on Vimeo.
    """
    response = None
    payload = {
        'name': new_title # the 'name' field is used for the video title
    }
    try:
        # The PATCH method is used to update specific fields of a resource
        response = client.patch(f'/videos/{video_id}', data=payload)
        response.raise_for_status() # raise an HTTP error for bad response
        print(f" Successfully updated video ID {video_id} title to: '{new_title}'")
        return True
    except requests.exceptions.HTTPError as e:
        print(f" HTTP error updating video title for ID {video_id}: {e}")
        if e.response is not None:
            print(f" Response content: {e.response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f" Connection error updating video title for ID {video_id}: {e}")
    except Exception as e:
        print(f" An unexpected error occurred while updating video title for {video_id}: {e}")
    return False

def main():
    """
    Main function to orchestrate scanning Vimeo Team Library (excluding specified folders)
    for recent videos, and updating their titles.
    """
    if not VIMEO_ACCESS_TOKEN and not (VIMEO_CLIENT_ID and VIMEO_CLIENT_SECRET):
        print("ERROR: Authentication credentials not found.")
        print("Please ensure either VIMEO_ACCESS_TOKEN or both VIMEO_CLIENT_ID and VIMEO_CLIENT_SECRET are set in your .env file.")
        return

    authenticated_user_id = get_authenticated_user_id()
    if not authenticated_user_id:
        print("ERROR: Could not retrieve authenticated user ID. Please check your access token and its scopes.")
        return

    print(f"Authenticated User ID: {authenticated_user_id}")
    print(f"Starting Vimeo Team Library processing (excluding folders: {', '.join(EXCLUDED_FOLDER_IDS)}.)")

    # Calculate the datetime for lookback period
    forty_eight_hours_ago = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    print(f"\nLooking for videos uploaded in the last {LOOKBACK_HOURS} hours (since {forty_eight_hours_ago.isoformat()}).")

    # fetch all videos from the user's library with parent folder info
    recent_videos_from_api = get_recent_videos_with_folder_info(forty_eight_hours_ago) or []

    if not recent_videos_from_api:
        print("No recent videos found in your Team Library within the last {LOOKBACK_HOURS} hours, or an error occurred. Exiting.")
        return

    videos_to_process = []
    skipped_by_folder_count = 0

    print(f"\nFiltering videos based on upload time and excluded folders:")
    for i, video_info in enumerate(recent_videos_from_api):
        video_uri = video_info.get('uri')
        parent_folder_info = video_info.get('parent_folder') # This is a dict if video is in a folder

        video_id = get_video_id_from_uri(video_uri)
        if not video_id:
            print(f" Warning: Video {i+1} missing URI or could not extract ID. Skipping.")
            continue # Skip if video ID cannot be determined

        # 1. Check if video is in an excluded folder
        is_excluded_folder = False
        if parent_folder_info and isinstance(parent_folder_info, dict):
            parent_folder_uri = parent_folder_info.get('uri')
            folder_id = get_folder_id_from_uri(parent_folder_uri)
            if folder_id and folder_id in EXCLUDED_FOLDER_IDS:
                print(f" Video ID {video_id} is in excluded folder ID {folder_id}. Skipping.")
                is_excluded_folder = True
                skipped_by_folder_count += 1
        # Handle videos not in any folder (parent_folder might be None)
        elif parent_folder_info is None:
            # If a video is not in any folder, it effectively passes the folder exclusion check:
            pass
        else:
            print(f" Warning: Video ID {video_id} has unexpected parent_folder type: {type(parent_folder_info)}. Not excluding folder based on this.")
        
        if is_excluded_folder:
            continue # Skip this video if its folder is excluded

        videos_to_process.append(video_info) # Add to list if not excluded by folder

    print(f"\nFinished filtering by folder. Total videos selected for processing: {len(videos_to_process)}")
    print(f" Skipped due to being in an excluded folder: {skipped_by_folder_count} videos")

    if not videos_to_process:
        print(f"\nNo recent videos found matching criteria after folder exclusion. Exiting.")
        return

    processed_count = 0
    skipped_count_during_update = 0

    print("\nProcessing filtered videos for title updates:")
    for i, video_info in enumerate(videos_to_process):
        video_uri = video_info.get('uri')
        current_title = video_info.get('name')
        upload_date_str = video_info.get('created_time')

        # Redundant checks as videos_to_process should be clean, good for safety
        if not video_uri:
            print(f"\nVideo {i+1}: Missing URI in video info. Skipping.")
            skipped_count_during_update += 1
            continue

        video_id = get_video_id_from_uri(video_uri)
        if not video_id:
            print(f"\nVideo {i+1} (URI: {video_uri}): Could not extract video ID from URI. Skipping.")
            skipped_count_during_update += 1
            continue

        print(f"\nProcessing Video {i+1} (ID: {video_id}):")
        if not current_title:
            print(f" Could not retrieve current title for video ID {video_id}. Skipping.")
            skipped_count_during_update += 1
            continue
        if not upload_date_str:
            print(f" Could not retrieve upload date for video ID {video_id}. Skipping.")
            continue

        print(f" Current Title: '{current_title}'")
        print(f" Upload Date (raw): '{upload_date_str}'")

        try:
            dt_object = datetime.fromisoformat(upload_date_str.replace('Z', '+00:00')) # Handle 'Z' for UTC
            formatted_date = dt_object.strftime(DATE_FORMAT)
            print(f" Formatted date: {formatted_date}")

            # check if the title already contains the date to avoid duplicates
            if formatted_date in current_title:
                print(f" Video title already contains the date '{formatted_date}'. No update needed.")
                skipped_count_during_update += 1
                continue

            new_title = f"{current_title} ({formatted_date})"
            print(f" New Title will be: '{new_title}'")

            if update_video_title(video_id, new_title):
                processed_count += 1
            else:
                skipped_count_during_update += 1 # count as skipped if update failed

        except ValueError as e:
            print(f" Error parsing date '{upload_date_str}': {e}. Please check DATE_FORMAT. Skipping.")
            skipped_count_during_update += 1
        except Exception as e:
            print(f" An unexpected error occurred during date processing or title update for video ID {video_id}: {e}. Skipping.")
            skipped_count_during_update += 1

    print(f"\n--- Processing Summary ---")
    print(f"Videos processed and updated: {processed_count}")
    print(f"Videos Skipped (during update): {skipped_count_during_update}")
    print("---------------------------")

if __name__ == '__main__':
    main()