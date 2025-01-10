from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import os
from datetime import datetime
import isodate
import json
from tqdm import tqdm
import time
from youtube_transcript_api import YouTubeTranscriptApi


class YouTubeDataCollector:
    def __init__(self, api_key):
        try:
            print(f"\nInitializing YouTube API...")
            self.youtube = build("youtube", "v3", developerKey=api_key)
            print("API connection successful!")
        except Exception as e:
            print(f"Error initializing YouTube API: {str(e)}")
            raise

    def search_videos_by_genre(self, genre, max_results=500):
        """
        Search for videos of a specific genre and collect their IDs globally,
        sorted by view count (most viewed first)
        """
        video_ids = []
        next_page_token = None

        try:
            print(f"\nSearching for top {max_results} most viewed videos globally")
            print(f"Genre/Topic: {genre}")
            pbar = tqdm(total=max_results, desc="Collecting video IDs")

            while len(video_ids) < max_results:
                try:
                    search_request = self.youtube.search().list(
                        q=genre,
                        part="id",  # Only request the ID part
                        type="video",
                        maxResults=min(50, max_results - len(video_ids)),
                        pageToken=next_page_token,
                        order="viewCount",  # Sort by view count
                        regionCode=None,  # No region restriction
                        relevanceLanguage="",  # No language restriction
                    )

                    search_response = search_request.execute()

                    if "items" in search_response:
                        for item in search_response["items"]:
                            video_ids.append(item["id"]["videoId"])
                            pbar.update(1)

                        if len(video_ids) >= max_results:
                            break

                        next_page_token = search_response.get("nextPageToken")
                        if not next_page_token:
                            print("\nNo more videos available globally")
                            break
                    else:
                        print("\nNo videos found in response")
                        break

                except HttpError as e:
                    print(
                        f"\nYouTube API error during search: {e.resp.status} - {e.content}"
                    )
                    break

            pbar.close()
            print(f"\nTotal videos found: {len(video_ids)}")
            return video_ids

        except Exception as e:
            print(f"Error in search: {str(e)}")
            return []

    def get_video_captions(self, video_id):
        """
        Get captions for a video using YouTubeTranscriptApi
        """
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            # Try to get English transcript first
            try:
                transcript = transcript_list.find_transcript(["en"])
            except:
                # If no English, get the first available transcript
                transcript = transcript_list.find_manually_created_transcript()

            caption_data = transcript.fetch()
            # Combine all caption text
            full_text = " ".join([entry["text"] for entry in caption_data])
            return True, full_text
        except Exception as e:
            return False, ""

    def get_video_details(self, video_ids):
        """
        Collect detailed information for a list of video IDs
        """
        video_data = []

        try:
            print(f"\nGetting details for {len(video_ids)} videos")
            pbar = tqdm(total=len(video_ids), desc="Processing videos")

            # Process in batches of 50 to optimize API calls
            for i in range(0, len(video_ids), 50):
                batch_ids = video_ids[i : i + 50]
                try:
                    request = self.youtube.videos().list(
                        part="snippet,contentDetails,statistics", id=",".join(batch_ids)
                    )

                    response = request.execute()

                    if "items" in response:
                        for item in response["items"]:
                            # Check for captions
                            has_captions, caption_text = self.get_video_captions(
                                item["id"]
                            )

                            video_info = {
                                "Video URL": f"https://www.youtube.com/watch?v={item['id']}",
                                "Title": item["snippet"]["title"],
                                "Description": item["snippet"]["description"],
                                "Channel Title": item["snippet"]["channelTitle"],
                                "Keyword Tags": ",".join(
                                    item["snippet"].get("tags", [])
                                ),
                                "YouTube Video Category": item["snippet"]["categoryId"],
                                "Video Published at": item["snippet"]["publishedAt"],
                                "Video Duration": str(
                                    isodate.parse_duration(
                                        item["contentDetails"]["duration"]
                                    )
                                ),
                                "View Count": item["statistics"].get("viewCount", 0),
                                "Comment Count": item["statistics"].get(
                                    "commentCount", 0
                                ),
                                "Captions Available": has_captions,
                                "Caption Text": caption_text if has_captions else "",
                            }
                            video_data.append(video_info)
                            pbar.update(1)

                except Exception as e:
                    print(f"\nError processing batch: {str(e)}")
                    continue

                # Add a small delay to avoid API quota issues
                time.sleep(0.1)

            pbar.close()
            return video_data

        except Exception as e:
            print(f"Error in get_video_details: {str(e)}")
            return []

    def collect_data(self, genre):
        """
        Main function to collect all required data
        """
        try:
            start_time = time.time()
            print("\nStarting data collection process...")

            # Create output directory
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)

            # Search for videos
            video_ids = self.search_videos_by_genre(genre)

            if not video_ids:
                print("No videos found!")
                return

            # Get video details
            video_data = self.get_video_details(video_ids)

            if not video_data:
                print("No video details could be retrieved!")
                return

            # Save to CSV
            safe_genre = "".join(
                c for c in genre if c.isalnum() or c in (" ", "-", "_")
            ).strip()
            output_file = os.path.join(
                output_dir,
                f"youtube_{safe_genre}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            )
            df = pd.DataFrame(video_data)
            df.to_csv(output_file, index=False, encoding="utf-8")

            end_time = time.time()
            duration = end_time - start_time

            print(f"\nData collection completed!")
            print(f"Total videos processed: {len(video_data)}")
            print(f"Time taken: {duration/60:.2f} minutes")
            print(f"Data saved to: {output_file}")

        except Exception as e:
            print(f"Error in collect_data: {str(e)}")
