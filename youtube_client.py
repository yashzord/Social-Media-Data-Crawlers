import os
import requests
import logging
import re
from dotenv import load_dotenv
import time

load_dotenv()

logger = logging.getLogger("YouTubeClient")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class YouTubeClient:
    def __init__(self):
        self.api_key = os.getenv("YOUTUBE_API_KEY")
        self.api_key_t = os.getenv("YOUTUBE_KEY")
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.hate_speech_api_key = os.getenv("MODERATE_HATESPEECH_API_KEY")
        self.hate_speech_api_url = "https://api.moderatehatespeech.com/api/v1/moderate/"

    def get_channel_details(self, channel_id, toxicity=False):
        api_key = self.api_key_t if toxicity else self.api_key
        logger.info(f"Fetching details for channel ID: {channel_id}")
        url = f"{self.base_url}/channels?part=snippet,statistics&id={channel_id}&key={self.api_key}"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching channel details: {response.status_code}")
            return None
        
        data = response.json()
        return data['items'][0]

    def get_channel_videos(self, channel_id, limit=50, toxicity=False):
        api_key = self.api_key_t if toxicity else self.api_key
        logger.info(f"Fetching videos for channel ID: {channel_id}")
        url = f"{self.base_url}/search?part=snippet&channelId={channel_id}&maxResults={limit}&order=viewCount&type=video&key={self.api_key}"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching videos: {response.status_code}")
            return None
        
        data = response.json()
        return data.get('items', [])

    def get_video_details(self, video_id, toxicity=False):
        api_key = self.api_key_t if toxicity else self.api_key
        logger.info(f"Fetching details for video ID: {video_id}")
        url = f"{self.base_url}/videos?part=snippet,statistics&id={video_id}&key={self.api_key}"
        response = requests.get(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching video details: {response.status_code}")
            return None
        
        data = response.json()
        return data['items'][0]

    def analyze_toxicity(self, comment_text, retries=3, delay=2):
        if not comment_text:
            logger.warning("Skipping toxicity analysis: No text provided")
            return None

        for attempt in range(retries):
            try:
                headers = {"Content-Type": "application/json"}
                payload = {
                    "token": self.hate_speech_api_key,
                    "text": comment_text
                }
                response = requests.post(self.hate_speech_api_url, headers=headers, json=payload)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data.get("response") == "Success":
                            return {
                                "is_toxic": data.get("class"),
                                "toxicity": data.get("confidence")
                            }
                        else:
                            logger.warning(f"Toxicity API error: {data.get('response')}")
                            return None
                    except ValueError:
                        logger.error(f"Error analyzing toxicity: Response was not valid JSON. Raw response: {response.text}")
                        return None
                else:
                    logger.warning(f"Toxicity API returned status {response.status_code}")
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Error analyzing toxicity (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(delay)  
                    delay *= 2  
                else:
                    logger.error("Max retries reached. Skipping this comment.")
                    return None



    def get_video_comments(self, video_id, limit=100):
        logger.info(f"Fetching up to {limit} comments for video ID: {video_id}")
        comments = []
        page_token = None
        comment_index = 0

        while len(comments) < limit:
            url = f"{self.base_url}/commentThreads?part=snippet,replies&videoId={video_id}&maxResults=100&key={self.api_key}"
            if page_token:
                url += f"&pageToken={page_token}"
            
            response = requests.get(url)
            if response.status_code != 200:
                logger.error(f"Error fetching comments: {response.status_code}")
                break

            data = response.json()
            for item in data.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']
                text = comment.get("textDisplay", "")
                clean_text = re.sub(r'<.*?>', '', text) 
                toxicity_data = self.analyze_toxicity(clean_text)

                if toxicity_data and toxicity_data.get("class") == "flag":
                    print(f"Toxic comment detected at index {comment_index}")

                filtered_comment = {
                    "authorDisplayName": comment.get("authorDisplayName"),
                    "textDisplay": clean_text,
                    "publishedAt": comment.get("publishedAt"),
                    "likeCount": comment.get("likeCount", 0),
                    "toxicity_data": toxicity_data,
                    # "replies": self.get_comment_replies(item['id'], max_replies=10)
                    "replies": item.get("replies", {}).get("comments", [])
                }
                comments.append(filtered_comment)
                comment_index += 1

                if len(comments) >= limit:
                    break

            page_token = data.get('nextPageToken')
            if not page_token:
                break  

        return comments[:limit]