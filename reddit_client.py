import logging
import colorlog
import requests
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the current working directory
current_dir = os.getcwd()

# Set the log file path in the current directory
log_file_path = os.path.join(current_dir, "toxicity_analysis.log")

# Configure logger
logger = logging.getLogger("RedditClient")
logger.setLevel(logging.INFO)

color_formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    log_colors={
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_red",
    }
)

sh = logging.StreamHandler()
sh.setFormatter(color_formatter)
logger.addHandler(sh)

file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log_file_handler = logging.FileHandler(log_file_path)
log_file_handler.setFormatter(file_formatter)
logger.addHandler(log_file_handler)





MODERATE_API_TOKEN = os.getenv("MODERATE_API_TOKEN")
MODERATE_API_URL = "https://api.moderatehatespeech.com/api/v1/moderate/"

class RedditClient:
    API_BASE = "https://oauth.reddit.com"

    def __init__(self):
        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.access_token = self.get_access_token()

    def get_access_token(self):
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        data = {"grant_type": "client_credentials"}
        headers = {"User-Agent": "RedditClient/0.1"}
        response = requests.post("https://www.reddit.com/api/v1/access_token", auth=auth, data=data, headers=headers)
        response.raise_for_status()
        return response.json()["access_token"]

    def execute_request(self, endpoint):
        headers = {"Authorization": f"bearer {self.access_token}", "User-Agent": "RedditClient/0.1"}
        url = f"{self.API_BASE}{endpoint}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Error fetching data: {response.status_code}")
            return None
        return response.json()

    def get_hot_posts(self, subreddit, limit=500):
        all_posts = []
        after = None
        count = 0
        
        logger.info(f"Fetching up to {limit} hot posts from {subreddit}")

        while count < limit:
            remaining = limit - count
            batch_limit = min(remaining, 100)
            endpoint = f"/r/{subreddit}/hot?limit={batch_limit}"
            if after:
                endpoint += f"&after={after}"

            response = self.execute_request(endpoint)
            if not response or 'data' not in response:
                logger.error("No data received, stopping further requests.")
                break

            posts = response['data']['children']
            all_posts.extend(posts)
            count += len(posts)
            
            logger.info(f"Fetched {count} posts so far")

            if not posts or 'after' not in response['data']:
                break  # No more posts to fetch

            after = response['data']['after']

        logger.info(f"Total posts fetched: {len(all_posts)}")
        return all_posts

    def get_comments(self, subreddit, post_id):
        endpoint = f"/r/{subreddit}/comments/{post_id}"
        logger.info(f"Fetching comments for post {post_id} from {subreddit}")
        return self.execute_request(endpoint)

def get_toxicity_score(text, max_retries=3, delay=2):
    if not MODERATE_API_TOKEN:
        logger.error("ModerateHatespeech API token not found. Please set it in the .env file.")
        return None

    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "token": MODERATE_API_TOKEN,
        "text": text
    }

    retries = 0
    while retries < max_retries:
        logger.info(f"Attempting to get toxicity score (attempt {retries + 1}) for text: {text[:50]}...")
        try:
            response = requests.post(MODERATE_API_URL, json=data, headers=headers)
            if response.status_code != 200:
                logger.error(f"Non-200 status code (attempt {retries + 1}): {response.status_code}, Response Content: {response.text}")
                retries += 1
                time.sleep(delay)  
                continue

            if not response.text.strip():  
                logger.error(f"Received empty response content (attempt {retries + 1}).")
                retries += 1
                time.sleep(delay)
                continue

            try:
                result = response.json()
            except ValueError as ve:
                logger.error(f"Error parsing JSON response (attempt {retries + 1}): {ve}")
                retries += 1
                time.sleep(delay)
                continue

            if result.get("response") == "Success":
                logger.info(f"Received successful response for toxicity score on attempt {retries + 1}.")
                return {
                    "toxicity_score": float(result.get("confidence", 0.0)),
                    "is_toxic": result.get("class", "normal") == "flag",
                    "profanity_detected": "profanity" in result.get("class", "").lower()
                }
            else:
                logger.error(f"API response failed (attempt {retries + 1}): {result.get('response')}")
                retries += 1
                time.sleep(delay) 
                continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during request (attempt {retries + 1}): {e}")
            retries += 1
            time.sleep(delay)

    logger.error(f"Failed to get toxicity score after {max_retries} attempts.")
    return None