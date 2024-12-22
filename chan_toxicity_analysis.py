import logging
from logging.handlers import RotatingFileHandler
import pymongo
from dotenv import load_dotenv
import os
import time
import requests
from requests.exceptions import HTTPError, RequestException
import hashlib

load_dotenv()

logger = logging.getLogger("ToxicityAnalysis")
logger.setLevel(logging.INFO)
log_file = 'chan_toxicity_analysis.log'
max_log_size = 10 * 1024 * 1024 
backup_count = 1
rotating_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
rotating_handler.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
rotating_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)
logger.addHandler(rotating_handler)
logger.addHandler(stream_handler)

API_URL = "https://api.moderatehatespeech.com/api/v1/moderate/"
MAX_RETRIES = 5
RETRY_DELAY = 5
MAX_RETRY_DELAY = 60

MODERATE_HATESPEECH_API_KEY = os.getenv("CHAN_MODERATE_HATESPEECH_API_KEY")
if not MODERATE_HATESPEECH_API_KEY:
    raise ValueError("MODERATE_HATESPEECH_API_KEY environment variable not set.")

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
if not MONGO_DB_URL:
    raise ValueError("MONGO_DB_URL environment variable not set.")
mongo_client = pymongo.MongoClient(MONGO_DB_URL)

db = mongo_client['4chan_moderate_data']
g_tv_moderate_threads_collection = db['g_tv_moderate_threads']

class ToxicityAnalyzer:
    def __init__(self):
        self.session = requests.Session()
        self.api_key = MODERATE_HATESPEECH_API_KEY

    def analyze_text(self, text):
        if not text.strip() or text.strip() == '[deleted]':
            # Handles empty or deleted text
            return {'class': 'unknown', 'confidence': 0.0}

        payload = {
            "token": self.api_key,
            "text": text
        }
        headers = {
            "Content-Type": "application/json"
        }
        retries = 0
        delay = RETRY_DELAY

        while retries < MAX_RETRIES:
            try:
                response = self.session.post(API_URL, json=payload, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()

                if data.get("response") == "Success":
                    return {
                        "class": data.get("class"),
                        "confidence": float(data.get("confidence"))
                    }
                else:
                    logger.error(f"API Error: {data.get('response')}")
                    return {'class': 'unknown', 'confidence': 0.0}

            except (HTTPError, RequestException) as e:
                logger.error(f"Error communicating with ModerateHatespeech API: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                retries += 1
                delay = min(delay * 2, MAX_RETRY_DELAY)

        logger.error(f"Max retries reached for text analysis. Text: {text[:30]}...")
        return {'class': 'unknown', 'confidence': 0.0}

def get_content_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def process_thread(thread):
    thread_id = thread['_id']
    board = thread.get('board')
    thread_number = thread.get('thread_number')
    is_deleted = thread.get('is_deleted', False)

    if is_deleted:
        logger.info(f"Thread {board}/{thread_number} is marked as deleted. Skipping toxicity analysis.")
        return

    logger.info(f"Processing thread {board}/{thread_number}")

    original_post = thread.get('original_post', {})
    replies = thread.get('replies', [])

    analyzer = ToxicityAnalyzer()

    original_com = original_post.get('com', '')
    if original_com.strip() == '[deleted]':
        logger.info(f"Original post in thread {board}/{thread_number} is deleted. Skipping toxicity analysis.")
    else:
        original_hash = get_content_hash(original_com)
        original_toxicity = thread.get('original_post_toxicity', {})

        if original_toxicity.get('content_hash') != original_hash:
            toxicity = analyzer.analyze_text(original_com)
            toxicity['content_hash'] = original_hash
            toxicity['com'] = original_com

            g_tv_moderate_threads_collection.update_one(
                {'_id': thread_id},
                {'$set': {'original_post_toxicity': toxicity}}
            )
            logger.info(f"Updated toxicity for original post in thread {board}/{thread_number}")
        else:
            logger.info(f"Original post in thread {board}/{thread_number} has not changed, skipping toxicity analysis")

    existing_replies_toxicity = thread.get('replies_toxicity', [])
    replies_toxicity_dict = {item['reply_no']: item for item in existing_replies_toxicity}

    updated_replies_toxicity = []
    for reply in replies:
        reply_no = reply.get('no')
        reply_com = reply.get('com', '')

        if reply_com.strip() == '[deleted]':
            logger.info(f"Reply {reply_no} in thread {board}/{thread_number} is deleted. Skipping toxicity analysis.")
            continue

        reply_hash = get_content_hash(reply_com)

        existing_toxicity = replies_toxicity_dict.get(reply_no, {})

        if existing_toxicity.get('content_hash') != reply_hash:
            toxicity = analyzer.analyze_text(reply_com)
            toxicity['content_hash'] = reply_hash
            toxicity['reply_no'] = reply_no
            toxicity['com'] = reply_com
            updated_replies_toxicity.append(toxicity)
            logger.info(f"Updated toxicity for reply {reply_no} in thread {board}/{thread_number}")
        else:
            updated_replies_toxicity.append(existing_toxicity)
            logger.info(f"Reply {reply_no} in thread {board}/{thread_number} has not changed, skipping toxicity analysis")

    g_tv_moderate_threads_collection.update_one(
        {'_id': thread_id},
        {'$set': {'replies_toxicity': updated_replies_toxicity}}
    )

def process_threads():

    while True:
        try:
            threads = g_tv_moderate_threads_collection.find({})
            for thread in threads:
                process_thread(thread)
            logger.info("Completed processing all threads. Sleeping for a while...")
            time.sleep(420)
        except Exception as e:
            logger.error(f"Error during processing: {e}")
            time.sleep(60)

if __name__ == "__main__":
    process_threads()
