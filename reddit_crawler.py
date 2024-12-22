import logging
import pymongo
import os
import time
from dotenv import load_dotenv
from faktory import Client, Worker
from reddit_client import RedditClient, get_toxicity_score  # Import get_toxicity_score here
from datetime import datetime, timedelta
import multiprocessing
from requests.exceptions import HTTPError

# Load environment variables
load_dotenv()

# Configurations and logging setup
MONGO_DB_URL = os.getenv("MONGO_DB_URL") or "mongodb://localhost:27017/"
FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL") or 'tcp://:raj123@localhost:7419'
MAX_RETRIES = 5
RETRY_DELAY = 5

# Setup logger
logger = logging.getLogger("RedditCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)


#-------------------------------------------------------------------------------------

def initialize_mongo_client():
    """Initialize MongoDB client within each worker to ensure fork-safety."""
    mongo_client = pymongo.MongoClient(MONGO_DB_URL)
    db = mongo_client['reddit_Data_moderate_speech']
    return db




##------------------------------------------------------------
def retry_on_network_and_http_errors(func, *args):
    retries = 0
    delay = RETRY_DELAY
    while retries < MAX_RETRIES:
        try:
            return func(*args)
        except HTTPError as http_err:
            status_code = http_err.response.status_code
            if 400 <= status_code < 500:
                if status_code == 404:
                    logger.warning(f"Resource not found (404). Post {args[1]} might be deleted.")
                else:
                    logger.error(f"Client error (status {status_code}) occurred for post {args[1]}. No retry.")
                break
            elif 500 <= status_code < 600:
                logger.error(f"Server error (status {status_code}) occurred. Retrying in {delay} seconds...")
            else:
                logger.error(f"Unexpected HTTP error: {http_err}")
            time.sleep(delay)
            retries += 1
            delay *= 2
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Network error: {req_err}. Retrying in {delay} seconds...")
            time.sleep(delay)
            retries += 1
            delay *= 2
    logger.error(f"Max retries reached. Failed to execute {func.__name__} after {MAX_RETRIES} attempts.")
    return None

def find_dead_threads(previous_catalog_thread_numbers, current_catalog_thread_numbers):
   
   
    dead_thread_numbers = set(previous_catalog_thread_numbers).difference(set(current_catalog_thread_numbers))
    return dead_thread_numbers





## ------------------------------------------------------------------------------------------------------------------------------


def crawl_post(subreddit, post_id, collection_name):
    db = initialize_mongo_client()
    collection = db[collection_name]
    reddit_client = RedditClient()

    try:
        post_data = retry_on_network_and_http_errors(reddit_client.get_comments, subreddit, post_id)
    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        if status_code == 404:
            logger.warning(f"Post {post_id} Marking as deleted.")
            collection.update_one(
                {"post_id": post_id},
                {
                    "$set": {
                        "post_title": "[Deleted Title]",
                        "post_content": "[Deleted Content]",
                        "is_deleted": True,
                        "crawled_at": datetime.now()
                    },
                    "$push": {"crawl_history": datetime.now()}
                },
                upsert=True
            )
        else:
            logger.error(f"HTTP error occurred: {http_err}")
        return

    if post_data is None or len(post_data[0]['data']['children']) == 0:
        logger.warning(f"Post {post_id} might be deleted or unavailable.")
        collection.update_one(
            {"post_id": post_id},
            {
                "$set": {
                    "post_title": "[Deleted Title]",
                    "post_content": "[Deleted Content]",
                    "is_deleted": True,
                    "crawled_at": datetime.now()
                },
                "$push": {"crawl_history": datetime.now()}
            },
            upsert=True
        )
        return

    current_time = datetime.now()
    post_title = post_data[0]['data']['children'][0]['data'].get('title', '[Deleted Title]')
    post_content = post_data[0]['data']['children'][0]['data'].get('selftext', '[Deleted Content]')

    #------------------------------------------------------- Handle empty post content
    
    if not post_content.strip(): 
        post_content = "[No Content]"
        logger.warning(f"Post {post_id} in subreddit {subreddit} has no content.")

    # Use get_toxicity_score for title and content
    title_moderate_score = get_toxicity_score(post_title) if post_title and post_title != '[Deleted Title]' else None
    content_moderate_score = get_toxicity_score(post_content) if post_content and post_content != '[Deleted Content]' else {"is_toxic": False, "toxicity_score": 0.0}

    all_comments = post_data[1]['data']['children']
    comments = []
    for comment in all_comments:
        comment_data = comment['data']
        comment_text = comment_data.get('body', '[Deleted]')
        if comment_text and comment_text != '[Deleted]':
            comment_entry = {
                "comment_id": comment_data.get('id', ''),
                "author": comment_data.get('author', '[Deleted]'),
                "body": comment_text,
                "upvote_score": comment_data.get('score', 0),
                "created_utc": datetime.fromtimestamp(comment_data.get('created_utc', 0)),
                "moderate_class": None,
                "moderate_confidence": 0.0
            }

            # ---------------------------------------------------   Get moderate speech score for comment
            comment_moderate_score = get_toxicity_score(comment_text)
            if comment_moderate_score:
                comment_entry['moderate_class'] = "flag" if comment_moderate_score["is_toxic"] else "normal"
                comment_entry['moderate_confidence'] = comment_moderate_score["toxicity_score"]

            comments.append(comment_entry)

    post_info = {
        "subreddit": subreddit,
        "post_id": post_id,
        "post_title": post_title,
        "post_content": post_content,
        "upvotes": post_data[0]['data']['children'][0]['data'].get('ups', 0),
        "downvotes": post_data[0]['data']['children'][0]['data'].get('downs', 0),
        "comment_count": post_data[0]['data']['children'][0]['data'].get('num_comments', 0),
        "comments": comments,
        "crawled_at": current_time,
        "is_deleted": False,
        "submitted_at": datetime.fromtimestamp(post_data[0]['data']['children'][0]['data'].get('created_utc', 0)),
        "title_moderate_class": "flag" if title_moderate_score and title_moderate_score["is_toxic"] else "normal",
        "title_moderate_confidence": title_moderate_score["toxicity_score"] if title_moderate_score else 0.0,
        "content_moderate_class": "flag" if content_moderate_score and content_moderate_score["is_toxic"] else "normal",
        "content_moderate_confidence": content_moderate_score["toxicity_score"] if content_moderate_score else 0.0
    }

    collection.update_one(
        {"post_id": post_id},
        {
            "$set": post_info,
            "$push": {"crawl_history": current_time}
        },
        upsert=True
    )
    logger.info(
        f"Processed post {post_id} from subreddit {subreddit}. "
        f"Title moderate class: {post_info['title_moderate_class']}, "
        f"Content moderate class: {post_info['content_moderate_class']}."
    )





##-------------------------------------------------------------------------------------------------------


def crawl_subreddit(subreddit, collection_name):
    db = initialize_mongo_client()
    collection = db[collection_name]
    reddit_client = RedditClient()

    hot_posts = retry_on_network_and_http_errors(reddit_client.get_hot_posts, subreddit, 500)
    if hot_posts is None:
        logger.error(f"Failed to retrieve hot posts from {subreddit}")
        return

    logger.info(f"Retrieved {len(hot_posts)} hot posts from subreddit {subreddit}.")
    current_post_ids = [post['data']['id'] for post in hot_posts]
    
    

    existing_posts = collection.find({"subreddit": subreddit, "is_deleted": False}, {"post_id": 1})
    existing_post_ids = [post['post_id'] for post in existing_posts]

    # Mark dead posts
    dead_post_ids = find_dead_threads(existing_post_ids, current_post_ids)
    if dead_post_ids:
        logger.info(f"Found {len(dead_post_ids)} deleted posts in subreddit {subreddit}. Marking them as deleted.")
        collection.update_many(
            {"post_id": {"$in": list(dead_post_ids)}},
            {"$set": {"is_deleted": True, "crawled_at": datetime.now()}}
        )

    # Queue jobs to crawl each post in hot_posts
    for post in hot_posts:
        post_id = post['data']['id']
        existing_post = collection.find_one({"post_id": post_id})
        
        if existing_post is None or (existing_post['upvotes'] != post['data']['ups'] or existing_post['comment_count'] != post['data']['num_comments']):
            with Client() as client:
                client.queue('crawl_post', args=(subreddit, post_id, collection_name), queue='crawl_post')
                logger.info(f"Queued job to crawl post {post_id} from {subreddit} due to detected changes or new post.")
                
                
                
                
                




def start_worker():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    worker = Worker(queues=['crawl_subreddit', 'crawl_post'])
    worker.register('crawl_subreddit', crawl_subreddit)
    worker.register('crawl_post', crawl_post)
    logger.info("Worker started. Listening for jobs...")
    worker.run()
    
    
    ##------------------------------------------------------------------------------------------------------

def schedule_crawl_jobs(tech_movie_interval=10800, politics_interval=10800):
  
    
    
    os.environ['FAKTORY_URL'] = os.getenv('FAKTORY_SERVER_URL')

    # Fetch subreddits from environment variables
    permanent_subreddits = os.getenv('SUBREDDITS_TECH_MOVIE', '').split(',')
    politics_subreddit = os.getenv('SUBREDDITS_POLITICS', '')
    
    
    print(f"Scheduling crawl jobs for: {permanent_subreddits}")
    print(f"Scheduling crawl jobs for: {politics_subreddit}")
    
    
    
    
    politics_start_date = datetime(2024, 11, 16)
    politics_end_date = datetime(2024, 12, 15, 23, 59, 59)
    
    last_tech_movie_time = datetime.min
    last_politics_time = datetime.min
    
    while True:
        current_time = datetime.now()
        
        with Client() as client:
            # Queue tech and movie subreddits
            if (current_time - last_tech_movie_time).total_seconds() >= tech_movie_interval:
                for subreddit in permanent_subreddits:
                    client.queue('crawl_subreddit', args=(subreddit, 'posts'), queue='crawl_subreddit')
                    logger.info(f"Queued job to crawl subreddit: {subreddit}")
                last_tech_movie_time = current_time
            
            # Queue politics 
            if politics_start_date <= current_time <= politics_end_date and (current_time - last_politics_time).total_seconds() >= politics_interval:
                client.queue('crawl_subreddit', args=(politics_subreddit, 'reddit_politics'), queue='crawl_subreddit')
                logger.info(f"Queued job to crawl subreddit: {politics_subreddit}")
                last_politics_time = current_time
            elif current_time > politics_end_date:
                logger.info(f"Stopped crawling {politics_subreddit} as the end date has passed.")
        
        time.sleep(1)  





def monitor_queue():
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL
    while True:
        with Client() as client:
            client.send("PING")
            response = client.read()
            if response == b"+PONG\r\n":
                logger.info("Faktory server is running.")
            else:
                logger.error("Failed to connect to Faktory server.")
        
        time.sleep(120) 

if __name__ == "__main__":
    os.environ['FAKTORY_URL'] = FAKTORY_SERVER_URL

    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()

    monitor_process = multiprocessing.Process(target=monitor_queue)
    monitor_process.start()

    try:
        schedule_crawl_jobs(tech_movie_interval=10800, politics_interval=10800)    ##  time limit 
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        monitor_process.terminate()
        logger.info("All processes terminated. Exiting.")