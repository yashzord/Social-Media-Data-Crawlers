import logging
import pymongo
from dotenv import load_dotenv
import os
import time
import requests
from pyfaktory import Client, Producer, Consumer, Job
import multiprocessing
import datetime
from chan_moderate_client import ChanModerateClient
from requests.exceptions import HTTPError, RequestException
import html
import re

# Loading all environment variables from the .env file
load_dotenv()

# Setting up the connection with MongoDB
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
if not MONGO_DB_URL:
    raise ValueError("MONGO_DB_URL environment variable not set.")
client = pymongo.MongoClient(MONGO_DB_URL)

# My database created will be called 4chan_moderate_data and there is 1 collection.
db = client['4chan_moderate_data']
g_tv_moderate_threads_collection = db['g_tv_moderate_threads']


# Logging to help with debugging
logger = logging.getLogger("ChanModerateCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
fh = logging.FileHandler("chan_moderate_crawler.log")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)

# Retrieving Faktory URL and Dynamically reading the Boards to Crawl from .env file.
FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL")
if not FAKTORY_SERVER_URL:
    raise ValueError("FAKTORY_SERVER_URL environment variable not set.")
BOARDS = os.getenv("BOARDS_MODERATE").split(',')

# Constants used when retrying incase of http errors
MAX_RETRIES = 5
RETRY_DELAY = 5
MAX_RETRY_DELAY = 60

# Error handling incase of HTTP or network errors, same as the error handling in execute_request in chan_client.
def retry_on_network_and_http_errors(func, *args):
    retries = 0
    delay = RETRY_DELAY
    while retries < MAX_RETRIES:
        try:
            return func(*args)
        except HTTPError as http_err:
            status_code = http_err.response.status_code

            # Case when thread maybe deleted or fell into archive board or resource not found in general
            if status_code == 404:
                logger.warning(f"Resource not found (404). Thread {args[1]} might be deleted.")
                return None
            
            # Case when Too Many Requests (Rate Limit Error)
            elif status_code == 429:
                retry_after = int(http_err.response.headers.get("Retry-After", delay))
                logger.warning(f"Rate limit hit (429). Retrying after {retry_after} seconds...")
                time.sleep(retry_after)

            # Client-side error
            elif 400 <= status_code < 500:
                retry_after = int(http_err.response.headers.get("Retry-After", delay)) if status_code == 429 else delay
                logger.warning(f"Client error {status_code} occurred for thread {args[1]}. Retrying in {retry_after} seconds...")
                time.sleep(retry_after)

            # Case when Server Side Error
            elif 500 <= status_code < 600:
                logger.error(f"Server error (status {status_code}) occurred. Retrying in {delay} seconds...")
                time.sleep(delay)
                # Capped exponential backoff
                delay = min(delay * 2, MAX_RETRY_DELAY)
            else:
                logger.error(f"Unexpected HTTP error: {http_err}")
                return None

        except RequestException as req_err:
            logger.error(f"Network error: {req_err}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)

    logger.error(f"Max retries reached. Failed to execute {func.__name__} after {MAX_RETRIES} attempts.")
    return None

# Gets all the thread numbers of a catalog, Used in crawl_board function.
def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_numbers.append(thread["no"])
    return thread_numbers

# For a specific board, it fetches all the exisiting threads from the database.
def get_existing_thread_ids_from_db(board):
    return [thread["thread_number"] for thread in g_tv_moderate_threads_collection.find({"board": board})]

# Compares newly crawled threads with the ones already exisiting in the database.
# Returns the difference which is the threads missing from current crawl so might be deleted.
def find_deleted_threads(previous_thread_numbers, current_thread_numbers):
    return set(previous_thread_numbers) - set(current_thread_numbers)

# Test Case - Handles Missing Values/Threads by replacing it with "Deleted" string and more....
# Marks a thread as deleted in MongoDB while keeping previous context.
def mark_thread_as_deleted(board, thread_number):

    logger.info(f"Marking thread {thread_number} on /{board}/ as deleted.")

    # We get the exisiting thread to collect context about that specific thread.
    try:
        existing_thread = g_tv_moderate_threads_collection.find_one({"board": board, "thread_number": thread_number})
    except pymongo.errors.PyMongoError as e:
        logger.error(f"Error fetching thread {thread_number} from MongoDB: {e}")
        return

    if existing_thread:
        if not existing_thread.get("is_deleted", False):
            history_entry = {
                "crawled_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "original_post": existing_thread.get("original_post"),
                "replies": existing_thread.get("replies"),
                "number_of_replies": existing_thread.get("number_of_replies", 0)
            }
            # We actually mark the thread as deleted / updates the Database.
            # We also add History - previous context to modify json structure after Deletion.
            g_tv_moderate_threads_collection.update_one(
                {"board": board, "thread_number": thread_number},
                {
                    "$set": {
                        "original_post.com": "[deleted]",
                        "replies": [{**reply, "com": "[deleted]"} for reply in existing_thread.get("replies", [])],
                        "number_of_replies": 0,
                        "deleted_at": existing_thread.get("deleted_at", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                        "is_deleted": True,
                        "history": [history_entry]
                    }
                }
            )
            logger.info(f"Thread {thread_number} on /{board}/ has been marked as deleted in MongoDB and historical data / previous context has been recorded.")
        else:
            logger.info(f"Thread {thread_number} on /{board}/ is already marked as deleted. No further updates to history.")
    else:
        logger.info(f"No existing data found for thread {thread_number} on /{board}/ to mark as deleted.")

# Cleans up "com" for OP and replies
def clean_html_content(html_content):
    decoded_html = html.unescape(html_content)
    # Uses Regex
    cleaned_text = re.sub(r'<.*?>', '', decoded_html)  
    return cleaned_text

# we filter the unwanted fields from the original post and replies.
def filter_thread_data(thread_data):
    # Attributes to exclude from the original post
    original_post_exclude_fields = [
        "now", "filename", "ext", "w", "h", "tn_w", "tn_h", "md5",
        "fsize", "resto", "m_img", "imagelimit", "semantic_url", 
        "custom_spoiler", "replies", "images", "sticky", "closed", "capcode", 
        "unique_ips", "tail_size", "tim", "bumplimit", "no", "time", "id",
        "country_name", "board_flag", "flag_name"

    ]
    # Attributes to exclude from replies
    replies_exclude_fields = [
        "now", "filename", "ext", "w", "h", "tn_w", "tn_h", "md5", "tim",
        "fsize", "capcode", "resto", "time", "id", "country_name", "m_img"
    ]

    # Converting the UNIX Timestamp to a human-readable format
    def convert_timestamp_to_readable(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

    filtered_original_post = {
        key: value for key, value in thread_data['posts'][0].items()
        if key not in original_post_exclude_fields
    }
    filtered_original_post['com'] = clean_html_content(filtered_original_post.get('com', ''))
    filtered_original_post['OP_Created_at'] = convert_timestamp_to_readable(thread_data['posts'][0]['time'])

    filtered_replies = [
        {**{key: value for key, value in reply.items() if key not in replies_exclude_fields},
         "com": clean_html_content(reply.get('com', '')),
         "Reply_Created_at": convert_timestamp_to_readable(reply['time'])}
        for reply in thread_data['posts'][1:]
    ]

    return filtered_original_post, filtered_replies


# Function to Crawl a Single thread, Uses get_thread from chan_client which has the API endpoint for a specific Thread.
# Handles Deleted/Archived Data, Duplicate Data, Actual String content Changes for OP and Replies.
# Handles Number of replies, Added replies, Deleted Replies, Inserting a thread into DB.
def crawl_thread(board, thread_number):
    chan_client = ChanModerateClient()
    logger.info(f"Fetching thread {board}/{thread_number}...")

    # Fetch thread data
    thread_data = retry_on_network_and_http_errors(chan_client.get_thread, board, thread_number)

    # Handle thread not found
    if thread_data is None:
        logger.warning(f"Thread {thread_number} might be deleted or unavailable.")

        # Mark thread as deleted if it exists in the database
        try:
            existing_thread = g_tv_moderate_threads_collection.find_one({"board": board, "thread_number": thread_number})
        except pymongo.errors.PyMongoError as e:
            logger.error(f"Error fetching thread {thread_number} from MongoDB: {e}")
            return 0

        if existing_thread:
            if not existing_thread.get("is_deleted", False):
                mark_thread_as_deleted(board, thread_number)
        else:
            logger.info(f"No existing data found for thread {thread_number} on /{board}/ to mark as deleted.")

        return 0
    else:
        logger.info(f"Successfully fetched thread {board}/{thread_number}.")



    filtered_original_post, filtered_replies = filter_thread_data(thread_data)

    # Prepare data for updating
    crawled_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Checking if the thread is already in the database
    try:
        existing_thread = g_tv_moderate_threads_collection.find_one({"board": board, "thread_number": thread_number})
    except pymongo.errors.PyMongoError as e:
        logger.error(f"Error fetching thread {thread_number} from MongoDB: {e}")
        return 0

    if existing_thread:
        if not existing_thread.get("is_deleted", False):
            # Adding history entry with only `crawled_at` and `number_of_replies`
            number_of_active_replies = len([reply for reply in filtered_replies if reply.get('com') != '[deleted]'])
            history_entry = {
                "crawled_at": crawled_at,
                "number_of_replies": number_of_active_replies
            }
            # Adding a history entry before making any changes to the thread
            try:
                g_tv_moderate_threads_collection.update_one(
                    {"board": board, "thread_number": thread_number},
                    {"$push": {"history": history_entry}}
                )
            except pymongo.errors.PyMongoError as e:
                logger.error(f"Error updating history for thread {thread_number}: {e}")
                return 0

        # Checking if the original post has changed compared to the database.
        existing_original_post = existing_thread.get('original_post', {})
        if existing_original_post.get('com') != filtered_original_post.get('com'):
            # Updating the Json for a thread accordingly if Original Post Content changed.
            try:
                g_tv_moderate_threads_collection.update_one(
                    {"board": board, "thread_number": thread_number},
                    {"$set": {
                        "original_post": filtered_original_post,
                        "updated_at": crawled_at,
                        "is_deleted": False
                    }}
                )
                logger.info(f"Updated original post content for thread {thread_number} on /{board}/.")
            except pymongo.errors.PyMongoError as e:
                logger.error(f"Error updating original post for thread {thread_number}: {e}")
                return 0
        else:
            logger.info(f"Original post unchanged for thread {thread_number} on /{board}/.")

        # Checking if replies have changed
        existing_replies = existing_thread.get("replies", [])
        existing_replies_dict = {reply.get('no'): reply for reply in existing_replies}
        new_replies_dict = {reply.get('no'): reply for reply in filtered_replies}

        # Tracking changes in individual replies
        updated_replies = []
        replies_changed = False

        # Processing existing and new replies
        for reply_no in new_replies_dict:
            new_reply = new_replies_dict[reply_no]
            if reply_no in existing_replies_dict:
                existing_reply = existing_replies_dict[reply_no]
                if existing_reply.get('com') != new_reply.get('com'):
                    # Content has changed
                    updated_replies.append(new_reply)
                    replies_changed = True
                else:
                    # No change
                    updated_replies.append(existing_reply)
            else:
                # New reply
                updated_replies.append(new_reply)
                replies_changed = True

        # Handling deleted replies
        deleted_reply_nos = set(existing_replies_dict.keys()) - set(new_replies_dict.keys())
        if deleted_reply_nos:
            replies_changed = True
            for reply_no in deleted_reply_nos:
                deleted_reply = existing_replies_dict[reply_no]
                deleted_reply['com'] = '[deleted]'
                updated_replies.append(deleted_reply)

        # Sorting updated_replies by 'no'
        updated_replies.sort(key=lambda x: x.get('no'))

        # Calculating the number of active (non-deleted) replies
        number_of_active_replies = len([reply for reply in updated_replies if reply.get('com') != '[deleted]'])

        # Updating the replies only if they have changed
        if replies_changed:
            try:
                g_tv_moderate_threads_collection.update_one(
                    {"board": board, "thread_number": thread_number},
                    {"$set": {
                        "replies": updated_replies,
                        "number_of_replies": number_of_active_replies,
                        "updated_at": crawled_at,
                        "is_deleted": False
                    }}
                )
                logger.info(f"Updated replies content for thread {thread_number} on /{board}/.")
            except pymongo.errors.PyMongoError as e:
                logger.error(f"Error updating replies for thread {thread_number}: {e}")
                return 0
        else:
            logger.info(f"Replies unchanged for thread {thread_number} on /{board}/.")

        existing_replies_count = len([reply for reply in existing_replies if reply.get('com') != '[deleted]'])
        new_replies_count = number_of_active_replies
        if new_replies_count > existing_replies_count:
            logger.info(f"Updated thread {thread_number} on /{board}/ with {new_replies_count - existing_replies_count} new replies.")
        elif new_replies_count < existing_replies_count:
            logger.info(f"Updated thread {thread_number} on /{board}/ to reflect deleted replies.")
            logger.info(f"Thread {thread_number} on /{board}/ has had replies deleted and updated in MongoDB.")
        else:
            logger.info(f"No new posts detected for thread {thread_number} on /{board}/.")
    else:
        # Inserting a new thread
        number_of_active_replies = len([reply for reply in filtered_replies if reply.get('com') != '[deleted]'])
        thread_info = {
            "board": board,
            "thread_number": thread_number,
            "original_post": filtered_original_post,
            "replies": filtered_replies,
            "number_of_replies": number_of_active_replies,
            "Initially_crawled_at": crawled_at,
            "is_deleted": False,
            "history": [{
                "crawled_at": crawled_at,
                "number_of_replies": number_of_active_replies
            }]
        }
        try:
            result = g_tv_moderate_threads_collection.insert_one(thread_info)
            logger.info(f"Inserted thread {thread_number} from /{board}/ into MongoDB with ID: {result.inserted_id}")
        except pymongo.errors.PyMongoError as e:
            logger.error(f"Error inserting thread {thread_number}: {e}")
            return 0

    return 1


# Crawls a specific board, uses get_catalog from chan_client to list all active threads in a specific board.
def crawl_board(board):
    chan_client = ChanModerateClient()
    catalog = retry_on_network_and_http_errors(chan_client.get_catalog, board)

    if catalog is None:
        logger.error(f"Failed to retrieve catalog for board /{board}/")
        return

    # Latest Crawl Thread Numbers which are active thread numbers in a specific board.
    current_thread_numbers = thread_numbers_from_catalog(catalog)
    total_original_posts = len(current_thread_numbers)

    # Fetching existing thread numbers from the database
    previous_thread_numbers = get_existing_thread_ids_from_db(board)

    # Using the find deleted thread defined above.
    # Handling Deleted Threads again just to make sure.
    deleted_threads = find_deleted_threads(previous_thread_numbers, current_thread_numbers)
    if deleted_threads:
        logger.info(f"Found {len(deleted_threads)} deleted threads on /{board}/: {deleted_threads}")
        for thread_number in deleted_threads:
            mark_thread_as_deleted(board, thread_number)

    # Queueing Jobs for crawl thread in faktory (Enqueued)
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for thread_number in current_thread_numbers:
            job = Job(jobtype="crawl-moderate-thread", args=(board, thread_number), queue="crawl-moderate-thread")
            producer.push(job)

    logger.info(f"Queued crawl jobs for all threads on /{board}/")
    logger.info(f"Total original posts crawled from /{board}/: {total_original_posts}")

# Schedules the Crawl after every specific interval. In our case it should be 6 hours or TBD.
def schedule_crawl_jobs_continuously(interval_minutes=360):
    # Keeps track of which crawl we are currently performing.
    crawl_count = 0
    while True:
        crawl_count += 1
        # Enqueues Job's for crawl-moderate-thread (so 2 jobs as 1 for g and 1 for tv)
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for board in BOARDS:
                job = Job(jobtype="crawl-moderate-board", args=(board,), queue="crawl-moderate-board")
                producer.push(job)
            logger.info(f"Scheduled crawl job #{crawl_count} for all boards.")

        logger.info(f"Crawl #{crawl_count} finished. Waiting for {interval_minutes} minutes before the next crawl.")
        # Sleeps after every crawl, in our case it should be 6 hrs = 360 mins = 21600 s
        time.sleep(interval_minutes * 60)

# We Produced a job for crawl thread and crawl board and here we consume those jobs to be in sync.
# Producer-Consumer Model.
def start_worker():
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-moderate-board","crawl-moderate-thread"], concurrency=5)
        consumer.register("crawl-moderate-board", crawl_board)
        consumer.register("crawl-moderate-thread", crawl_thread)
        logger.info("Worker started. Listening for jobs...")
        consumer.run()

# We multiprocess the start worker to run in parallel
# We call schedule_crawl_jobs_continuously here to start the crawls.
# We dont stop it/Interrupt the crawler till the end of the class.
# We specify the minutes to wait before crawling after the first crawl for subsequent crawl-> 360 mins 
if __name__ == "__main__":
    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()
    # As of now crawling every 20 minutes, but might change it.
    schedule_crawl_jobs_continuously(interval_minutes=20)

    try:
        worker_process.join()
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        worker_process.join()
        logger.info("Processes stopped.")