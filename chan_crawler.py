import logging
import pymongo
from dotenv import load_dotenv
import os
import time
from pyfaktory import Client, Producer, Consumer, Job
import multiprocessing
import datetime
from chan_client import ChanClient
from requests.exceptions import HTTPError, RequestException
import html
import re

# Loading all environment variables from the .env file
load_dotenv()

# Setting up the connection with MongoDB
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
client = pymongo.MongoClient(MONGO_DB_URL)

# My database created will be called 4chan_data and there are 2 collections.
db = client['4chan_data']
g_tv_threads_collection = db['g_tv_threads']
# pol_threads_collection = db['pol_threads']

# Logging to help with debugging
logger = logging.getLogger("ChanCrawler")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

# FileHandler to log everything to a file
log_file_path = "chan_crawler.log"  # Define the log file path
fh = logging.FileHandler(log_file_path)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Retrieving Faktory URL and Dynamically reading the Boards to Crawl from .env file.
FAKTORY_SERVER_URL = os.getenv("FAKTORY_SERVER_URL")
BOARDS = os.getenv("BOARDS").split(',')

# Constants used when retrying incase of http errors
MAX_RETRIES = 5
RETRY_DELAY = 5
MAX_RETRY_DELAY = 60

# Defining the date range for /pol/ board Collection
# POL_START_DATE = datetime.datetime(2024, 11, 1)
# POL_END_DATE = datetime.datetime(2024, 11, 14, 23, 59, 59)

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

    # collection = pol_threads_collection if board == 'pol' else g_tv_threads_collection
    return [thread["thread_number"] for thread in g_tv_threads_collection.find({"board": board})]

# Compares newly crawled threads with the ones already exisiting in the database.
# Returns the difference which is the threads missing from current crawl so might be deleted.
def find_deleted_threads(previous_thread_numbers, current_thread_numbers):

    return set(previous_thread_numbers) - set(current_thread_numbers)

# Test Case - Handles Missing Values/Threads by replacing it with "Deleted" string and more....
# Marks a thread as deleted in MongoDB while keeping previous context.
def mark_thread_as_deleted(board, thread_number):

    logger.info(f"Marking thread {thread_number} on /{board}/ as deleted.")
    # collection = pol_threads_collection if board == 'pol' else g_tv_threads_collection
    collection = g_tv_threads_collection

    # We get the exisiting thread to collect context about that specific thread.
    existing_thread = collection.find_one({"board": board, "thread_number": thread_number})

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
            collection.update_one(
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
    chan_client = ChanClient()
    logger.info(f"Fetching thread {board}/{thread_number}...")

    # Getting the thread data for a speciifc thread after running Http and Network Errors on it beforehand.
    thread_data = retry_on_network_and_http_errors(chan_client.get_thread, board, thread_number)
    
    # If a thread is deleted or archived or not found.
    if thread_data is None:
        logger.warning(f"Thread {thread_number} might be deleted or unavailable.")
        
        # Use appropriate collection for board
        # collection = pol_threads_collection  if board == 'pol' else g_tv_threads_collection
        collection = g_tv_threads_collection
        existing_thread = collection.find_one({"board": board, "thread_number": thread_number})   

        if existing_thread:
            # Updating existing thread to mark it as deleted
            if not existing_thread.get("is_deleted", False):  # Only mark as deleted if it's not already deleted
                mark_thread_as_deleted(board, thread_number)
        else:
            logger.info(f"No existing data found for thread {thread_number} on /{board}/ to mark as deleted.")

        return 0
    else:
        logger.info(f"Successfully fetched thread {board}/{thread_number}.")


    filtered_original_post, filtered_replies = filter_thread_data(thread_data)

    # Preparing the current data for storing in history
    # collection = pol_threads_collection if board == 'pol' else g_tv_threads_collection
    collection = g_tv_threads_collection
    number_of_replies = len(filtered_replies)
    crawled_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Checking if the thread is already in the database
    existing_thread = collection.find_one({"board": board, "thread_number": thread_number})

    if existing_thread:
        if not existing_thread.get("is_deleted", False):
            # Adding history entry with only `crawled_at` and `number_of_replies`
            history_entry = {
                "crawled_at": crawled_at,
                "number_of_replies": number_of_replies
            }
            # Adding a history entry before making any changes to the thread
            collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$push": {"history": history_entry}}  # Add to the history array
            )

        # Checking if the original post has changed compared to the database.
        if existing_thread['original_post'] != filtered_original_post:
            # Updating the Json for a thread accordingly if Original Post Content changed.
            collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {"original_post": filtered_original_post, "updated_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "is_deleted": False}}
            )
            logger.info(f"Updated original post content for thread {thread_number} on /{board}/.")

        # Checking if replies have changed
        existing_replies = existing_thread.get("replies", [])
        new_replies = filtered_replies
        existing_replies_count = len(existing_replies)
        new_replies_count = len(new_replies)

        # Tracking changes in individual replies
        updated_replies = []
        reply_deleted = False
        for i in range(min(existing_replies_count, new_replies_count)):
            if existing_replies[i] != new_replies[i]:
                updated_replies.append(new_replies[i])

        # If there are updates to replies, we also update them in the database
        if updated_replies:
            collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {"replies": new_replies[:existing_replies_count], "updated_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "is_deleted": False}}
            )
            logger.info(f"Updated replies content for thread {thread_number} on /{board}/.")

        # If at all new replies are added in latest crawl when compared to previous crawl
        # Update the count and content of replies in the database accordingly by pushing/adding.
        if new_replies_count > existing_replies_count:
            new_replies_to_add = new_replies[existing_replies_count:]
            collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$push": {"replies": {"$each": new_replies_to_add}}, "$set": {"number_of_replies": new_replies_count, "updated_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}
            )
            logger.info(f"Updated thread {thread_number} on /{board}/ with {new_replies_count - existing_replies_count} new replies.")

        # If at all replies are deleted in latest crawl when compared to previous crawl
        # Update the count and content of replies in the database accordingly by removing/deleting.
        elif new_replies_count < existing_replies_count:
            updated_replies = new_replies
            collection.update_one(
                {"board": board, "thread_number": thread_number},
                {"$set": {
                    "replies": updated_replies,
                    "number_of_replies": new_replies_count,
                    "updated_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }}
            )
            logger.info(f"Updated thread {thread_number} on /{board}/ to reflect deleted replies.")
            logger.info(f"Thread {thread_number} on /{board}/ has had replies deleted and updated in MongoDB.")
        else:
            logger.info(f"No new posts detected for thread {thread_number} on /{board}/.")
    else:
        # Normal case of inserting a thread into DB for the first time.
        thread_info = {
            "board": board,
            "thread_number": thread_number,
            "original_post": filtered_original_post,
            "replies": filtered_replies,
            "number_of_replies": len(filtered_replies),
            "Initially_crawled_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "is_deleted": False,
            "history": [{
                "crawled_at": crawled_at,
                "number_of_replies": number_of_replies
            }]
        }
        result = collection.insert_one(thread_info)
        logger.info(f"Inserted thread {thread_number} from /{board}/ into MongoDB with ID: {result.inserted_id}")

    return 1

# Crawls a specific board, uses get_catalog from chan_client to list all active threads in a specific board.
def crawl_board(board):
    chan_client = ChanClient()
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

    # Skip if /pol/ is outside the specified date range
    # if board == 'pol' and (datetime.datetime.now() < POL_START_DATE or datetime.datetime.now() > POL_END_DATE):
    #     logger.info(f"Skipping /pol/ as it is outside the specified date range (Nov 1-14).")
    #     return

    # Queueing Jobs for crawl thread in faktory (Enqueued)
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for thread_number in current_thread_numbers:
            job = Job(jobtype="crawl-thread", args=(board, thread_number), queue="crawl-thread")
            producer.push(job)

    logger.info(f"Queued crawl jobs for all threads on /{board}/")
    logger.info(f"Total original posts crawled from /{board}/: {total_original_posts}")

# Schedules the Crawl after every specific interval. In our case it should be 6 hours or TBD.
def schedule_crawl_jobs_continuously(interval_minutes=360):
    # Keeps track of which crawl we are currently performing.
    crawl_count = 0
    while True:
        crawl_count += 1
        # Enqueues Job's for crawl-board (so 2 jobs as 1 for g and 1 for tv)
        with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
            producer = Producer(client=client)
            for board in BOARDS:
                job = Job(jobtype="crawl-board", args=(board,), queue="crawl-board")
                producer.push(job)
            logger.info(f"Scheduled crawl job #{crawl_count} for all boards.")

        logger.info(f"Crawl #{crawl_count} finished. Waiting for {interval_minutes} minutes before the next crawl.")
        # Sleeps after every crawl, in our case it should be 6 hrs = 360 mins = 21600 s
        time.sleep(interval_minutes * 60)

# We Produced a job for crawl thread and crawl board and here we consume those jobs to be in sync.
# Producer-Consumer Model.
def start_worker():
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-board", "crawl-thread"], concurrency=5)
        consumer.register("crawl-board", crawl_board)
        consumer.register("crawl-thread", crawl_thread)
        logger.info("Worker started. Listening for jobs...")
        consumer.run()

# We multiprocess the start worker to run in parallel
# We call schedule_crawl_jobs_continuously here to start the crawls.
# We dont stop it/Interrupt the crawler till the end of the class.
# We specify the minutes to wait before crawling after the first crawl for subsequent crawl-> 360 mins 
if __name__ == "__main__":
    worker_process = multiprocessing.Process(target=start_worker)
    worker_process.start()
    # As of now crawling every 10 minutes, but might change it.
    schedule_crawl_jobs_continuously(interval_minutes=15)

    try:
        worker_process.join()
    except KeyboardInterrupt:
        logger.info("Stopping processes...")
        worker_process.terminate()
        worker_process.join()
        logger.info("Processes stopped.")