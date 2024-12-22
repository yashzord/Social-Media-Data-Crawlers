import logging
import requests
import time
from requests.exceptions import HTTPError, RequestException

# Logging to help with debugging
logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

# Constants used when retrying incase of http errors
MAX_RETRIES = 5
RETRY_DELAY = 5
MAX_RETRY_DELAY = 60

class ChanClient:
    # Base Url
    API_BASE = "http://a.4cdn.org"

    def __init__(self):
        # Caches the last modified time for each API call
        self.last_modified_times = {}

    def execute_request(self, api_call, headers={}, retries=MAX_RETRIES, retrying_wait_time=RETRY_DELAY):
        """
        Executes the Http Get request to crawl from 4chan API Endpoints
        Also, implemented Error Handling and retrying incase of Http Errors
        """
        for attempt in range(1, retries + 1):
            try:
                # Actual Get Request part
                response = requests.get(api_call, headers=headers)

                if response.status_code == 304:
                    logger.info("No new data since last modified.")
                    return None
                
                response.raise_for_status()

                # If available. we save the last modified time to use in subsequent requests.
                if 'Last-Modified' in response.headers:
                    self.last_modified_times[api_call] = response.headers['Last-Modified']

                return response.json()
            
            except HTTPError as http_err:
                status_code = response.status_code

                # Case when thread maybe deleted or fell into archive board or resource not found in general
                if status_code == 404:
                    logger.warning(f"Resource not found (404): {api_call}")
                    return None
                
                # Client-side error
                elif 400 <= status_code < 500:
                    retry_after = int(response.headers.get("Retry-After", retrying_wait_time)) if status_code == 429 else retrying_wait_time
                    logger.warning(f"Client error {status_code} on {api_call}. Retrying in {retry_after} seconds (Attempt {attempt}/{retries})...")
                    time.sleep(retry_after)

                # Case when Server Side Error
                elif 500 <= status_code < 600:
                    logger.error(f"Server error {status_code} on {api_call}. Retrying in {retrying_wait_time} seconds (Attempt {attempt}/{retries})...")
                    time.sleep(retrying_wait_time)
                    # Capped exponential backoff
                    retrying_wait_time = min(retrying_wait_time * 2, MAX_RETRY_DELAY)
                else:
                    logger.error(f"Unexpected HTTP error: {http_err}")
                    return None
                
            except RequestException as req_err:
                # Network-related error, retrying with backoff
                logger.error(f"Network error: {req_err}. Retrying in {retrying_wait_time} seconds (Attempt {attempt}/{retries})...")
                time.sleep(retrying_wait_time)
                retrying_wait_time = min(retrying_wait_time * 2, MAX_RETRY_DELAY)  

        logger.error(f"Max retries reached. Failed to execute request after {retries} attempts: {api_call}")
        return None

    # Connects to API endpoint which fetches all the threads in a specific board.
    def get_threads(self, board):
        api_call = f"{self.API_BASE}/{board}/threads.json"
        headers = {}
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]

        return self.execute_request(api_call, headers)

    # Connects to API endpoint which fetches all the attributes of a specific thread on a specific board.
    def get_thread(self, board, thread_number):
        api_call = f"{self.API_BASE}/{board}/thread/{thread_number}.json"
        headers = {}
        if api_call in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[api_call]

        return self.execute_request(api_call, headers)

    # Connects to API endpoint which fetches the catalog (list of live threads) of a specific board.
    def get_catalog(self, board):
        api_call = f"{self.API_BASE}/{board}/catalog.json"
        headers = {}
        if board in self.last_modified_times:
            headers['If-Modified-Since'] = self.last_modified_times[board]

        return self.execute_request(api_call, headers)