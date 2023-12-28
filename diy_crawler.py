import os
import logging
import requests
import csv
import threading
from queue import Queue
from urllib.parse import urlparse
import argparse

# set logger format to include day and hour and minute and second
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d %I:%M:%S %p') 

# Read environment variables
ALLOWED_DOMAINS = os.getenv("ALLOWED_DOMAINS").split(",")
START_URLS = os.getenv("START_URLS").split(",")
INVALID_SUBSTRINGS = os.getenv("INVALID_SUBSTRINGS").split(",") if os.getenv("INVALID_SUBSTRINGS") else []
VALID_SUBSTRINGS = os.getenv("VALID_SUBSTRINGS").split(",")

# Global variables
visited_urls = set()
task_queue = Queue()
result_queue = Queue()
csv_lock = threading.Lock()

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from urllib.parse import urlparse, urlunparse
import re


def is_valid_url(url):
    parsed_url = urlparse(url)
    if parsed_url.netloc in ALLOWED_DOMAINS:
        if any(substring in url for substring in VALID_SUBSTRINGS):
            if not any(substring in url for substring in INVALID_SUBSTRINGS):
                return True
    return False

def get_base_url(url):
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return base_url


def get_full_url(url):
    # remove the fragments from the url
    parsed_url = urlparse(url)
    # Reconstruct the URL without the fragment
    return urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, parsed_url.query, ''))

def extract_urls(html_content, base_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    urls = set()

    # Find all links in the HTML content
    for link in soup.find_all('a', href=True):
        href = link['href']

        # Construct full URL from relative path
        full_url = urljoin(base_url, href)
        # Remove the fragment from the URL
        full_url = get_full_url(full_url)

        # Parse the URL to check its components
        parsed_url = urlparse(full_url)
        if parsed_url.scheme not in ['http', 'https']:
            # logging.info(f"Skip url that's not HTTP or HTTPS {full_url}")
            continue  # Skip if it's not a HTTP or HTTPS URL

        # Check if the URL is in the allowed domain and contains valid substrings
        if is_valid_url(full_url) and full_url not in visited_urls:
            urls.add(full_url)

    return list(urls)

class Scheduler:
    def __init__(self):
        # this number has to be initialized to non zero, otherwise the first executor will think that all executors are finished
        self.num_visiting_executors = 10 
        self.currently_visiting_lock = threading.Lock()
        for url in START_URLS:
            print(f"Adding url {url} to queue")
            task_queue.put(url)

    def is_valid_url(self, url):
        return is_valid_url(url)

    def add_to_queue(self, url):
        if url not in visited_urls and self.is_valid_url(url):
            visited_urls.add(url)
            task_queue.put(url)

    def check_and_set_shutdown(self):
        with self.currently_visiting_lock:
            self.num_visiting_executors = len([ex for ex in executors if ex.is_visiting])

def scrape_url_requests(url):
    thread_id = threading.get_ident()  # Get the current thread's ID
    thread_id = str(thread_id)[-4:]

    # logging.info(f"[Thread {thread_id}]Scraping url {url}")
    import requests
    content = requests.get(url).content

    logging.info(f"[Thread {thread_id}]Finished scraping url {url}")
    return content

def scrape_url(url):
    return scrape_url_selenium(url)

def scrape_url_selenium(url):
    thread_id = threading.get_ident()  # Get the current thread's ID
    thread_id = str(thread_id)[-4:]

    # logging.info(f"[Thread {thread_id}]Scraping url {url}")
    import requests
    pat_token = os.environ.get("PAT_TOKEN")

    headers = {
        "Authentication": f"Bearer {pat_token}",
        "Content-Type": "application/json",
    }

    data = {
        "url": url 
    }

    import json
    server_url_1 = "https://db-ml-models-dev-us-west.cloud.databricks.com/driver-proxy-api/o/3217006663075879/0821-201245-z38c0xa6/8847/scrape"
    server_url_2 = "https://db-ml-models-dev-us-west.cloud.databricks.com/driver-proxy-api/o/3217006663075879/1225-234404-qeht3jzp/8847/scrape"
    # randomly choose one server url
    import random 
    server_url = random.choice([server_url_1, server_url_2])

    response = requests.post(server_url, headers=headers, data=json.dumps(data))

    if "content" in response.json():
        content = response.json()["content"]

        result_queue.put((url, content))
        logging.info(f"[Thread {thread_id}]Finished scraping url {url}")
        return content
    else:
        logging.info(f"[Thread {thread_id}]Failed scraping url {url}, response {response.text}")
        raise Exception(f"Failed scraping url {url}") 


num_rows_added = 0

def write_to_csv(output_file_name):
    global num_rows_added
    # if is mac os
    if os.uname().sysname == "Darwin":
        file_name = output_file_name 
    else:
        file_name = f"/databricks/driver/{output_file_name}"
    with csv_lock:
        with open(file_name, mode='a', newline='') as file:
            writer = csv.writer(file)
            while not result_queue.empty():
                num_rows_added += 1
                url, content = result_queue.get()
                writer.writerow([url, content])

class Executor(threading.Thread):
    def __init__(self, scheduler, output_file_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scheduler = scheduler
        self.is_visiting = False
        self.output_file_name = output_file_name

    def run(self):
        global num_rows_added
        # add a random wait between 0 to 30 seconds
        import random
        import time
        time.sleep(random.random() * 30)

        thread_id = threading.get_ident()  # Get the current thread's ID
        thread_id = str(thread_id)[-4:]
        logging.info(f"[Thread {thread_id}] Executor started")

        while (task_queue.qsize() + self.scheduler.num_visiting_executors) > 0:
            if not task_queue.empty():
                url = task_queue.get()
                if self.scheduler.is_valid_url(url):
                    self.is_visiting = True
                    content = scrape_url(url)
                    base_url = get_base_url(url)
                    extracted_urls = extract_urls(content, base_url)
                    for url in extracted_urls:
                        if url not in visited_urls:
                            self.scheduler.add_to_queue(url)
                    logging.info(f"Finished adding urls to queue, current queue size {task_queue.qsize()}, visited urls: {len(visited_urls)}, visiting size {self.scheduler.num_visiting_executors}, num rows added {num_rows_added}")
                    self.is_visiting = False
                    self.scheduler.check_and_set_shutdown()
                    write_to_csv(self.output_file_name)
            else:
                # if task queue is empty, wait for 1 second
                time.sleep(1)
                # print(f"Waiting for 1 second, current queue size {task_queue.qsize()}, visited urls: {len(visited_urls)}, visiting size {self.scheduler.num_visiting_executors}, num rows added {num_rows_added}")
        logging.info(f"[Thread {thread_id}] Executor finished")

def main():
    parser = argparse.ArgumentParser(description='DIY Crawler')
    parser.add_argument('--num_workers', type=int, default=150, help='Number of workers')
    parser.add_argument('--output_file_name', type=str, default='output.csv', help='Output file name')
    args = parser.parse_args()
    # assert that the output file name is csv
    assert args.output_file_name.endswith(".csv")
    print(f"Number of workers: {args.num_workers}, output file name: {args.output_file_name}")

    scheduler = Scheduler()
    global executors
    executors = [Executor(scheduler, args.output_file_name) for _ in range(args.num_workers)]

    for ex in executors:
        ex.start()

    for ex in executors:
        ex.join()


if __name__ == "__main__":
    main()

