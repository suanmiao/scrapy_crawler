
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import csv
import requests
from urllib.parse import urlparse
from queue import Queue
import threading
from scrapy import signals
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from concurrent.futures import ThreadPoolExecutor
import threading

def check_status_code(url):
    # given the url, do a HEAD request and return the status code
    try:
        response = requests.head(url, allow_redirects=True)
        return response.status_code
    except Exception as e:
        print(f"Error while checking status code: {e}")
        return 200 


def crawl_url(driver, url):
    thread_id = threading.get_ident()  # Get the current thread's ID
    thread_id = str(thread_id)[-4:]

    # print(f"[Thread {thread_id}] Start crawling url {url}")
    driver.get(url)
    import time
    # get page_source and check size
    initial_page_size = len(driver.page_source)

    time.sleep(0.3)

    # check the page_source size again
    current_page_size = len(driver.page_source)

    # if the page_source size is the same, return the page_source
    if initial_page_size == current_page_size:
        print(f"Url {url} is static, returning page_source")
        return driver.page_source
    else:
        # wait for another 1 second
        time.sleep(0.5)
        new_page_size = len(driver.page_source)
        # if the page_source size is the same, return the page_source
        if new_page_size == current_page_size:
            print(f"Url {url} is a bit static, returning page_source")
            return driver.page_source

        # wait for another 1 second
        time.sleep(1)

        # return the page_source
        print(f"Url {url} is dynamic, returning page_source")
        return driver.page_source


import threading

class SeleniumHelper:
    def __init__(self, max_drivers=200):
        self.max_drivers = max_drivers
        self.drivers = []
        self.active_driver_count = 0
        self.lock = threading.Lock()

    def create_driver(self):
        # Existing code to create a WebDriver instance
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Set a user agent similar to a Chrome browser
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        chrome_options.add_argument(f"user-agent={user_agent}")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.implicitly_wait(10)
        return driver

    def get_driver(self):
        with self.lock:
            if len(self.drivers) > 0:
                print(f"Reusing existing driver, current num: {self.active_driver_count + len(self.drivers)}")
                return self.drivers.pop(0)
            elif self.active_driver_count < self.max_drivers:
                self.active_driver_count += 1
                print(f"Creating a new driver, current num: {self.active_driver_count + len(self.drivers)}")
                return self.create_driver()
            else:
                return None

    def release_driver(self, driver):
        with self.lock:
            self.drivers.append(driver)


    def process_request(self, url):
        status_code = check_status_code(url)
        # if the status code not start with 2 or 3
        if not str(status_code).startswith(("2", "3")):
            return status_code, ""
        driver = self.get_driver()
        if not driver:
            raise Exception("No available drivers or maximum limit reached")

        try:
            return status_code, crawl_url(driver, url)
        except Exception as e:
            print(f"Error while crawling url: {e}")
        finally:
            self.release_driver(driver)

    def spider_closed(self):
        with self.lock:
            for driver in self.drivers:
                driver.quit()
            self.drivers = []

            # Reset the count of active drivers
            self.active_driver_count = 0


