# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter

from scrapy import signals
from scrapy.http import HtmlResponse
from concurrent.futures import ThreadPoolExecutor
import threading
import logging

logging.basicConfig(level=logging.INFO)


def crawl_url(driver, url):
    logging.info(f"Start crawling url {url}")
    driver.get(url)
    import time

    time.sleep(3)

    body = driver.page_source
    logging.info(f"Finished crawling url {url}")
    return body


import aiohttp
import asyncio
import json
from scrapy.http import HtmlResponse
import os
# get the pat token from environment variable 
# if the current platform is Mac 
if os.uname().sysname == "Darwin":
    pat_token = os.environ.get("PAT_TOKEN")
else:
    # pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
    pat_token = os.environ.get("PAT_TOKEN")


class RemoteSeleniumMiddleware:
    async def process_request(self, request, spider):
        url = "https://db-ml-models-dev-us-west.cloud.databricks.com/driver-proxy-api/o/3217006663075879/0821-201245-z38c0xa6/8847/scrape"

        headers = {
            "Authentication": f"Bearer {pat_token}",
            "Content-Type": "application/json",
        }

        data = {
            "url": request.url, 
        }

        # Use aiohttp ClientSession for asynchronous POST request
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=json.dumps(data)) as response:
                body = await response.json()
                body_content = body["content"]

        # Return the response to Scrapy
        return HtmlResponse(request.url, body=body_content, encoding='utf-8', request=request)

    def _process_request(self, request, spider):
        return asyncio.get_event_loop().run_until_complete(self.process_request(request, spider))


# from selenium import webdriver
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options
# from webdriver_manager.chrome import ChromeDriverManager


# class SeleniumMiddleware:
#     def __init__(self, max_drivers=200):
#         self.max_drivers = max_drivers
#         self.drivers = []
#         self.active_driver_count = 0
#         self.lock = threading.Lock()

#     def create_driver(self):
#         # Existing code to create a WebDriver instance
#         chrome_options = Options()
#         chrome_options.add_argument("--headless")
#         chrome_options.add_argument("--no-sandbox")
#         chrome_options.add_argument("--disable-dev-shm-usage")
        
#         # Set a user agent similar to a Chrome browser
#         user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
#         chrome_options.add_argument(f"user-agent={user_agent}")

#         driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
#         driver.implicitly_wait(20)
#         return driver

#     def get_driver(self):
#         with self.lock:
#             if len(self.drivers) > 0:
#                 logging.info(f"Reusing existing driver, current num: {len(self.drivers)}")
#                 return self.drivers.pop(0)
#             elif self.active_driver_count < self.max_drivers:
#                 self.active_driver_count += 1
#                 logging.info(f"Creating a new driver, current num: {len(self.drivers)}")
#                 return self.create_driver()
#             else:
#                 return None

#     def release_driver(self, driver):
#         with self.lock:
#             self.drivers.append(driver)


#     def process_request(self, request, spider):
#         driver = self.get_driver()
#         if not driver:
#             raise Exception("No available drivers or maximum limit reached")

#         try:
#             import requests

#             body = requests.get(url=request.url).text

#             # body = crawl_url(driver, request.url)
#             return HtmlResponse(request.url, body=body, encoding='utf-8', request=request)
#         finally:
#             self.release_driver(driver)

#     def spider_closed(self):
#         with self.lock:
#             for driver in self.drivers:
#                 driver.quit()
#             self.drivers = []

#             # Reset the count of active drivers
#             self.active_driver_count = 0



class CustomSpiderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class CustomSpiderDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
