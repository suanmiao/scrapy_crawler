# Databricks notebook source
# MAGIC %sh
# MAGIC
# MAGIC sudo apt update
# MAGIC
# MAGIC # Download Google Chrome to the specified directory
# MAGIC wget -P /databricks/driver/ https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# MAGIC
# MAGIC # Install Google Chrome
# MAGIC sudo dpkg -i /databricks/driver/google-chrome-stable_current_amd64.deb
# MAGIC sudo apt-get install -f -y
# MAGIC google-chrome --version
# MAGIC
# MAGIC # Download ChromeDriver to the specified directory
# MAGIC wget -P /databricks/driver/ https://chromedriver.storage.googleapis.com/92.0.4515.107/chromedriver_linux64.zip
# MAGIC
# MAGIC # Unzip ChromeDriver in the target directory and move it to /usr/bin
# MAGIC unzip /databricks/driver/chromedriver_linux64.zip -d /databricks/driver/
# MAGIC sudo mv /databricks/driver/chromedriver /usr/bin/chromedriver
# MAGIC sudo chown root:root /usr/bin/chromedriver
# MAGIC sudo chmod +x /usr/bin/chromedriver
# MAGIC

# COMMAND ----------

# %sh

# sudo apt update
# wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# sudo dpkg -i google-chrome-stable_current_amd64.deb
# sudo apt-get install -f -y
# google-chrome --version


# wget https://chromedriver.storage.googleapis.com/92.0.4515.107/chromedriver_linux64.zip

# unzip chromedriver_linux64.zip
# sudo mv chromedriver /usr/bin/chromedriver
# sudo chown root:root /usr/bin/chromedriver
# sudo chmod +x /usr/bin/chromedriver


# COMMAND ----------

# Please make sure your cluster has scrapy installed, otherwise use the command below
!pip install scrapy
!pip install selenium
!pip install webdriver-manager

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import csv
import requests
from urllib.parse import urlparse
from queue import Queue
import threading


# COMMAND ----------

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

        driver = self.get_driver()
        if not driver:
            raise Exception("No available drivers or maximum limit reached")

        try:
            # import requests
            # return requests.get(url=url)

            return crawl_url(driver, url)
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



# COMMAND ----------

from flask import Flask, request, jsonify

selenium_helper = SeleniumHelper(max_drivers=500)


app = Flask("selenium_server")

@app.route('/scrape', methods=['POST'])
def scrape_url():
    data = request.json
    url = data.get('url')

    if not url:
        return jsonify({'error': 'No URL provided'}), 400

    try:
        content = selenium_helper.process_request(url)
        return jsonify({'content': content})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8847)

