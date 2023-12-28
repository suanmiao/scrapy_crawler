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

from flask import Flask, request, jsonify
from selenium_helper import SeleniumHelper

selenium_helper = SeleniumHelper(max_drivers=500)


app = Flask("selenium_server")

@app.route('/scrape', methods=['POST'])
def scrape_url():
    data = request.json
    url = data.get('url')

    if not url:
        return jsonify({'error': 'No URL provided'}), 400

    try:
        status_code, content = selenium_helper.process_request(url)
        return jsonify({
            'content': content,
            'status_code': status_code
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8847)
