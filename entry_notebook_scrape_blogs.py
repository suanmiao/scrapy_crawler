# Databricks notebook source
!pip install scrapy

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
import os

os.environ["PAT_TOKEN"] = pat_token

# COMMAND ----------

import subprocess
import os
import threading

# Set environment variables
os.environ['ALLOWED_DOMAINS'] = 'databricks.com'
os.environ['START_URLS'] = 'https://databricks.com/blog,https://www.databricks.com/blog/Integrating-NVIDIA-TensorRT-LLM'
os.environ['INVALID_SUBSTRINGS'] = ''
os.environ['VALID_SUBSTRINGS'] = 'databricks.com/blog'

# Define the command to run
command = """
cd custom_spider
scrapy crawl custom_spider -o /databricks/driver/output.csv --loglevel=INFO --nolog
"""

# Function to continuously output subprocess's stdout
def stream_output(process):
    for line in iter(process.stdout.readline, b''):
        print(line, end='')

# Start the subprocess
process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

# Start a thread to stream the output
output_thread = threading.Thread(target=stream_output, args=(process,))
output_thread.start()

# Wait for the command to complete
process.wait()

# Check if there were any errors
stderr_output = process.stderr.read()
if stderr_output:
    print("Error in executing command:")
    print(stderr_output)


# COMMAND ----------

# %sh
# export ALLOWED_DOMAINS=databricks.com
# export START_URLS=https://databricks.com/blog,https://www.databricks.com/blog/Integrating-NVIDIA-TensorRT-LLM
# export INVALID_SUBSTRINGS=""
# export VALID_SUBSTRINGS="databricks.com/blog"

# cd custom_spider
# scrapy crawl custom_spider -o /databricks/driver/output.csv --loglevel=INFO --nolog
