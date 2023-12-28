# Databricks notebook source
pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
import os

os.environ["PAT_TOKEN"] = pat_token

# COMMAND ----------

# MAGIC %sh
# MAGIC export ALLOWED_DOMAINS=databricks.com
# MAGIC export START_URLS=https://databricks.com/blog,https://www.databricks.com/blog/Integrating-NVIDIA-TensorRT-LLM,https://databricks.com/blog/page/75,https://databricks.com/blog/page/111,https://databricks.com/blog/page/171
# MAGIC export INVALID_SUBSTRINGS=""
# MAGIC export VALID_SUBSTRINGS="databricks.com/blog"
# MAGIC
# MAGIC
# MAGIC python3 diy_crawler.py
