export ALLOWED_DOMAINS=databricks.com
export START_URLS=https://databricks.com/blog,https://www.databricks.com/blog/Integrating-NVIDIA-TensorRT-LLM,https://databricks.com/blog/page/1,https://databricks.com/blog/page/40,https://databricks.com/blog/page/75,https://databricks.com/blog/page/111,https://databricks.com/blog/page/171,https://databricks.com/blog/2013/10/27/databricks-and-the-apache-spark-platform.html,https://databricks.com/blog/2014/01/01/simr.html,https://databricks.com/blog/author/wayne-chan/page/3
export INVALID_SUBSTRINGS="/page/"
export VALID_SUBSTRINGS="databricks.com/blog"

# load environment variables from .env
set -a
[ -f .env ] && . .env
# print env variable PAT_TOKEN
echo $PAT_TOKEN

# remove the file scraped_data.csv if exist
rm -f scraped_data.csv

python3 diy_crawler.py