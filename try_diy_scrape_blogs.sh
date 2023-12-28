export ALLOWED_DOMAINS=databricks.com
export START_URLS=https://databricks.com/blog,https://www.databricks.com/blog/Integrating-NVIDIA-TensorRT-LLM,https://databricks.com/blog/page/75,https://databricks.com/blog/page/111,https://databricks.com/blog/page/171
export INVALID_SUBSTRINGS=""
export VALID_SUBSTRINGS="databricks.com/blog"

# load environment variables from .env
set -a
[ -f .env ] && . .env
# print env variable PAT_TOKEN
echo $PAT_TOKEN

# remove the file scraped_data.csv if exist
rm -f scraped_data.csv

python3 diy_crawler.py