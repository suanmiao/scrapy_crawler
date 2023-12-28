export ALLOWED_DOMAINS=databricks.com
export START_URLS=https://databricks.com/blog,https://www.databricks.com/blog/Integrating-NVIDIA-TensorRT-LLM
export INVALID_SUBSTRINGS=""
export VALID_SUBSTRINGS="databricks.com/blog"

# load environment variables from .env
set -a
[ -f .env ] && . .env
# print env variable PAT_TOKEN
echo $PAT_TOKEN

cd custom_spider
scrapy crawl custom_spider -o output.csv --loglevel=INFO --nolog