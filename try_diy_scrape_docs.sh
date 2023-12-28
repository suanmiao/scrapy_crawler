export ALLOWED_DOMAINS=docs.databricks.com,databricks.com
export START_URLS=https://docs.databricks.com/en/index.html,https://docs.databricks.com/en/generative-ai/generative-ai.html
export INVALID_SUBSTRINGS=""
export VALID_SUBSTRINGS="docs.databricks.com"

# load environment variables from .env
set -a
[ -f .env ] && . .env
# print env variable PAT_TOKEN
echo $PAT_TOKEN

# remove the file scraped_data.csv if exist
rm -f crawled_data/scraped_docs.csv

python3 diy_crawler.py --output_file_name crawled_data/scraped_docs.csv