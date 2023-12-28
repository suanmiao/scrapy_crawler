import scrapy
import os
import logging
from urllib.parse import urljoin, urlparse


# set the log level
logging.basicConfig(level=logging.INFO)


# read the environment variable ALLOWED_DOMAINS and START_URLS
ALLOWED_DOMAINS = os.getenv("ALLOWED_DOMAINS")
START_URLS = os.getenv("START_URLS")
INVALID_SUBSTRINGS = os.getenv("INVALID_SUBSTRINGS")
VALID_SUBSTRINGS = os.getenv("VALID_SUBSTRINGS")

logging.info("ALLOWED_DOMAINS: %s", ALLOWED_DOMAINS)
logging.info("START_URLS: %s", START_URLS)
logging.info("INVALID_SUBSTRINGS: %s", INVALID_SUBSTRINGS)
logging.info("VALID_SUBSTRINGS: %s", VALID_SUBSTRINGS)

# split by comma and remove whitespace
ALLOWED_DOMAINS = [domain.strip() for domain in ALLOWED_DOMAINS.split(",")]
START_URLS = [url.strip() for url in START_URLS.split(",")]
# if each start url doesn't have a scheme, add https://
START_URLS = [url if "://" in url else "https://" + url for url in START_URLS]

if INVALID_SUBSTRINGS != "":
    INVALID_SUBSTRINGS = INVALID_SUBSTRINGS.split(",")
else:
    INVALID_SUBSTRINGS = []

VALID_SUBSTRINGS = VALID_SUBSTRINGS.split(",")

logging.info("Parsed ALLOWED_DOMAINS: %s", ALLOWED_DOMAINS)
logging.info("Parsed START_URLS: %s", START_URLS)
logging.info("Parsed INVALID_SUBSTRINGS: %s", INVALID_SUBSTRINGS)
logging.info("Parsed VALID_SUBSTRINGS: %s", VALID_SUBSTRINGS)


class CustomSpider(scrapy.Spider):
    name = "custom_spider"
    allowed_domains = ALLOWED_DOMAINS
    start_urls = START_URLS 
    valid_substrings = VALID_SUBSTRINGS
    invalid_substrings = INVALID_SUBSTRINGS
    valid_websites_counter = 0

    def is_valid_url(self, url):
        """
        Checks if the url matches the desired pattern
        """
        has_valid_substring = False
        for valid_substring in self.valid_substrings:
            if valid_substring in url:
                has_valid_substring = True
                break

        if not has_valid_substring:
            # print(f"Url {url} doesn't have valid string {self.valid_substrings}")
            return False
        
        for invalid_substring in self.invalid_substrings:
            if invalid_substring in url:
                # print(f"Url {url} has invalid string {invalid_substring}")
                return False
        
        return has_valid_substring

    def parse(self, response):
        # Check the Content-Type of the response
        content_type = response.headers.get('Content-Type', b'').decode('utf-8').lower()
        
        # List of non-textual content types to exclude
        non_textual_types = [
            'image/jpeg', 'image/png', 'image/gif',
            'audio/mpeg', 'audio/x-wav', 'audio/wav',
            'video/mp4', 'video/ogg'
            # Add other non-textual MIME types as needed
        ]

        if any(nt_type in content_type for nt_type in non_textual_types):
            self.logger.info(f"Skipping non-textual content type: {content_type}")
            return
        
        # Check if URL is valid
        if not self.is_valid_url(response.url):
            # print(f"Found {response.url} not having substring {sub_string}")
            return
        
        # e.g. checking if the domain in the 'docs.databricks.com' is in the URL
        url = response.url
        yield {
            'url': url,
            'content': response.text,
        }

        # Increment the counter for each valid website
        self.valid_websites_counter += 1
        if self.valid_websites_counter % 100 == 0:
            self.logger.info(f"Number of valid websites scraped so far: {self.valid_websites_counter}")

        # Get the next URLs to crawl (you should replace this with your own logic)
        next_urls = response.css('a::attr(href)').getall()
        iframe_urls = response.css('iframe::attr(src)').getall()

        all_urls = next_urls + iframe_urls

        for next_url in all_urls:
            # Create an absolute URL and check its validity
            absolute_url = urljoin(response.url, next_url.strip())
            if bool(urlparse(absolute_url).netloc) and self.is_valid_url(absolute_url):
                # print(f"Trying absolute url {absolute_url}")

                yield response.follow(absolute_url, self.parse)

    def closed(self, reason):
        self.logger.info(f"Total valid websites scraped: {self.valid_websites_counter}")
        if self.valid_websites_counter == 1:
            raise ValueError("You only scrapped one website, invalid!")
