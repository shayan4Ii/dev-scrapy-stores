import scrapy
import json
import os
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import CloseSpider
import time


class CvsSpider(scrapy.Spider):
    name = "cvs"
    allowed_domains = ["www.cvs.com"]
    
    # Constants
    RESULTS_PER_PAGE = 5
    MAX_RETRIES = 3
    RETRY_DELAY = 10  # seconds
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 1,
    }

    def start_requests(self):
        """Generate initial requests for each zipcode."""
        # Read zipcodes from JSON file
        with open('zipcodes.json', 'r') as f:
            zipcodes_data = json.load(f)

        for city_data in zipcodes_data:
            city = city_data['city']
            state = city_data['state']
            cbsa = city_data['cbsa']
            for zipcode in city_data['zip_codes']:
                self.logger.info(f"Fetching stores in {city}, {state}, {zipcode}")
                url = f"https://www.cvs.com/api/locator/v2/stores/search?searchBy=USER-TEXT&latitude=&longitude=&searchText={zipcode}&searchRadiusInMiles=&maxItemsInResult=&filters=&resultsPerPage={self.RESULTS_PER_PAGE}&pageNum=1"
                yield scrapy.Request(
                    url,
                    self.parse,
                    headers=self.get_headers(),
                    meta={'page': 1, 'zipcode': zipcode, 'retry_count': 0},
                    errback=self.errback_httpbin,
                    dont_filter=True
                )

    def errback_httpbin(self, failure):
        """Handle request failures."""
        request = failure.request
        if request.meta['retry_count'] < self.MAX_RETRIES:
            self.logger.warning(f"Retrying {request.url}, attempt {request.meta['retry_count'] + 1}")
            time.sleep(self.RETRY_DELAY)
            return request.replace(
                meta={'retry_count': request.meta['retry_count'] + 1},
                dont_filter=True
            )
        else:
            self.logger.error(f"Failed to scrape {request.url} after {self.MAX_RETRIES} attempts")

    def parse(self, response):
        """Parse the JSON response and yield store data."""
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.url}")
            return

        # Yield each store from storeList as it is
        stores = data.get('storeList', [])
        self.logger.info(f"Found {len(stores)} stores for zipcode {response.meta['zipcode']}")
        for store in stores:
            yield self.validate_store(store)

        # Check if there are more pages
        total_results = data.get('totalResults', 0)
        current_page = response.meta['page']

        if total_results > current_page * self.RESULTS_PER_PAGE:
            self.logger.info(f"Found more than {current_page * self.RESULTS_PER_PAGE} stores. Fetching next page...")
            next_page = current_page + 1
            next_url = response.url.replace(f"pageNum={current_page}", f"pageNum={next_page}")
            yield scrapy.Request(
                next_url,
                self.parse,
                headers=self.get_headers(),
                meta={'page': next_page, 'zipcode': response.meta['zipcode'], 'retry_count': 0},
                errback=self.errback_httpbin,
                dont_filter=True
            )

    def validate_store(self, store):
        """Validate and clean store data."""
        # Add validation logic here
        return store

    @staticmethod
    def get_headers():
        """Get headers for the API request."""
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "consumer": "SLP",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"99\", \"Google Chrome\";v=\"127\", \"Chromium\";v=\"127\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-api-key": os.getenv('CVS_API_KEY', 'k6DnPo1puMOQmAhSCiRGYvzMYOSFu903'),
            "Referer": "https://www.cvs.com/store-locator/landing",
            "Referrer-Policy": "origin-when-cross-origin"
        }

    def closed(self, reason):
        """Called when spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
