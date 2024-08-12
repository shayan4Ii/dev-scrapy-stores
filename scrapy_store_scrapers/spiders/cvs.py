import scrapy
import json
import os
from typing import Dict, Any, Iterator
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import CloseSpider
import time


class CvsSpider(scrapy.Spider):
    name: str = "cvs"
    allowed_domains: list[str] = ["www.cvs.com"]
    
    # Constants
    RESULTS_PER_PAGE: int = 5
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 10  # seconds
    API_KEY: str = 'k6DnPo1puMOQmAhSCiRGYvzMYOSFu903'
    
    custom_settings: Dict[str, Any] = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 1,
    }

    def start_requests(self) -> Iterator[scrapy.Request]:
        """Generate initial requests for each zipcode."""
        # Read zipcodes from JSON file
        with open('zipcodes.json', 'r') as f:
            zipcodes_data = json.load(f)

        for city_data in zipcodes_data:
            city = city_data['city']
            state = city_data['state']
            for zipcode in city_data['zip_codes']:
                self.logger.info(f"Fetching stores in {city}, {state}, {zipcode}")
                url = f"https://www.cvs.com/api/locator/v2/stores/search?searchBy=USER-TEXT&latitude=&longitude=&searchText={zipcode}&searchRadiusInMiles=&maxItemsInResult=&filters=&resultsPerPage={self.RESULTS_PER_PAGE}&pageNum=1"
                yield scrapy.Request(
                    url,
                    self.parse,
                    headers=self.get_headers(),
                    meta={'page': 1, 'zipcode': zipcode},
                    dont_filter=True
                )

    def parse(self, response: scrapy.http.Response) -> Iterator[Dict[str, Any]]:
        """Parse the JSON response and yield store data."""
        try:
            data: Dict[str, Any] = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.url}")
            return

        # Yield each store from storeList as it is
        stores: list[Dict[str, Any]] = data.get('storeList', [])
        self.logger.info(f"Found {len(stores)} stores for zipcode {response.meta['zipcode']}")
        for store in stores:
            yield store

        # Check if there are more pages
        total_results: int = data.get('totalResults', 0)
        current_page: int = response.meta['page']

        if total_results > current_page * self.RESULTS_PER_PAGE:
            self.logger.info(f"Found more than {current_page * self.RESULTS_PER_PAGE} stores. Fetching next page...")
            next_page: int = current_page + 1
            next_url: str = response.url.replace(f"pageNum={current_page}", f"pageNum={next_page}")
            yield scrapy.Request(
                next_url,
                self.parse,
                headers=self.get_headers(),
                meta={'page': next_page, 'zipcode': response.meta['zipcode']},
                dont_filter=True
            )

    def get_headers(self) -> Dict[str, str]:
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
            "x-api-key": self.API_KEY,
            "Referer": "https://www.cvs.com/store-locator/landing",
            "Referrer-Policy": "origin-when-cross-origin"
        }
