import scrapy
import json
from typing import Dict, Any, Iterator, Set, Union


class CvsSpider(scrapy.Spider):
    name: str = "cvs"
    allowed_domains: list[str] = ["www.cvs.com"]
    
    # Constants
    RESULTS_PER_PAGE: int = 25
    API_KEY: str = 'k6DnPo1puMOQmAhSCiRGYvzMYOSFu903'
    
    custom_settings: Dict[str, Any] = {
        'CONCURRENT_REQUESTS': 32,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_store_ids: Set[str] = set()

    def start_requests(self) -> Iterator[scrapy.Request]:
        """Generate initial requests for each zipcode."""
        # Read zipcodes from JSON file

        zipcodes_data = self._load_zipcode_data()

        for zipcode_data in zipcodes_data:
            zipcode = zipcode_data['zipcode']
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
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.url}")
            return

        # Process each store from storeList
        stores = data.get('storeList', [])
        self.logger.info(f"Found {len(stores)} stores for zipcode {response.meta['zipcode']}")
        for store in stores:
            store_id = store.get('storeInfo', {}).get('storeId')
            if store_id and store_id not in self.processed_store_ids:
                self.processed_store_ids.add(store_id)
                yield store

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
    
    def _load_zipcode_data(self) -> list[dict[str, Union[str, float]]]:
        """Load zipcode data from a JSON file."""
        try:
            with open("data/tacobell_zipcode_data.json") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found")
            return []
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file")
            return []
