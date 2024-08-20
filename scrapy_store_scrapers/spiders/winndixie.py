import json
from typing import Any, Generator

import scrapy
from scrapy.http import Request, Response


class WinnDixieSpider(scrapy.Spider):
    """Spider for scraping WinnDixie store locations."""

    name = "winndixie"
    allowed_domains = ["www.winndixie.com"]

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests for the spider."""
        url = "https://www.winndixie.com/V2/storelocator/getStores"

        try:
            zipcodes = self.load_zipcodes("zipcodes.json")
        except (FileNotFoundError, ValueError) as e:
            self.logger.error(f"Error loading zipcodes: {str(e)}")
            return
        
        for zipcode in zipcodes:
            data = {
                "search": zipcode,
                "strDefaultMiles": "25",
                "filter": ""
            }

            yield Request(
                method='POST',
                headers=self.get_headers(),
                url=url,
                body=json.dumps(data),
                callback=self.parse
            )

    def parse(self, response: Response) -> Generator[dict[str, Any], None, None]:
        """Parse the response and yield filtered store data."""
        try:
            stores = response.json()
            # filtered_stores = self.filter_stores(stores)
            for store in stores:
                yield store
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from response: {response.url}")

    @staticmethod
    def load_zipcodes(zipcode_file: str) -> list[str]:
        """Load zipcodes from the JSON file."""
        with open(zipcode_file, 'r') as f:
            locations = json.load(f)
        
        zipcodes = set()
        for location in locations:
            zipcodes.update(location.get('zip_codes', []))
        return list(zipcodes)

    @staticmethod
    def filter_stores(stores: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter stores based on criteria of website to show on frontend."""
        return [
            store for store in stores
            if (
                'liquor' not in store['Location']['LocationTypeDescription'].lower()
                or (store['StoreCode'] == '1489' and not store['ParentStore'])
            )
        ]

    @staticmethod
    def get_headers() -> dict[str, str]:
        """Return headers for the HTTP request."""
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://www.winndixie.com",
            "referer": "https://www.winndixie.com/locator",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        }