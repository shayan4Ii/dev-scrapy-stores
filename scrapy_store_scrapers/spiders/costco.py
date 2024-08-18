import json
from typing import Generator

import scrapy
from scrapy.http import Response

class CostcoSpider(scrapy.Spider):
    name = "costco"
    allowed_domains = ["www.costco.com"]
    
    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_store_scrapers.pipelines.CostcoDuplicatesPipeline': 300,
        }
    }

    API_FORMAT_URL = "https://www.costco.com/AjaxWarehouseBrowseLookupView?langId=-1&numOfWarehouses=50&hasGas=false&hasTires=false&hasFood=false&hasHearing=false&hasPharmacy=false&hasOptical=false&hasBusiness=false&hasPhotoCenter=&tiresCheckout=0&isTransferWarehouse=false&populateWarehouseDetails=true&warehousePickupCheckout=false&latitude={}&longitude={}&countryCode=US"

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Read the JSON file containing latitude and longitude data and generate requests."""
        # Read the JSON file containing latitude and longitude data
        try:
            with open(r'data\tacobell_zipcode_data.json', 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            self.logger.error("File not found: data/tacobell_zipcode_data.json")
            return
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON file: data/tacobell_zipcode_data.json")
            return
        
        # Iterate through the locations and yield requests
        for location in locations:
            latitude = location['latitude']
            longitude = location['longitude']

            if not latitude or not longitude:
                self.logger.warning("Invalid latitude or longitude: %s, %s", latitude, longitude)
                continue

            url = self.API_FORMAT_URL.format(latitude, longitude)
            
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers=self.get_default_headers()
            )

    def parse(self, response: Response) -> Generator[dict, None, None]:
        """Parse the response and yield warehouse data."""
        # Your parsing logic here

        try:
            response_json = response.json()
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON response: %s", response.text)
            return

        for warehouse in response_json:

            if not isinstance(warehouse, dict):
                self.logger.error("Invalid warehouse data: %s", warehouse)
                continue

            yield warehouse

    @staticmethod
    def get_default_headers():
        """Get the default headers for the request."""
        return {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "connection": "keep-alive",
            "dnt": "1",
            "referer": "https://www.costco.com/warehouse-locations",
            "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "upgrade-insecure-requests": "1"
        }