import json
from typing import Generator

import scrapy


class InvalidJsonResponseException(Exception):
    pass

class MeijerSpider(scrapy.Spider):
    name = "meijer"
    allowed_domains = ["www.meijer.com"]

    API_FORMAT_URL = "https://www.meijer.com/bin/meijer/store/search?locationQuery={}&radius=20"

    def start_requests(self, response) -> Generator[scrapy.Request, None, None]:
        """Read the JSON file containing zipcodes data and generate requests."""
        # Read the JSON file containing zipcodes
        try:
            with open(r'zipcodes.json', 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            self.logger.error("File not found: data/tacobell_zipcode_data.json")
            return
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON file: data/tacobell_zipcode_data.json")
            return
        
        # Iterate through the locations and yield requests
        for location in locations:
            if 'zipcodes' not in location:
                self.logger.warning("No zipcode for location: {}".format(location))
                continue

            for zipcode in location['zipcodes']:
                url = self.API_FORMAT_URL.format(zipcode)
                
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=self.get_default_headers()
                )

    def parse(self, response: scrapy.http.Response) -> Generator[dict, None, None]:
        """Parse the response and yield store data."""
        try:
            response_json = response.json()
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON response: %s (%s)", response.text, response.url)
            
            raise InvalidJsonResponseException(f"Invalid JSON response from {response.url}")

        pagination = response_json.get('pagination', {})
        if pagination['totalResults'] > pagination['pageSize']:
            self.logger.warning("Results are paginated. Total results: %s, page size: %s", pagination['totalResults'], pagination['pageSize'])

        for store in response_json['pointsOfService']:
            yield store