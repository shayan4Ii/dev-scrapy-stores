import json
from typing import Generator, Dict, Any
import scrapy
from scrapy.exceptions import CloseSpider

class InvalidJsonResponseException(Exception):
    """Custom exception for invalid JSON responses."""
    pass

class MeijerSpider(scrapy.Spider):
    name = "meijer"
    allowed_domains = ["www.meijer.com"]

    custom_settings = {
        'DOWNLOADER_CLIENT_TLS_METHOD': 'TLSv1.2',
    }

    API_FORMAT_URL = "https://www.meijer.com/bin/meijer/store/search?locationQuery={}&radius=20"

    def __init__(self, *args, **kwargs):
        super(MeijerSpider, self).__init__(*args, **kwargs)
        self.zipcode_file = kwargs.get('zipcode_file', 'zipcodes.json')

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Read the JSON file containing zipcodes data and generate requests."""
        try:
            with open(self.zipcode_file, 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            raise CloseSpider(f"File not found: {self.zipcode_file}")
        except json.JSONDecodeError:
            raise CloseSpider(f"Invalid JSON file: {self.zipcode_file}")
        
        for location in locations:
            zipcodes = location.get('zip_codes', [])
            if not zipcodes:
                self.logger.warning(f"No zipcodes for location: {location}")
                continue

            for zipcode in zipcodes:
                url = self.API_FORMAT_URL.format(zipcode)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=self.get_default_headers(),
                    meta={'zipcode': zipcode}
                )
                break

            break

    def parse(self, response: scrapy.http.Response) -> Generator[Dict[str, Any], None, None]:
        """Parse the response and yield store data."""
        try:
            response_json = response.json()
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON response: {response.text[:100]}... ({response.url})")
            raise InvalidJsonResponseException(f"Invalid JSON response from {response.url}")

        pagination = response_json.get('pagination', {})
        total_results = pagination.get('totalResults', 0)
        page_size = pagination.get('pageSize', 0)

        if total_results > page_size:
            self.logger.warning(f"Results are paginated. Total results: {total_results}, page size: {page_size}")
            # TODO: Implement pagination handling if needed

        for store in response_json.get('pointsOfService', []):
            store['source_zipcode'] = response.meta.get('zipcode')
            yield store

    def get_default_headers(self) -> Dict[str, str]:
        """Return default headers for requests."""
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "max-age=0",
            "dnt": "1",
            "priority": "u=0, i",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
        }