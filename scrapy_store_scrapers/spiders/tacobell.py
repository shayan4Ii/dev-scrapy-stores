import json
from typing import Generator, Any, Union

import scrapy
from scrapy.http import Response
from scrapy_store_scrapers.items import ZipcodeLongLatItem
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose

class TacobellSpider(scrapy.Spider):
    name = "tacobell"
    allowed_domains = ["www.tacobell.com"]

    custom_settings = {
        'FEEDS': {
            'tacobell_zipcode_data.json': {
                'format': 'json',
                'item_classes': [ZipcodeLongLatItem]
            },
            'tacobell_data.json': {
                'format': 'json',
                'item_classes': [dict]
            }
        },
        'ITEM_PIPELINES': {
            'scrapy_store_scrapers.pipelines.TacobellDuplicatesPipeline': 300,
        }
    }

    LOCATION_API_URL_FORMAT = "https://api.tacobell.com/location/v1/{}"
    STORES_API_URL = "https://www.tacobell.com/tacobellwebservices/v4/tacobell/stores?latitude={}&longitude={}"

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """
        Load zipcodes from the JSON file and generate requests for each zipcode.

        Returns:
            Generator[scrapy.Request, None, None]: A generator yielding scrapy.Request objects.
        """
        with open('zipcodes.json', 'r') as f:
            zipcodes_data = json.load(f)

        for city_data in zipcodes_data:
            for zipcode in city_data['zip_codes']:
                url = self.LOCATION_API_URL_FORMAT.format(zipcode)
                yield scrapy.Request(url, self.parse_zipcode_info, cb_kwargs={'zipcode': zipcode})

    def parse_zipcode_info(self, response: Response, zipcode: str) -> Generator[Union[scrapy.Request, ZipcodeLongLatItem], None, None]:
        """
        Parse the zipcode location JSON and yield the location data. Also generate a request for the stores data.

        Args:
            response (Response): The response object containing the zipcode location JSON.
            zipcode (str): The zipcode being processed.

        Yields:
            Union[scrapy.Request, ZipcodeLongLatItem]: Either a new request for store data or a ZipcodeLongLatItem.
        """
        try:
            zipcode_data = response.json()["geometry"]
        except Exception as e:
            self.logger.error(f"Error parsing zipcode location JSON from {response.url}: {e}")
            return

        yield scrapy.Request(self.STORES_API_URL.format(zipcode_data['lat'], zipcode_data['lng']), self.parse_stores)

        loader = ItemLoader(item=ZipcodeLongLatItem(), response=response)
        loader.default_output_processor = TakeFirst()

        loader.add_value('zipcode', zipcode)
        loader.add_value('latitude', zipcode_data['lat'])
        loader.add_value('longitude', zipcode_data['lng'])

        yield loader.load_item()

    def parse_stores(self, response: Response) -> Generator[dict[str, Any], None, None]:
        """
        Parse the stores JSON and yield the store data.

        Args:
            response (Response): The response object containing the stores JSON.

        Yields:
            Dict[str, Any]: Store data dictionaries.
        """
        try:
            stores_data = response.json()['nearByStores']
        except Exception as e:
            self.logger.error(f"Error parsing stores JSON from {response.url}: {e}")
            return

        yield from stores_data