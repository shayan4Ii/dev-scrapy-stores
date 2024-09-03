import re
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
        
        for store in stores_data:
            store_info = {}

            store_info['number'] = store.get('storeNumber')
            store_info['phone_number'] = store.get('phoneNumber')
            store_info['address'] = self._get_address(store.get('address', {}))
            store_info['hours'] = self._get_hours(store.get("openingHours", {}).get("weekDayOpeningList", {}))
            store_info['location'] = self._get_location(store.get('geoPoint', {}))
            

            yield store_info

    @staticmethod
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(r'(\d+)([ap]m)', r'\1 \2', time_str)

    def _get_hours(self, opening_hours: dict) -> dict:
        hours_info = {}
        for hours_info in opening_hours:
            day = hours_info.get('weekDay')

            open = self.format_time(hours_info.get('openingTime',{}).get('formattedTime'))
            close = self.format_time(hours_info.get('closingTime',{}).get('formattedTime'))

            hours_info[day] = {
                'open': open,
                'close': close
            }

        return hours_info

    def _get_address(self, address_info: dict) -> str:
        """Get the formatted store address."""
        try:
            address_parts = [
                address_info.get("line1", ""),
                address_info.get("line2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("town", "")
            state = address_info.get("region", {}).get("isocode", "").replace("US-", "")
            zipcode = address_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, loc_info: dict) -> dict:
        """Get the store location in GeoJSON Point format."""
        try:
            latitude = loc_info.get('latitude')
            longitude = loc_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}