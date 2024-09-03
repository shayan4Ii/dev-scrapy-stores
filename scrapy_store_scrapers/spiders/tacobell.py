import re
import json
from typing import Generator, Any, Dict

import scrapy
from scrapy.http import Response

class TacobellZipcodeSpider(scrapy.Spider):
    name = "tacobell_zipcode_spider"
    allowed_domains = ["api.tacobell.com"]

    custom_settings = {
        'FEEDS': {
            'zipcode_data.json': {
                'format': 'json',
            }
        }
    }

    LOCATION_API_URL_FORMAT = "https://api.tacobell.com/location/v1/{}"

    def start_requests(self):
        with open('zipcodes.json', 'r') as f:
            zipcodes_data = json.load(f)

        for city_data in zipcodes_data:
            for zipcode in city_data['zip_codes']:
                url = self.LOCATION_API_URL_FORMAT.format(zipcode)
                yield scrapy.Request(url, self.parse_zipcode_info, cb_kwargs={'zipcode': zipcode})

    def parse_zipcode_info(self, response, zipcode):
        try:
            zipcode_data = response.json()["geometry"]
        except Exception as e:
            self.logger.error(f"Error parsing zipcode location JSON from {response.url}: {e}")
            return

        yield {
            'zipcode': zipcode,
            'latitude': zipcode_data['lat'],
            'longitude': zipcode_data['lng']
        }

class TacobellStoreSpider(scrapy.Spider):
    name = "tacobell_store_spider"
    allowed_domains = ["www.tacobell.com"]

    STORES_API_URL = "https://www.tacobell.com/tacobellwebservices/v4/tacobell/stores?latitude={}&longitude={}"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_store_ids = set()

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        with open(r'data\tacobell_zipcode_data.json', 'r') as f:
            zipcode_data = json.load(f)

        for entry in zipcode_data:
            url = self.STORES_API_URL.format(entry['latitude'], entry['longitude'])
            yield scrapy.Request(url, self.parse_stores)

    def parse_stores(self, response: Response) -> Generator[Dict[str, Any], None, None]:
        try:
            stores_data = response.json()['nearByStores']
        except Exception as e:
            self.logger.error(f"Error parsing stores JSON from {response.url}: {e}")
            return
        
        for store in stores_data:
            store_number = store.get('storeNumber')
            
            if store_number in self.seen_store_ids:
                self.logger.info(f"Duplicate store found: {store_number}")
                continue
            
            self.seen_store_ids.add(store_number)

            store_info = {}
            store_info['number'] = store_number
            store_info['phone_number'] = store.get('phoneNumber')
            store_info['address'] = self._get_address(store.get('address', {}))
            store_info['hours'] = self._get_hours(store.get("openingHours", {}).get("weekDayOpeningList", {}))
            store_info['location'] = self._get_location(store.get('geoPoint', {}))
            store_info['raw_dict'] = store            

            yield store_info

    @staticmethod
    def format_time(time_str: str) -> str:
        return re.sub(r'(\d+)([ap]m)', r'\1 \2', time_str)

    def _get_hours(self, opening_hours: dict) -> dict:
        hours_info = {}
        for day_hours in opening_hours:
            day = day_hours.get('weekDay')

            open = self.format_time(day_hours.get('openingTime',{}).get('formattedHour'))
            close = self.format_time(day_hours.get('closingTime',{}).get('formattedHour'))

            hours_info[day] = {
                'open': open,
                'close': close
            }
        return hours_info

    def _get_address(self, address_info: dict) -> str:
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