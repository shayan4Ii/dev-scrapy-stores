from typing import Iterable, Dict
import scrapy
from scrapy import Request
from scrapy.http import Response

from scrapy_store_scrapers.utils import *



class PadgettAdvisors(scrapy.Spider):
    name = "padgettadvisors"


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            params = {
                'action': 'asl_load_stores',
                'load_all': '0',
                'layout': '1',
                'lat': f'{zipcode.get("latitude")}',
                'lng': f'{zipcode.get("longitude")}',
            }
            yield scrapy.FormRequest(
                url="https://www.padgettadvisors.com/wp-admin/admin-ajax.php",
                formdata=params,
                callback=self.parse
            )

    def parse(self, response: Response, **kwargs) -> Iterable[Dict]:
        stores = json.loads(response.text)
        for store in stores:
            yield {
                "number": store['id'],
                "name": store['title'],
                "address": self._get_address(store),
                "location": self._get_location(store),
                "phone_number": store['phone'],
                "hours": self._get_hours(store),
                "url": "https://www.padgettadvisors.com/locations/",
                "raw": store
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store.get("street", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store.get("city", "")
            state = store.get("state", "")
            zipcode = store.get("postal_code", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_location(self, store: Dict) -> Dict:
        try:
            lat = float(str(store.get("lat", 0)))
            lon = float(str(store.get("lng", 0)))
            
            if not lat or not lon:
                return {}

            return {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}


    def _get_hours(self, store: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        new_item = {}
        try:
            open_hours = json.loads(store['open_hours'])
            for day in days:
                for d in open_hours:
                    if d in day:
                        hours = open_hours[d]
                        if hours == "0":
                            continue
                        open, close = hours[0].split("-")
                        new_item[day] = {
                            "open": open.lower().strip(),
                            "close": close.lower().strip(),
                        }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}