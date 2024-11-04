from typing import Any, Iterable
import scrapy
from scrapy.http import Response
from scrapy_store_scrapers.utils import *



class BobEvans(scrapy.Spider):
    name = "bobevans"


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://www.bobevans.com/api/location/search?query={zipcode['zipcode']}"
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response) -> Iterable[Dict]:
        kitchens = json.loads(response.text)
        for kitchen in kitchens:
            kitchen_id = f"{kitchen['id']}"
            yield {
                "number": kitchen_id,
                "name": kitchen['name'],
                "address": self._get_address(kitchen),
                "location": self._get_location(kitchen),
                "phone_number": kitchen.get("telephone"),
                "hours": self._get_hours(kitchen),
                "url": f"https://www.bobevans.com/locations/{kitchen['name'].lower()}",
                "raw": kitchen
            }


    def _get_address(self, kitchen: Dict) -> str:
        try:
            address_parts = [
                kitchen['streetAddress'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = kitchen.get("city", "")
            state = kitchen.get("state", "")
            zipcode = kitchen.get("zip", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_location(self, kitchen: Dict) -> Dict:
        try:
            lat = float(str(kitchen.get("latitude")))
            lon = float(str(kitchen.get("longitude")))
            return {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}
        

    def _get_hours(self, kitchen: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        new_item = {}
        try:
            open = datetime.strptime(kitchen['businessHours'][0]['startDate'], "%Y-%m-%dT%H:%M:%S%z").strftime('%I:%M %p').lower().lstrip('0')
            close = datetime.strptime(kitchen['businessHours'][0]['endDate'], "%Y-%m-%dT%H:%M:%S%z").strftime('%I:%M %p').lower().lstrip('0')
            for day in days:
                new_item[day] = {
                    "open": open,
                    "close": close
                }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}