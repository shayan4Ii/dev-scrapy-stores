import scrapy
from typing import Dict, Iterable
from scrapy.http import Response, Request
import json
from scrapy_store_scrapers.utils import *



class WaffleHouse(scrapy.Spider):
    name = "wafflehouse"


    def start_requests(self) -> Iterable[Request]:
        url = "https://locations.wafflehouse.com/"
        yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response: Response):
        obj = json.loads(response.xpath("//script[@id='__NEXT_DATA__']/text()").get())
        for location in obj.get("props",{}).get("pageProps",{}).get("locations",[]):
            yield {
                "number": location.get("storeCode"),
                "name": location.get("businessName"),
                "address": self._get_address(location),
                "location": self._get_location(location),
                "phone_number": location.get("phoneNumbers", [""])[0],
                "hours": self._get_hours(location),
                "services": location.get("services",[]),
                "url": location.get("websiteURL"),
                "raw": location
            }


    def _get_address(self, location: Dict) -> str:
        try:
            address_parts = [
                location.get('addressLines', [''])[0],
                location.get('city', ''),
                location.get('state', ''),
                location.get('postalCode', '')
            ]
            street = address_parts[0]
            city_state_zip = f"{address_parts[1]}, {address_parts[2]} {address_parts[3]}".strip()
            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""

    
    def _get_location(self, location: Dict) -> Dict:
        try:
            lat = float(str(location.get("latitude", 0)))
            lon = float(str(location.get("longitude", 0)))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return {
                    "type": "Point", 
                    "coordinates": [lon, lat]
                }
            self.logger.warning("Invalid coordinates: lat=%s, lon=%s", lat, lon)
            return {}
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}
        
    
    def _get_hours(self, location: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        new_item = {}
        try:
            hours = location.get("businessHours",[])
            if hours:
                hours = [hour for hour in hours if hour]
                for day, hour in zip(days, hours):
                    new_item[day] = {
                        "open": convert_to_12h_format(hour[0]),
                        "close": convert_to_12h_format(hour[1])
                    }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
        