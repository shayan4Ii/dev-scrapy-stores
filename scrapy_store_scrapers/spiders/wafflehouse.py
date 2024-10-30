import scrapy
from typing import Dict, Iterable
from scrapy.http import Response, Request
from scrapy_store_scrapers.utils import *



class WaffleHouse(scrapy.Spider):
    name = "wafflehouse"


    def start_requests(self) -> Iterable[Request]:
        url = "https://locations.wafflehouse.com/"
        yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response: Response):
        obj = json.loads(response.xpath("//script[@id='__NEXT_DATA__']/text()").get())
        for store in obj.get("props",{}).get("pageProps",{}).get("locations",[]):
            yield {
                "number": store.get("storeCode"),
                "name": store.get("businessName"),
                "address": self._get_address(store),
                "location": self._get_location(store),
                "phone_number": store.get("phoneNumbers", [""])[0],
                "hours": self._get_hours(store),
                "url": store.get("websiteURL"),
                "raw": store
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store.get("addressLines", [""])[0],
            ]
            street = ", ".join(filter(None, address_parts))

            city = store.get("city", "")
            state = store.get("state", "")
            zipcode = store.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""

    
    def _get_location(self, store: Dict) -> Dict:
        try:
            lat = float(str(store.get("latitude", 0)))
            lon = float(str(store.get("longitude", 0)))
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
            hours = store.get("businessHours",[])
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
        