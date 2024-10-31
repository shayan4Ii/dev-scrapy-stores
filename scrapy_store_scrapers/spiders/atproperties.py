from typing import Iterable, Any, Dict

import scrapy
from scrapy import Request
from scrapy.http import Response

from scrapy_store_scrapers.utils import *



class Atproperties(scrapy.Spider):
    name = "atproperties"


    def start_requests(self) -> Iterable[Request]:
        url = "https://www.atproperties.com/offices"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response) -> Iterable[Dict]:
        offices = json.loads(response.xpath("//office-list").re_first('\[.*?\]\"').strip('"'))
        for office in offices:
            yield {
                "number": f"{office['id']}",
                "name": office['name'],
                "address": self._get_address(office),
                "location": {
                    "type": "Point",
                    "coordinates": [office['latLng']['lng'], office['latLng']['lat']]
                },
                "phone_number": office['phone'],
                "hours": self._get_hours(office),
                "url": office['detailsUrl'],
                "raw": office
            }


    def _get_address(self, office: Dict) -> str:
        try:
            address_parts = [
                office.get("streetAddress", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = office.get("city", "")
            state = office.get("state", "")
            zipcode = office.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, office: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        new_item = {}
        try:
            if not office['hours']:
                return {}
            for day, hours in office['hours'].items():
                if hours.isalpha() or not hours:
                    continue
                if day.lower() in days:
                    open, close = hours.split("-")
                    new_item[day.lower()] = {
                        "open": convert_to_12h_format(open.strip()),
                        "close": convert_to_12h_format(close.strip())
                    }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}