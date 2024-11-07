from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *
from scrapy.exceptions import DropItem


class DraftHouse(scrapy.Spider):
    name = "drafthouse"


    def start_requests(self) -> Iterable[Request]:
        url = "https://drafthouse.com/locations"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response) -> Iterable[Request]:
        locations = response.xpath("//li//h2//a[contains(@href, '/theater/')]/@href").getall()
        for location in locations:
            url = f"https://drafthouse.com/s/mother/v2/core/venue/theater/{location.split('/')[-1]}"
            yield scrapy.Request(url, self.parse_location, cb_kwargs={"source": location})


    def parse_location(self, response: Response, source: str) -> Iterable[Dict]:
        location = json.loads(response.text)['data']
        longtitude = float(location.get('longitude')) if location.get('longitude') else None
        latitude = float(location.get('latitude')) if location.get('latitude') else None
        yield {
            "number": f"{location['id']}",
            "name": location['title'],
            "address": self._get_address(location['address']),
            "phone_number": location['telephone'],
            "location": {
                "type": "Point",
                "coordinates": [longtitude,latitude],
            },
            "url": source,
            "raw": location
        }


    def _get_address(self, address: Dict) -> str:
        try:
            address_parts = [
                address.get("street1", ""),
                address.get("street2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address.get("city", "")
            state = address.get("state", "")
            zipcode = address.get("postalCode", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""