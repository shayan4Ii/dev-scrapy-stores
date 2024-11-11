from typing import Any, Iterable
import scrapy
from scrapy.http import Response
from scrapy_store_scrapers.utils import *



class Zipscarwash(scrapy.Spider):
    name = "zipscarwash"
    custom_settings = dict(
        CONCURRENT_REQUESTS = 1,
        DOWNLOAD_DELAY = 2.5
    )


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            payload = {
                'operation': 'searchLocations',
                'zipcode': zipcode['zipcode'],
                'state': '',
                'distance': '60',
                'countImageMobile': '1',
                'isMobile': '0',
                'imageNoTag': 'https://symphony.cdn.tambourine.com/zips-car-wash/media/zips-location-header-613aaee829a15.jpg',
                'imageMobile': 'https://symphony.cdn.tambourine.com/zips-car-wash/media/header-location-mobile-614ddaf1ee3d9.jpg',
            }
            yield scrapy.FormRequest(
                url="https://www.zipscarwash.com/ajax/functions.php",
                formdata=payload,
                callback=self.parse,
            )


    def parse(self, response: Response):
        locations = json.loads(response.text)['locations']
        for location in locations:
            yield {
                "number": f"{location['id']}",
                "name": location['business_name'],  # same for all locations
                "phone_number": location['phone_numbers'],
                "address": self._get_address(location),
                "location": {
                    "type": "Point",
                    "coordinates": [
                        float(location["lon"]),
                        float(location["lat"])
                    ]
                },
                "services": [" ".join(row.xpath(".//p/text()").getall()).strip("-").strip() for row in scrapy.Selector(text=json.loads(response.text)['locations'][0]['rows']).xpath("//tr")],
                "hours": self._get_hours(location),
                "url": f"https://www.zipscarwash.com/drive-through-car-wash-locations?code={location['zipcode']}&distance=60",
                "raw": location
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store['street'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = store['city']
            state = store['state']
            zipcode = store['zipcode']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, location: Dict) -> dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            business_hours = json.loads(location['business_hours'])
            for day, block in business_hours.items():
                hours_range = block['blocks'][0]
                if day.lower() in days:
                    hours[day.lower()] = {
                        "open": convert_to_12h_format(hours_range['from']),
                        "close": convert_to_12h_format(hours_range['to'])
                    }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
