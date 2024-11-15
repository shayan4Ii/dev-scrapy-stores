from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *



class DontDriveDirty(scrapy.Spider):
    name = "dontdrivedirty"


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://www.dontdrivedirty.com/locationsandpricing/?zipcode={zipcode['zipcode']}"
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        stores = response.xpath("//a[contains(@href, '/location/')]/@href").getall()
        yield from response.follow_all(stores, self.parse_store)

    
    def parse_store(self, response: Response):
        obj = json.loads(response.xpath("//script[@type='application/ld+json' and contains(text(), 'geo')]/text()").get())
        if not obj['geo']['latitude'] or not obj['geo']['longitude']:
            return
        yield {
            "name": response.xpath("//h1/text()").get(),
            "phone_number": obj.get("telephone"),
            "address": self._get_address(obj['address']),
            "location": {
                "type": "Point",
                "coordinates": [
                    float(obj["geo"]["longitude"]),
                    float(obj["geo"]["latitude"])
                ]
            },
            "hours": self._get_hours(obj),
            "services": ["Members Quick Lane"] if response.xpath("//a[contains(@href, 'new-member-perk')]") else [],
            "url": response.url,
            "coming_soon": "coming soon" in response.xpath("//p[@class='h2' and contains(text(), 'Coming Soon')]/text()").get('').lower(),
            "raw": obj
        }


    def _get_address(self, address: Dict) -> str:
        try:
            address_parts = [
                address['streetAddress'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = address['addressLocality']
            state = address['addressRegion']
            zipcode = address['postalCode']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        
    
    def _get_hours(self, obj: Dict) -> dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            opening_hours = obj['openingHours'].lower()
            if opening_hours.strip() == "-":
                return {}
            if "mon - sun" in opening_hours:
                for day in days:
                    hours_range = obj['openingHours'].lower().replace("mon - sun", "").strip()
                    hours[day] = {
                        "open": hours_range.split(" - ")[0].strip().replace(".",""),
                        "close": hours_range.split(" - ")[1].strip().replace(".","")
                    }
            elif "mon - sat" in opening_hours:
                for day in days[:-1]:
                    hours_range = obj['openingHours'].lower().split("mon - sat")[0].replace("mon - sat", "").strip()
                    hours[day] = {
                        "open": hours_range.split(" - ")[0].strip().replace(".",""),
                        "close": hours_range.split(" - ")[1].strip().replace(".","")
                    }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}