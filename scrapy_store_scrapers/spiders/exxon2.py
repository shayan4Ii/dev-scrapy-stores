from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *



class Exxon(scrapy.Spider):
    name = "exxon2"


    def start_requests(self) -> Iterable[Request]:
        url = "https://www.exxon.com/en/find-station/united-states"
        yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response: Response):
        states = response.xpath("//div[@id='content']//a[@target='_self']/@href").getall()
        for state in states:
            yield response.follow(state, callback=self.parse_state)


    def parse_state(self, response: Response):
        stations = response.xpath("//div[@id='content']//a[@target='_self']/@href").getall()
        for station in stations:
            yield response.follow(station, callback=self.parse_station)

    
    def parse_station(self, response: Response):
        stations = response.xpath("//div[@id='content']//a[@target='_self']/@href").getall()
        if stations:
            for station in stations:
                yield response.follow(station, callback=self.parse_station)
        string = response.xpath("//script[contains(text(), 'LocalBusiness')]/text()").get()
        if string is None:
            return
        obj = json.loads(string)
        yield {
            "name": obj["name"],
            "phone_number": obj["telephone"],
            "address": self._get_address(obj),
            "location": {
                "type": "Point",
                "coordinates": [
                    float(obj["geo"]["longitude"]),
                    float(obj["geo"]["latitude"])
                ]
            },
            "hours": self._get_hours(obj),
            "services": response.xpath("//h3[contains(text(), 'amenities')]/following-sibling::ul[contains(@class, 'station-details-featuredItem')]/li/text()").getall(),
            "url": response.url,
            "raw": obj
        }

    
    def _get_address(self, obj: Dict) -> str:
        try:
            address_parts = [
                obj['address']['streetAddress'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = obj['address']["addressLocality"]
            state = obj['address']['addressCountry']
            zipcode = obj['address']['postalCode']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()


            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, obj: Dict) -> dict:
        hours = {}
        try:
            hours_data = obj["openingHoursSpecification"][0][0]
            for day in hours_data['@dayOfWeek']:
                hours[day.lower()] = {
                    "open": convert_to_12h_format(hours_data['@opens']),
                    "close": convert_to_12h_format(hours_data['@closes'])
                }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}