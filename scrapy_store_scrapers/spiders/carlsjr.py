import copy
import scrapy
from scrapy_store_scrapers.utils import *


class CarlsJr(scrapy.Spider):
    name = "carlsjr"
    start_urls = ["https://locations.carlsjr.com/"]


    def parse(self, response: Response):
        states = response.xpath("//a[@class='Directory-listLink']/@href").getall()
        yield from response.follow_all(states, self.parse_state)


    def parse_state(self, response: Response):
        cities = response.xpath("//a[@class='Directory-listLink']/@href").getall()
        yield from response.follow_all(cities, self.parse_city)

    
    def parse_city(self, response: Response):
        stores = response.xpath("//a[contains(@class,'Teaser-ctaLink')]/@href").getall()
        yield from response.follow_all(stores, self.parse_store)


    def parse_store(self, response: Response):
        yield {
            "name": response.xpath("//h1/text()").get(),
            "phone_number": response.xpath("//meta[@property='restaurant:contact_info:phone_number']/@content").get(),
            "address": self._get_address(response),
            "location": {
                "type": "Point",
                "coordinates": [
                    float(response.xpath("//meta[@itemprop='longitude']/@content").get()),
                    float(response.xpath("//meta[@itemprop='latitude']/@content").get())
                ]
            },
            "services": response.xpath("//li[@itemprop='makesOffer']//span[@itemprop='name']/text()").getall(),
            "hours": self._get_hours(response),
            "url": response.url
        }


    def _get_address(self, response: Response) -> str:
        try:
            address_parts = [
                response.xpath("//meta[@itemprop='streetAddress']/text()").get(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("//span[@class='Address-field Address-city']/text()").get()
            state = response.xpath("//abbr[@itemprop='addressRegion']/text()").get()
            zipcode = response.xpath("//span[@class='Address-field Address-postalCode']/text()").get()
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, response: Response) -> dict:
        days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
        hours = {}
        try:
            hours_range = json.loads(response.xpath("//div[@class='c-hours-details-wrapper js-hours-table']/@data-days").get())
            for day, hour_rng in zip(days, hours_range):
                hours[day] = {
                    "open": convert_to_12h_format(hour_rng[0]),
                    "close": convert_to_12h_format(hour_rng[1])
                }
            for day, hour_range in copy.deepcopy(hours).items():
                if hour_range['open'] is None and hour_range['close'] is None:
                    del hours[day]
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}