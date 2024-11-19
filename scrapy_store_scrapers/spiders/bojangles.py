import copy
from typing import Iterable, Any, Dict

import scrapy
from scrapy import Request
from scrapy.http import Response

from scrapy_store_scrapers.utils import *



class Bojangles(scrapy.Spider):
    name = "bojangles"


    def start_requests(self) -> Iterable[Request]:
        url = "https://locations.bojangles.com/"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        states = response.xpath("//a[@class='c-directory-list-content-item-link']/@href").getall()
        yield from response.follow_all(urls=states, callback=self.parse_state)


    def parse_state(self, response: Response):
        cities = response.xpath("//a[@class='c-directory-list-content-item-link']/@href").getall()
        yield from response.follow_all(urls=cities, callback=self.parse_city)


    def parse_city(self, response: Response):
        is_store_page = response.xpath("//h1[@id='location-name']")
        if is_store_page:
            # this is store page
            yield from self.parse_store(response)
        else:
            stores = response.xpath("//a[@data-ya-track='visit_page']/@href").getall()
            yield from response.follow_all(urls=stores, callback=self.parse_store)


    def parse_store(self, response: Response):
        store = {
            "number": response.xpath("//script[contains(text(), 'pageSetId')]/text()").re_first(r'(?:ids\"\:)(.*?)(?:,)'),
            "name": response.xpath("//h1[@id='location-name']/text()").get(),
            "address": self._get_address(response),
            "phone_number": response.xpath("//meta[@id='telephone']/@content").get(),
            "location": {
                "type": "Point",
                "coordinates": [float(coordinate) for coordinate in response.xpath("//meta[@name='geo.position']/@content").get().split(";")[::-1]]
            },
            "url": response.url,
            "coming_soon": bool(response.xpath("//div[@class='PromoBanner-text' and contains(text(), 'Coming soon')]")),
            "hours": self._get_hours(response)
        }
        yield store


    def _get_address(self, response: Response) -> str:
        try:
            address_parts = [
                response.xpath("//span[@class='c-address-street-1 ']/text()").get(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("//span[@itemprop='addressLocality']/text()").get()
            state = response.xpath("//abbr[@itemprop='addressRegion']/text()").get()
            zipcode = response.xpath("//span[@itemprop='postalCode']/text()").get()
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
            hours_range = json.loads(response.xpath("//div[@class='c-location-hours-details-wrapper js-location-hours']/@data-days").get('{}'))
            if not hours_range:
                self.logger.info(f"Hours are not available for store: {response.url}")
                return {}
            for day in days:
                for hour_range in hours_range:
                    d = hour_range['day'].lower()
                    if d in day:
                        if not hour_range['intervals']:
                            continue
                        start = str(hour_range['intervals'][0]['start'])
                        end = str(hour_range['intervals'][0]['end'])
                        if len(start) == 3:
                            start = f"0{start}"
                        if len(end) == 3:
                            end = f"0{end}"
                        hours[day] = {
                            "open": convert_to_12h_format(start),
                            "close": convert_to_12h_format(end)
                        }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}