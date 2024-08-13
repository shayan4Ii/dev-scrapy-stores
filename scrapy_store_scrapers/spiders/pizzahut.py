import scrapy
from typing import Generator
from scrapy_store_scrapers.items import PizzahutStoreItem
from datetime import datetime
import json

STATE_LINKS_XPATH = '//div[@class="Directory-box" and .//text() = "Pizza Hut Locations"]//ul/li/a/@href'
CITY_LINKS_XPATH = '//a[@class="Directory-listLink"]/@href'
LOCATION_LINKS_XPATH = '//a[@class="Teaser-titleLink"]/@href'

ADDRESS_ELEM_XPATH = '//address[@id="address"]'

STREET_ADDRESS_XPATH = './/span[@class="c-address-street-1"]/text()'
STREET_ADDRESS_2_XPATH = './/span[@class="c-address-street-2"]/text()'
CITY_XPATH = './/span[@class="c-address-city"]/text()'
REGION_XPATH = './/abbr[@itemprop="addressRegion"]/text()'
POSTAL_CODE_XPATH = './/span[@itemprop="postalCode"]/text()'

LATITUDE_XPATH = '//meta[@itemprop="latitude"]/@content'
LONGITUDE_XPATH = '//meta[@itemprop="longitude"]/@content'

PHONE_XPATH = '//span[@id="telephone"]/text()'

HOURS_JSON_XPATH = '//div[@id="carryout-hours"]/div/div/@data-days'

SERVICE_XPATH = '//ul[@class="Core-services"]/li//span[@itemprop="name"]/text()'


class PizzahutSpider(scrapy.Spider):
    name: str = "pizzahut"
    allowed_domains: list[str] = ["locations.pizzahut.com"]
    start_urls: list[str] = ["http://locations.pizzahut.com/"]

    def parse(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to individual state pages."""
        state_links = response.xpath(STATE_LINKS_XPATH).getall()

        if not state_links:
            self.logger.warning(f"No state links found on {response.url}")
            return

        for link in state_links:
            yield response.follow(link, callback=self.parse_state)

    def parse_state(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """Parse the state page and follow links to individual city pages."""

        city_links = response.xpath(CITY_LINKS_XPATH).getall()

        if not city_links:
            self.logger.warning(f"No city links found on {response.url}")
            return

        for link in city_links:
            yield response.follow(link, callback=self.parse_city)

    def parse_city(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """Parse the city page and follow links to individual location pages."""

        location_links = response.xpath(LOCATION_LINKS_XPATH).getall()

        if not location_links:
            self.logger.warning(f"No location links found on {response.url}")
            return

        for link in location_links:
            yield response.follow(link, callback=self.parse_location)

    def parse_location(self, response: scrapy.http.Response) -> PizzahutStoreItem:
        """Parse the location page and extract store information."""

        store_data = PizzahutStoreItem()

        address_elem = response.xpath(ADDRESS_ELEM_XPATH)

        street_address = self.clean_text(
            address_elem.xpath(STREET_ADDRESS_XPATH).get())
        street_address_2 = self.clean_text(
            address_elem.xpath(STREET_ADDRESS_2_XPATH).get())
        complete_street_address = self.clean_text(
            f"{street_address} {street_address_2}")

        city = self.clean_text(address_elem.xpath(CITY_XPATH).get())
        region = self.clean_text(address_elem.xpath(REGION_XPATH).get())
        postal_code = self.clean_text(
            address_elem.xpath(POSTAL_CODE_XPATH).get())

        store_data['address'] = f"{complete_street_address}, {city}, {region} {postal_code}"

        if not all([complete_street_address, city, region, postal_code]):
            self.logger.warning(
                f"Incomplete address for store: {store_data['address']}")

        store_data['phone_number'] = self.clean_text(
            response.xpath(PHONE_XPATH).get())

        try:
            latitude = float(response.xpath(LATITUDE_XPATH).get())
            longitude = float(response.xpath(LONGITUDE_XPATH).get())
            store_data['location'] = {
                'type': 'Point',
                'coordinates': [longitude, latitude]
            }
        except (TypeError, ValueError):
            store_data['location'] = None
            self.logger.warning(
                f"Invalid location data for store: {store_data['address']}")

        hours_json = response.xpath(HOURS_JSON_XPATH).get()

        if not hours_json:
            self.logger.warning(
                f"No hours data found for store: {store_data['address']}")
            store_data['hours'] = None
        else:
            try:
                store_data['hours'] = {}
                hours_data = json.loads(hours_json)

                for day_dict in hours_data:
                    day = day_dict['day'].lower()

                    if not day_dict['intervals']:
                        self.logger.warning(
                            f"No intervals found for {day} for store: {store_data['address']} with url: {response.url}")
                        store_data['hours'][day] = None
                        continue
                    elif len(day_dict['intervals']) > 1:
                        self.logger.error(
                            f"Multiple intervals found for {day} for store: {store_data['address']} with url: {response.url}")

                    open_time = day_dict['intervals'][0]['start']
                    close_time = day_dict['intervals'][0]['end']
                    store_data['hours'][day] = {
                        "open": self.convert_to_12_hour(str(open_time)),
                        "close": self.convert_to_12_hour(str(close_time))
                    }
            except Exception as e:
                self.logger.error(
                    f"Failed to parse hours data: {e} for store: {store_data['address']} with url: {response.url}")
                store_data['hours'] = None

        store_data['services'] = response.xpath(SERVICE_XPATH).getall()

        return store_data

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """Convert 24-hour time string to 12-hour format."""
        padded_time = time_str.zfill(4)
        time_obj = datetime.strptime(padded_time, '%H%M')
        return time_obj.strftime('%I:%M %p').lower()

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and strip whitespace from text"""
        return text.strip() if text else ""
