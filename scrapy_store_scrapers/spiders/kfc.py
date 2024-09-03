import json
from datetime import datetime
from typing import Generator

import scrapy
from scrapy.http import Response

class KfcSpider(scrapy.Spider):
    name = "kfc"
    allowed_domains = ["locations.kfc.com"]
    start_urls = ["https://locations.kfc.com"]

    LOCATION_URL_XPATH = '//ul[@class="Directory-listLinks"]/li/a/@href'
    STORE_URLS_XPATH = '//ul[@class="Directory-listTeasers Directory-row"]//h2/a/@href'
    ADDRESS_ELEM_XPATH = '//address[@id="address"]'

    STREET_ADDRESS_XPATH = './/span[@class="c-address-street-1"]/text()'
    STREET_ADDRESS_2_XPATH = './/span[@class="c-address-street-2"]/text()'
    CITY_XPATH = './/span[@class="c-address-city"]/text()'
    REGION_XPATH = './/abbr[@itemprop="addressRegion"]/text()'
    POSTAL_CODE_XPATH = './/span[@itemprop="postalCode"]/text()'

    PHONE_XPATH = '//div[@class="Core-body"]//div[@id="phone-main"]/text()'
    SERVICES = '//ul[@class="CoreServices"]/li/span/text()'
    HOURS_JSON_XPATH = '//div[@id="hours-accordion-content"]/div/div/@data-days'

    LATITUDE_XPATH = '//meta[@itemprop="latitude"]/@content'
    LONGITUDE_XPATH = '//meta[@itemprop="longitude"]/@content'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.addresses_scraped = set()

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """
        Parse the main page and follow links to location pages or store pages.
        """
        location_urls = response.xpath(self.LOCATION_URL_XPATH).getall()
        store_urls = response.xpath(self.STORE_URLS_XPATH).getall()
        if location_urls:
            for location_url in location_urls:
                yield response.follow(location_url, self.parse)
        elif store_urls:
            for store_url in store_urls:
                yield response.follow(store_url, self.parse_store)
        else:
            yield self.parse_store(response)

    def parse_store(self, response: Response):
        """
        Parse individual store page and extract store details.
        """
        item = {}

        # Extract store name
        item['name'] = self.clean_text(response.xpath('normalize-space(//span[@id="location-name"])').get())

        address_elem = response.xpath(self.ADDRESS_ELEM_XPATH)

        # Extract and combine address components
        street_address = self.clean_text(address_elem.xpath(self.STREET_ADDRESS_XPATH).get())
        street_address_2 = self.clean_text(address_elem.xpath(self.STREET_ADDRESS_2_XPATH).get())
        complete_street_address = self.clean_text(f"{street_address} {street_address_2}")

        city = self.clean_text(address_elem.xpath(self.CITY_XPATH).get())
        region = self.clean_text(address_elem.xpath(self.REGION_XPATH).get())
        postal_code = self.clean_text(address_elem.xpath(self.POSTAL_CODE_XPATH).get())

        item['address'] = f"{complete_street_address}, {city}, {region} {postal_code}"
        item['phone_number'] = response.xpath(self.PHONE_XPATH).get()
        
        # Extract and validate location data
        try:
            latitude = float(response.xpath(self.LATITUDE_XPATH).get())
            longitude = float(response.xpath(self.LONGITUDE_XPATH).get())
            item['location'] = {
                'type': 'Point',
                'coordinates': [longitude, latitude]
            }
        except (TypeError, ValueError):
            item['location'] = None
            self.logger.warning(
                f"Invalid location data for store: {item['address']}")
            # Skip store if location data is invalid
            return 

        # Extract and parse hours data
        hours_json = response.xpath(self.HOURS_JSON_XPATH).get()

        if not hours_json:
            self.logger.warning(f"No hours data found for store: {item['address']}")
            item['hours'] = None
        else:
            try:
                item['hours'] = {}
                hours_data = json.loads(hours_json)

                for day_dict in hours_data:
                    day = day_dict['day'].lower()

                    if not day_dict['intervals']:
                        self.logger.warning(f"No intervals found for {day} for store: {item['address']} with url: {response.url}")
                        item['hours'][day] = None
                        continue
                    elif len(day_dict['intervals']) > 1:
                        self.logger.error(f"Multiple intervals found for {day} for store: {item['address']} with url: {response.url}")

                    open_time = day_dict['intervals'][0]['start']
                    close_time = day_dict['intervals'][0]['end']
                    item['hours'][day] = {
                        "open": self.convert_to_12_hour(str(open_time)),
                        "close": self.convert_to_12_hour(str(close_time))
                    }
            except Exception as e:
                self.logger.error(f"Failed to parse hours data: {e} for store: {item['address']} with url: {response.url}")
                item['hours'] = None

        item['services'] = response.xpath(self.SERVICES).getall()

        address = item['address'].lower().replace(" ", "").replace(",", "")
        if address in self.addresses_scraped:
            self.logger.debug(f"Duplicate address found: {item['address']}")
            return
        self.addresses_scraped.add(address)

        return item
    
    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """
        Convert 24-hour time string to 12-hour format.
        """
        padded_time = time_str.zfill(4)
        time_obj = datetime.strptime(padded_time, '%H%M')
        return time_obj.strftime('%I:%M %p').lower()

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean and strip whitespace from text.
        """
        return text.strip() if text else ""