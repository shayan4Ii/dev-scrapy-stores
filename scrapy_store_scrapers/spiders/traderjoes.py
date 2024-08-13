import re
import json5 as json
from typing import Generator, Dict, Any
from datetime import datetime

import scrapy
from scrapy.exceptions import DropItem
from scrapy.loader import ItemLoader
from scrapy.http import Response
from itemloaders.processors import TakeFirst, MapCompose

from scrapy_store_scrapers.items import TraderjoesStoreItem

class TraderjoesSpider(scrapy.Spider):
    name = "traderjoes"
    allowed_domains = ["locations.traderjoes.com"]
    start_urls = ["https://locations.traderjoes.com/"]

    # Constants
    LINKS_XPATH = '//div[@class="itemlist"]/a/@href'
    STORE_LINKS_XPATH = '//a[@class="capital listitem"]/@href'
    JSON_SCRIPT_XPATH = "//script[@type='application/ld+json' and contains(text(),'GroceryStore')]/text()"
    NAME_XPATH = "//h1/text()"

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Recursively follow links on the main page until store pages are reached."""

        links = response.xpath(self.LINKS_XPATH).getall()
        store_links = response.xpath(self.STORE_LINKS_XPATH).getall()
        if links:
            yield from self.follow_links(response, links, self.parse)
        elif store_links:
            yield from self.follow_links(response, store_links, self.parse_store)
        else:
            self.logger.warning(f"No links found on {response.url}")

    def follow_links(self, response: Response, links: list, callback) -> Generator[scrapy.Request, None, None]:
        for link in links:
            yield response.follow(link, callback=callback)

    def parse_store(self, response: Response) -> TraderjoesStoreItem:
        loader = ItemLoader(item=TraderjoesStoreItem(), response=response)
        loader.default_output_processor = TakeFirst()

        script_text = self.clean_text(response.xpath(self.JSON_SCRIPT_XPATH).get())

        if not script_text:
            raise DropItem(f"No script text found on {response.url}")
        
        
        try:
            store_raw_dict = json.loads(script_text)
        except Exception as e:
            raise DropItem(f"Invalid JSON on {response.url}")

        self.populate_item(loader, store_raw_dict)
        return loader.load_item()

    def populate_item(self, loader: ItemLoader, store_raw_dict: Dict[str, Any]) -> None:
        loader.add_xpath('name', self.NAME_XPATH, MapCompose(self.clean_store_name))
        loader.add_value('number', int(store_raw_dict.get("@id", 0)))

        address_dict = store_raw_dict.get("address", {})
        address = self.format_address(address_dict)
        loader.add_value('address', address)

        loader.add_value('phone_number', store_raw_dict.get("telephone"))

        geo_info = store_raw_dict.get("geo", {})
        loader.add_value('location', self.format_location(geo_info))

        hours_raw_list = store_raw_dict.get("openingHoursSpecification", [])
        hours_dict = self.format_hours(hours_raw_list)
        loader.add_value('hours', hours_dict)

    @staticmethod
    def format_address(address_dict: Dict[str, str]) -> str:
        return f"{address_dict.get('streetAddress', '')}, {address_dict.get('addressLocality', '')}, {address_dict.get('addressRegion', '')} {address_dict.get('postalCode', '')}"

    @staticmethod
    def format_location(geo_info: Dict[str, float]) -> Dict[str, Any]:
        return {
            'type': 'Point',
            'coordinates': [geo_info.get("longitude", 0), geo_info.get("latitude", 0)]
        }

    def format_hours(self, hours_raw_list: list) -> Dict[str, Dict[str, str]]:
        return {
            day.get("dayOfWeek")[0].lower(): {
                'open': self.convert_to_12_hour(day.get("opens", "")),
                'close': self.convert_to_12_hour(day.get("closes", ""))
            }
            for day in hours_raw_list
        }

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        try:
            time_obj = datetime.strptime(time_str, '%H:%M')
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            return ""

    @staticmethod
    def clean_store_name(name: str) -> str:
        return re.sub(r'\s*\(\d+\)\s*$', '', name)
    
    @staticmethod
    def clean_text(text: str) -> str:
        return text.strip() if text else ""