from datetime import datetime
import re 
import json
from typing import Generator

import scrapy
import scrapy.http
from scrapy.exceptions import DropItem
from scrapy.loader import ItemLoader
from scrapy_store_scrapers.items import TraderjoesStoreItem
from itemloaders.processors import TakeFirst, MapCompose

class TraderjoesSpider(scrapy.Spider):
    name = "traderjoes"
    allowed_domains = ["locations.traderjoes.com"]
    start_urls = ["http://locations.traderjoes.com/"]

    # Constants
    LINKS_XPATH = '//div[@class="itemlist"]/a/@href'
    STORE_LINKS_XPATH = '//a[@class="capital listitem"]/@href'
    JSON_SCRIPT_XPATH = "//script[@type='application/ld+json' and contains(text(),'GroceryStore')]/text()"
    NAME_XPATH = "//h1/text()"

    def parse(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """Recursively follow links on the main page until store pages are reached."""
        
        if response.xpath(self.LINKS_XPATH):
            for link in response.xpath(self.LINKS_XPATH).getall():
                yield response.follow(link, callback=self.parse)
                break
        elif response.xpath(self.STORE_LINKS_XPATH):
            for link in response.xpath(self.STORE_LINKS_XPATH).getall():
                yield response.follow(link, callback=self.parse_store)
                break
        else:
            self.logger.warning(f"No links found on {response.url}")
    
    def parse_store(self, response: scrapy.http.Response) -> TraderjoesStoreItem:
        loader = ItemLoader(item=TraderjoesStoreItem(), response=response)
        loader.default_output_processor = TakeFirst()

        script_text = self.clean_text(response.xpath(self.JSON_SCRIPT_XPATH).get())

        if not script_text:
            raise DropItem(f"No script text found on {response.url}")
        
        script_text = self.remove_comments(script_text)
        
        store_raw_dict = json.loads(script_text)

        loader.add_xpath('name', self.NAME_XPATH, MapCompose(self.clean_store_name))
        loader.add_value('number', int(store_raw_dict.get("@id")))

        address_dict = store_raw_dict.get("address")

        address = f'{address_dict.get("streetAddress")}, {address_dict.get("addressLocality")}, {address_dict.get("addressRegion")} {address_dict.get("postalCode")}'

        loader.add_value('address', address)
        loader.add_value('phone_number', store_raw_dict.get("telephone"))

        geo_info = store_raw_dict.get("geo")
        loader.add_value('location', {
            'type': 'Point',
            'coordinates': [geo_info.get("longitude"), geo_info.get("latitude")]
        })

        hours_dict = {}
        hours_raw_list = store_raw_dict.get("openingHoursSpecification")

        for day in hours_raw_list:
            day_name = day.get("dayOfWeek")[0]
            hours_dict[day_name.lower()] = {
                'open': self.convert_to_12_hour(day.get("opens")),
                'close': self.convert_to_12_hour(day.get("closes"))
            }
        
        loader.add_value('hours', hours_dict)
        return loader.load_item()

    @staticmethod
    def remove_comments(json_text):
        # Remove single-line comments
        json_text = re.sub(r'(?<!:)//.*', '', json_text)
        
        # Remove multi-line comments
        json_text = re.sub(r'/\*[\s\S]*?\*/', '', json_text)
        
        # Remove any trailing commas
        json_text = re.sub(r',\s*}', '}', json_text)
        json_text = re.sub(r',\s*]', ']', json_text)
        
        return json_text

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """Convert 24-hour time string to 12-hour format."""
        time_obj = datetime.strptime(time_str, '%H:%M')
        return time_obj.strftime('%I:%M %p').lower()

    @staticmethod
    def clean_store_name(name):
        return re.sub(r'\s*\(\d+\)\s*$', '', name)
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and strip whitespace from text"""
        return text.strip() if text else ""