import json
from datetime import datetime
from typing import Generator, Dict, Any

import scrapy
from scrapy.http import Response
from scrapy.exceptions import DropItem
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
from scrapy_store_scrapers.items import MetrobytStoreItem

class MetrobytSpider(scrapy.Spider):
    name = "metrobyt"
    allowed_domains = ["www.metrobyt-mobile.com"]
    start_urls = ["https://www.metrobyt-mobile.com/stores/"]

    # Constants
    STATE_LINKS_XPATH = "//a[@class='lm-homepage__state']/@href"
    STORE_LINKS_XPATH = "//a[@class='lm-state__store']/@href"
    SCRIPT_TEXT_XPATH = "//script[@type='application/ld+json']/text()"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.branch_codes = set()

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to individual state pages."""
        state_links = response.xpath(self.STATE_LINKS_XPATH).getall()

        if not state_links:
            self.logger.warning(f"No state links found on {response.url}")
            return 
        
        for link in state_links:
            yield response.follow(link, callback=self.parse_state)

    def parse_state(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the state page and follow links to individual store pages."""
        store_links = response.xpath(self.STORE_LINKS_XPATH).getall()

        if not store_links:
            self.logger.warning(f"No store links found on {response.url}")
            return

        for link in store_links:
            yield response.follow(link, callback=self.parse_store)
            break  # Remove this line to scrape all stores

    def parse_store(self, response: Response) -> MetrobytStoreItem:
        loader = ItemLoader(item=MetrobytStoreItem(), response=response)
        loader.default_output_processor = TakeFirst()

        script_text = self.clean_text(response.xpath(self.SCRIPT_TEXT_XPATH).get())

        if not script_text:
            raise DropItem(f"No script text found on {response.url}")
        
        store_raw_dict = json.loads(script_text)

        branch_code = store_raw_dict["branchCode"]
        if branch_code in self.branch_codes:
            raise DropItem(f"Duplicate store with branch code {branch_code}")
        
        self.branch_codes.add(branch_code)
        
        formatted_address = self._format_address(store_raw_dict["address"]["streetAddress"])

        loader.add_value('address', formatted_address)
        loader.add_value('phone_number', store_raw_dict["telephone"])

        geo_info = store_raw_dict["geo"]
        loader.add_value('location', {
            'type': 'Point',
            'coordinates': [geo_info['longitude'], geo_info['latitude']]
        })
        
        hours_dict = self.parse_hours(store_raw_dict["openingHoursSpecification"])
        loader.add_value('hours', hours_dict)

        loader.add_value('raw_dict', store_raw_dict)

        return loader.load_item()

    def _format_address(self, street_address: str) -> str:
        """Format the street address."""
        street_address = ", ".join(street_address.rsplit("\n", 1))
        street_address = street_address.replace("\n", " ")

        return self.clean_text(street_address)

    @staticmethod
    def parse_hours(hours_info_list: list[str]) -> Dict[str, Dict[str, str]]:
        """Parse hours information into a structured dictionary."""
        day_abbr_dict = {
            "Mo": "monday", "Tu": "tuesday", "We": "wednesday",
            "Th": "thursday", "Fr": "friday", "Sa": "saturday", "Su": "sunday"
        }
        hours_dict = {}

        for hours_info in hours_info_list:
            day_abbr, hours_text = hours_info.split(" ", 1)
            day = day_abbr_dict.get(day_abbr)
            open_time, close_time = map(MetrobytSpider.convert_to_12_hour, hours_text.split("-"))
            hours_dict[day] = {"open": open_time, "close": close_time}

        return hours_dict

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """Convert 24-hour time string to 12-hour format."""
        time_obj = datetime.strptime(time_str, '%H:%M')
        return time_obj.strftime('%I:%M %p').lower()

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and strip whitespace from text"""
        return text.strip() if text else ""