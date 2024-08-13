from datetime import datetime
import json
import scrapy
from typing import Generator
from scrapy_store_scrapers.items import MetrobytStoreItem

STATE_LINKS_XPATH = "//a[@class='lm-homepage__state']/@href"
STORE_LINKS_XPATH = "//a[@class='lm-state__store']/@href"

SCRIPT_TEXT_XPATH = "//script[@type='application/ld+json']/text()"

class MetrobytSpider(scrapy.Spider):
    name = "metrobyt"
    allowed_domains = ["www.metrobyt-mobile.com"]
    start_urls = ["https://www.metrobyt-mobile.com/stores/"]

    def parse(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to individual state pages."""
        state_links = response.xpath(STATE_LINKS_XPATH).getall()

        if not state_links:
            self.logger.warning(f"No state links found on {response.url}")
            return 
        
        for link in state_links:
            yield response.follow(link, callback=self.parse_state)
            break

    def parse_state(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """Parse the state page and follow links to individual store pages."""
        store_links = response.xpath(STORE_LINKS_XPATH).getall()

        if not store_links:
            self.logger.warning(f"No store links found on {response.url}")
            return

        for link in store_links[:20]:
            yield response.follow(link, callback=self.parse_store)
            
        
    def parse_store(self, response: scrapy.http.Response) -> MetrobytStoreItem:
        store_data = MetrobytStoreItem()

        script_text = self.clean_text(response.xpath(SCRIPT_TEXT_XPATH).get())

        if not script_text:
            self.logger.error(f"No script text found on {response.url}")
            return
        
        store_raw_dict = json.loads(script_text)
        store_data["address"] = store_raw_dict["address"]["streetAddress"].replace("\n", " ")
        store_data["phone_number"] = store_raw_dict["telephone"]

        geo_info = store_raw_dict["geo"]
        store_data['location'] = {
                'type': 'Point',
                'coordinates': [geo_info['longitude'], geo_info['latitude']]
            }
        
        hours_info_list: list[str] = store_raw_dict["openingHoursSpecification"]
        hours_dict = {}
        day_abbr_dict = {
            "Mo": "monday",
            "Tu": "tuesday",
            "We": "wednesday",
            "Th": "thursday",
            "Fr": "friday",
            "Sa": "saturday",
            "Su": "sunday"
        }

        for hours_info in hours_info_list:
            day_abbr, hours_text = hours_info.split(" ", 1)
            day = day_abbr_dict.get(day_abbr)
            hours_dict[day] = {
                "open": self.convert_to_12_hour(hours_text.split("-")[0]),
                "close": self.convert_to_12_hour(hours_text.split("-")[1])
            }
        
        store_data["hours"] = hours_dict

        return store_data

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """Convert 24-hour time string to 12-hour format."""
        time_obj = datetime.strptime(time_str, '%H:%M')
        return time_obj.strftime('%I:%M %p').lower()


    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and strip whitespace from text"""
        return text.strip() if text else ""

        
    
