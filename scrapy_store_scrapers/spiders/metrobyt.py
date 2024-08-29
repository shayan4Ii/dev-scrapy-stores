import json
from datetime import datetime
from typing import Any, Generator

import scrapy
from scrapy.exceptions import DropItem
from scrapy.http import Response

class MetrobytSpider(scrapy.Spider):
    """Spider for scraping Metro by T-Mobile store information."""

    name = "metrobyt"
    allowed_domains = ["www.metrobyt-mobile.com"]
    start_urls = ["https://www.metrobyt-mobile.com/stores/"]

    STATE_LINKS_XPATH = "//a[@class='lm-homepage__state']/@href"
    STORE_LINKS_XPATH = "//a[@class='lm-state__store']/@href"
    SCRIPT_TEXT_XPATH = "//script[@type='application/ld+json']/text()"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.branch_codes = set()

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to individual state pages."""
        try:
            state_links = response.xpath(self.STATE_LINKS_XPATH).getall()

            if not state_links:
                self.logger.warning(f"No state links found on {response.url}")
                return

            for link in state_links:
                yield response.follow(link, callback=self.parse_state)
        except Exception as e:
            self.logger.error(f"Error parsing main page: {e}")

    def parse_state(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the state page and follow links to individual store pages."""
        try:
            store_links = response.xpath(self.STORE_LINKS_XPATH).getall()

            if not store_links:
                self.logger.warning(f"No store links found on {response.url}")
                return

            for link in store_links:
                yield response.follow(link, callback=self.parse_store)
        except Exception as e:
            self.logger.error(f"Error parsing state page: {e}")

    def parse_store(self, response: Response) -> dict[str, Any]:
        """Parse individual store page and extract store information."""
        try:
            script_text = self.clean_text(response.xpath(self.SCRIPT_TEXT_XPATH).get())

            if not script_text:
                self.logger.warning(f"No script text found on {response.url}")
                raise DropItem(f"No script text found on {response.url}")

            store_data = json.loads(script_text)

            branch_code = store_data.get("branchCode")
            if not branch_code:
                self.logger.warning(f"Missing branch code on {response.url}")
                raise DropItem(f"Missing branch code on {response.url}")

            if branch_code in self.branch_codes:
                raise DropItem(f"Duplicate store with branch code {branch_code}")

            self.branch_codes.add(branch_code)

            return self.extract_store_info(store_data, response.url)
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error on {response.url}: {e}")
            raise DropItem(f"JSON decode error on {response.url}")
        except Exception as e:
            self.logger.error(f"Error parsing store page {response.url}: {e}")
            raise DropItem(f"Error parsing store page {response.url}")

    def extract_store_info(self, store_data: dict[str, Any], url: str) -> dict[str, Any]:
        """Extract and format store information from raw data."""
        try:
            address = self.format_address(store_data.get("address", {}).get("streetAddress", ""))
            phone_number = store_data.get("telephone")
            geo_info = store_data.get("geo", {})
            location = self.extract_location(geo_info)
            hours = self.parse_hours(store_data.get("openingHoursSpecification", []))

            if not all([address, phone_number, location, hours]):
                self.logger.warning(f"Missing data for store on {url}")

            return {
                "address": address,
                "phone_number": phone_number,
                "location": location,
                "hours": hours,
                "raw_dict": store_data
            }
        except Exception as e:
            self.logger.error(f"Error extracting store info from {url}: {e}")
            raise DropItem(f"Error extracting store info from {url}")

    def format_address(self, street_address: str) -> str:
        """Format the street address."""
        if not street_address:
            return ""
        formatted_address = ", ".join(street_address.rsplit("\n", 1))
        return self.clean_text(formatted_address.replace("\n", " "))

    @staticmethod
    def extract_location(geo_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location information."""
        try:
            return {
                "type": "Point",
                "coordinates": [float(geo_info.get("longitude", 0)), float(geo_info.get("latitude", 0))]
            }
        except (ValueError, TypeError):
            return {}

    @staticmethod
    def parse_hours(hours_info_list: list[str]) -> dict[str, dict[str, str]]:
        """Parse hours information into a structured dictionary."""
        day_abbr_dict = {
            "Mo": "monday", "Tu": "tuesday", "We": "wednesday",
            "Th": "thursday", "Fr": "friday", "Sa": "saturday", "Su": "sunday"
        }
        hours_dict = {}

        for hours_info in hours_info_list:
            try:
                day_abbr, hours_text = hours_info.split(" ", 1)
                day = day_abbr_dict.get(day_abbr)
                open_time, close_time = map(MetrobytSpider.convert_to_12_hour, hours_text.split("-"))
                hours_dict[day] = {"open": open_time, "close": close_time}
            except ValueError:
                continue

        return hours_dict

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """Convert 24-hour time string to 12-hour format."""
        try:
            time_obj = datetime.strptime(time_str, '%H:%M')
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return ""

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and strip whitespace from text."""
        return text.strip() if text else ""