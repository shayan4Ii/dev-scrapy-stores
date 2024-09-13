import json
import re
from typing import Dict, List, Optional, Tuple, Generator

import scrapy
from scrapy.http import Response


class TjmaxxSpider(scrapy.Spider):
    """Spider for scraping TJMaxx store information."""

    name = "tjmaxx"
    allowed_domains = ["tjmaxx.tjx.com"]
    start_urls = ["https://tjmaxx.tjx.com/store/stores/allStores.jsp"]

    # XPath selectors
    STORE_URLS_XPATH = '//li[contains(@class, "storelist-item")]/a/@href'
    NAME_XPATH = '//h4[@class="store-name"]/text()'
    STORE_DETAILS_ELEM_XPATH = '//div[@class="store-details"]'
    PHONE_XPATH = './/div[@class="store-phone"]/text()'
    ADDRESS_TEXTS_XPATH = './/div[@class="store-address"]/text()'
    HOURS_TEXT_XPATH = '//div[@id="title-block"]//time[@itemprop="openingHours"]/text()'
    LATITUDE_XPATH = '//input[@id="lat"]/@value'
    LONGITUDE_XPATH = '//input[@id="long"]/@value'
    SERVICES_XPATH = '//div[@id="title-block"]//ul[@class="store-features"]/li/text()'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to individual store pages."""
        store_urls = response.xpath(self.STORE_URLS_XPATH).getall()
        for store_url in store_urls:
            yield response.follow(store_url, callback=self.parse_store)

    def parse_store(self, response: Response) -> dict:
        """Parse individual store page and extract store information."""
        try:
            parsed_store = {
                'name': self.clean_text(response.xpath(self.NAME_XPATH).get()),
                'phone': self.clean_text(response.xpath(self.STORE_DETAILS_ELEM_XPATH).xpath(self.PHONE_XPATH).get()),
                'address': self._get_address(response),
                'hours': self._get_hours(response),
                'location': self._get_location(response),
                'services': response.xpath(self.SERVICES_XPATH).getall(),
                'url': response.url,
            }

            # Discard items missing required fields
            required_fields = ['address', 'location', 'url']
            if all(parsed_store.get(field) for field in required_fields):
                return parsed_store
            else:
                missing_fields = [field for field in required_fields if not parsed_store.get(field)]
                self.logger.warning(f"Discarding item due to missing required fields: {', '.join(missing_fields)}")
                return None
        except Exception as e:
            self.logger.error(f"Error parsing store {response.url}: {str(e)}", exc_info=True)
            return None

    def _get_location(self, response: Response) -> Optional[dict]:
        """Extract and format location coordinates."""
        try:
            latitude = response.xpath(self.LATITUDE_XPATH).get()
            longitude = response.xpath(self.LONGITUDE_XPATH).get()

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude for store: {response.url}")
            return None
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return None

    def _get_address(self, response: Response) -> str:
        """Get the formatted store address."""
        try:
            address_texts = response.xpath(self.STORE_DETAILS_ELEM_XPATH).xpath(self.ADDRESS_TEXTS_XPATH).getall()
            return ", ".join(self.clean_text(text) for text in address_texts)
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
            return ""

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """Clean and normalize text."""
        return text.strip() if text else ""

    @staticmethod
    def normalize_hours_text(hours_text: str) -> str:
        """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
        return re.sub(r'[^a-z0-9:]', '', hours_text.lower().replace('to', '').replace('thru', ''))

    def _get_hours(self, response: Response) -> dict:
        """Extract and parse store hours."""
        try:
            hours = response.xpath(self.HOURS_TEXT_XPATH).get()
            if not hours:
                self.logger.warning(f"No hours found for store: {response.url}")
                return {}

            normalized_hours = self.normalize_hours_text(hours)
            return self._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    def _parse_business_hours(self, input_text: str) -> dict:
        """Parse business hours from input text."""
        DAY_MAPPING = {
            'sun': 'sunday', 'mon': 'monday', 'tue': 'tuesday', 'wed': 'wednesday',
            'thu': 'thursday', 'fri': 'friday', 'sat': 'saturday',
        }
        result = {day: {'open': None, 'close': None} for day in DAY_MAPPING.values()}

        if input_text == "open24hours":
            return {day: {'open': '12:00 am', 'close': '11:59 pm'} for day in DAY_MAPPING.values()}
        elif 'open24hours' in input_text:
            input_text = input_text.replace('open24hours', '12:00am11:59pm')

        day_ranges = self._extract_business_hour_range(input_text)
        single_days = self._extract_business_hours(input_text)

        self._process_day_ranges(day_ranges, result, DAY_MAPPING)
        self._process_single_days(single_days, result, DAY_MAPPING)

        for day, hours in result.items():
            if hours['open'] is None or hours['close'] is None:
                self.logger.warning(f"Missing hours for {day} (input_text={input_text})")

        return result

    def _process_day_ranges(self, day_ranges: List[Tuple[str, str, str, str]],
                            result: Dict[str, Dict[str, Optional[str]]],
                            DAY_MAPPING: Dict[str, str]) -> None:
        """Process day ranges and update the result dictionary."""
        for start_day, end_day, open_time, close_time in day_ranges:
            start_index = list(DAY_MAPPING.keys()).index(start_day)
            end_index = list(DAY_MAPPING.keys()).index(end_day)
            if end_index < start_index:
                end_index += 7
            for i in range(start_index, end_index + 1):
                day = list(DAY_MAPPING.keys())[i % 7]
                full_day = DAY_MAPPING[day]
                if result[full_day]['open'] and result[full_day]['close']:
                    self.logger.debug(f"Day {full_day} already has hours, skipping range {start_day} to {end_day}")
                    continue
                result[full_day]['open'] = open_time
                result[full_day]['close'] = close_time

    def _process_single_days(self, single_days: List[Tuple[str, str, str]],
                             result: Dict[str, Dict[str, Optional[str]]],
                             DAY_MAPPING: Dict[str, str]) -> None:
        """Process single days and update the result dictionary."""
        for day, open_time, close_time in single_days:
            full_day = DAY_MAPPING[day]
            if result[full_day]['open'] and result[full_day]['close']:
                self.logger.debug(f"Day {full_day} already has hours, skipping individual day {day}")
                continue
            result[full_day]['open'] = open_time
            result[full_day]['close'] = close_time

    def _extract_business_hour_range(self, input_string: str) -> List[Tuple[str, str, str, str]]:
        """Extract business hour ranges from input string."""
        days_re = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
        day_suffix_re = r"(?:day)?"
        optional_colon_re = r"(?::)?"
        time_re = r"(\d{1,2}(?::\d{2})?)([ap]m)"

        time_only_re = f"^{time_re}{time_re}$"
        
        if "daily" in input_string:
            time_match = re.search(f"{time_re}{time_re}", input_string)
            if time_match:
                open_time = f"{time_match.group(1)} {time_match.group(2)}"
                close_time = f"{time_match.group(3)} {time_match.group(4)}"
                return [("sun", "sat", open_time, close_time)]
        
        time_only_match = re.match(time_only_re, input_string)
        if time_only_match:
            open_time = f"{time_only_match.group(1)} {time_only_match.group(2)}"
            close_time = f"{time_only_match.group(3)} {time_only_match.group(4)}"
            return [("sun", "sat", open_time, close_time)]

        pattern = f"({days_re}{day_suffix_re})({days_re}{day_suffix_re}){optional_colon_re}?{time_re}{time_re}"
        matches = re.finditer(pattern, input_string)
        
        return [
            (match.group(1)[:3], match.group(2)[:3], 
             f"{match.group(3)} {match.group(4)}", f"{match.group(5)} {match.group(6)}")
            for match in matches
        ]

    def _extract_business_hours(self, input_string: str) -> List[Tuple[str, str, str]]:
        """Extract individual business hours from input string."""
        days_re = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
        day_suffix_re = r"(?:day)?"
        optional_colon_re = r"(?::)?"
        time_re = r"(\d{1,2}(?::\d{2})?)([ap]m)"
        
        pattern = f"({days_re}{day_suffix_re}){optional_colon_re}?{time_re}{time_re}"
        matches = re.finditer(pattern, input_string)
        
        return [
            (match.group(1)[:3], f"{match.group(2)} {match.group(3)}", f"{match.group(4)} {match.group(5)}")
            for match in matches
        ]