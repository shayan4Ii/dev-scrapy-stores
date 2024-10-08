import re
from typing import Optional, Dict, List, Tuple, Generator
from urllib.parse import urljoin

import scrapy
from scrapy.http import Response


class IrvingOilSpider(scrapy.Spider):
    """Spider for scraping Irving Oil store data."""

    name = "irvingoil"
    allowed_domains = ["www.irvingoil.com"]
    start_urls = ["https://www.irvingoil.com/location/geojson/%7B%22ibp%22:false%7D"]

    NAME_XPATH = '//h1[@class="page-title"]/span/text()'
    ADDRESS_ELEM_STRING_XPATH = 'string(//div[@class="location__address"])'
    SERVICES_XPATH = '//div[@class="location__amenities--amenities"]/ul/li/text()'
    HOURS_XPATH = 'normalize-space(//div[contains(@class,"location__hours")]/table/tbody)'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the initial JSON response and yield requests for individual store pages."""
        try:
            data = response.json()
            for store in data.get('features', []):
                store_properties = store.get('properties', {})
                store_link = store_properties.get('link')
                if store_link:
                    yield scrapy.Request(
                        url=urljoin(response.url, store_link),
                        callback=self.parse_store,
                        cb_kwargs={'store_info': store_properties},
                    )
                else:
                    self.logger.warning(f"Missing store link for store: {store_properties.get('name')}")
        except Exception as e:
            self.logger.error(f"Error parsing JSON response: {e}", exc_info=True)

    def parse_store(self, response: Response, store_info: dict) -> Optional[dict]:
        """Parse individual store pages and extract relevant information."""
        if not self._is_us_store(response):
            return None

        item = {
            'name': store_info.get('name'),
            'address': self._get_address(store_info),
            'phone_number': self._get_phone(store_info),
            'location': self._get_location(store_info),
            'services': response.xpath(self.SERVICES_XPATH).getall(),
            'hours': self._get_hours(response),
            'url': response.url,
            'raw': store_info
        }

        # Discard items missing required fields
        required_fields = ['address', 'location', 'url', 'raw']
        if all(item.get(field) for field in required_fields):
            return item
        else:
            missing_fields = [field for field in required_fields if not item.get(field)]
            self.logger.warning(f"Discarding item due to missing required fields: {', '.join(missing_fields)}")
            return None

    def _get_address(self, store_info: dict) -> Optional[str]:
        """Extract and format store address."""
        try:
            address = store_info.get('address')
            if address:
                return address.replace('<br/>', ', ')
            else:
                self.logger.warning(f"Missing address for store: {store_info.get('name')}")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting address: {e}", exc_info=True)
            return None

    def _get_phone(self, store_info: dict) -> Optional[str]:
        """Extract and format phone number."""
        try:
            phone_number = store_info.get('phone')
            if phone_number:
                cleaned_number = re.sub(r'\D', '', phone_number)
                if len(cleaned_number) != 10:
                    self.logger.warning(f"Invalid phone number format: {phone_number}")
                    return None
                return phone_number
            else:
                self.logger.warning(f"Missing phone number for store: {store_info.get('name')}")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting phone number: {e}", exc_info=True)
            return None

    def _is_us_store(self, response: Response) -> bool:
        """Check if store is in the US."""
        address_info = response.xpath(self.ADDRESS_ELEM_STRING_XPATH).get()
        return address_info and "united states" in address_info.lower()

    def _get_location(self, store_info: dict) -> Optional[dict]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('lat')
            longitude = store_info.get('long')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store: {store_info.get('storeNumber')}")
            return None
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return None

    def _get_hours(self, response: Response) -> dict:
        """Extract and parse store hours."""
        try:
            hours = response.xpath(self.HOURS_XPATH).get()
            if not hours:
                self.logger.warning(f"No hours found for store: {response.url}")
                return {}

            normalized_hours = self._normalize_hours_text(hours)
            return self._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    @staticmethod
    def _normalize_hours_text(hours_text: str) -> str:
        """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
        return re.sub(r'[^a-z0-9:]', '', hours_text.lower().replace('to', '').replace('thru', ''))

    def _parse_business_hours(self, input_text: str) -> dict:
        """Parse business hours from input text."""
        DAY_MAPPING = {
            'sun': 'sunday', 'mon': 'monday', 'tue': 'tuesday', 'wed': 'wednesday',
            'thu': 'thursday', 'fri': 'friday', 'sat': 'saturday',
        }
        result = {day: {'open': None, 'close': None} for day in DAY_MAPPING.values()}

        if input_text == "open24hours":
            return {day: {'open': '12:00 am', 'close': '11:59 pm'} for day in DAY_MAPPING.values()}
        elif '24hours' in input_text:
            input_text = input_text.replace('24hours', '12:00am11:59pm')

        day_ranges = self._extract_business_hour_range(input_text)
        single_days = self._extract_business_hours(input_text)

        self._process_day_ranges(day_ranges, result, DAY_MAPPING)
        self._process_single_days(single_days, result, DAY_MAPPING)

        for day, hours in result.items():
            if hours['open'] is None or hours['close'] is None:
                self.logger.warning(f"Missing hours for {day} (input_text={input_text})")

        return result

    def _process_day_ranges(self, day_ranges: List[Tuple[str, str, str, str]],
                            result: dict[str, dict[str, Optional[str]]],
                            DAY_MAPPING: dict[str, str]) -> None:
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
                             result: dict[str, dict[str, Optional[str]]],
                             DAY_MAPPING: dict[str, str]) -> None:
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