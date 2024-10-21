import re
import json
from typing import Any, Generator, Optional

import scrapy
from scrapy.http import Request, Response
from scrapy.exceptions import DropItem


class SbarroSpider(scrapy.Spider):
    """Spider for scraping Sbarro restaurant location data."""
    
    # Spider configuration
    name = "sbarro"
    allowed_domains = ["sbarro.com"]
    
    # API endpoints and data sources
    STORES_API_URL = "https://sbarro.com/locations/?user_search={zipcode}&radius=25&unit=MI&count=All"
    ZIPCODE_FILE_PATH = "data/tacobell_zipcode_data.json"
    
    # Regular expressions for parsing
    TIME_FORMAT_RE = r'(\d+)([ap]m)'
    NORMALIZE_HOURS_RE = r'[^a-z0-9:]'
    DAYS_RE = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
    DAY_SUFFIX_RE = r"(?:day)?"
    OPTIONAL_COLON_RE = r"(?::)?"
    TIME_RE = r"(\d{1,2}(?::\d{2})?)([ap]m)"
    TIME_ONLY_RE = r"^(\d{1,2}(?::\d{2})?)([ap]m)(\d{1,2}(?::\d{2})?)([ap]m)$"
    BUSINESS_HOURS_PATTERN = r"({days}{day_suffix})({days}{day_suffix}){colon}?{time}{time}"
    SINGLE_DAY_PATTERN = r"({days}{day_suffix}){colon}?{time}{time}"
    
    # XPath selectors
    LOCATIONS_SELECTOR = '//section[@class="locations-result"]'
    LOCATION_URL_SELECTOR = './h2[@class="location-name "]/a/@href'
    LATITUDE_SELECTOR = './@data-latitude'
    LONGITUDE_SELECTOR = './@data-longitude'
    MAIN_CONTENT_SELECTOR = '//div[@class="row nopad location-group-wrap" and contains(@id, "location")]'
    STORE_NAME_SELECTOR = '//h1[@class="location-name "]/text()'
    PHONE_SELECTOR = '//div[@id="location-content-ctas"]/div[@class="location-phone location-cta"]//span[@class="btn-label"]/text()'
    ADDRESS_SELECTOR = '//p[contains(@class,"location-address")]/text()'
    SERVICES_SELECTOR = '//div[@id="location-content-ctas"]/div[@class="location-ordering location-cta"]//span[@class="btn-label"]/text()'
    HOURS_SELECTOR = '//div[@class="location-hours"]'
    
    # Time constants
    DEFAULT_24H_OPEN = "12:00 am"
    DEFAULT_24H_CLOSE = "11:59 pm"
    
    # Day mapping for standardization
    DAY_MAPPING = {
        'sun': 'sunday', 'mon': 'monday', 'tue': 'tuesday', 'wed': 'wednesday',
        'thu': 'thursday', 'fri': 'friday', 'sat': 'saturday',
    }
    
    # Required fields for store data
    REQUIRED_FIELDS = {'address', 'location', 'url'}
    
    # GeoJSON constants
    GEOJSON_TYPE = "Point"

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests based on zipcode data."""
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            url = self.STORES_API_URL.format(zipcode=zipcode["zipcode"])
            yield scrapy.Request(
                url=url,
                callback=self.parse,
            )

    def _load_zipcode_data(self) -> list[dict[str, Any]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.ZIPCODE_FILE_PATH, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode file not found: %s", self.ZIPCODE_FILE_PATH)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode file: %s", self.ZIPCODE_FILE_PATH)
        except Exception as e:
            self.logger.error("Error loading zipcode data: %s", str(e), exc_info=True)
        return []

    def parse(self, response: Response) -> Generator[Request, None, None]:
        """Parse the store locations list page."""
        try:
            locations = response.xpath(self.LOCATIONS_SELECTOR)
            for location in locations:
                url = location.xpath(self.LOCATION_URL_SELECTOR).get()
                if not url:
                    self.logger.warning("Missing URL for location")
                    continue
                    
                latitude = location.xpath(self.LATITUDE_SELECTOR).get()
                longitude = location.xpath(self.LONGITUDE_SELECTOR).get()
                
                yield response.follow(
                    url,
                    callback=self.parse_store,
                    cb_kwargs={"latitude": latitude, "longitude": longitude, "zipcode_url": response.url},
                )
        except Exception as e:
            self.logger.error("Error parsing locations page: %s", str(e), exc_info=True)

    def parse_store(self, response: Response, latitude: Optional[str], longitude: Optional[str], zipcode_url: Optional[str]) -> Generator[dict[str, Any], None, None]:
        """Parse individual store details page."""
        try:
            main_content = response.xpath(self.MAIN_CONTENT_SELECTOR)
            
            store = {
                "name": main_content.xpath(self.STORE_NAME_SELECTOR).get(),
                "phone_number": main_content.xpath(self.PHONE_SELECTOR).get('').strip(),
                "address": main_content.xpath(self.ADDRESS_SELECTOR).get(),
                "location": self._get_location(latitude, longitude),
                "services": main_content.xpath(self.SERVICES_SELECTOR).getall(),
                "hours": self._get_hours(response),
                "url": response.url,
            }
            
            missing_fields = self.REQUIRED_FIELDS - set(k for k, v in store.items() if v)
            if missing_fields:
                self.logger.warning("Missing required fields: %s for store: %s, %s, %s", missing_fields, store.get('url'), response.url, zipcode_url)
                raise DropItem(f"Missing required fields: {missing_fields}")
            
            yield store
            
        except Exception as e:
            self.logger.error("Error parsing store page: %s", str(e), exc_info=True)

    def _get_location(self, latitude: Optional[str], longitude: Optional[str]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            if latitude is not None and longitude is not None:
                return {
                    "type": self.GEOJSON_TYPE,
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude for store")
        except ValueError as e:
            self.logger.warning("Invalid coordinate values: %s", str(e))
        except Exception as e:
            self.logger.error("Error processing location data: %s", str(e), exc_info=True)
        return {}

    @staticmethod
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(SbarroSpider.TIME_FORMAT_RE, r'\1 \2', time_str)

    def _normalize_hours_text(self, hours_text: str) -> str:
        """Normalize hours text by removing non-alphanumeric characters."""
        return re.sub(
            self.NORMALIZE_HOURS_RE,
            '',
            hours_text.lower().replace('to', '').replace('thru', '')
        )

    def _get_hours(self, response: Response) -> dict[str, dict[str, Optional[str]]]:
        """Extract and parse store hours."""
        try:
            hours = response.xpath(f'normalize-space({self.HOURS_SELECTOR})').get()
            if not hours:
                self.logger.warning("No hours found for store: %s", response.url)
                return {}

            normalized_hours = self._normalize_hours_text(hours)
            return self._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error("Error extracting hours: %s", str(e), exc_info=True)
            return {}

    def _parse_business_hours(self, input_text: str) -> dict[str, dict[str, Optional[str]]]:
        """Parse business hours from input text."""
        result = {day: {'open': None, 'close': None} for day in self.DAY_MAPPING.values()}

        if input_text == "open24hours":
            return {day: {'open': self.DEFAULT_24H_OPEN, 'close': self.DEFAULT_24H_CLOSE} 
                   for day in self.DAY_MAPPING.values()}
                   
        if 'open24hours' in input_text:
            input_text = input_text.replace('open24hours', f'{self.DEFAULT_24H_OPEN}{self.DEFAULT_24H_CLOSE}')

        day_ranges = self._extract_business_hour_range(input_text)
        for start_day, end_day, open_time, close_time in day_ranges:
            start_index = list(self.DAY_MAPPING.keys()).index(start_day)
            end_index = list(self.DAY_MAPPING.keys()).index(end_day)
            if end_index < start_index:
                end_index += 7
            
            for i in range(start_index, end_index + 1):
                day = list(self.DAY_MAPPING.keys())[i % 7]
                full_day = self.DAY_MAPPING[day]
                if result[full_day]['open'] and result[full_day]['close']:
                    self.logger.debug(
                        "Day %s already has hours (input_text=%s), skipping range %s to %s",
                        full_day, input_text, start_day, end_day
                    )
                    continue
                result[full_day]['open'] = open_time
                result[full_day]['close'] = close_time

        single_days = self._extract_business_hours(input_text)
        for day, open_time, close_time in single_days:
            full_day = self.DAY_MAPPING[day]
            if result[full_day]['open'] and result[full_day]['close']:
                self.logger.debug(
                    "Day %s already has hours (input_text=%s), skipping individual day %s",
                    full_day, input_text, day
                )
                continue
            result[full_day]['open'] = open_time
            result[full_day]['close'] = close_time

        for day, hours in result.items():
            if hours['open'] is None or hours['close'] is None:
                self.logger.warning("Missing hours for %s (input_text=%s)", day, input_text)

        return result

    def _extract_business_hour_range(self, input_string: str) -> list[tuple[str, str, str, str]]:
        """Extract business hour ranges from input string."""
        if "daily" in input_string:
            time_match = re.search(f"{self.TIME_RE}{self.TIME_RE}", input_string)
            if time_match:
                open_time = f"{time_match.group(1)} {time_match.group(2)}"
                close_time = f"{time_match.group(3)} {time_match.group(4)}"
                return [("sun", "sat", open_time, close_time)]
        
        time_only_match = re.match(self.TIME_ONLY_RE, input_string)
        if time_only_match:
            open_time = f"{time_only_match.group(1)} {time_only_match.group(2)}"
            close_time = f"{time_only_match.group(3)} {time_only_match.group(4)}"
            return [("sun", "sat", open_time, close_time)]

        pattern = self.BUSINESS_HOURS_PATTERN.format(
            days=self.DAYS_RE,
            day_suffix=self.DAY_SUFFIX_RE,
            colon=self.OPTIONAL_COLON_RE,
            time=self.TIME_RE
        )
        matches = re.finditer(pattern, input_string, re.MULTILINE)
        
        results = []
        for match in matches:
            start_day = match.group(1)[:3]
            end_day = match.group(2)[:3]
            open_time = f"{match.group(3)} {match.group(4)}"
            close_time = f"{match.group(5)} {match.group(6)}"
            results.append((start_day, end_day, open_time, close_time))
        
        return results

    def _extract_business_hours(self, input_string: str) -> list[tuple[str, str, str]]:
        """Extract individual business hours from input string."""
        pattern = self.SINGLE_DAY_PATTERN.format(
            days=self.DAYS_RE,
            day_suffix=self.DAY_SUFFIX_RE,
            colon=self.OPTIONAL_COLON_RE,
            time=self.TIME_RE
        )
        matches = re.finditer(pattern, input_string, re.MULTILINE)
        
        results = []
        for match in matches:
            day = match.group(1)[:3]
            open_time = f"{match.group(2)} {match.group(3)}"
            close_time = f"{match.group(4)} {match.group(5)}"
            results.append((day, open_time, close_time))
        
        return results
