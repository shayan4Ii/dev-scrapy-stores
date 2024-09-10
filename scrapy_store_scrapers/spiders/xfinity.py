import re
import scrapy


class XfinitySpider(scrapy.Spider):
    name = "xfinity"
    allowed_domains = ["www.xfinity.com"]
    start_urls = ["https://www.xfinity.com/local/"]

    ADDRESS_ELEM_XPATH = '//h1/address[@id="address"]'

    STREET_ADDRESS_XPATH = './/span[@class="c-address-street-1"]/text()'
    STREET_ADDRESS_2_XPATH = './/span[contains(@class, "c-address-street-2")]/text()'
    CITY_XPATH = './/span[@itemprop="addressLocality"]/text()'
    REGION_XPATH = './/abbr[@itemprop="addressRegion"]/text()'
    POSTAL_CODE_XPATH = './/span[@itemprop="postalCode"]/text()'

    LATITUDE_XPATH = '//meta[@itemprop="latitude"]/@content'
    LONGITUDE_XPATH = '//meta[@itemprop="longitude"]/@content'
    HOURS_TEXT_XPATH = 'normalize-space(//table[@class="c-location-hours-details"]/tbody)'


    def parse(self, response):
        location_urls = response.xpath('//a[@data-ya-track="directory_links"]/@href').getall()

        if location_urls:
            for url in location_urls:
                yield response.follow(url, callback=self.parse)
        
        store_urls = response.xpath('//a[@data-ya-track="dir_viewdetails"]/@href').getall()
        for store_url in store_urls:
            yield response.follow(store_url, callback=self.parse_store)

    def parse_store(self, response):
        parsed_store = {}

        parsed_store['address'] = self._get_address(response)
        parsed_store['location'] = self._get_location(response)
        parsed_store['hours'] = self._get_hours(response)
        
        parsed_store['url'] = response.url

        for key, value in parsed_store.items():
            if value is None or (isinstance(value, (list, dict)) and not value):
                self.logger.warning(f"Missing or empty data for {key}")

        yield parsed_store

    def _get_location(self, response) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = response.xpath(self.LATITUDE_XPATH).get()
            longitude = response.xpath(self.LONGITUDE_XPATH).get()

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude for store")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}

    def _get_address(self, response) -> str:
        """Get the formatted store address."""
        try:
            address_elem = response.xpath(self.ADDRESS_ELEM_XPATH)
            street_address = self.clean_text(address_elem.xpath(self.STREET_ADDRESS_XPATH).get())
            street_address_2 = self.clean_text(address_elem.xpath(self.STREET_ADDRESS_2_XPATH).get())

            address_parts = [street_address, street_address_2]
            street = ", ".join(filter(None, address_parts))

            city = self.clean_text(address_elem.xpath(self.CITY_XPATH).get())
            state = self.clean_text(address_elem.xpath(self.REGION_XPATH).get())
            zipcode = self.clean_text(address_elem.xpath(self.POSTAL_CODE_XPATH).get())

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address for store")
            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
            return ""
    
    def _get_hours(self, response) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = response.xpath(self.HOURS_TEXT_XPATH).get()
            if not hours:
                self.logger.warning(f"No hours found for store")
                return {}

            normalized_hours = self.normalize_hours_text(hours)
            return self._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    def _parse_business_hours(self, input_text: str) -> dict[str, dict[str, str]]:
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
                self.logger.warning(f"Missing hours for {day}({input_text=})")

        return result

    def _process_day_ranges(self, day_ranges: list[tuple[str, str, str, str]], 
                            result: dict[str, dict[str, str]], 
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

    def _process_single_days(self, single_days: list[tuple[str, str, str]], 
                             result: dict[str, dict[str, str]], 
                             DAY_MAPPING: dict[str, str]) -> None:
        """Process single days and update the result dictionary."""
        for day, open_time, close_time in single_days:
            full_day = DAY_MAPPING[day]
            if result[full_day]['open'] and result[full_day]['close']:
                self.logger.debug(f"Day {full_day} already has hours, skipping individual day {day}")
                continue
            result[full_day]['open'] = open_time
            result[full_day]['close'] = close_time

    def _extract_business_hour_range(self, input_string: str) -> list[tuple[str, str, str, str]]:
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

    def _extract_business_hours(self, input_string: str) -> list[tuple[str, str, str]]:
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