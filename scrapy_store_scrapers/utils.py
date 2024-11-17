from datetime import datetime
from typing import Dict, Iterable, Any, Generator, Union, List
import json
from scrapy.http import Response, Request

def should_abort_request(request):
    not_allowed = [".facebook.net","googlemanager.com","stackadapt.com","google-analytics.com","clarity.ms","googletagmanager.com"
                   "youtube.com"]
    return (
        request.resource_type == "image"
        or ".jpg" in request.url
        or ".woff" in request.url
        or any([True for domain in not_allowed if domain in request.url])
    )

def convert_to_12h_format(time_str: str) -> str:
    """Convert time to 12-hour format."""
    time_str = time_str.lower()
    if not time_str:
        return None
    try:
        if "am" in time_str:
            time_str = time_str.lower().replace("am", "").strip()
            period = "am"
        elif "pm" in time_str:
            time_str = time_str.lower().replace("pm","").strip()
            period = "pm"
        else:
            period = None

        if ":" in time_str:
            _format = '%H:%M'
        elif "." in time_str:
            _format = '%H.%M'
        elif time_str.isdigit() and len(time_str) == 4:
            _format = '%H%M'
        else:   
            _format = '%H'
        time_obj = datetime.strptime(time_str, _format).time()
        
        if period:
            time_str = time_obj.strftime(f'%I:%M {period}').lower().lstrip('0')
        else:
            time_str = time_obj.strftime('%I:%M %p').lower().lstrip('0')

        return time_str
    except ValueError:
        return None
    

def load_zipcode_data(zipcode_file_path: str) -> list[dict[str, Union[str, float]]]:
    """Load zipcode data from a JSON file."""
    with open(zipcode_file_path, 'r') as f:
        return json.load(f)
    


import re
import logging
import json

class HoursExample():
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    @staticmethod
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(r'(\d+)([ap]m)', r'\1 \2', time_str)

    @staticmethod
    def normalize_hours_text(hours_text: str) -> str:
        """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
        return re.sub(r'[^a-z0-9:]', '', hours_text.lower().replace('to', '').replace('thru', ''))

    def _get_hours(self, raw_store_data: dict) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = raw_store_data.get("openingHours", "")
            if not hours:
                self.logger.warning(f"No hours found for store {raw_store_data.get('name', 'Unknown')}")
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

        # Extract and process day ranges
        day_ranges = self._extract_business_hour_range(input_text)
        for start_day, end_day, open_time, close_time in day_ranges:
            start_index = list(DAY_MAPPING.keys()).index(start_day)
            end_index = list(DAY_MAPPING.keys()).index(end_day)
            if end_index < start_index:  # Handle cases like "Saturday to Sunday"
                end_index += 7
            for i in range(start_index, end_index + 1):
                day = list(DAY_MAPPING.keys())[i % 7]
                full_day = DAY_MAPPING[day]
                if result[full_day]['open'] and result[full_day]['close']:
                    self.logger.debug(f"Day {full_day} already has hours({input_text=}), skipping range {start_day} to {end_day}")
                    continue
                result[full_day]['open'] = convert_to_12h_format(open_time)
                result[full_day]['close'] = convert_to_12h_format(close_time)

        # Extract and process individual days (overwriting any conflicting day ranges)
        single_days = self._extract_business_hours(input_text)
        for day, open_time, close_time in single_days:
            full_day = DAY_MAPPING[day]
            if result[full_day]['open'] and result[full_day]['close']:
                self.logger.debug(f"Day {full_day} already has hours({input_text=}), skipping individual day {day}")
                continue
            result[full_day]['open'] = convert_to_12h_format(open_time)
            result[full_day]['close'] = convert_to_12h_format(close_time)

        # Log warning for any missing days
        for day, hours in result.items():
            if hours['open'] is None or hours['close'] is None:
                self.logger.warning(f"Missing hours for {day}({input_text=})")

        return result

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
        if re.match(time_only_re, input_string):
            open_time = f"{time_only_match.group(1)} {time_only_match.group(2)}"
            close_time = f"{time_only_match.group(3)} {time_only_match.group(4)}"
            return [("sun", "sat", open_time, close_time)]

        pattern = f"({days_re}{day_suffix_re})({days_re}{day_suffix_re}){optional_colon_re}?{time_re}{time_re}"
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
        days_re = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
        day_suffix_re = r"(?:day)?"
        optional_colon_re = r"(?::)?"
        time_re = r"(\d{1,2}(?::\d{2})?)([ap]m)"
        
        pattern = f"({days_re}{day_suffix_re}){optional_colon_re}?{time_re}{time_re}"
        matches = re.finditer(pattern, input_string, re.MULTILINE)
        
        results = []
        for match in matches:
            day = match.group(1)[:3]
            open_time = f"{match.group(2)} {match.group(3)}"
            close_time = f"{match.group(4)} {match.group(5)}"
            results.append((day, open_time, close_time))
        
        return results
    

########################

# from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler
# from scrapy_impersonate.handler import ImpersonateDownloadHandler    


# class MuxDownloadHandler:
#     lazy = False
    
#     def __init__(self, crawler):
#         self.playwright_handler = ScrapyPlaywrightDownloadHandler.from_crawler(crawler)
#         self.impersonate_handler = ImpersonateDownloadHandler.from_crawler(crawler)

#     @classmethod
#     def from_crawler(cls, crawler):
#         return cls(crawler)

#     def download_request(self, request, spider):
#         if request.meta.get('playwright'):
#             return self.playwright_handler.download_request(request, spider)
#         elif request.meta.get('impersonate'):
#             return self.impersonate_handler.download_request(request, spider)
#         else:
#             return self.playwright_handler.download_request(request, spider)