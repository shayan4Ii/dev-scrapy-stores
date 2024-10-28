import json
from typing import Any, Generator, Iterable, Optional, Union, Dict
from scrapy.http import Response, Request
from scrapy.exceptions import CloseSpider, DropItem
from datetime import datetime
import requests

import scrapy



class BaseSpider(scrapy.Spider):
    name = "base"
    zipcode_file_path = "data/zipcode_lat_long.json"
    

    def _load_zipcode_data(self) -> list[dict[str, Union[str, float]]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s",
                              self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error(
                "Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []
    

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return ""
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            return time_str


    def _is_valid_item(self, item: Dict):
        required_fields = ["address", "location", "url"]
        exists = all(item.get(field) for field in required_fields)
        if exists:
            coordinates = item.get("location",{}).get("coordinates",[])
            if list(filter(None, coordinates)):
                return self._validate_coordinates(float(str(coordinates[1])), float(str(coordinates[0])))


    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        return -90 <= lat <= 90 and -180 <= lon <= 180