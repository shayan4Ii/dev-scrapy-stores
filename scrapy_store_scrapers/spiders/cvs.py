import json
import re
from typing import Any, Generator, Optional

import scrapy


class CvsSpider(scrapy.Spider):
    """Spider for scraping CVS store information."""

    name = "cvs"
    allowed_domains = ["www.cvs.com"]
    start_urls = ['https://www.cvs.com/store-locator/landing']
    
    RESULTS_PER_PAGE = 25
    API_KEY_RE = re.compile(r"slKey&#34;:&#34;(.*?)&#34;")

    custom_settings = {
        'CONCURRENT_REQUESTS': 32,
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.processed_store_ids: set[str] = set()

    def parse(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """Parse the initial response and start the store search process."""
        try:
            api_key = self._extract_api_key(response)
            zipcodes_data = self._load_zipcode_data()

            for zipcode_data in zipcodes_data:
                zipcode = zipcode_data['zipcode']
                url = self._build_search_url(zipcode)
                yield scrapy.Request(
                    url,
                    self.parse_stores,
                    headers=self._get_headers(api_key),
                    meta={'page': 1, 'zipcode': zipcode},
                    dont_filter=True
                )
                break
        except Exception as e:
            self.logger.error(f"Error in parse method: {e}", exc_info=True)

    def parse_stores(self, response: scrapy.http.Response) -> Generator[dict, None, None]:
        """Parse the store search results and yield store data."""
        try:
            data = json.loads(response.text)
            stores = data.get('storeList', [])
            self.logger.info(f"Found {len(stores)} stores for zipcode {response.meta['zipcode']}")

            for store in stores:
                store_id = store.get('storeInfo', {}).get('storeId')
                if store_id and store_id not in self.processed_store_ids:
                    self.processed_store_ids.add(store_id)
                    yield self._get_parsed_store_data(store)

            yield from self._handle_pagination(response, data)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.url}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error in parse_stores method: {e}", exc_info=True)

    def _get_parsed_store_data(self, store: dict) -> dict:
        """Extract and structure store data from the API response."""
        store_info = store.get('storeInfo', {})
        store_address_info = store.get('address', {})
        
        return {
            "number": self._get_number(store_info),
            "phone_number": self._get_phone_number(store_info),
            "address": self._get_address(store_address_info),
            "location": self._get_location(store_info),
            "hours": self._get_hours(store),
            "raw_dict": store
        }

    @staticmethod
    def _get_number(store_info: dict) -> str:
        """Get the store number."""
        return store_info.get("storeId", "")

    @staticmethod
    def _get_phone_number(store_info: dict) -> Optional[str]:
        """Get the store phone number."""
        phone_numbers = store_info.get("phoneNumbers", [])
        if phone_numbers and 'pharmacy' in phone_numbers[0]:
            return phone_numbers[0]['pharmacy']
        return None

    def _get_hours(self, store: dict) -> dict:
        """Get the store hours."""
        day_name_map = {
            "SUN": "sunday", "MON": "monday", "TUE": "tuesday",
            "WED": "wednesday", "THU": "thursday", "FRI": "friday", "SAT": "saturday"
        }

        hours = {}
        dep_hours = store.get("hours", {}).get("departments", [])
        for dep in dep_hours:
            if dep.get("name") == "pharmacy":
                for day_hours in dep.get("regHours", []):
                    day = day_hours.get("weekday")
                    full_day_name = day_name_map.get(day, day)
                    hours[full_day_name] = {
                        "open": day_hours.get("startTime", "").lower(),
                        "close": day_hours.get("endTime", "").lower()
                    }
                break
        return hours

    def _get_location(self, store_info: dict) -> Optional[dict]:
        """Get the store location in GeoJSON Point format."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')
            
            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude")
            return None
        except ValueError:
            self.logger.warning(f"Invalid latitude or longitude values: {latitude}, {longitude}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return None

    def _get_address(self, store_address_info: dict) -> str:
        """Get the formatted store address."""
        return ", ".join(filter(None, [
            store_address_info.get("street", ""),
            store_address_info.get("city", ""),
            store_address_info.get("state", ""),
            store_address_info.get("zip", "")
        ]))

    def _extract_api_key(self, response: scrapy.http.Response) -> str:
        """Extract the API key from the response."""
        api_key_match = self.API_KEY_RE.search(response.text)
        if not api_key_match:
            raise ValueError("Failed to extract API key from response")
        return api_key_match.group(1)

    def _build_search_url(self, zipcode: str) -> str:
        """Build the URL for store search."""
        return (f"https://www.cvs.com/api/locator/v2/stores/search?"
                f"searchBy=USER-TEXT&latitude=&longitude=&searchText={zipcode}&"
                f"searchRadiusInMiles=&maxItemsInResult=&filters=&"
                f"resultsPerPage={self.RESULTS_PER_PAGE}&pageNum=1")

    @staticmethod
    def _get_headers(api_key: str) -> dict:
        """Get the headers for API requests."""
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "consumer": "SLP",
            "priority": "u=1, i",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-api-key": api_key,
            "Referer": "https://www.cvs.com/store-locator/landing",
            "Referrer-Policy": "origin-when-cross-origin"
        }

    def _load_zipcode_data(self) -> list:
        """Load zipcode data from a JSON file."""
        try:
            with open("data/tacobell_zipcode_data.json") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found")
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file")
        return []

    def _handle_pagination(self, response: scrapy.http.Response, data: dict) -> Generator[scrapy.Request, None, None]:
        """Handle pagination for store results."""
        total_results = data.get('totalResults', 0)
        current_page = response.meta['page']

        if total_results > current_page * self.RESULTS_PER_PAGE:
            self.logger.info(f"Found more than {current_page * self.RESULTS_PER_PAGE} stores. Fetching next page...")
            next_page = current_page + 1
            next_url = response.url.replace(f"pageNum={current_page}", f"pageNum={next_page}")
            yield scrapy.Request(
                next_url,
                self.parse_stores,
                headers=self._get_headers(response.meta.get('api_key')),
                meta={'page': next_page, 'zipcode': response.meta['zipcode']},
                dont_filter=True
            )