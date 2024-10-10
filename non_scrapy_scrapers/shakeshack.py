import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urljoin

from curl_cffi import requests
from parsel import Selector


class ShakeShackScraper:
    """A scraper for Shake Shack store information."""

    BASE_URL = "https://shakeshack.com/locations"

    # XPath expressions
    HOURS_CONTAINER_XPATH = '//div[@id="location-info--middle__hours"]'
    HOURS_ROW_XPATH = "./div"
    DAY_XPATH = './div[@class="location--weekend"]/text()'
    HOURS_TEXT_XPATH = './div[@class="location--hours"]/text()'
    LOCATION_URLS_XPATH = (
        '//div[@class="geolocation-location js-hide"]//a[text()="More Info"]/@href'
    )
    JSON_LD_XPATH = (
        '//script[@type="application/ld+json" and contains(text(),"LocalBusiness")]/text()'
    )
    SERVICES_XPATH = '//div[@id="location-info--middle__types"]/div/p/text()'

    # Mapping of day abbreviations to full day names
    DAY_ABBR_TO_DAY = {
        "sun": "sunday",
        "mon": "monday",
        "tue": "tuesday",
        "wed": "wednesday",
        "thu": "thursday",
        "fri": "friday",
        "sat": "saturday",
    }

    # Time separator used in hours parsing
    TIME_SEPARATOR = " – "

    # User-Agent and headers
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    )
    SESSION_HEADERS = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "User-Agent": USER_AGENT,
    }

    # Logger configuration
    LOG_FORMAT = "[%(levelname)s] %(asctime)s %(name)s: %(message)s"
    LOG_FILE_PATH = "logs/logs.log"

    # File saving configuration
    DATA_FOLDER = "data"
    FILENAME_PATTERN = "shakeshack-{date}.json"

    def __init__(self):
        """Initialize the ShakeShackScraper."""
        self.session = self._setup_session()
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Set up and configure the logger."""
        logger = logging.getLogger("ShakeShackScraper")
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(self.LOG_FORMAT)

        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(self.LOG_FILE_PATH), exist_ok=True)

        # File handler to write logs to file
        file_handler = logging.FileHandler(self.LOG_FILE_PATH)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Stream handler to output logs to console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger

    def _setup_session(self) -> requests.Session:
        """Set up and configure the requests session."""
        session = requests.Session()
        session.headers.update(self.SESSION_HEADERS)
        return session

    def _get_hours(self, sel: Selector, store_url: str) -> Dict[str, Dict[str, str]]:
        """Extract and parse store hours."""
        hours = {}
        try:
            hours_container = sel.xpath(self.HOURS_CONTAINER_XPATH)
            if not hours_container:
                self.logger.warning(f"No hours found for store {store_url}")
                return hours

            for row in hours_container.xpath(self.HOURS_ROW_XPATH):
                day = row.xpath(self.DAY_XPATH).get(default="").strip().lower()
                hours_text = row.xpath(self.HOURS_TEXT_XPATH).get(default="").strip().lower()

                if not day or not hours_text:
                    continue

                open_time, close_time = hours_text.split(self.TIME_SEPARATOR)
                day_abbr = day[:3]
                day_name = self.DAY_ABBR_TO_DAY.get(day_abbr)
                if day_name:
                    hours[day_name] = {"open": open_time, "close": close_time}
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
        return hours

    def _get_address(self, address_info: Dict[str, Any]) -> str:
        """Format store address."""
        try:
            print(address_info)
            address_parts = [
                address_info.get("streetAddress", ""),
                # address_info.get("address_2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("addressLocality", "")
            state = address_info.get("addressRegion", "")
            zipcode = address_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(
                    f"Missing address information: {address_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    @staticmethod
    def _get_location(loc_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("latitude")
            longitude = loc_info.get("longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)],
                }
        except (ValueError, TypeError) as error:
            logging.warning(f"Invalid latitude or longitude values: {error}")
        return {}

    def get_location_urls(self) -> List[str]:
        """Retrieve all location URLs from the main locations page."""
        try:
            response = self.session.get(self.BASE_URL, timeout=10)
            response.raise_for_status()
            sel = Selector(response.text)
            location_urls = sel.xpath(self.LOCATION_URLS_XPATH).getall()
            full_urls = [urljoin(self.BASE_URL, loc) for loc in location_urls]
            return full_urls
        except requests.RequestException as e:
            self.logger.error(f"Error fetching location URLs: {e}", exc_info=True)
            return []

    def get_location_data(
        self, location_url: str
    ) -> tuple[Optional[List[Dict[str, Any]]], str, Optional[Selector]]:
        """Fetch and parse the JSON-LD data from a location's page."""
        try:
            response = self.session.get(location_url, timeout=10)
            response.raise_for_status()
            sel = Selector(response.text)
            json_text = sel.xpath(self.JSON_LD_XPATH).get()
            if not json_text:
                self.logger.warning(f"No JSON-LD data found at {location_url}")
                return None, response.url, None

            json_data = json.loads(json_text)
            data = json_data.get("@graph", [])
            return data, response.url, sel
        except (json.JSONDecodeError, requests.RequestException) as e:
            self.logger.error(f"Error fetching data from {location_url}: {e}", exc_info=True)
            return None, location_url, None

    def parse_location_data(
        self, data: Optional[List[Dict[str, Any]]], url: str, sel: Selector
    ) -> Dict[str, Any]:
        """Extract relevant information from the JSON-LD data."""
        parsed_store = {}
        if not data:
            self.logger.warning(f"No data to parse for URL: {url}")
            return parsed_store

        if len(data) > 1:
            self.logger.warning(f"Multiple LocalBusiness entries found at {url}")

        for item in data:
            if item.get("@type") == "LocalBusiness":
                address_info = item.get("address", {})
                # Check if addressCountry is not United States
                address_country = address_info.get("addressCountry")
                if address_country != "United States":
                    self.logger.info(
                        f"Skipping location at {url} as it is not in United States"
                    )
                    return {}  # Discard this record

                location_info = item.get("geo", {})

                parsed_store = {
                    "name": item.get("name"),
                    "phone_number": item.get("telephone"),
                    "address": self._get_address(address_info),
                    "location": self._get_location(location_info),
                    "hours": self._get_hours(sel, url),
                    "services": sel.xpath(self.SERVICES_XPATH).getall(),
                    "url": url,
                }
                break  # Only one LocalBusiness entry is expected per location
        else:
            self.logger.warning(f"No LocalBusiness data found at {url}")
        return parsed_store

    def scrape_stores(self) -> Generator[Dict[str, Any], None, None]:
        """Scrape store information and yield each store's data."""
        self.logger.info("Starting to scrape stores")
        location_urls = self.get_location_urls()
        if not location_urls:
            self.logger.error("No location URLs found.")
            return

        total_locations = len(location_urls)
        for idx, location_url in enumerate(location_urls, start=1):
            location_url = "https://shakeshack.com/location/wells-fargo-center-pa#/"
            self.logger.info(f"Scraping location {idx}/{total_locations}: {location_url}")
            data, url, sel = self.get_location_data(location_url)
            if data and sel:
                parsed_store = self.parse_location_data(data, url, sel)
                if parsed_store:
                    yield parsed_store
                else:
                    self.logger.warning(f"Failed to parse data for {location_url}")
            else:
                self.logger.warning(f"No data retrieved for {location_url}")
            break
        self.logger.info("Finished scraping stores")

    def save_to_file(self, stores: List[Dict[str, Any]], filename: Optional[str] = None) -> None:
        """Save scraped store data to a JSON file."""
        if filename is None:
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"{self.DATA_FOLDER}/{self.FILENAME_PATTERN.format(date=current_date)}"

        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        try:
            with open(filename, "w") as f:
                json.dump(stores, f, indent=2)
            self.logger.info(f"Saved {len(stores)} stores to {filename}")
        except IOError as e:
            self.logger.error(f"Error saving data to file: {e}", exc_info=True)


def main():
    """Main function to run the Shake Shack scraper."""
    scraper = ShakeShackScraper()
    stores = [store for store in scraper.scrape_stores() if store]
    scraper.save_to_file(stores)


if __name__ == "__main__":
    main()
