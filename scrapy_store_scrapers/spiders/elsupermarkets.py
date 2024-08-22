import scrapy
from typing import Any, Generator

class ElsupermarketsSpider(scrapy.Spider):
    """
    Spider for scraping store information from elsupermarkets.com
    """
    name = "elsupermarkets"
    allowed_domains = ["elsupermarkets.com"]
    start_urls = ["https://elsupermarkets.com/wp-json/elsuper/v1/stores"]

    def parse(self, response: scrapy.http.Response) -> Generator[dict, None, None]:
        """
        Parse the JSON response and yield store information with parsed hours

        :param response: The response object from the HTTP request
        :return: Generator of store dictionaries with parsed hours
        """
        self.logger.info(f"Parsing response from {response.url}")
        try:
            stores = response.json()
        except ValueError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            return

        for store in stores:
            try:
                parsed_store = self.parse_store(store)
                yield parsed_store
            except Exception as e:
                self.logger.error(f"Error parsing store: {e}")

    def parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        """
        Parse individual store information and add parsed hours

        :param store: Dictionary containing store information
        :return: Store dictionary with added parsed hours
        """
        hours_range = store.get("hours", "")
        if not hours_range:
            self.logger.warning(f"No hours found for store: {store.get('id', 'Unknown')}")
            return store

        hours_range = hours_range.lower()
        try:
            open_time, close_time = hours_range.split(" - ")
        except ValueError:
            self.logger.error(f"Invalid hours format for store: {store.get('id', 'Unknown')}")
            return store

        hours = self.create_hours_dict(open_time, close_time)
        store["parsed_hours"] = hours
        return store

    @staticmethod
    def create_hours_dict(open_time: str, close_time: str) -> dict[str, dict[str, str]]:
        """
        Create a dictionary of store hours for each day of the week

        :param open_time: Opening time string
        :param close_time: Closing time string
        :return: Dictionary of store hours for each day
        """
        return {
            day: {"open": open_time, "close": close_time}
            for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        }
