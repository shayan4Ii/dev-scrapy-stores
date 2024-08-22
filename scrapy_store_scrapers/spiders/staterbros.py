import scrapy
import phpserialize
from scrapy_store_scrapers.items import StaterbrosStoreItem
import logging
from typing import Any, Optional, Generator

class StaterbrosSpider(scrapy.Spider):
    """
    Spider for scraping store information from Stater Bros website.
    """
    name = "staterbros"
    allowed_domains = ["www.staterbros.com"]
    start_urls = ["https://www.staterbros.com/wp-json/api/stores/"]

    STORE_PAGE_URL_FORMAT = "https://www.staterbros.com/stores/{}"

    NAME_XPATH = "//h1[@itemprop='headline']/text()"
    ADDRESS_XPATH = "//div[contains(@class, 'elementor-widget-heading') and .//h3[contains(./text(), 'Address')]]/following-sibling::div[contains(@class, 'elementor-widget-text-editor')]/div/text()"
    PHONE_XPATH = "//div[contains(@class, 'elementor-widget-heading') and .//h3[contains(./text(), 'Phone Number')]]/following-sibling::div[contains(@class, 'elementor-widget-text-editor')]/div/text()"
    MON_SUN_HOURS_XPATH = "//div[@class='elementor-icon-box-content'][contains(.//span,'Store Hours')]//p[contains(., 'MON-SUN')]/text()"
    SERVICES_XPATH = "//div[@class='elementor-widget-wrap elementor-element-populated' and .//span[contains(text(),'Store Features')]]//div[@class='elementor-widget-container']/p/text()"

    def parse(self, response: scrapy.http.Response) -> Generator[scrapy.Request, None, None]:
        """
        Parse the initial response and yield requests for individual store pages.

        Args:
            response (scrapy.http.Response): The response object for the initial API call.

        Yields:
            scrapy.Request: Requests for individual store pages.
        """
        try:
            data = response.json()
            stores_lat_lng = self.get_store_lat_lng(data)

            for store_no, lat_lng_dict in stores_lat_lng.items():
                yield scrapy.Request(
                    self.STORE_PAGE_URL_FORMAT.format(store_no),
                    callback=self.parse_store_page,
                    cb_kwargs={"store_no": store_no, "lat": lat_lng_dict["lat"], "lng": lat_lng_dict["lng"]},
                )
        except Exception as e:
            self.logger.error(f"Error in parse method: {str(e)}")

    def parse_store_page(self, response: scrapy.http.Response, store_no: str, lat: str, lng: str) -> StaterbrosStoreItem:
        """
        Parse individual store pages and extract relevant information.

        Args:
            response (scrapy.http.Response): The response object for the store page.
            store_no (str): The store number.
            lat (str): The latitude of the store.
            lng (str): The longitude of the store.

        Returns:
            StaterbrosStoreItem: An item containing the extracted store information.
        """
        try:
            store_data = StaterbrosStoreItem()
            store_data["number"] = store_no
            store_data["name"] = response.xpath(self.NAME_XPATH).re_first('#\d+(.*)').strip()
            store_data["address"] = self.clean_text(response.xpath(self.ADDRESS_XPATH).get())
            store_data["phone_number"] = self.clean_text(response.xpath(self.PHONE_XPATH).get())
            store_data["services"] = self.get_services(response)
            store_data["location"] = self.get_location(lat, lng)
            store_data["hours"] = self.get_hours(response)
            return store_data
        except Exception as e:
            self.logger.error(f"Error in parse_store_page method for store {store_no}: {str(e)}")
            return None

    def get_hours(self, response: scrapy.http.Response) -> dict[str, dict[str, str]]:
        """
        Extract store hours from the response.

        Args:
            response (scrapy.http.Response): The response object for the store page.

        Returns:
            dict[str, dict[str, str]]: A dictionary containing the store hours for each day.
        """
        try:
            hours = response.xpath(self.MON_SUN_HOURS_XPATH).get()
            self.logger.info(f"Extracted hours: {hours}")
            
            if not hours:
                return {}
            
            hours = hours.replace(" ", "").lower()
            
            if "–" in hours:
                open_time, close_time = hours.split("–")
            else:
                open_time, close_time = hours.split("-")
            
            hours_dict = {}
            for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
                hours_dict[day] = {
                    "open": open_time,
                    "close": close_time
                }
            
            return hours_dict
        except Exception as e:
            self.logger.error(f"Error in get_hours method: {str(e)}")
            return {}

    def get_location(self, lat: str, long: str) -> dict[str, Any]:
        """
        Create a location dictionary from latitude and longitude.

        Args:
            lat (str): The latitude of the store.
            long (str): The longitude of the store.

        Returns:
            dict[str, Any]: A dictionary containing the location information.
        """
        try:
            return {
                "type": "Point",
                "coordinates": [float(long), float(lat)]
            }
        except ValueError as e:
            self.logger.error(f"Error converting lat/long to float: {str(e)}")
            return {}

    def get_services(self, response: scrapy.http.Response) -> list[str]:
        """
        Extract services from the response.

        Args:
            response (scrapy.http.Response): The response object for the store page.

        Returns:
            list[str]: A list of services offered by the store.
        """
        try:
            services = response.xpath(self.SERVICES_XPATH).getall()
            return [self.clean_text(service) for service in services if self.clean_text(service)]
        except Exception as e:
            self.logger.error(f"Error in get_services method: {str(e)}")
            return []

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """
        Clean and strip whitespace from text.

        Args:
            text (Optional[str]): The text to clean.

        Returns:
            str: The cleaned text.
        """
        return text.strip() if text else ""

    @staticmethod
    def get_store_lat_lng(data: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
        """
        Extract latitude and longitude information for each store.

        Args:
            data (list[dict[str, Any]]): The raw data from the initial API response.

        Returns:
            dict[str, dict[str, str]]: A dictionary containing latitude and longitude for each store.
        """
        stores_lat_lng = {}
        for data_dict in data:
            if data_dict["store_number"] not in stores_lat_lng:
                stores_lat_lng[data_dict["store_number"]] = {}
            
            if data_dict["meta_key"] == "map":
                meta_value = data_dict["meta_value"]
                try:
                    meta_value_dict = phpserialize.loads(meta_value.encode(), decode_strings=True)
                    stores_lat_lng[data_dict["store_number"]]["lat"] = meta_value_dict["lat"]
                    stores_lat_lng[data_dict["store_number"]]["lng"] = meta_value_dict["lng"]
                except Exception as e:
                    logging.error(f"Error deserializing meta_value for store {data_dict['store_number']}: {str(e)}")

        return stores_lat_lng