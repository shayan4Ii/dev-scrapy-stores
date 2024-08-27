from typing import Generator, Optional
import re
import logging
from scrapy import Spider
from scrapy.http import Response, Request

class ShopmarketbasketSpider(Spider):
    """Spider for scraping ShopMarketBasket store information."""

    name = "shopmarketbasket"
    allowed_domains = ["www.shopmarketbasket.com"]
    start_urls = ["https://www.shopmarketbasket.com/store-locations-rest"]

    NAME_XPATH = "//section[@class='flyer-header']/h2/span/text()"
    PHONE_XPATH = "//div[contains(@class, 'field--name-field-phone-number')]/div/a/text()"
    SERVICES_XPATH = "//div[@class='departments']/ul/li/text()"
    MON_SAT_HOURS_XPATH = "normalize-space(//div[contains(@class,'field--name-field-hours')]/div/p[contains(., 'Monday - Saturday')])"
    SUN_HOURS_XPATH = "normalize-space(//div[contains(@class,'field--name-field-hours')]/div/p[contains(., 'Sunday')])"
    ADDRESS_CONTAINER_XPATH = "//p[@class='address']"
    STREET_ADDRESS_XPATH = ".//span[@class='address-line1']/text()"
    CITY_XPATH = ".//span[@class='locality']/text()"
    STATE_XPATH = ".//span[@class='administrative-area']/text()"
    ZIP_XPATH = ".//span[@class='postal-code']/text()"

    MON_SAT_HOURS_RANGE_REGEX = r"Monday - Saturday(.*)"
    SUN_HOURS_REGEX = r"Sunday(.*)"

    def parse(self, response: Response) -> Generator[Request, None, None]:
        """Parse the initial response and yield requests for individual store pages."""
        try:
            stores = response.json()
            for store in stores[:20]:
                yield response.follow(
                    store["path"],
                    callback=self.parse_store,
                    cb_kwargs={"geo_loc_str": store["field_geolocation"]},
                    errback=self.handle_error
                )
        except Exception as e:
            self.logger.error(f"Error parsing store list: {str(e)}")

    def parse_store(self, response: Response, geo_loc_str: str) -> dict:
        """Parse individual store page and extract store information."""
        try:
            return {
                "name": self.get_name(response),
                "address": self.get_address(response),
                "phone": self.get_phone(response),
                "location": self.get_location(geo_loc_str),
                "hours": self.get_hours(response),
                "services": self.get_services(response)
            }
        except Exception as e:
            self.logger.error(f"Error parsing store page: {str(e)}")
            return {}

    def get_name(self, response: Response) -> Optional[str]:
        """Extract store name."""
        return self.safe_extract(response, self.NAME_XPATH)

    def get_phone(self, response: Response) -> Optional[str]:
        """Extract store phone number."""
        return self.safe_extract(response, self.PHONE_XPATH)

    def get_services(self, response: Response) -> list:
        """Extract store services."""
        return response.xpath(self.SERVICES_XPATH).getall()

    def get_hours(self, response: Response) -> dict[str, dict[str, str]]:
        """Extract store hours."""
        hours_info = {}

        mon_sat_hours_range_text = response.xpath(self.MON_SAT_HOURS_XPATH).re_first(self.MON_SAT_HOURS_RANGE_REGEX,"").strip()
        sun_hours = response.xpath(self.SUN_HOURS_XPATH).re_first(self.SUN_HOURS_REGEX,"").strip()


        if mon_sat_hours_range_text and sun_hours:
            mon_sat_open, mon_sat_close = mon_sat_hours_range_text.split("-")
            sun_open, sun_close = sun_hours.split("-")

            for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
                if day == "sunday":
                    hours_info[day] = {
                        "open": sun_open.strip(),
                        "close": sun_close.strip()
                    }
                else:
                    hours_info[day] = {
                        "open": mon_sat_open.strip(),
                        "close": mon_sat_close.strip()
                    }
        else:
            self.logger.error("Error extracting hours")

        return hours_info
    
    def get_address(self, response: Response) -> str:
        """Extract store address."""
        try:
            address_container = response.xpath(self.ADDRESS_CONTAINER_XPATH)
            street = self.safe_extract(address_container, self.STREET_ADDRESS_XPATH)
            city = self.safe_extract(address_container, self.CITY_XPATH)
            state = self.safe_extract(address_container, self.STATE_XPATH)
            postal_code = self.safe_extract(address_container, self.ZIP_XPATH)

            return f"{street}, {city}, {state} {postal_code}"
        except Exception as e:
            self.logger.error(f"Error extracting address: {str(e)}")
            return ""

    def get_location(self, geo_loc_str: str) -> Optional[dict]:
        """Extract and format store location."""
        try:
            latitude, longitude = map(float, geo_loc_str.split(","))
            return {
                "type": "Point",
                "coordinates": [longitude, latitude]
            }
        except Exception as e:
            self.logger.error(f"Error extracting location: {str(e)}")
            return None

    def safe_extract(self, selector, xpath: str) -> str:
        """Safely extract text from an XPath selector."""
        return selector.xpath(xpath).get('').strip()
        
    def handle_error(self, failure):
        """Handle request failures."""
        self.logger.error(f"Request failed: {failure.value}")