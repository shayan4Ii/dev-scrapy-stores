import logging
from typing import Generator, Optional

import scrapy
from scrapy.http import Response


class TractorSupplySpider(scrapy.Spider):
    """Spider for scraping TractorSupply store information."""

    name = "tractorsupply"
    allowed_domains = ["www.tractorsupply.com"]
    start_urls = ["https://www.tractorsupply.com/tsc/store-locations"]
    # Custom settings to avoid getting banned
    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        # "DOWNLOAD_DELAY": 1,
        # "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    }

    # XPath constants
    STATE_URLS_XPATH = '//ul[@class="store-list"]/li/a/@href'
    STORE_LI_ELEMS_XPATH = '//ul[@class="store-list"]/li[@class="store-list-item"]'
    STORE_INPUT_XPATH_FORMAT = './input[@class="{}"]/@value'
    NAME_XPATH = '//div[@class="store-details"]/h1/text()'
    STORE_NUMBER_XPATH = '//div[@class="store-details"]/h1/span[@class="store-no"]/text()'
    PHONE_XPATH = '//span[@itemprop="Telephone"]/text()'
    ADDRESS_ELEM_XPATH = '//div[@class="store-address"]//address'
    STREET_XPATH = './span[@itemprop="streetAddress"]/text()'
    CITY_XPATH = './span[@itemprop="addressLocality"]/text()'
    STATE_XPATH = './span[@itemprop="addressRegion"]/text()'
    ZIP_XPATH = './span[@itemprop="postalCode"]/text()'
    SERVICES_ELEMS_XPATH = '//div[contains(@class, "store-services")]//div[@class="card-header"]/h2'
    PETVET_CLINIC_ELEM_XPATH = '//div[@id="pet-vet-clinic-info"]'
    STORE_DAYS_XPATH = '//div[@itemprop="openingHoursSpecification"]/div[contains(@class,"store-days")]/div/span/text()'
    STORE_TIMES_ELEM_XPATH = '//div[@itemprop="openingHoursSpecification"]/div[contains(@class,"store-time")]/div/span'
    OPEN_TIME_XPATH = './span[@itemprop="opens"]/text()'
    CLOSE_TIME_XPATH = './span[@itemprop="closes"]/text()'
    LATITUDE_XPATH = '//meta[@itemprop="latitude"]/@content'
    LONGITUDE_XPATH = '//meta[@itemprop="longitude"]/@content'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and yield requests for state pages."""
        state_urls = response.xpath(self.STATE_URLS_XPATH).getall()

        for state_url in state_urls:
            state_url = state_url.lower().replace("%20", "-")
            yield response.follow(state_url, callback=self.parse_state)

    def parse_state(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse state pages and yield requests for individual store pages."""
        for store_url in self._get_store_urls(response):
            yield response.follow(store_url, callback=self.parse_store)

    def parse_store(self, response: Response) -> Optional[dict]:
        """Parse individual store pages and extract store information."""
        try:
            item = {
                "name": self.clean_text(response.xpath(self.NAME_XPATH).get()),
                "number": self.clean_text(response.xpath(self.STORE_NUMBER_XPATH).re_first(r'#(.*)')),
                "phone_number": self.clean_text(response.xpath(self.PHONE_XPATH).get()),
                "address": self._get_address(response),
                "location": self._get_location(response),
                "hours": self._get_hours(response),
                "services": self._get_services(response),
                "url": response.url,
            }

            # Discard items missing required fields
            required_fields = ["address", "location", "url"]
            if all(item.get(field) for field in required_fields):
                return item
            else:
                self.logger.warning(f"Discarding item due to missing required fields: {response.url}")
                return None

        except Exception as e:
            self.logger.error(f"Error parsing store {response.url}: {e}", exc_info=True)
            return None

    def _get_store_urls(self, response: Response) -> Generator[str, None, None]:
        """Extract store URLs from the response."""
        try:
            class_names = ["store-city", "store-state", "store-zipcode", "store-storeid"]
            store_li_elems = response.xpath(self.STORE_LI_ELEMS_XPATH)

            for store_li in store_li_elems:
                store_attrs = []
                for class_name in class_names:
                    attr_value = store_li.xpath(self.STORE_INPUT_XPATH_FORMAT.format(class_name)).get('')
                    attr_value = attr_value.replace(" ", "")
                    store_attrs.append(attr_value)
                store_id = store_attrs.pop()
                store_attrs = "-".join(store_attrs)
                store_url = f"https://www.tractorsupply.com/tsc/store_{store_attrs}_{store_id}"
                yield store_url
        except Exception as e:
            self.logger.error(f"Error getting store URLs: {e}", exc_info=True)

    def _get_location(self, response: Response) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = response.xpath(self.LATITUDE_XPATH).get()
            longitude = response.xpath(self.LONGITUDE_XPATH).get()

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude for store: {response.url}")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}

    def _get_address(self, response: Response) -> str:
        """Get the formatted store address."""
        try:
            address_elem = response.xpath(self.ADDRESS_ELEM_XPATH)
            street_address = self.clean_text(address_elem.xpath(self.STREET_XPATH).get())

            street = street_address.title()
            city = self.clean_text(address_elem.xpath(self.CITY_XPATH).get()).title()
            state = self.clean_text(address_elem.xpath(self.STATE_XPATH).get())
            zipcode = self.clean_text(address_elem.xpath(self.ZIP_XPATH).get())

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address for store: {response.url}")
            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
            return ""

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """Clean and normalize text."""
        return text.strip() if text else ""

    @staticmethod
    def normalize_spaces(text: Optional[str]) -> str:
        """Normalize spaces in text."""
        return " ".join(text.split()) if text else ""

    def _get_hours(self, response: Response) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}

            days = response.xpath(self.STORE_DAYS_XPATH).getall()
            times = response.xpath(self.STORE_TIMES_ELEM_XPATH)

            for day, time in zip(days, times):
                day = day.lower().strip().strip(':')
                open_time = self.normalize_spaces(time.xpath(self.OPEN_TIME_XPATH).get())
                close_time = self.normalize_spaces(time.xpath(self.CLOSE_TIME_XPATH).get())
                hours[day] = {
                    "open": open_time,
                    "close": close_time
                }
            
            if not hours:
                self.logger.warning(f"Missing store hours for: {response.url}")
            
            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}
        
    def _get_services(self, response: Response) -> list[str]:
        """Extract store services."""
        try:
            services_elems = response.xpath(self.SERVICES_ELEMS_XPATH)

            services = [service.xpath('normalize-space(.)').get() for service in services_elems]

            if response.xpath(self.PETVET_CLINIC_ELEM_XPATH):
                services.append("PetVet Clinic")
            return services
        except Exception as e:
            self.logger.error(f"Error getting store services: {e}", exc_info=True)
            return []