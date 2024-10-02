from typing import Any, Generator, Optional
from scrapy.http import Response
from scrapy.exceptions import DropItem

import scrapy



class PilotSpider(scrapy.Spider):
    name = "pilot"
    allowed_domains = ["locations.pilotflyingj.com"]
    start_urls = ["https://locations.pilotflyingj.com/us"]

    # XPath constants
    LOCATION_URLS_XPATH = '//ul[@class="Directory-listLinks"]/li/a/@href'
    STORE_URLS_XPATH = '//ul[@class="Directory-listTeasers Directory-row"]/li//a[@data-ya-track="businessname"]/@href'
    STORE_NAME_XPATH = '//h1[@itemprop="name"]/text()'
    ADDRESS_ELEM_XPATH = '//address[@itemprop="address"]'
    STREET_ADDRESS_XPATH = './/span[@class="c-address-street-1"]/text()'
    STREET_ADDRESS_2_XPATH = './/span[@class="c-address-street-2"]/text()'
    CITY_XPATH = './/span[@class="c-address-city"]/text()'
    REGION_XPATH = './/abbr[@class="c-address-state"]/text()'
    POSTAL_CODE_XPATH = './/span[@itemprop="postalCode"]/text()'
    PHONE_NUMBER_XPATH = '//div[@itemprop="telephone"]/text()'
    LATITUDE_XPATH = '//meta[@itemprop="latitude"]/@content'
    LONGITUDE_XPATH = '//meta[@itemprop="longitude"]/@content'
    HOURS_CONTAINER_XPATH = '//div[@class="c-hours-details-wrapper js-hours-table"]'
    HOURS_ROWS_XPATH = './/table[@class="c-hours-details"]/tbody/tr'
    HOURS_DAY_XPATH = './td[@class="c-hours-details-row-day"]/text()'
    HOURS_OPEN_XPATH = './/span[@class="c-hours-details-row-intervals-instance-open"]/text()'
    HOURS_CLOSE_XPATH = './/span[@class="c-hours-details-row-intervals-instance-close"]/text()'
    SERVICES_XPATH = '//ul[@class="Amenities-list js-amenities-list"]/li/text()'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to locations or stores."""
        try:
            location_urls = response.xpath(self.LOCATION_URLS_XPATH).getall()
            store_urls = response.xpath(self.STORE_URLS_XPATH).getall()

            for url in location_urls:
                yield response.follow(url, callback=self.parse)

            for url in store_urls:
                yield response.follow(url, callback=self.parse_store)

            if not location_urls and not store_urls:
                yield self.parse_store(response)
        except Exception as e:
            self.logger.error(f"Error parsing main page: {e}", exc_info=True)

    def parse_store(self, response: Response) -> dict[str, Any]:
        """Parse individual store pages and extract relevant information."""
        try:
            parsed_store = {
                'number': response.xpath(self.STORE_NAME_XPATH).re_first(r'#(\d+)'),
                'name': self.clean_text(' '.join(response.xpath(self.STORE_NAME_XPATH).getall())),
                'address': self._get_address(response),
                'phone_number': self.clean_text(response.xpath(self.PHONE_NUMBER_XPATH).get()),
                'location': self._get_location(response),
                'services': self._get_services(response),
                'url': response.url,
            }

            required_fields = ['address', 'location', 'url']
            if all(parsed_store.get(field) for field in required_fields):
                return parsed_store
            else:
                missing_fields = [field for field in required_fields if not parsed_store.get(field)]
                raise DropItem(f"Missing required fields: {', '.join(missing_fields)}")
        except Exception as e:
            self.logger.error(f"Error parsing store {response.url}: {e}", exc_info=True)
            return {}

    def _get_services(self, response: Response) -> list[str]:
        """Extract store services."""
        try:
            services = response.xpath(self.SERVICES_XPATH).getall()
            return [service.strip() for service in services if service.strip()]
        except Exception as e:
            self.logger.error(f"Error extracting services: {e}", exc_info=True)
            return []

    def _get_location(self, response: Response) -> dict[str, Any]:
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
            street_address = self.clean_text(address_elem.xpath(self.STREET_ADDRESS_XPATH).get())
            street_address_2 = self.clean_text(address_elem.xpath(self.STREET_ADDRESS_2_XPATH).get())

            street = ", ".join(filter(None, [street_address, street_address_2]))
            city = self.clean_text(address_elem.xpath(self.CITY_XPATH).get())
            state = self.clean_text(address_elem.xpath(self.REGION_XPATH).get())
            zipcode = self.clean_text(address_elem.xpath(self.POSTAL_CODE_XPATH).get())

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