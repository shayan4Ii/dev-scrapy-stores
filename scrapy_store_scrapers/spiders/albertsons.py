import scrapy
from typing import Iterator, Optional
from scrapy_store_scrapers.items import AlbertsonsStoreItem

# Constants for XPath expressions
DIRECTORY_LIST_LINKS = '//ul[@class="Directory-listLinks"]'
DIRECTORY_LIST_TEASERS = '//ul[@class="Directory-listTeasers Directory-row"]'
STORE_LINK = './@href'
STORE_COUNT = './@data-count'

STORE_NAME = '//h1/span[@class="RedesignHero-subtitle Heading--lead"]/text()'
ADDRESS_ELEM = '//address[@itemprop="address"]'
STREET_ADDRESS = './/span[@class="c-address-street-1"]/text()'
CITY = './/span[@class="c-address-city"]/text()'
REGION = './/abbr[@itemprop="addressRegion"]/text()'
POSTAL_CODE = './/span[@itemprop="postalCode"]/text()'
PHONE_NUMBER = '//div[@id="phone-main"]/text()'
LATITUDE = '//meta[@itemprop="latitude"]/@content'
LONGITUDE = '//meta[@itemprop="longitude"]/@content'
HOURS_CONTAINER = '//div[@class="RedesignCore-hours js-intent-core-hours is-hidden"]'
HOURS_ROWS = './/table[@class="c-hours-details"]/tbody/tr'
HOURS_DAY = './td[@class="c-hours-details-row-day"]/text()'
HOURS_OPEN = './/span[@class="c-hours-details-row-intervals-instance-open"]/text()'
HOURS_CLOSE = './/span[@class="c-hours-details-row-intervals-instance-close"]/text()'
SERVICES = '//ul[@id="service-list"]/li//*[@itemprop="name"]/text()'


class AlbertsonsSpider(scrapy.Spider):
    """
    Spider for scraping Albertsons store information from local.albertsons.com
    """
    name = "albertsons"
    allowed_domains = ["local.albertsons.com"]
    start_urls = ["https://local.albertsons.com/az.html"]

    def parse(self, response: scrapy.http.Response) -> Iterator[scrapy.Request]:
        """
        Parse the main page and follow links to individual store pages or sub-directories
        """
        self.logger.info(f"Parsing page: {response.url}")
        if not (response.xpath(DIRECTORY_LIST_LINKS) or response.xpath(DIRECTORY_LIST_TEASERS)):
            self.logger.warning(f"No directory links or teasers found on {response.url}")
            return

        if response.xpath(DIRECTORY_LIST_LINKS):
            for a_elem in response.xpath(f'{DIRECTORY_LIST_LINKS}/li/a'):
                link = a_elem.xpath(STORE_LINK).get()
                is_multiple_stores = a_elem.xpath(STORE_COUNT).get('').strip() != '(1)'
                if is_multiple_stores:
                    yield response.follow(link, callback=self.parse)
                else:
                    yield response.follow(link, callback=self.parse_store)
        elif response.xpath(DIRECTORY_LIST_TEASERS):
            for link in response.xpath(f'{DIRECTORY_LIST_TEASERS}/li/article/h2/a/@href').getall():
                yield response.follow(link, callback=self.parse_store)

    def parse_store(self, response: scrapy.http.Response) -> Iterator[AlbertsonsStoreItem]:
        """
        Parse individual store pages and extract store information
        """
        self.logger.info(f"Parsing store: {response.url}")
        store_data = AlbertsonsStoreItem()

        store_data['name'] = self.clean_text(response.xpath(STORE_NAME).get())
        if not store_data['name']:
            self.logger.warning(f"No store name found for {response.url}")
            return

        address_elem = response.xpath(ADDRESS_ELEM)
        street_address = self.clean_text(address_elem.xpath(STREET_ADDRESS).get())
        city = self.clean_text(address_elem.xpath(CITY).get())
        region = self.clean_text(address_elem.xpath(REGION).get())
        postal_code = self.clean_text(address_elem.xpath(POSTAL_CODE).get())

        store_data['address'] = f"{street_address}, {city}, {region} {postal_code}"
        if not all([street_address, city, region, postal_code]):
            self.logger.warning(f"Incomplete address for store: {store_data['name']}")

        store_data['phone_number'] = response.xpath(PHONE_NUMBER).get()

        try:
            latitude = float(response.xpath(LATITUDE).get())
            longitude = float(response.xpath(LONGITUDE).get())
            store_data['location'] = {
                'type': 'Point',
                'coordinates': [longitude, latitude]
            }
        except (TypeError, ValueError):
            store_data['location'] = None
            self.logger.warning(f"Invalid location data for store: {store_data['name']}")

        store_hours_container = response.xpath(HOURS_CONTAINER)[0]
        hours_detail_rows = store_hours_container.xpath(HOURS_ROWS)

        hours = {}
        for row in hours_detail_rows:
            day = self.clean_text(row.xpath(HOURS_DAY).get()).lower()
            open_time = self.clean_text(row.xpath(HOURS_OPEN).get()).lower()
            close_time = self.clean_text(row.xpath(HOURS_CLOSE).get()).lower()
            hours[day] = {"open": open_time, "close": close_time}

        store_data['hours'] = hours

        services = response.xpath(SERVICES).getall()
        store_data['services'] = [
            self.clean_service(service) for service in services
        ]

        yield store_data

    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """Clean and strip whitespace from text"""
        return text.strip() if text else ""

    @staticmethod
    def clean_service(service: str) -> str:
        """Clean service string"""
        service = service.replace("[c_groceryBrand]", "Albertsons").replace("[name]", "Albertsons").strip()
        return service
