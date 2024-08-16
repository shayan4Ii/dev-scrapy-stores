import scrapy
from scrapy.http import Response
from scrapy_store_scrapers.items import KFCStoreItem
from scrapy.loader import ItemLoader
from scrapy.http import Response
from itemloaders.processors import TakeFirst, MapCompose

class KfcSpider(scrapy.Spider):
    name = "kfc"
    allowed_domains = ["locations.kfc.com"]
    start_urls = ["https://locations.kfc.com/al/madison"]

    LOCATION_URL_XPATH = '//ul[@class="Directory-listLinks"]/li/a/@href'
    STORE_URLS_XPATH = '//ul[@class="Directory-listTeasers Directory-row"]//h2/a/@href'
    ADDRESS_ELEM_XPATH = '//address[@id="address"]'

    STREET_ADDRESS_XPATH = './/span[@class="c-address-street-1"]/text()'
    STREET_ADDRESS_2_XPATH = './/span[@class="c-address-street-2"]/text()'
    CITY_XPATH = './/span[@class="c-address-city"]/text()'
    REGION_XPATH = './/abbr[@itemprop="addressRegion"]/text()'
    POSTAL_CODE_XPATH = './/span[@itemprop="postalCode"]/text()'

    PHONE_XPATH = '//div[@class="Core-body"]//div[@id="phone-main"]/text()'
    SERVICES = '//ul[@class="CoreServices"]/li/span/text()'

    def parse(self, response: Response) :
        """
        
        Parse the response and yield further requests or store data.

        This method handles three scenarios:
        1. If the response contains location URLs, generate requests for each location URL.
        2. If the response contains store URLs, generate requests for each store URL.
        3. If the response contains store data, parse the store data.

        Args:
            response (Response): The response object to be parsed.
        
        Yields:
            Union[scrapy.Request, KFCStoreItem]: Either a new request or KFCStoreItem instance containing store data.

        """

        location_urls = response.xpath(self.LOCATION_URL_XPATH).getall()
        store_urls = response.xpath(self.STORE_URLS_XPATH).getall()
        if location_urls:
            for location_url in location_urls:
                yield response.follow(location_url, self.parse)
                break
        elif store_urls:
            for store_url in store_urls:
                yield response.follow(store_url, self.parse_store)
        else:
            yield self.parse_store(response)

    def parse_store(self, response: Response) -> KFCStoreItem:
        """
        Parse the store data from the response.
        
        Args:
            response (Response): The response object containing store data.
            
        Returns:
            KFCStoreItem: An instance of KFCStoreItem containing store data.
        """

        loader = ItemLoader(item=KFCStoreItem(), response=response)
        loader.default_output_processor = TakeFirst()

        loader.add_xpath('name', 'normalize-space(//span[@id="location-name"])')

        address_elem = response.xpath(self.ADDRESS_ELEM_XPATH)

        street_address = self.clean_text(
            address_elem.xpath(self.STREET_ADDRESS_XPATH).get())
        street_address_2 = self.clean_text(
            address_elem.xpath(self.STREET_ADDRESS_2_XPATH).get())
        complete_street_address = self.clean_text(
            f"{street_address} {street_address_2}")

        city = self.clean_text(address_elem.xpath(self.CITY_XPATH).get())
        region = self.clean_text(address_elem.xpath(self.REGION_XPATH).get())
        postal_code = self.clean_text(
            address_elem.xpath(self.POSTAL_CODE_XPATH).get())

        full_address = f"{complete_street_address}, {city}, {region} {postal_code}"

        loader.add_value('address', full_address)
        loader.add_xpath('phone_number', self.PHONE_XPATH)

        loader.add_value('services', response.xpath(self.SERVICES).getall())

        return loader.load_item()
    
    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean the text by removing leading and trailing whitespace.
        
        Args:
            text (str): The text to be cleaned.
            
        Returns:
            str: The cleaned text.
        """
        return text.strip() if text else ""

    
