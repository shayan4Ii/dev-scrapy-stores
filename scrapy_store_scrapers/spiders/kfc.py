from datetime import datetime
import scrapy
from scrapy.http import Response
from scrapy_store_scrapers.items import KFCStoreItem
from scrapy.loader import ItemLoader
from scrapy.http import Response
from itemloaders.processors import TakeFirst, MapCompose, Identity

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
    HOURS_JSON_XPATH = '//div[@id="hours-accordion-content"]/div/div/@data-days'

    def parse(self, response: Response):
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
        
        hours_json = response.xpath(HOURS_JSON_XPATH).get()

        if not hours_json:
            self.logger.warning(
                f"No hours data found for store: {store_data['address']}")
            store_data['hours'] = None
        else:
            try:
                store_data['hours'] = {}
                hours_data = json.loads(hours_json)

                for day_dict in hours_data:
                    day = day_dict['day'].lower()

                    if not day_dict['intervals']:
                        self.logger.warning(
                            f"No intervals found for {day} for store: {store_data['address']} with url: {response.url}")
                        store_data['hours'][day] = None
                        continue
                    elif len(day_dict['intervals']) > 1:
                        self.logger.error(
                            f"Multiple intervals found for {day} for store: {store_data['address']} with url: {response.url}")

                    open_time = day_dict['intervals'][0]['start']
                    close_time = day_dict['intervals'][0]['end']
                    store_data['hours'][day] = {
                        "open": self.convert_to_12_hour(str(open_time)),
                        "close": self.convert_to_12_hour(str(close_time))
                    }
            except Exception as e:
                self.logger.error(
                    f"Failed to parse hours data: {e} for store: {store_data['address']} with url: {response.url}")
                store_data['hours'] = None


        item = loader.load_item()

        services = response.xpath(self.SERVICES).getall()
        item['services'] = services

        return item
    
    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """Convert 24-hour time string to 12-hour format."""
        padded_time = time_str.zfill(4)
        time_obj = datetime.strptime(padded_time, '%H%M')
        return time_obj.strftime('%I:%M %p').lower()


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