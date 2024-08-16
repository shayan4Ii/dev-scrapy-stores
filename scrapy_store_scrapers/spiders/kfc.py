import scrapy
from scrapy.http import Response
from scrapy_store_scrapers.items import KFCStoreItem

class KfcSpider(scrapy.Spider):
    name = "kfc"
    allowed_domains = ["locations.kfc.com"]
    start_urls = ["http://locations.kfc.com/"]

    LOCATION_URL_XPATH = '//ul[@class="Directory-listLinks"]/li/a/@href'
    STORE_URLS_XPATH = '//a[@class="Directory-listLink"]/@href'

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

        

    

