import json
import re
import scrapy


class TjmaxxSpider(scrapy.Spider):
    name = "tjmaxx"
    allowed_domains = ["tjmaxx.tjx.com"]
    start_urls = ["https://tjmaxx.tjx.com/store/stores/storeLocator.jsp"]

    STORE_LI_XPATH = '//ul[@class="store-list"]/li'
    STORE_JSON_RE = re.compile(r"TJXdata.storeData = (.*)")

    def parse(self, response):
        session_conf = self.get_session_conf(response)
        zipcode = "10001"
        zipcode_url = self.get_zipcode_url(zipcode, session_conf)
        yield scrapy.Request(zipcode_url, callback=self.parse_stores)

    def parse_stores(self, response):
        
        store_json = self.STORE_JSON_RE.search(response.text).group(1)
        store_data = json.loads(store_json)

        for store in store_data:
            parsed_store = self.parse_store(store)
            yield parsed_store

    def parse_store(self, store):
        parsed_store = {}
        
        

        return parsed_store



    
    @staticmethod
    def get_session_conf(response):
        return response.xpath('//input[@name="_dynSessConf"]/@value').get()
    
    @staticmethod
    def get_zipcode_url(zipcode, session_conf):
        return f"https://tjmaxx.tjx.com/store/stores/storeLocator.jsp?_dyncharset=utf-8&_dynSessConf={session_conf}&%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.zipCode={zipcode}&_D%3A%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.zipCode=+&%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.city=&_D%3A%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.city=+&_D%3A%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.state=+&%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.state=ny&%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.locateSuccessUrl=https%3A%2F%2Ftjmaxx.tjx.com%2Fstore%2Fstores%2FstoreLocator.jsp&_D%3A%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.locateSuccessUrl=+&%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.locateFailUrl=https%3A%2F%2Ftjmaxx.tjx.com%2Fstore%2Fstores%2FstoreLocator.jsp&_D%3A%2Ftjx%2Fstore%2FTJXStoreLocatorFormHandler.locateFailUrl=+&submit=SEARCH&_D%3Asubmit=+&_DARGS=%2Fstore%2Fstores%2Fviews%2FstoreLocator.jsp.findStoresForm"
