import scrapy
from scrapy_store_scrapers.utils import *
from scrapy.http import Response
from scrapy_playwright.page import PageMethod
from urllib.parse import urlparse, parse_qs



class Rue21Spider(scrapy.Spider):
    name = "rue21"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        USER_AGENT = None,
        PLAYWRIGHT_PROCESS_REQUEST_HEADERS = None,
        CONCURRENT_REQUESTS = 2,
        PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60*1000
    )
    

    def start_requests(self) -> Iterable[scrapy.Request]:
        url = "https://rue21.com/a/store-locator/list"
        yield scrapy.Request(url, callback=self.parse, meta={
            "playwright": True, 
            "playwright_page_methods": [
                PageMethod("wait_for_selector", "//div[@id='main-slider-storelocator']/div")
            ]
        })


    def parse(self, response: Response):
        stores = response.xpath("//div[@id='main-slider-storelocator']/div")
        for store in stores:
            store_url = response.urljoin(store.xpath(".//a[@class='linkdetailstore']/@href").get())
            if store_url is None:
                continue
            yield scrapy.Request(store_url, callback=self.parse_store, meta={
                "playwright": True, 
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "//a[contains(@aria-label, 'Open this area in Google Maps')]")
                ]
            })


    def parse_store(self, response: Response):
        yield {
            "name": response.xpath("//h1[@class='header-store-name']/text()").get('').strip(),
            "address": " ".join(response.xpath("//a[@class='entry-item-address-link']/text()").getall()).strip().replace(", United States","").split("-")[0],
            "location": self._get_location(response),
            "hours": self._get_hours(response),
            "url": response.url,
        }


    def _get_hours(self, response: Response) -> dict:
        hours = {}
        try:
            hours_table = response.xpath("//table[@class='work-time table']/tbody/tr")
            for row in hours_table:
                day = row.xpath("./th/text()").get()
                hours_range = row.xpath("./td/text()").get()
                hours[day.lower()] = {
                    "open": convert_to_12h_format(hours_range.split("-")[0].strip()),
                    "close": convert_to_12h_format(hours_range.split("-")[1].strip())
                }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting hours: {e}")
            return {}


    def _get_location(self, response: Response) -> dict:
        try:
            map_url = response.xpath("//a[contains(@href, 'maps.google.com/maps?ll')]/@href").get()
            if map_url is None:
                return
            coordinates = parse_qs(urlparse(map_url).query)['ll'][0].split(',')
            return {
                "type": "Point",
                "coordinates": [float(coordinates[1]), float(coordinates[0])]
            }
        except Exception as e:
            self.logger.error(f"Error getting location: {e}")
            return None
