from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *
from scrapy.http import Response
from scrapy_playwright.page import PageMethod



class Nordstromrack(scrapy.Spider):
    name = 'nordstromrack'
    custom_settings = dict(
        PLAYWRIGHT_BROWSER_TYPE = "firefox",
        PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30*1000,
        PLAYWRIGHT_MAX_CONTEXTS = 1,
        DOWNLOAD_HANDLERS = {
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        USER_AGENT = None,
        PLAYWRIGHT_PROCESS_REQUEST_HEADERS = None,
        CONCURRENT_REQUESTS = 4,
        PLAYWRIGHT_LAUNCH_OPTIONS = {
            "headless": True,
        },
        DOWNLOADER_MIDDLEWARES = {
            'scrapy_store_scrapers.middlewares.ScrapyStoreScrapersDownloaderMiddleware': 80,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
        },
    )
    sitemap_retries = 0


    def start_requests(self) -> Iterable[Request]:
        url = 'https://www.nordstrom.com/browse/about/stores/sitemap'
        yield scrapy.Request(url=url, callback=self.parse, meta={
            "playwright": True,
            "playwright_page_methods": [
                PageMethod("wait_for_selector", "//div[@id='product-results-view']", timeout=10*1000)
            ]
        })


    def parse(self, response: Response):
        stores = response.xpath("//section[contains(@id, 'anchor-link')]//a[not(contains(@title, 'Back to Top'))]/@href").getall()
        for store in stores:
            yield scrapy.Request(url=response.urljoin(store), callback=self.parse_store, meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "//a[@class='NAP-shopOnline']", timeout=10*1000)
                ]
            })


    def parse_store(self, response: Response):
        yield {
            "name": " ".join(response.xpath("//span[@id='location-name']/span/text()").getall()),
            "address": self._get_address(response),
            "phone_number": response.xpath("//span[@id='telephone']/text()").get(),
            "location": {
                "type": "Point",
                "coordinates": [
                    float(response.xpath("//meta[@itemprop='longitude']/@content").get()),
                    float(response.xpath("//meta[@itemprop='latitude']/@content").get())
                ]
            },
            "hours": self._get_hours(response),
            "url": response.url,
        }

    
    def _get_address(self, response: Response) -> str:
        try:
            address_parts = [
                response.xpath("//meta[@itemprop='streetAddress']/text()").get(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("//span[@class='c-address-city']/text()").get()
            state = response.xpath("//abbr[@class='c-address-state']/text()").get()
            zipcode = response.xpath("//span[@class='c-address-postal-code']/text()").get()

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, response: Response) -> dict:
        days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
        hours = {}
        try:
            hours_data = json.loads(response.xpath("//div[@data-days]/@data-days").get())
            for day in days:
                for hour_range in hours_data:
                    pass
            return hours
        except Exception as e:
            self.logger.error(f"Error getting hours: {e}")
            return {}                            