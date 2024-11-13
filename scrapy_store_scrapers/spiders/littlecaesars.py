import scrapy
from scrapy_store_scrapers.utils import *
from scrapy_playwright.page import PageMethod
from playwright.async_api import Route, Page



class LittleCaesarsSpider(scrapy.Spider):
    name = "littlecaesars"
    custom_settings = dict(
        PLAYWRIGHT_ABORT_REQUEST = should_abort_request,
        PLAYWRIGHT_BROWSER_TYPE = "firefox",
        PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30*1000,
        PLAYWRIGHT_MAX_CONTEXTS = 1,
        DOWNLOAD_HANDLERS = {
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        USER_AGENT = None,
        PLAYWRIGHT_PROCESS_REQUEST_HEADERS = None,
        CONCURRENT_REQUESTS = 2,
        PLAYWRIGHT_LAUNCH_OPTIONS = {
            "headless": True,
        },
        DOWNLOADER_MIDDLEWARES = {
            'scrapy_store_scrapers.middlewares.ScrapyStoreScrapersDownloaderMiddleware': 80,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
        },
    )
    stores = []
    yielded = []


    def start_requests(self):
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://littlecaesars.com/en-us/order/pickup/stores/search/{zipcode['zipcode']}/"
            yield scrapy.Request(url, callback=self.parse, meta={
            "playwright": True,
            "playwright_include_page": True,
            "playwright_page_methods": [
                    PageMethod("route", "**/api/GetClosestStores", self.capture_request),
                ]
            })


    async def capture_request(self, route: Route):
        response = await route.fetch()
        try:    
            json_data = await response.json()
        except Exception as e:
            return await route.abort()
        if json_data.get("stores", []) is None:
            return await route.abort()
        self.stores.extend(json_data.get("stores", []))
        await route.fulfill(response=response, json=json_data)


    async def parse(self, response: Response):
        page: Page = response.meta.get("playwright_page")
        try:
            async with page.expect_response("**/api/GetClosestStores", timeout=20*1000) as request_info:
                pass
        except Exception as e:
            pass
        await page.close()
        for store in self.stores:
            if store['locationNumber'] not in self.yielded:
                self.yielded.append(store['locationNumber'])
                yield {
                    "number": f"{store['locationNumber']}",
                    "name": store['storeName'],
                    "address": self._get_address(store['address']),
                    "location": {
                        "type": "Point",
                        "coordinates": [store['longitude'], store['latitude']]
                    },
                    "hours": self._get_hours(store['storeHours']),
                    "phone_number": store['phone'],
                    "url": f"https://littlecaesars.com/en-us/store/{store['locationNumber']}/",
                    "raw": store
                }

    
    def _get_address(self, address: Dict) -> str:
        try:
            address_parts = [
                address['street'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = address['city']
            state = address['state']
            zipcode = address['zip']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, hours_data: Dict) -> dict:
        days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
        hours = {}
        try:
            for day, hour_range in zip(days, hours_data):
                hours[day] = {
                    "open": convert_to_12h_format(hour_range['openTime'].split("T")[1].replace(":00", "")),
                    "close": convert_to_12h_format(hour_range['closeTime'].split("T")[1].replace(":00", ""))
                }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}