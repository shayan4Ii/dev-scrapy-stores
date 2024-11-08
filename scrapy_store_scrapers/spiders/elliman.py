import scrapy
from scrapy_store_scrapers.utils import *
from scrapy_playwright.page import PageMethod
import re
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.python.failure import Failure



def should_abort_request(request):
    not_allowed = [".facebook.net","googlemanager.com","stackadapt.com","google-analytics.com","clarity.ms","googletagmanager.com"
                   "youtube.com"]
    return (
        request.resource_type == "image"
        or ".jpg" in request.url
        or ".woff" in request.url
        or any([True for domain in not_allowed if domain in request.url])
    )


class Elliman(scrapy.Spider):
    name = "elliman"
    page_count = 1
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
        CONCURRENT_REQUESTS = 1,
    )


    def start_requests(self) -> Iterable[Request]:
        url = f"https://www.elliman.com/offices/usa"
        yield scrapy.Request(url, callback=self.parse, meta={
            "playwright": True, 
            "playwright_page_methods": [
                PageMethod("wait_for_selector", "//div[contains(@id, 'brokeritem')]", state="attached")
            ],
            "dont_redirect": True
        })


    def parse(self, response: Response) -> Iterable[Request]:
        offices = response.xpath("//a[@class='org url']/@href").getall()
        for office in offices:
            url = f"{office.rstrip('/')}/profile"
            yield scrapy.Request(response.urljoin(url), callback=self.parse_office, meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "//input[@name='daddr' and @class='direction-input']", state="attached")
                ],
            })

        current_page = int(response.xpath("//span[@id='current-page' and @class='paging__item  is-active']/text()").get())
        if self.page_count == current_page:
            self.page_count +=1
            url = f"https://www.elliman.com/offices/usa/{self.page_count}-pg"
            yield scrapy.Request(url, callback=self.parse, meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "//div[contains(@id, 'brokeritem')]", state="attached")
                ],
            })


    def parse_office(self, response: Response) -> Iterable[Request]:
        yield {
            # "number": response.url.split("/")[-3].replace("-",""),
            "name": response.xpath("//div[@class='title']/text()").get(),
            "address": self._get_address(response),
            "location": self._get_location(response),
            "phone_number": response.xpath("//a[contains(@href, 'tel')]/text()").get('').lower().strip(),
            # "hours": None, # not available
            "url": response.url,
            # "services": [], not available
            # "raw": None,
        }


    def _get_address(self, response: Response) -> str:
        get_code = lambda string: re.search("\|\d{5}\|", string).group().strip("|")
        try:
            address_parts = [
                response.xpath("//span[@class='street-address']/text()").get('').strip(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("//h2/text()").get('').split(",")[0].strip()
            state = response.xpath("//h2/text()").get('').split(",")[1].strip()
            zipcode = get_code(re.search(r'(?:brokerdetails_XSLParams\)\;rd\=\")(.*?)(?:")', response.text).group(1))

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_location(self, response: Response) -> Dict:
        try:
            coordinates = response.xpath("//input[@name='daddr' and @class='direction-input']/@value").get('').split(",")

            lat = float(str(coordinates[0]))
            lon = float(str(coordinates[1]))
            return {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}