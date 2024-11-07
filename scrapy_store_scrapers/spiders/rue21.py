import scrapy
from scrapy_store_scrapers.utils import *
from scrapy.http import Response
from scrapy_playwright.page import PageMethod
from urllib.parse import urlparse, parse_qs, urljoin



class Rue21Spider(scrapy.Spider):
    name = "rue21"
    

    def start_requests(self) -> Iterable[scrapy.Request]:
        url = "https://sl.storeify.app/js/stores/6eca69-4e.myshopify.com/storeifyapps-storelocator-geojson.js?v=1730229668"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        obj = json.loads(response.text.strip("eqfeed_callback(").strip(")"))
        for store in obj['features']:
            yield {
                "number": f"{store['properties']['id']}",
                "name": store['properties']['name'],
                "address": store['properties']['address'].strip(", United States").split("-")[0],
                "location": {
                    "type": "Point",
                    "coordinates": [float(store['properties']['lng']), float(store['properties']['lat'])]
                },
                "hours": self._get_hours(scrapy.Selector(text=store['properties']['schedule'])),
                "url": urljoin("https://rue21.com/", store['properties']['url']),
                "raw": store
            }


    def _get_hours(self, sel: scrapy.Selector) -> dict:
        days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
        hours = {}
        try:
            hours_table = sel.xpath("//table/tr")
            for row in hours_table:
                day = row.xpath("./th/text()").get('').lstrip(r"{{").rstrip(r"}}").strip()
                for d in days:
                    if day in d:
                        hours_range = row.xpath("./td/text()").get()
                        hours[d] = {
                            "open": convert_to_12h_format(hours_range.split("-")[0].strip()),
                            "close": convert_to_12h_format(hours_range.split("-")[1].strip())
                        }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting hours: {e}")
            return {}
