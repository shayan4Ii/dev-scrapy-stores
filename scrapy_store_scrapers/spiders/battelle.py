import scrapy
from scrapy_store_scrapers.utils import *




class Battelle(scrapy.Spider):
    name = "battelle"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        USER_AGENT = None
    )


    def start_requests(self) -> Iterable[scrapy.Request]:
        url = "https://www.battelle.org/about-us/locations"
        yield scrapy.Request(url, callback=self.parse_locations, meta={"impersonate": "chrome99_android"})


    def parse_locations(self, response: Response):
        for location in response.xpath("//main/div[4]/div/div"):
            map_url = location.xpath(".//a[contains(@href, 'com/maps')]/@href").get()
            if '/dir//' in map_url:
                continue
            coordinates = map_url.split("@")[0].split("/dir/")[-1].split("/")[0].split(",")
            if len(coordinates) != 2:
                continue
            yield {
                "name": location.xpath(".//h3/text()").get(),
                "address": location.xpath(".//strong[contains(text(), 'Street Address')]/following-sibling::text()").get().strip().split("-")[0].strip(": "),
                "location": {
                    "type": "Point",
                    "coordinates": [float(coordinates[1]), float(coordinates[0])]
                },
            }
