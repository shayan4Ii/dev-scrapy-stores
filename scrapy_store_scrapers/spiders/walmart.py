import scrapy


class WalmartSpider(scrapy.Spider):
    name = "walmart"
    allowed_domains = ["www.walmart.com"]
    start_urls = ["https://www.walmart.com/store-directory"]

    def parse(self, response):
        pass
