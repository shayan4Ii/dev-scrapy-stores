import scrapy


class CvsSpider(scrapy.Spider):
    name = "cvs"
    allowed_domains = ["www.cvs.com"]
    start_urls = ["http://www.cvs.com/"]

    def parse(self, response):
        pass
