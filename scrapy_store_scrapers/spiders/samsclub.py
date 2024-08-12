import scrapy


class SamsclubSpider(scrapy.Spider):
    name = "samsclub"
    allowed_domains = ["www.samsclub.com"]
    start_urls = ["https://www.samsclub.com/api/node/vivaldi/browse/v2/clubfinder/search?isActive=true"]

    def parse(self, response):
        print(response.text)
        print(len(response.json()))