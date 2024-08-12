import scrapy
from typing import Dict, Iterator


class WalmartSpider(scrapy.Spider):
    name = "walmart"
    allowed_domains = ["www.walmart.com"]
    start_urls = ["https://www.walmart.com/store-directory"]

    @staticmethod
    def get_default_headers() -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "TE": "Trailers",
        }

    def start_requests(self) -> Iterator[scrapy.Request]:
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=self.get_default_headers(), callback=self.parse)

    def parse(self, response):
        pass
