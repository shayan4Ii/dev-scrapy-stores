import json
from typing import Generator

import scrapy


class PriceriteSpider(scrapy.Spider):
    name = "pricerite"
    allowed_domains = ["www.priceritemarketplace.com"]
    start_urls = ["https://www.priceritemarketplace.com/sm/planning/rsid/1000/store/?cfrom=footer"]

    SCRIPT_TEXT_XPATH = '//script[contains(text(), "window.__PRELOADED_STATE__=")]/text()'

    # Add custom headers
    custom_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
    }

    # Set custom headers for all requests
    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': custom_headers,
    }

    def parse(self, response: scrapy.http.Response) -> Generator[dict, None, None]:
        script_text = response.xpath(self.SCRIPT_TEXT_XPATH).get()
        script_text = script_text.replace("window.__PRELOADED_STATE__=", "").strip()

        if not script_text:
            self.logger.warning(f"No script text found on {response.url}")
            return
        
        raw_data = json.loads(script_text)

        for store in raw_data['stores']['availablePlanningStores']['items']:
            yield store