import requests
from parsel import Selector
import json
from datetime import datetime
import time
from urllib.parse import urljoin
from collections import deque
from typing import List, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

START_URL = 'https://www.walmart.com/store-directory'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0'
}

def convert_to_12h_format(time_str):
    t = datetime.strptime(time_str, '%H:%M').time()
    return t.strftime('%I:%M %p').lstrip('0')

class WalmartScraper:
    def __init__(self, start_url: str, request_headers: dict):
        self.start_url = start_url
        self.request_headers = request_headers
        self.visited_urls = set()
        self.store_urls = set()
        self.scraped_stores_data = []

    def get_page(self, url: str) -> Optional[Selector]:
        try:
            response = requests.get(url, headers=self.request_headers, timeout=10)
            response.raise_for_status()
            logger.info(f"Successfully fetched page: {url} (Status Code: {response.status_code})")
            return Selector(response.text)
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    @staticmethod
    def is_store_directory(url: str) -> bool:
        return 'store-directory' in url

    def get_urls(self, selector: Selector, base_url: str) -> List[str]:
        urls = selector.xpath('//h1[contains(text(), "Walmart Store Directory")]/following-sibling::div//div/a[contains(@href, "store")]/@href').getall()
        return [urljoin(base_url, url) for url in urls]

    def scrape_store_urls(self):
        urls_to_visit = deque([self.start_url])
        logger.info(f"Starting to scrape store URLs from {self.start_url}")

        while urls_to_visit:
            current_url = urls_to_visit.popleft()
            
            if current_url in self.visited_urls:
                logger.debug(f"Skipping already visited URL: {current_url}")
                continue
            
            self.visited_urls.add(current_url)
            logger.info(f"Processing URL: {current_url}")
            selector = self.get_page(current_url)
            
            if selector is None:
                logger.warning(f"Failed to get page for URL: {current_url}")
                continue
            
            self._process_urls(selector, current_url, urls_to_visit)
            
            time.sleep(1)  # Be respectful to the server
        
        logger.info(f"Finished scraping store URLs. Total URLs found: {len(self.store_urls)}")

    def _process_urls(self, selector: Selector, base_url: str, urls_to_visit: deque):
        urls = self.get_urls(selector, base_url)
        logger.debug(f"Found {len(urls)} URLs on page {base_url}")
        
        for url in urls:
            if self.is_store_directory(url) and url not in self.visited_urls:
                logger.debug(f"Adding store directory URL to visit: {url}")
                urls_to_visit.append(url)
            elif not self.is_store_directory(url):
                logger.debug(f"Adding store URL: {url}")
                self.store_urls.add(url)

    def scrape_store_details(self, store_url: str):
        logger.info(f"Scraping store details from: {store_url}")
        store_data = self._extract_store_data(store_url)
        if store_data is None:
            logger.warning(f"Failed to extract store data from: {store_url}")
            return None

        self._save_raw_store_data(store_data)
        formatted_data = self._format_store_output(store_data, store_url)
        logger.info(f"Successfully scraped store: {formatted_data['name']}")
        return formatted_data

    def _extract_store_data(self, store_url: str):
        selector = self.get_page(store_url)
        if selector is None:
            return None

        script = selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if script is None:
            return None

        data = json.loads(script)
        return data['props']['pageProps']['initialData']['initialDataNodeDetail']['data']['nodeDetail']

    def _save_raw_store_data(self, store_data: dict):
        save_path = f"{store_data['id']}.json"
        with open(save_path, 'w') as f:
            json.dump(store_data, f, indent=2)

    def _format_store_output(self, store_data: dict, store_url: str):
        formatted_output = {
            "name": store_data['displayName'],
            "address": self._format_address(store_data['address']),
            "city": store_data['address']['city'],
            "state": store_data['address']['state'],
            "phone_number": store_data['phoneNumber'],
            "Hours": self._format_hours(store_data['operationalHours']),
            "services": [service['displayName'] for service in store_data['services']],
            "url": store_url
        }
        return formatted_output

    def _format_address(self, address: dict):
        return f"{address['addressLineOne']}, {address['city']}, {address['state']} {address['postalCode']}"

    def _format_hours(self, operational_hours: list):
        formatted_hours = {}
        for day in operational_hours:
            formatted_hours[day['day']] = {
                "open": convert_to_12h_format(day['start']),
                "close": convert_to_12h_format(day['end'])
            }
        return formatted_hours

    def run(self):
        logger.info("Starting Walmart scraper")
        self.scrape_store_urls()
        logger.info(f"Total store URLs found: {len(self.store_urls)}")
        self._scrape_all_store_details()
        logger.info("Walmart scraper finished")

    def _scrape_all_store_details(self):
        logger.info("Starting to scrape individual store details")
        for index, store_url in enumerate(self.store_urls, 1):
            logger.info(f"Scraping store {index} of {len(self.store_urls)}")
            store_data = self.scrape_store_details(store_url)
            if store_data:
                self.scraped_stores_data.append(store_data)
            time.sleep(1)  # Be respectful to the server
        logger.info(f"Finished scraping details for {len(self.scraped_stores_data)} stores")

    def save_data(self, filename: str):
        logger.info(f"Saving scraped data to {filename}")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.scraped_stores_data, f, indent=2)
        logger.info(f"Data successfully saved to {filename}")

def main():
    scraper = WalmartScraper(START_URL, HEADERS)
    scraper.run()
    scraper.save_data('walmart_stores_data.json')

if __name__ == "__main__":
    main()
