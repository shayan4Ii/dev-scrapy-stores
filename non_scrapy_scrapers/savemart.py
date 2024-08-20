import logging
import json
from typing import List, Dict, Any
import requests
import re
import argparse

class SaveMartScraperException(Exception):
    """Custom exception for SaveMartScraper"""
    pass

class SaveMartScraper:
    """
    A class to scrape store information from the SaveMart website.
    """

    def __init__(self, base_url: str = "https://savemart.com", log_level: str = "INFO"):
        """
        Initialize the SaveMartScraper with necessary attributes.

        :param base_url: The base URL of the SaveMart website
        :param log_level: Logging level (default: INFO)
        """
        self.base_url = base_url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Set up file logging for errors
        error_handler = logging.FileHandler('scraper_errors.log')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        self.logger.addHandler(error_handler)

        self.chain_id = self._get_chain_id()

    def _get_chain_id(self) -> str:
        """
        Get the chain ID from the SaveMart website.

        :return: The chain ID as a string
        :raises SaveMartScraperException: If chain ID is not found
        """
        try:
            response = self.session.get(f"{self.base_url}/stores")
            response.raise_for_status()

            chain_id_re = re.compile(r'"chainId":"(.*?)"')
            chain_id = chain_id_re.search(response.text)

            if not chain_id:
                raise SaveMartScraperException("Chain ID not found")
            
            return chain_id.group(1)
        except requests.RequestException as e:
            self.logger.error(f"Error fetching chain ID: {str(e)}")
            raise SaveMartScraperException(f"Error fetching chain ID: {str(e)}")

    def scrape_stores(self) -> List[Dict[str, Any]]:
        """
        Main method to scrape store information.

        :return: A list of dictionaries containing store information
        """
        self.logger.info("Starting to scrape stores...")
        stores = self._get_all_stores()
        self.logger.info(f"Finished scraping. Total stores retrieved: {len(stores)}")
        return stores

    def _get_all_stores(self) -> List[Dict[str, Any]]:
        """
        Fetch all stores from the SaveMart API.

        :return: A list of dictionaries containing raw store data
        :raises SaveMartScraperException: If there's an error fetching stores
        """
        query = {
            "query": "\n    query allStores($chainId: UUID!) {\n  stores(chainId: $chainId) {\n    all {\n      ...StoreEntryFragment\n    }\n  }\n}\n    \n    fragment StoreEntryFragment on StoreEntry {\n  accessibility\n  address1\n  address2\n  amenities\n  chainId\n  city\n  country\n  departmentDetails {\n    ...StoreDetailsFragment\n  }\n  isOpenNow\n  language\n  latitude\n  longitude\n  number\n  payments\n  postalCode\n  primaryDetails {\n    ...StoreDetailsFragment\n  }\n  shoppingModes\n  state\n  storeId\n  swiftlyServices\n  timeZone\n  ... on FDStoreEntry {\n    isHybrid\n  }\n}\n    \n\n    fragment StoreDetailsFragment on StoreDetails {\n  description\n  hours {\n    ...StoreHoursDailyFragment\n  }\n  images {\n    imageId\n    purpose\n    storeId\n    url\n  }\n  name\n  nextStatus {\n    asOfDate\n    status\n  }\n  contactNumbers {\n    displayName\n    imageId\n    intention\n    value\n  }\n  status {\n    asOfDate\n    status\n  }\n  websites {\n    description\n    value\n  }\n  phoneNumbers {\n    description\n    value\n  }\n}\n    \n\n    fragment StoreHoursDailyFragment on StoreHoursDaily {\n  day\n  hours {\n    close\n    open\n  }\n}\n    ",
            "variables": {
                "chainId": self.chain_id
            }
        }

        try:
            response = self.session.post('https://sm.swiftlyapi.net/graphql', json=query)
            response.raise_for_status()
            data = response.json()
            stores = data['data']['stores']['all']
            self.logger.info(f"Successfully fetched {len(stores)} stores")
            return stores
        except requests.RequestException as e:
            self.logger.error(f"Error fetching stores: {str(e)}")
            raise SaveMartScraperException(f"Error fetching stores: {str(e)}")

    def save_to_file(self, data: List[Dict[str, Any]], filename: str = "savemart_stores.json") -> None:
        """
        Save scraped data to a JSON file.

        :param data: List of dictionaries containing store information
        :param filename: Name of the file to save the data (default: savemart_stores.json)
        """
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.info(f"Data successfully saved to {filename}")
        except IOError as e:
            self.logger.error(f"Error saving data to file: {str(e)}")
            raise SaveMartScraperException(f"Error saving data to file: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="SaveMart Store Scraper")
    parser.add_argument("--output", default="savemart_stores.json", help="Output file name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")
    args = parser.parse_args()

    scraper = SaveMartScraper(log_level=args.log_level)
    stores = scraper.scrape_stores()
    
    # Print the scraped stores
    print(json.dumps(stores, indent=2))
    
    # Save to file
    scraper.save_to_file(stores, args.output)

if __name__ == "__main__":
    main()