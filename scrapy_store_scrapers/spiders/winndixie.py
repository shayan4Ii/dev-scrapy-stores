import json
import scrapy


class WinndixieSpider(scrapy.Spider):
    name = "winndixie"
    allowed_domains = ["www.winndixie.com"]

    def start_requests(self):
        url = "https://www.winndixie.com/V2/storelocator/getStores"

        zipcodes = self.load_zipcodes("zipcodes.json")

        for zipcode in zipcodes:
            data = {
                "search": zipcode,
                "strDefaultMiles": "25",
                "filter": ""
            }

            yield scrapy.Request(
                method='POST',
                headers=self.get_headers(),
                url=url,
                body=json.dumps(data)
            )


    def parse(self, response):
        yield from response.json()

    def load_zipcodes(self, zipcode_file) -> list[str]:
        """Load zipcodes from the JSON file."""
        try:
            with open(zipcode_file, 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.zipcode_file}")
            raise FileNotFoundError(f"File not found: {self.zipcode_file}")
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON file: {self.zipcode_file}")
            raise ValueError(f"Invalid JSON file: {self.zipcode_file}")
        
        zipcodes = []
        for location in locations:
            zipcodes.extend(location.get('zip_codes', []))
        return zipcodes

    @staticmethod
    def filter_stores(stores):
        return [
            store for store in stores
            if (
                'liquor' not in store['Location']['LocationTypeDescription'].lower()
                or (store['StoreCode'] == '1489' and not store['ParentStore'])
            )
        ]

    @staticmethod
    def get_headers():
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://www.winndixie.com",
            "referer": "https://www.winndixie.com/locator",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        }
    