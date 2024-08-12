import scrapy
import json


class CvsSpider(scrapy.Spider):
    name = "cvs"
    allowed_domains = ["www.cvs.com"]

    def start_requests(self):
        # Read zipcodes from JSON file
        with open('zipcodes.json', 'r') as f:
            zipcodes_data = json.load(f)

        for city_data in zipcodes_data:
            city = city_data['city']
            state = city_data['state']
            cbsa = city_data['cbsa']
            for zipcode in city_data['zip_codes']:
                url = f"https://www.cvs.com/api/locator/v2/stores/search?searchBy=USER-TEXT&latitude=&longitude=&searchText={zipcode}&searchRadiusInMiles=&maxItemsInResult=&filters=&resultsPerPage=5&pageNum=1"
                yield scrapy.Request(
                    url,
                    self.parse,
                    headers=self.get_headers(),
                    meta={
                        'city': city,
                        'state': state,
                        'cbsa': cbsa,
                        'zipcode': zipcode
                    }
                )

    def parse(self, response):
        # Parse the JSON response
        data = json.loads(response.text)
        # Process the data as needed
        # For example, you can yield items or make further requests
        for store in data.get('stores', []):
            yield {
                'city': response.meta['city'],
                'state': response.meta['state'],
                'cbsa': response.meta['cbsa'],
                'zipcode': response.meta['zipcode'],
                'store_data': store
            }

    @staticmethod
    def get_headers():
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "consumer": "SLP",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"99\", \"Google Chrome\";v=\"127\", \"Chromium\";v=\"127\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-api-key": "k6DnPo1puMOQmAhSCiRGYvzMYOSFu903",
            "Referer": "https://www.cvs.com/store-locator/landing",
            "Referrer-Policy": "origin-when-cross-origin"
        }
