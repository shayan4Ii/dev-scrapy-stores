import scrapy
import json


class CvsSpider(scrapy.Spider):
    name = "cvs"
    allowed_domains = ["www.cvs.com"]

    def start_requests(self):
        # Read zipcodes from JSON file
        with open('zipcodes.json', 'r') as f:
            zipcodes = json.load(f)

        for zipcode in zipcodes:
            url = f"https://www.cvs.com/api/locator/v2/stores/search?searchBy=USER-TEXT&latitude=&longitude=&searchText={zipcode}&searchRadiusInMiles=&maxItemsInResult=&filters=&resultsPerPage=5&pageNum=1"
            yield scrapy.Request(url, self.parse, headers=self.get_headers())

    def parse(self, response):
        # Parse the JSON response
        data = json.loads(response.text)
        # Process the data as needed
        # For example, you can yield items or make further requests

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
