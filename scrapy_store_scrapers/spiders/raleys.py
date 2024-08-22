import json
import scrapy


class RaleysSpider(scrapy.Spider):
    name = "raleys"
    allowed_domains = ["www.raleys.com"]

    def start_requests(self):
        url = 'https://www.raleys.com/api/store'
        data = self.get_payload(0)
        yield scrapy.Request(method="POST", url=url, body=json.dumps(data), headers={'Content-Type': 'application/json'}, callback=self.parse)

    def parse(self, response):
        data = response.json()
        stores = data['data']
        yield from stores

        if data['offset'] <= data['total']:
            url = 'https://www.raleys.com/api/store'
            data = self.get_payload(data['offset'])
            yield scrapy.Request(method="POST", url=url, body=json.dumps(data), headers={'Content-Type': 'application/json'}, callback=self.parse)


    @staticmethod
    def get_payload(offset):
        return {
            "offset": offset,
            "rows": 75,
            "searchParameter": {
                "shippingMethod": "pickup",
                "searchString": "",
                "latitude": "",
                "longitude": "",
                "maxMiles": 99999,
                "departmentIds": []
            }
        }