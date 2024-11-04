import scrapy
from scrapy.http import JsonRequest

from scrapy_store_scrapers.utils import *



class Kumon(scrapy.Spider):
    name = "kumon"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        USER_AGENT = None
    )


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            json_data = {
                'latitude': zipcode['latitude'],
                'longitude': zipcode['longitude'],
                'radius': 25,
                'distanceUnit': 'mi',
                'countryCode': 'USA',
                'showVirtualFlg': 0,
                'searchAddress': '',
            }
            yield JsonRequest(
                url="https://www.kumon.com/Services/KumonWebService.asmx/GetCenterListByRadius",
                callback=self.parse,
                data=json_data,
                method="POST",
                meta={"impersonate": "safari15_5"}
            )


    def parse(self, response: Response) -> Iterable[Request]:
        centers = json.loads(response.text)['d']
        for center in centers:
            partial_item = {
                "name": center['CenterName'],
                "address": self._get_address(center),
                "location": {
                    "type": "Point",
                    "coordinates": [float(center['Lng']), float(center['Lat'])]
                },
                "phone_number": center['Phone'],
                "hours": {},
                "url": "https://www.kumon.com/" + center['EpageUrl'],
                "raw": center
            }
            yield scrapy.Request(
                url=partial_item['url'],
                callback=self.parse_center,
                cb_kwargs={"partial_item": partial_item},
                meta={"impersonate": "safari15_5"}
            )


    def parse_center(self, response: Response, partial_item: Dict) -> Dict:
        partial_item.update({
            "hours": self._get_hours(response)
        })
        item = partial_item
        return item


    def _get_address(self, center: Dict) -> str:
        try:
            address_parts = [
                center.get("Address", ""),
                center.get("Address2", ""),
                center.get("Address3", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = center.get("City", "")
            state = center.get("StateCode", "")
            zipcode = center.get("ZipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
    

    def _get_hours(self, response: Response) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        get_day = lambda d: next((day for day in days if d.lower() in day), None)
        new_item = {}
        try:
            for day in response.xpath("//input[@id='hour3']/following-sibling::div//li"):
                name = day.xpath("./span[@class='day']/text()").get().lower().strip(":")
                open, close = day.xpath(".//span[@class='class-hr']/text()").get().split("-")
                new_item[get_day(name)] = {
                    "open": convert_to_12h_format(open),
                    "close": convert_to_12h_format(close)
                }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}