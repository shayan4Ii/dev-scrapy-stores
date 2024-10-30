from typing import Iterable, Dict

import scrapy
from scrapy import Request
from scrapy.http import Response, JsonRequest

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
    center_processed = set()


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


    def parse(self, response: Response, **kwargs):
        centers = json.loads(response.text)['d']
        for center in centers:
            center_id = center['CenterGUID']
            if center_id in self.center_processed:
                continue
            self.center_processed.add(center_id)
            yield {
                "number": center_id,
                "name": center['CenterName'],
                "address": self._get_address(center),
                "location": {
                    "type": "Point",
                    "coordinates": [float(center['Lng']), float(center['Lat'])]
                },
                "phone_number": center['Phone'],
                "hours": {},
                "services": [],
                "url": "https://www.kumon.com/" + center['EpageUrl'],
                "raw": center
            }


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