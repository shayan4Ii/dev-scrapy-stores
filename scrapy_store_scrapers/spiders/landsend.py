import scrapy
from scrapy_store_scrapers.utils import *


class LandSend(scrapy.Spider):
    name = "landsend"
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "USER_AGENT": None,
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 0.1
    }


    def start_requests(self):
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://www.landsend.com/pp/StoreLocator?lat={zipcode['latitude']}&lng={zipcode['longitude']}&radius=200&S=S&L=L&C=undefined&N=N"
            yield scrapy.Request(url, callback=self.parse, meta={"impersonate": "chrome99_android"})


    def parse(self, response: Response) -> Iterable[Dict]:
        if response.xpath("//markers/marker"):
            for marker in response.xpath("//markers/marker"):
                yield {
                    "number": marker.xpath("./@storenumber").get(),
                    "name": marker.xpath("./@location").get(),
                    "address": self._get_address(marker),
                    "phone_number": marker.xpath("./@phonenumber").get(),
                    "location": {
                        "type": "Point",
                        "coordinates": [
                            float(marker.xpath("./@lng").get()),
                            float(marker.xpath("./@lat").get())
                        ]
                    },
                    "hours": self._get_hours(marker),
                }


    def _get_address(self, response: Response) -> str:
        try:
            address_parts = [
                response.xpath("./@address").get(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("./@city").get()
            state = response.xpath("./@state").get()
            zipcode = response.xpath("./@zip").get()

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""

    def _get_hours(self, marker: scrapy.Selector) -> List[Dict]:
        """Extract and parse store hours."""
        try:
            hours_string = marker.xpath("./@storehours").get()
            if not hours_string:
                self.logger.warning(f"No hours found for store")
                return {}

            hours_example = HoursExample()
            normalized_hours = hours_example.normalize_hours_text(hours_string)
            return hours_example._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}