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
                    "name": marker.xpath("./@name").get(),
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
                response.xpath("//markers/marker/@address").get(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("//markers/marker/@city").get()
            state = response.xpath("//markers/marker/@state").get()
            zipcode = response.xpath("//markers/marker/@zip").get()

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, marker: scrapy.Selector) -> List[Dict]:
        hours = {}
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        try:
            hours_string = marker.xpath("./@storehours").get()
            # 'Mon-Sat 10 am-7 pm Sun 11 am-6 pm '
            if 'closed' not in hours_string.lower():
                sunday_open, sunday_close = hours_string.split("Sun")[-1].strip().split("-")
                hours['sunday'] = {
                    "open": convert_to_12h_format(sunday_open),
                    "close": convert_to_12h_format(sunday_close)
                }
            # 'Mon-Thurs 10am-8pm Fri-Sat 10am-9pm Sun 11am-6pm'
            if 'mon-sat' in hours_string.lower():
                rest_days_open, rest_days_close = hours_string.split("Sun")[0].lower().strip("mon-sat").split("-")
                for day in days[:-1]:
                    hours[day] = {
                        "open": convert_to_12h_format(rest_days_open.strip()),
                        "close": convert_to_12h_format(rest_days_close.strip())
                    }
            elif "fri-sat" in hours_string.lower():
                fri_sat_open, fri_sat_close = hours_string.split("Sun")[0].lower().split("fri-sat")[-1].split("-")
                for day in days[-3:-1]:
                    hours[day] = {
                        "open": convert_to_12h_format(fri_sat_open.strip()),
                        "close": convert_to_12h_format(fri_sat_close.strip())
                    }
                mon_thurs_open, mon_thurs_close = hours_string.split("Sun")[0].lower().split("fri-sat")[0].split("mon-thurs")[-1].split("-")
                for day in days[:-3]:
                    hours[day] = {
                        "open": convert_to_12h_format(mon_thurs_open.strip()),
                        "close": convert_to_12h_format(mon_thurs_close.strip())
                    }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return []
