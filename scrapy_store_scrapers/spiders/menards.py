from datetime import datetime
import json
import scrapy


class MenardsSpider(scrapy.Spider):
    name = "menards"
    allowed_domains = ["www.menards.com"]
    start_urls = ["https://www.menards.com/store-details/locator.html"]

    JSON_XPATH = '//meta[@id="initialStores"]/@data-initial-stores'
    STORE_INFO_JSON_XPATH = '//store-details/@*[name()=":store-info"]'
    STORE_HOURS_JSON_XPATH = '//store-details/@*[name()=":store-info-hours"]'


    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        },
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
    }

    def parse(self, response):

        for store_url in self._get_store_urls(response):
            yield scrapy.Request(store_url, callback=self.parse_store)

    def _get_store_urls(self, response):
        json_text = response.xpath(self.JSON_XPATH).get()
        stores = json.loads(json_text)

        for store in stores:
            store_id = store["number"]
            store_url = f"https://www.menards.com/store-details/store.html?store={store_id}"
            yield store_url

    def parse_store(self, response):
        parsed_store = {}

        store_info_dict = self._get_store_info_dict(response)
        store_hours_dict = self._get_store_hours_dict(response)

        parsed_store["number"] = store_info_dict["number"]
        parsed_store["name"] = store_info_dict["name"]
        parsed_store["phone_number"] = store_info_dict["phone"]

        parsed_store["address"] = self._get_address(store_info_dict)
        parsed_store["location"] = self._get_location(store_info_dict)
        parsed_store["hours"] = self._get_hours(store_hours_dict)

        parsed_store["services"] =[service.get('displayName') for service in store_info_dict.get("services", [])]
        parsed_store["url"] = response.url

        store_info_dict['hours'] = store_hours_dict

        parsed_store["raw"] = store_info_dict
        return parsed_store


    
    def _get_store_info_dict(self, response):
        store_info_text = response.xpath(self.STORE_INFO_JSON_XPATH).get()
        return json.loads(store_info_text)

    def _get_store_hours_dict(self, response):
        store_hours_text = response.xpath(self.STORE_HOURS_JSON_XPATH).get()
        return json.loads(store_hours_text)
    
    def _get_address(self, store_info) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("street", ""),
                # store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information: {store_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info):
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store: {store_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}

    def _get_hours(self, hours_info: list) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}

            for hour_info in hours_info:
                day = self._get_day_from_date(hour_info["date"])
                open_time = self._convert_to_12h_format(hour_info["open"])
                close_time = self._convert_to_12h_format(hour_info["close"])
                
                hours[day] = {
                    "open": open_time,
                    "close": close_time
                }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}
        

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            time_obj = datetime.strptime(time_str, '%H:%M:%S').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return time_str


    @staticmethod
    def _get_day_from_date(date_str: str) -> str:
        """Get day of the week from date string."""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj.strftime("%A").lower()
        except ValueError:
            return date_str
