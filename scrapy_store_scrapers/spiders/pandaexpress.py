import json
from typing import Any
import scrapy


class PandaexpressSpider(scrapy.Spider):
    name = "pandaexpress"
    allowed_domains = ["www.pandaexpress.com"]
    start_urls = ["https://www.pandaexpress.com/locations"]

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'COOKIES_ENABLED': True,
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.pandaexpress.com/',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        },
        'CONCURRENT_REQUESTS': 64,
    }

    def parse(self, response):

        if response.xpath('//div[@class="location_single__content"]'):
            yield self.parse_store(response)
        
        all_loc_urls = response.xpath('//div[@class="locations_filter__content"]//div/a[@data-ga-action="storeDetailsClick" or @data-ga-action="locationClick"]/@href').getall()
        for loc_url in all_loc_urls:
            yield response.follow(loc_url, callback=self.parse)

    def parse_store(self, response):
        json_text = response.xpath('//script[@type="application/ld+json"]/text()').get()
        data = json.loads(json_text)
        return {
            "name": response.xpath('//div[contains(@class, "name")]/h1/text()').get(),
            "phone_number": data.get("telephone"),
            "location": self._get_location(data),
            "address": self._get_address(data.get("address", {})),
            "hours": self._get_hours(data),
            "url": response.url,
            "raw": data
        }

    def _get_location(self, store_info) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = store_info['geo']['latitude']
            longitude = store_info['geo']['longitude']

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude for store")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}
    
    def _get_address(self, address_info: dict[str, Any]) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("streetAddress", ""),
                # address_info.get("address_2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("addressLocality", "")
            state = address_info.get("addressRegion", "")
            zipcode = address_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(
                    f"Missing address information: {address_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_hours(self, store_info: dict[str, Any]) -> dict:
        """Extract and format store hours."""
        try:
            hours_raw_list = store_info.get("openingHoursSpecification", [])
            hours_dict = {}
            for hours_obj in hours_raw_list:
                day = hours_obj.get("dayOfWeek", "")
                opens = hours_obj.get("opens", "").lower()
                closes = hours_obj.get("closes", "").lower()
                if day and opens and closes:
                    hours_dict[day.lower()] = {
                        "open": opens,
                        "close": closes
                    }
            return hours_dict
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}