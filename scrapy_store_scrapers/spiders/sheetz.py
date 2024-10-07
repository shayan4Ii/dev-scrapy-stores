import logging
from typing import Optional, Generator, Dict, List, Any

import scrapy
from scrapy.http import Request, Response
import json


class SheetzSpider(scrapy.Spider):
    name = "sheetz"
    allowed_domains = ["orders.sheetz.com"]
    # dont ignore 404
    handle_httpstatus_list = [404]
    API_SEARCH_FORMAT_URL = "https://orders.sheetz.com/anybff/api/stores/search?latitude=40.47275&longitude=-78.42507&page={pg_no}&size=15"

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "client-version": "2.39.1-4332",
            "content-type": "application/json",
            "priority": "u=1, i",
            "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "Referer": "https://orders.sheetz.com/findASheetz",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        }
    }

    def start_requests(self):

        url = self.API_SEARCH_FORMAT_URL.format(pg_no=0)

        body = json.dumps({})

        yield scrapy.Request(
            url,
            method='POST',
            body=body,
            callback=self.parse,
            cb_kwargs=dict(next_pg_no=1)
        )

    def parse(self, response, next_pg_no):
        data = json.loads(response.text)

        stores = data.get('stores', [])

        for store in stores:
            yield self._parse_store(store)

        if stores:
            url = self.API_SEARCH_FORMAT_URL.format(pg_no=next_pg_no)
            body = json.dumps({})
            yield scrapy.Request(
                url,
                method='POST',
                body=body,
                callback=self.parse,
                cb_kwargs=dict(next_pg_no=next_pg_no + 1)
            )
           
    def _parse_store(self, store):
        return {
            'number': str(store.get('storeNumber')),
            'address': self._get_address(store),
            'phone_number': store.get('phone'),
            'location': self._get_location(store),
            'url': "https://orders.sheetz.com/findASheetz",
            'raw': store
        }

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            street = store_info.get("address", "").strip()
            city = store_info.get("city", "").strip()
            state = store_info.get("state", "").strip()
            zipcode = store_info.get("zip", "").strip()

            city_state_zip = f"{city}, {state} {zipcode}".strip()
            full_address = ", ".join(filter(None, [street, city_state_zip]))

            if not full_address:
                self.logger.warning(
                    f"Missing address information for store: {store_info.get('storeNumber', 'Unknown')}")
            return full_address
        except Exception as e:
            self.logger.error(
                f"Error formatting address for store {store_info.get('storeNumber', 'Unknown')}: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> Optional[dict]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(
                f"Missing latitude or longitude for store: {store_info.get('storeNumber')}")
            return None
        except ValueError as error:
            self.logger.warning(
                f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(
                f"Error extracting location: {error}", exc_info=True)
        return None
