import json
import logging
from typing import Any, Union, Generator

import scrapy
from scrapy.http import Response

class RaleysSpider(scrapy.Spider):
    name = "raleys"
    allowed_domains = ["www.raleys.com"]

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """
        Initiates the crawling process by sending the first request.

        Yields:
            scrapy.Request: The initial request to start crawling.
        """
        url = 'https://www.raleys.com/api/store'
        data = self.get_payload(0)
        try:
            yield scrapy.Request(
                method="POST",
                url=url,
                body=json.dumps(data),
                headers={'Content-Type': 'application/json'},
                callback=self.parse
            )
        except Exception as e:
            self.logger.error(f"Error in start_requests: {str(e)}")

    def parse(self, response: Response) -> Generator[Union[dict, scrapy.Request], None, None]:
        """
        Parses the response and yields store data. If there are more pages,
        it sends a new request for the next page.

        Args:
            response (Response): The response object from the request.

        Yields:
            dict[str, Any]: Store data from the response.
            scrapy.Request: Next page request if there are more pages.
        """
        try:
            data = response.json()
            stores = data['data']
            yield from stores

            if data['offset'] <= data['total']:
                url = 'https://www.raleys.com/api/store'
                new_data = self.get_payload(data['offset'])
                yield scrapy.Request(
                    method="POST",
                    url=url,
                    body=json.dumps(new_data),
                    headers={'Content-Type': 'application/json'},
                    callback=self.parse
                )
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON from response: {response.text}")
        except KeyError as e:
            self.logger.error(f"Missing key in response data: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse method: {str(e)}")

    @staticmethod
    def get_payload(offset: int) -> dict[str, Any]:
        """
        Generates the payload for the API request.

        Args:
            offset (int): The offset for pagination.

        Returns:
            dict[str, Any]: The payload dictionary for the API request.
        """
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