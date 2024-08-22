import json
import logging
from typing import Iterator, Dict, Any

import scrapy
from scrapy.http import Response


class SchnucksSpider(scrapy.Spider):
    """
    A spider to scrape location data from Schnucks' website.
    """
    name = "schnucks"
    allowed_domains = ["locations.schnucks.com"]
    start_urls = ["http://locations.schnucks.com/"]

    SCRIPT_TEXT_XPATH = '//script[@id="__NEXT_DATA__"]/text()'

    def parse(self, response: Response) -> Iterator[Dict[str, Any]]:
        """
        Parse the response and extract location data.

        Args:
            response (Response): The response object from the request.

        Yields:
            Iterator[Dict[str, Any]]: An iterator of dictionaries containing location data.
        """
        self.logger.info(f"Parsing response from {response.url}")

        try:
            json_text = response.xpath(self.SCRIPT_TEXT_XPATH).get()
            if not json_text:
                self.logger.error("Failed to find script tag with location data")
                return

            json_data = json.loads(json_text)
            locations = json_data["props"]["pageProps"]["locations"]

            self.logger.info(f"Found {len(locations)} locations")

            yield from locations

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON data: {str(e)}")
        except KeyError as e:
            self.logger.error(f"Failed to access expected key in JSON data: {str(e)}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")