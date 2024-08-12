import scrapy
import json
import re
from datetime import datetime
from typing import Iterator, Dict, Any, List
from scrapy_store_scrapers.items import SamsclubItem
import logging

class SamsclubSpider(scrapy.Spider):
    """Spider for scraping Sam's Club store information."""

    name = "samsclub"
    allowed_domains = ["www.samsclub.com"]

    # Constants
    CLUB_FINDER_URL = "https://www.samsclub.com/api/node/vivaldi/browse/v2/clubfinder/search?isActive=true"
    CLUB_URL_TEMPLATE = "https://www.samsclub.com/club/{}"

    def start_requests(self) -> Iterator[scrapy.Request]:
        """Initiate the scraping process."""
        headers = self.get_default_headers()
        yield scrapy.Request(url=self.CLUB_FINDER_URL, headers=headers, callback=self.parse)

    def parse(self, response: scrapy.http.Response) -> Iterator[scrapy.Request]:
        """Parse the club finder response and yield requests for individual clubs."""
        try:
            all_clubs = response.json()
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response from club finder")
            return

        headers = self.get_default_headers()

        for club in all_clubs:
            club_url = self.CLUB_URL_TEMPLATE.format(club['clubId'])
            yield scrapy.Request(url=club_url, headers=headers, callback=self.parse_club)

    def parse_club(self, response: scrapy.http.Response) -> SamsclubItem:
        """Parse individual club page and create SamsclubItem."""
        club_data = self.extract_club_data(response)
        return self.create_club_item(club_data, response)

    def extract_club_data(self, response: scrapy.http.Response) -> Dict[str, Any]:
        """Extract club data from the response."""
        script_text = response.xpath('//script[@id="tb-djs-wml-redux-state"]/text()').get()
        data_dict = json.loads(script_text)
        return data_dict['clubDetails']

    def create_club_item(self, club_data: Dict[str, Any], response: scrapy.http.Response) -> SamsclubItem:
        """Create a SamsclubItem from the extracted club data."""
        club_id = club_data['id']
        club_name = club_data['name']
        club_phone = self.format_phone(club_data['phone'])
        club_full_address = self.format_address(club_data['address'])
        club_latitude, club_longitude = self.extract_geo_info(club_data['geoPoint'])
        club_hours = self.format_hours(club_data['operationalHours'])
        services = self.extract_services(response)

        return SamsclubItem(
            name=f"{club_name} #{club_id}",
            address=club_full_address,
            phone=club_phone,
            hours=club_hours,
            location={
                "type": "Point",
                "coordinates": [club_longitude, club_latitude]
            },
            services=services
        )

    @staticmethod
    def format_phone(phone: str) -> str:
        """Format the phone number to (XXX) XXX-XXXX."""
        digits = re.sub(r'\D', '', phone)
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        return phone  # Return original if not 10 digits

    @staticmethod
    def format_address(address: Dict[str, str]) -> str:
        """Format the full address string."""
        return f"{address['address1']}, {address['city']}, {address['state']} {address['postalCode']}"

    @staticmethod
    def extract_geo_info(geo_info: Dict[str, float]) -> tuple:
        """Extract latitude and longitude from geo info."""
        return geo_info['latitude'], geo_info['longitude']

    def format_hours(self, hours: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Format the operational hours."""
        club_hours = {}
        day_order = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

        for day, day_hours in hours.items():
            formatted_hours = {
                'open': self.convert_to_12_hour(day_hours['startHrs']),
                'close': self.convert_to_12_hour(day_hours['endHrs'])
            }

            if day == 'monToFriHrs':
                for weekday in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']:
                    club_hours[weekday] = formatted_hours
            else:
                day = self.rename_day(day)
                club_hours[day] = formatted_hours

        return {day: club_hours[day] for day in day_order if day in club_hours}

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        """Convert 24-hour time string to 12-hour format."""
        time_obj = datetime.strptime(time_str, '%H:%M')
        return time_obj.strftime('%I:%M %p').lower()

    @staticmethod
    def rename_day(day: str) -> str:
        """Rename day from API format to standard format."""
        rename_dict = {
            'mondayHrs': 'monday', 'tuesdayHrs': 'tuesday', 'wednesdayHrs': 'wednesday',
            'thursdayHrs': 'thursday', 'fridayHrs': 'friday', 'saturdayHrs': 'saturday',
            'sundayHrs': 'sunday'
        }
        return rename_dict.get(day, day)

    @staticmethod
    def extract_services(response: scrapy.http.Response) -> List[str]:
        """Extract services from the response."""
        return response.xpath('//div[@class="bst-accordion-item-title"]/text()').getall()

    @staticmethod
    def get_default_headers() -> Dict[str, str]:
        """Return default headers for requests."""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.samsclub.com/club-finder',
            'Origin': 'https://www.samsclub.com'
        }
