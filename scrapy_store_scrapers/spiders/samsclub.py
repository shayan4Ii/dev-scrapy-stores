import scrapy
import json
from datetime import datetime
from typing import Dict, List, Any, Iterator
from scrapy_store_scrapers.items import SamsclubItem

class SamsclubSpider(scrapy.Spider):
    name = "samsclub"
    allowed_domains: List[str] = ["www.samsclub.com"]

    def start_requests(self) -> Iterator[scrapy.Request]:
        url: str = "https://www.samsclub.com/api/node/vivaldi/browse/v2/clubfinder/search?isActive=true"
        headers: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.samsclub.com/club-finder',
            'Origin': 'https://www.samsclub.com'
        }
        yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response: scrapy.http.Response) -> Iterator[scrapy.Request]:
        all_clubs: List[Dict[str, Any]] = response.json()

        headers: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.samsclub.com/club-finder',
            'Upgrade-Insecure-Requests': '1'
        }

        for club in all_clubs:
            club_url: str = f"https://www.samsclub.com/club/{club['clubId']}"
            yield scrapy.Request(url=club_url, headers=headers, callback=self.parse_club)
            break

    @staticmethod
    def convert_to_12_hour(time_str: str) -> str:
        time_obj: datetime = datetime.strptime(time_str, '%H:%M')
        return time_obj.strftime('%I:%M %p').lower()

    def parse_club(self, response: scrapy.http.Response) -> SamsclubItem:
        print(response.url)
        print(response.text)
        script_text: str = response.xpath('//script[@id="tb-djs-wml-redux-state"]/text()').get()
        data_dict: Dict[str, Any] = json.loads(script_text)
        raw_club_info: Dict[str, Any] = data_dict['clubDetails']
        club_id: str = raw_club_info['id']

        club_name: str = raw_club_info['name']
        club_phone: str = raw_club_info['phone']

        address: Dict[str, str] = raw_club_info['address']

        club_address: str = address['address1']
        club_city: str = address['city']
        club_state: str = address['state']
        club_zip: str = address['postalCode']
        
        club_full_address: str = f"{club_address}, {club_city}, {club_state} {club_zip}"

        geo_info: Dict[str, float] = raw_club_info['geoPoint']

        club_latitude: float = geo_info['latitude']
        club_longitude: float = geo_info['longitude']

        hours: Dict[str, Dict[str, str]] = raw_club_info['operationalHours']

        club_hours: Dict[str, Dict[str, str]] = {}

        for day, day_hours in hours.items():
            hours_dict: Dict[str, str] = {
                'open': self.convert_to_12_hour(day_hours['startHrs']),
                'close': self.convert_to_12_hour(day_hours['endHrs'])
            }

            rename_dict: Dict[str, str] = {
                'mondayHrs': 'monday',
                'tuesdayHrs': 'tuesday',
                'wednesdayHrs': 'wednesday',
                'thursdayHrs': 'thursday',
                'fridayHrs': 'friday',
                'saturdayHrs': 'saturday',
                'sundayHrs': 'sunday'
            }

            if day == 'monToFriHrs':
                week_days: List[str] = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
                for weekday in week_days:
                    club_hours[weekday] = hours_dict
            else:
                day = rename_dict[day]
                club_hours[day] = hours_dict

        # Define the correct order of days
        day_order: List[str] = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        
        # Sort club hours based on the defined order
        club_hours = {day: club_hours[day] for day in day_order if day in club_hours}
        
        services: List[str] = response.xpath('//div[@class="bst-accordion-item-title"]/text()').getall()
        
        item: SamsclubItem = SamsclubItem(
            name=f"{club_name} #{club_id}",
            address=club_full_address,
            phone=club_phone,
            hours=club_hours,
            location={
                "type": "Point",
                "coordinates": [
                    club_longitude,
                    club_latitude
                ]
            },
            services=services
        )
        
        return item


        
