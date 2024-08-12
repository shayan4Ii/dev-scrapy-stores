import scrapy


class SamsclubSpider(scrapy.Spider):
    name = "samsclub"
    allowed_domains = ["www.samsclub.com"]

    def start_requests(self):
        url = "https://www.samsclub.com/api/node/vivaldi/browse/v2/clubfinder/search?isActive=true"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.samsclub.com/club-finder',
            'Origin': 'https://www.samsclub.com'
        }
        yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        all_clubs = response.json()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.samsclub.com/club-finder',
            'Upgrade-Insecure-Requests': '1'
        }

        for club in all_clubs:
            club_url = f"https://www.samsclub.com/club/{club['clubId']}"
            yield scrapy.Request(url=club_url, headers=headers, callback=self.parse_club)
            break
            
    def parse_club(self, response):
        print(response.url)
        print(response.text)
