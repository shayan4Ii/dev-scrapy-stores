import scrapy


class AlbertsonsSpider(scrapy.Spider):
    name = "albertsons"
    allowed_domains = ["local.albertsons.com"]
    start_urls = ["https://local.albertsons.com/az.html"]
    # start_urls = ["http://local.albertsons.com/"]

    def parse(self, response):
        if response.xpath('//ul[@class="Directory-listLinks"]'):
            for a_elem in response.xpath('//ul[@class="Directory-listLinks"]/li/a'):
                link = a_elem.xpath('./@href').get()
                is_multiple_stores = a_elem.xpath(
                    './@data-count').get('').strip() != '(1)'
                if is_multiple_stores:
                    yield response.follow(link, callback=self.parse)
                else:
                    yield response.follow(link, callback=self.parse_store)
        elif response.xpath('//ul[@class="Directory-listTeasers Directory-row"]'):
            for link in response.xpath('//ul[@class="Directory-listTeasers Directory-row"]/li/article/h2/a/@href').getall():
                yield response.follow(link, callback=self.parse_store)
        # //ul[@class="Directory-listTeasers Directory-row"]/li/article/h2/a/@href

    def parse_store(self, response):
        store_data = {}

        store_data['name'] = response.xpath(
            '//h1/span[@class="RedesignHero-subtitle Heading--lead"]/text()').get('').strip()

        address_elem = response.xpath('//address[@itemprop="address"]')

        street_address = address_elem.xpath(
            './/span[@class="c-address-street-1"]/text()').get('').strip()
        city = address_elem.xpath(
            './/span[@class="c-address-city"]/text()').get('').strip()
        region = address_elem.xpath(
            './/abbr[@itemprop="addressRegion"]/text()').get('').strip()
        postal_code = address_elem.xpath(
            './/span[@itemprop="postalCode"]/text()').get('').strip()

        store_data['address'] = f"{street_address}, {city}, {region} {postal_code}"

        store_data['phone_number'] = response.xpath(
            '//div[@id="phone-main"]/text()').get('')

        latitude = response.xpath(
            '//meta[@itemprop="latitude"]/@content').get('')
        longitude = response.xpath(
            '//meta[@itemprop="longitude"]/@content').get('')

        if latitude and longitude:
            store_data['location'] = {
                'type': 'Point',
                'coordinates': [float(longitude), float(latitude)]
            }
        else:
            store_data['location'] = None

        store_hours_container = response.xpath(
            '//div[@class="RedesignCore-hours js-intent-core-hours is-hidden"]')[0]
        hours_detail_rows = store_hours_container.xpath(
            './/table[@class="c-hours-details"]/tbody/tr')

        hours = {}

        for row in hours_detail_rows:
            day = row.xpath(
                './td[@class="c-hours-details-row-day"]/text()').get('').strip().lower()
            open_time = row.xpath(
                './/span[@class="c-hours-details-row-intervals-instance-open"]/text()').get('').strip()
            close_time = row.xpath(
                './/span[@class="c-hours-details-row-intervals-instance-close"]/text()').get('').strip()
            hours[day] = {
                "open": open_time.lower(),
                "close": close_time.lower()
            }

        store_data['hours'] = hours

        services = response.xpath(
            '//ul[@id="service-list"]/li//*[@itemprop="name"]/text()').getall()

        store_data['services'] = [
            service.replace("[c_groceryBrand]", "Albertsons").replace("[name]", "Albertsons")
            for service in services]

        yield store_data
