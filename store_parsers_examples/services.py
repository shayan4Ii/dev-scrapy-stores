from scrapy.http import TextResponse
from scrapy.http import Response
import logging

class ServicesExample:
    SERVICES_XPATH = '//li[@class="service-item"]/text()'
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _get_services(self, response: Response) -> list[str]:
        """Extract store services."""
        try:
            services = response.xpath(self.SERVICES_XPATH).getall()
            return [service.strip() for service in services if service.strip()]
        except Exception as e:
            self.logger.error(f"Error extracting services: {e}", exc_info=True)
            return []

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Create example instance
    services_processor = ServicesExample()
    
    # Test HTML data
    sample_html = """
    <html>
        <body>
            <ul class="services-list">
                <li class="service-item">Drive-thru</li>
                <li class="service-item">Delivery</li>
                <li class="service-item">Curbside pickup</li>
                <li class="service-item">Online ordering</li>
            </ul>
        </body>
    </html>
    """
    
    # Create a mock response
    mock_response = TextResponse(url="http://example.com", body=sample_html, encoding='utf-8')
    
    # Process and display results
    services = services_processor._get_services(mock_response)
    print("\nProcessed Services:")
    print(services)