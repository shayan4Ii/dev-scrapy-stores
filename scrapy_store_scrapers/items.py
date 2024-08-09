from scrapy import Item, Field
from typing import Optional


class AlbertsonsStoreItem(Item):
    name: str = Field()
    address: str = Field()
    phone_number: str = Field()
    location: dict[str, list[float]] = Field()
    hours: dict[str, dict[str, str]] = Field()
    services: list[str] = Field()