from scrapy import Item, Field
from typing import Union


class AlbertsonsStoreItem(Item):
    name: str = Field()
    address: str = Field()
    phone_number: str = Field()
    location: dict[str, Union[str, list[float]]] = Field()
    hours: dict[str, dict[str, str]] = Field()
    services: list[str] = Field()

class SamsclubItem(Item):
    name: str = Field()
    address: str = Field()
    phone: str = Field()
    hours: dict[str, dict[str, str]] = Field()
    location: dict[str, Union[str, list[float]]] = Field()
    services: list[str] = Field()
