from scrapy import Item, Field
from typing import Union, Dict, List


class AlbertsonsStoreItem(Item):
    name: str = Field()
    address: str = Field()
    phone_number: str = Field()
    location: Dict[str, Union[str, List[float]]] = Field()
    hours: Dict[str, Dict[str, str]] = Field()
    services: List[str] = Field()

class SamsclubItem(Item):
    name: str = Field()
    number: int = Field()
    address: str = Field()
    phone: str = Field()
    hours: Dict[str, Dict[str, str]] = Field()
    location: Dict[str, Union[str, List[float]]] = Field()
    services: List[str] = Field()

class WalmartStoreItem(Item):
    name: str = Field()
    number: int = Field()
    address: str = Field()
    phone_number: str = Field()
    hours: Dict[str, Dict[str, str]] = Field()
    location: Dict[str, Union[str, List[float]]] = Field()
    services: List[str] = Field()

class PizzahutStoreItem(Item):
    address: str = Field()
    phone_number: str = Field()
    hours: Dict[str, Dict[str, str]] = Field()
    location: Dict[str, Union[str, List[float]]] = Field()
    services: List[str] = Field()

class MetrobytStoreItem(Item):
    address: str = Field()
    phone_number: str = Field()
    hours: Dict[str, Dict[str, str]] = Field()
    location: Dict[str, Union[str, List[float]]] = Field()

class TraderjoesStoreItem(Item):
    name: str = Field()
    store_status: str = Field()
    number: int = Field()
    address: str = Field()
    phone_number: str = Field()
    hours: Dict[str, Dict[str, str]] = Field()
    location: Dict[str, Union[str, List[float]]] = Field()

class ZipcodeLongLatItem(Item):
    zipcode: str = Field()
    latitude: float = Field()
    longitude: float = Field()

class KFCStoreItem(Item):
    name: str = Field()
    address: str = Field()
    phone_number: str = Field()
    hours: Dict[str, Dict[str, str]] = Field()
    location: Dict[str, Union[str, List[float]]] = Field()
    