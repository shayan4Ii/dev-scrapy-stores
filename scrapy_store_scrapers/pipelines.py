# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import logging
from typing import Any, Optional

from scrapy.exceptions import DropItem
from scrapy import Spider
from scrapy_store_scrapers.items import ZipcodeLongLatItem

class TacobellDuplicatesPipeline:

    def __init__(self):
        self.seen_store_ids = set()

    def process_item(self, item, spider):
        print(type(item), item)
        if isinstance(item, ZipcodeLongLatItem):
            return item
        
        store_id = item['storeNumber']

        if store_id in self.seen_store_ids:
            raise DropItem(f"Duplicate store found: {item}")
        else:
            self.seen_store_ids.add(store_id)

        return item

class CostcoDuplicatesPipeline:
    def __init__(self):
        self.seen_store_ids = set()
        self.duplicate_count = 0
        self.processed_count = 0
        self.logger = logging.getLogger(__name__)


    def open_spider(self, spider: Spider) -> None:
        """Initialize the pipeline when the spider opens."""
        self.logger.info("CostcoDuplicatesPipeline initialized")

    def close_spider(self, spider: Spider) -> None:
        """Log statistics when the spider closes."""
        self.logger.info(f"Processed items: {self.processed_count}")
        self.logger.info(f"Duplicate items dropped: {self.duplicate_count}")
        self.logger.info(f"Unique stores found: {len(self.seen_store_ids)}")

    def process_item(self, item: dict[str, Any], spider: Spider) -> Optional[dict[str, Any]]:
        """
        Process each item to check for duplicates.
        
        Args:
            item (Dict[str, Any]): The scraped item
            spider (Spider): The spider that scraped the item
        
        Returns:
            Optional[Dict[str, Any]]: The item if it's not a duplicate, None otherwise
        
        Raises:
            DropItem: If the item is a duplicate
        """
        self.processed_count += 1
        
        store_id = item.get('stlocID')
        if not store_id:
            self.logger.warning(f"Item without stlocID encountered: {item}")
            return item

        if store_id in self.seen_store_ids:
            self.duplicate_count += 1
            raise DropItem(f"Duplicate store found: {store_id}")
        
        self.seen_store_ids.add(store_id)
        return item

    def get_stats(self) -> dict[str, int]:
        """Return current statistics."""
        return {
            "processed_count": self.processed_count,
            "duplicate_count": self.duplicate_count,
            "unique_stores": len(self.seen_store_ids)
        }

class WinnDixieDuplicatesPipeline:
    def __init__(self):
        self.seen_store_ids = set()
        self.duplicate_count = 0
        self.processed_count = 0
        self.logger = logging.getLogger(__name__)


    def open_spider(self, spider: Spider) -> None:
        """Initialize the pipeline when the spider opens."""
        self.logger.info("WinnDixieDuplicatesPipeline initialized")

    def close_spider(self, spider: Spider) -> None:
        """Log statistics when the spider closes."""
        self.logger.info(f"Processed items: {self.processed_count}")
        self.logger.info(f"Duplicate items dropped: {self.duplicate_count}")
        self.logger.info(f"Unique stores found: {len(self.seen_store_ids)}")

    def process_item(self, item: dict[str, Any], spider: Spider) -> Optional[dict[str, Any]]:
        """
        Process each item to check for duplicates.
        
        Args:
            item (Dict[str, Any]): The scraped item
            spider (Spider): The spider that scraped the item
        
        Returns:
            Optional[Dict[str, Any]]: The item if it's not a duplicate, None otherwise
        
        Raises:
            DropItem: If the item is a duplicate
        """
        self.processed_count += 1
        
        store_id = item.get('StoreCode')
        if not store_id:
            self.logger.warning(f"Item without StoreCode encountered: {item}")
            return item

        if store_id in self.seen_store_ids:
            self.duplicate_count += 1
            raise DropItem(f"Duplicate store found: {store_id}")
        
        self.seen_store_ids.add(store_id)
        return item

    def get_stats(self) -> dict[str, int]:
        """Return current statistics."""
        return {
            "processed_count": self.processed_count,
            "duplicate_count": self.duplicate_count,
            "unique_stores": len(self.seen_store_ids)
        }