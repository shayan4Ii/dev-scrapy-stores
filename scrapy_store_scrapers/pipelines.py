# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
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

    def process_item(self, item, spider):
        
        store_id = item['stlocID']

        if store_id in self.seen_store_ids:
            raise DropItem(f"Duplicate store found: {item}")
        else:
            self.seen_store_ids.add(store_id)

        return item
