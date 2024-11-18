# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from scrapy.exceptions import DropItem  


class DuplicateItemPipeline:
    items = set()    
    

    def process_item(self, item, spider):
        number = item.get("number")
        address = item.get("address")

        if number:
            if number in self.items:
                raise DropItem(f"Duplicate item found: {item['number']}")
            self.items.add(number)
        elif address:
            if address in self.items:
                raise DropItem(f"Duplicate item found: {item['address']}")
            self.items.add(address)
        else:
            spider.logger.warning("Item cannot be checked for duplicates because it is missing a number or name")

        return item
