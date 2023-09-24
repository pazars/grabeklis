# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GrabeklisItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class LSMArticle(scrapy.Item):
    url = scrapy.Field()
    publish_date = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    lead = scrapy.Field()
    article = scrapy.Field()

    def has_missing_content(self):
        for value in vars(self).values():
            if len(value) == 0:
                return True
        return False
