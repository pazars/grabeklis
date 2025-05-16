# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


# TODO: Write unit tests for this
class LSMArticle(scrapy.Item):
    url = scrapy.Field()
    date = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    summary = scrapy.Field()
    article = scrapy.Field()
    error = scrapy.Field()

    def check_if_failed(self):
        for param in vars(self).values():
            if "error" in param:
                return True
        return False
