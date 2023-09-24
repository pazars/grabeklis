import re
import sys
import json

from pathlib import Path
from datetime import datetime
from scrapy.spiders import SitemapSpider
from grabeklis.items import LSMArticle


class LSMSitemapSpider(SitemapSpider):
    name = "lsmsitemap"
    sitemap_urls = ["https://www.lsm.lv/sitemap.xml"]

    # Bez rakstiem ir arī tēmas, bet tās ir kā rakstu kolekcijas un tiek izlaistas.
    sitemap_rules = [
        ("/raksts/", "parse_raksts"),
    ]

    # Get the directory path of the running script
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir.parent.parent / "data"

    url_history_path = data_dir / "lsm_history.json"

    lsm_articles = []
    lsm_history = set()
    if url_history_path.exists():
        with open(url_history_path, "r") as f:
            lsm_history = set(json.load(f))

    def parse_raksts(self, response):
        if response.url in self.lsm_history:
            self.logger.info(f"Already scraped: {response.url}")
            return

        publish_date_raw = response.xpath('//div[@class="info-item time"]/text()').get()

        # Remove newline characters
        publish_date = re.sub(r"\n", "", publish_date_raw)

        # Remove multiple consecutive spaces and leading/trailing spaces
        publish_date = re.sub(r"\s+", " ", publish_date).strip()

        item = LSMArticle(
            url=response.url,
            publish_date=publish_date,
            category=response.xpath(
                '//div[@class="info-item category"]/a/text()'
            ).get(),
            title=response.xpath('//h1[@class="article-title"]/text()').get(),
            lead=response.xpath('//h2[@class="article-lead"]/text()').get(),
            article=response.xpath('//div[@class="article__body"]/p/text()').get(),
        )

        if not item.has_missing_content():
            self.lsm_articles.append(dict(item))
            self.lsm_history.add(response.url)

        if sys.getsizeof(self.lsm_articles) > 1024 * 10:  # 10 MB
            time_str = datetime.now().strftime("%Y%m%d%H%M%S")
            file_path = self.data_dir / f"lsm_articles_{time_str}.json"
            with open(file_path, "w") as f:
                json.dump(list(self.lsm_articles), f, indent=4)
            self.lsm_articles = []

            self.lsm_history.add(response.url)
            with open(self.url_history_path, "w") as f:
                json.dump(list(self.lsm_history), f, indent=4)

        yield item
