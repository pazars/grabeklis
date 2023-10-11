import re
import sys
import json
import pytz

from pathlib import Path
from datetime import datetime, timedelta

from scrapy import signals
from scrapy.spiders import SitemapSpider
from grabeklis.items import LSMArticle
from grabeklis import settings, utils


class LSMSitemapSpider(SitemapSpider):
    """
    A spider for scraping articles from lsm.lv based on sitemap entries.
    """

    name = "lsmsitemap"
    sitemap_urls = ["https://www.lsm.lv/sitemap.xml"]

    # There are also other sites like 'tēmas' but we ignore them.
    sitemap_rules = [
        ("/raksts/", "parse_article"),
    ]

    fmt = "%Y-%m-%dT%H:%M:%S%z"
    tz_info = pytz.timezone("Europe/Riga")

    # Get the directory path of the running script
    project_dir = Path(settings.PROJECT_DIR)
    data_dir = project_dir / "data"

    spider_dir = data_dir / name

    dumps_dir = project_dir / "dumps"

    archive_name = "items_all.json"

    this_run = {}
    lsm_articles = []
    lsm_history = set()

    batch_file_size = 1024 * 20  # 1 MB x n
    batch_prefix = "batch_articles"

    num_articles_saved = 0

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        return cls(crawler)

    def __init__(self, crawler, *args, **kwargs):
        """
        Initializes the LSMSitemapSpider object.

        Args:
            crawler (Crawler): The crawler object.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None
        """
        super(LSMSitemapSpider, self).__init__(*args, **kwargs)
        self.crawler = crawler
        self.crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)
        self.stats = self.crawler.stats
        self.tstart = datetime.now(tz=self.tz_info)
        self.stats.set_value("start_time_tz", self.tstart.strftime(self.fmt))

        if not self.spider_dir.exists():
            self.spider_dir.mkdir()

        self.spider_run_dir = self.spider_dir / self.tstart.strftime("%Y%m%d%H%M%S")
        self.spider_run_dir.mkdir()

        self.url_history_path = self.spider_dir / "history.json"
        self.archive_path = self.spider_dir / self.archive_name

        if self.url_history_path.exists():
            with open(self.url_history_path, "r") as f:
                self.lsm_history = set(json.load(f))

        last_article_dtime = utils.find_last_article_date(self.archive_path)
        self.last_article_dtime = self.tz_info.localize(last_article_dtime)

    def sitemap_filter(self, entries):
        """
        Filter the entries in a sitemap based on their last modification date.

        Args:
            entries (List[Dict]): The list of sitemap entries to be filtered.

        Returns:
            Generator[Dict]: A generator yielding the filtered sitemap entries.
        """

        for entry in entries:
            url = entry["loc"]

            if "/assets/" in url:
                """
                Sitemap index urls

                Last modification date is continuously updated even for old articles.
                Meaning that comparing last modification date with last scrape date
                will still lead to going over the same articles.

                To avoid this, we first extract the datetime from the sitemap url,
                which are given as a year and week number. We then compare that
                with the last scrape date.
                """
                match = re.findall(r"_(\d{4})W(\d+).xml", url)
                if len(match) == 0:
                    continue

                year, week = match[0]
                entry_dtime = self.datetime_from_year_week(year, week)
            else:
                if url in self.lsm_history:
                    continue

                # Article urls
                entry_dtime = datetime.strptime(entry["lastmod"], self.fmt)

            if entry_dtime > self.last_article_dtime:
                yield entry

    def datetime_from_year_week(self, year, week):
        """
        Convert a year and week number into a datetime object.

        Args:
            year (int): The year for the desired date.
            week (int): The week number for the desired date.

        Returns:
            datetime: The datetime object representing the desired date.
        """
        year = int(year)
        week = int(week)

        # Create a date object for January 1st of the given year
        january_1st = datetime(year, 1, 1, 23, 59, 59)

        days_to_add = week * 7

        # Create the datetime object for the desired date
        result_date = january_1st + timedelta(days=days_to_add)

        # Timezone localization
        result_date = self.tz_info.localize(result_date)

        return result_date

    def parse_article(self, response):
        """
        Parses the response of a request to scrape an article from the LSM.lv website.

        Args:
            response (scrapy.http.Response): The response object containing the scraped data.

        Returns:
            scrapy.Item: The scraped article item.
        """
        self.logger.info(f"Scraping: {response.url}")

        if response.url in self.lsm_history:
            self.logger.info(f"Already scraped: {response.url}")
            return

        publish_date = response.xpath('//div[@class="info-item time"]/text()').get()

        if not publish_date:
            yield LSMArticle()

        # Remove newline characters
        publish_date = re.sub(r"\n", "", publish_date)

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

        if sys.getsizeof(self.lsm_articles) >= self.batch_file_size:
            self.save_articles()

        yield item

    def save_articles(self):
        time_str = self.tstart.strftime("%Y%m%d%H%M%S")
        file_path = self.spider_run_dir / f"{self.batch_prefix}_{time_str}.json"
        with open(file_path, "w") as file:
            json.dump(list(self.lsm_articles), file)

        self.num_articles_saved += len(self.lsm_articles)
        self.lsm_articles = []

        # with open(self.url_history_path, "w") as f:
        #     json.dump(list(self.lsm_history), f, indent=4)

    def spider_closed(self, spider, reason):
        """
        A function that is called when the spider is closed.

        Args:
            spider (object): The spider object that is being closed.
            reason (str): The reason why the spider is being closed.

        Returns:
            None
        """
        if len(self.lsm_articles) > 0:
            self.save_articles()

        tfinish = datetime.now().astimezone().isoformat()
        self.stats.set_value("finish_time_tz", tfinish)
        self.stats.set_value("item_saved_count", self.num_articles_saved)

        utils.join_jsons(
            pattern=self.batch_prefix + "*",
            spider_run_dir=self.spider_run_dir,
            archive_fname=self.archive_name,
        )

        utils.make_history_file(
            spider_name=self.name,
            archive_fname=self.archive_name,
            history_fname="history.json",
        )
