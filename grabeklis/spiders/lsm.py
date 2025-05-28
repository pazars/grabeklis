import re
import pytz
import traceback

from datetime import datetime, timedelta

from scrapy.spiders import SitemapSpider

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure
from pymongo.collection import Collection

from grabeklis import utils
from grabeklis.items import LSMArticle


IGNORE_ARTICLE_CATEGORIES = (
    "Apmaksāta informācija*",
    "Spilgtākie video",
    "Infografikas",
    "YouTube apskats",
    "Animācijas",
    "Audio",
    "Komiksi un karikatūras",
    "Podkāsti",
    "Raidījumi",
)


class LSMSitemapSpider(SitemapSpider):
    """
    A spider for scraping articles from lsm.lv based on sitemap entries.

    scrapy crawl <name>
    to scrape everything

    scrapy crawl <name> -s  CLOSESPIDER_ITEMCOUNT=<#>
    to limit number of results to <#>
    concurrent requests not in queue still executed

    scrapy crawl <name> -a dt-from=20231012153000
    to limit articles no older than YYYYMMDDHHMMSS

    scrapy crawl <name> -a save=false
    to not save results in files (useful for testing)

    """

    # Spider name
    name = "lsmsitemap"

    # Scraping entry point
    sitemap_urls = ["https://www.lsm.lv/sitemap.xml"]

    # Parse only urls with 'raksts'
    sitemap_rules = [
        ("/raksts/", "parse_article"),
    ]

    # Sitemap's datetime format
    fmt = "%Y-%m-%dT%H:%M:%S%z"

    # Timezone in which articles are published
    tz_info = pytz.timezone("Europe/Riga")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        return cls(crawler, *args, **kwargs)

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
        self.settings = crawler.settings

        # MongoDB
        self.mongo_uri = kwargs.get("mongo_uri")
        self.mongo_db = kwargs.get("mongo_db")
        self.mongo_collection = kwargs.get("mongo_collection")

        if self.mongo_uri:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.db = self.mongo_client[self.mongo_db]

            self.collection_ok = self.db[self.mongo_collection]
            self.collection_nok = self.db[self.mongo_collection + "_failed"]

            try:
                self.collection_ok.create_index([("url", 1)], unique=True)
                self.collection_nok.create_index([("url", 1)], unique=True)
            except OperationFailure as e:
                # Firestore does not support creating indices from connectors
                self.logger.warning(e)

        # User option: earliest publish dates to scrape
        self.dt_from = datetime(1900, 1, 1, 0, 0)
        if "dt_from" in kwargs:
            # User-specified earliest datetime scraped
            self.dt_from = datetime.strptime(kwargs["dt_from"], "%Y%m%d%H%M%S")

        # Add timezone info because scraped articles with timezone
        # Otherwise can't compare dates (naive vs. aware)
        self.dt_from = self.tz_info.localize(self.dt_from, is_dst=None)
        
        # Adjust dt_from so that isocalendar() week starts on Sunday (not Monday)
        # If dt_from is not Sunday, move to previous Sunday
        dt_from_sunday = self.dt_from - timedelta(days=(self.dt_from.weekday() + 1) % 7)
        self.year_from, self.week_from = dt_from_sunday.isocalendar()[:2]

        self.logger.debug(f"dt_from: {self.dt_from}")
        self.logger.debug(f"From {self.year_from}W{self.week_from}")

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
                entry_dtime = self._datetime_from_year_week(year, week)
            else:
                entry_dtime = datetime.strptime(entry["lastmod"], self.fmt)
                year, week, _ = entry_dtime.isocalendar()

            if int(year) >= self.year_from and int(week) >= self.week_from:
                yield entry

    def parse_article(self, response):
        """
        Parses the response of a request to scrape an article from the LSM.lv website.

        Args:
            response (scrapy.http.Response): The response object containing the scraped data.

        Returns:
            scrapy.Item: The scraped article item.
        """
        now = datetime.now()
        if now.hour == 23 and now.minute >= 45:
            # At midnigh all today's dates are labeled as yesterday.
            # Yesterday's dates are given a standard-looking date.
            # To avoid mislabeling article publish dates, we simply don't scrape
            # close to midnight.
            self.crawler.engine.close_spider(
                self, "Approaching midnight; Dates will update."
            )

        # Best estimate of when the download was started

        download_time = timedelta(seconds=response.meta.get("download_latency"))
        dt_start = now - download_time

        item = self._prepare_item_from_response(response, dt_start)

        if 'date' not in item:
            self._mongo_insert_or_update(self.collection_nok, item)
            return
        elif item['date'] < self.dt_from:
            return

        if item.check_if_failed():
            if hasattr(self, "collection_nok"):
                self._mongo_insert_or_update(self.collection_nok, item)
        else:
            if hasattr(self, "collection_ok"):
                self._mongo_insert_or_update(self.collection_ok, item)

        yield item

    def _datetime_from_year_week(self, year, week):
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

    def _prepare_item_from_response(self, response, dt_start: datetime):
        """Extract and parse any relevant information from an article."""

        try:
            # First check article category.
            # Some contain mostly audio, video or pictures. Those are excluded.
            # Paid articles are also excluded.
            category = response.xpath(
                '//div[@class="info-item category"]/a/text()'
            ).get()
            category = self._tidy_string(category)

            if category in IGNORE_ARTICLE_CATEGORIES:
                self.logger.info(f"Article category '{category}' in ignore list")

            publish_date = response.xpath('//div[@class="info-item time"]/text()').get()
            publish_date = self._tidy_string(publish_date)

            # This year's dates don't have year, yesterday's date say yesterday etc.
            publish_date = utils.parse_datetime(publish_date, dt_start)

            # Main article <div>
            article_div = response.xpath('//div[@class="article__body"]')

            # Select text from <p> or <blockquote> elements in article <div>
            article_as_list = article_div.xpath(
                "./p/text()|./blockquote/p/text()"
            ).extract()

            # Edge case: if no text found, try extracting from child <div> elements
            if not article_as_list or len(article_as_list) == 0:
                article_as_list = article_div.xpath("./div//text()").extract()

            # Convert from list of strings to a single string
            article = " ".join(article_as_list)

            article = self._tidy_string(article)

            # Sometimes there is a <p> element inside <h2> with the text
            lead_div = response.xpath('//h2[@class="article-lead"]')

            lead = lead_div.xpath("./text()|./following-sibling::p/text()").get()
            if lead is None or len(lead) < 2:  # can be ' '
                lead_parts = lead_div.xpath(
                    ".//text()|./following-sibling::p//text()"
                ).extract()
                lead = " ".join(lead_parts).replace("  ", " ").strip()

            lead = self._tidy_string(lead)

            title = response.xpath('//h1[@class="article-title"]/text()').get()
            title = self._tidy_string(title)

            url = response.url

            item = LSMArticle(
                url=url,
                date=publish_date,
                category=category,
                title=title,
                summary=lead,
                article=article,
            )

        except Exception:
            err = traceback.format_exc()
            return LSMArticle(url=response.url, error=err)

        return item

    def _tidy_string(self, s: str) -> str:
        """Common string parsing ops"""

        # Remove newline characters
        s = re.sub(r"\n", "", s)

        # Remove non-breaking space character
        # Used in articles to avoid situations where e.g. - is end of line
        s = re.sub(r"\xa0", " ", s)

        # Remove multiple consecutive spaces and leading/trailing spaces
        s = re.sub(r"\s+", " ", s).strip()

        if len(s) == 0:
            raise RuntimeError("No information found.")

        return s

    def _mongo_insert_or_update(self, collection: Collection, item: dict) -> None:
        """
        Checks if a document with the given URL exists in the collection and updates it,
        or inserts a new document if it doesn't exist (upsert).

        Args:
            collection (Collection): The MongoDB collection to interact with.
            item (dict): A dictionary representing the document to insert or update.
                         It is expected to have a 'url' key.
        """
        filter_criteria = {"url": item.get("url")}
        update_data = {"$set": item}

        try:
            result = collection.update_one(filter_criteria, update_data, upsert=True)
            if result.upserted_id is not None:
                self.logger.info(
                    f"Inserted new document with URL '{item.get('url')}' and _id: {result.upserted_id}"
                )
            elif result.modified_count > 0:
                self.logger.info(
                    f"Updated existing document with URL '{item.get('url')}'"
                )
            else:
                self.logger.warning(
                    f"No document matched URL '{item.get('url')}' and no update occurred"
                )
        except DuplicateKeyError:
            self.logger.error(
                f"DuplicateKeyError for '{item.get('url')}'. Ensure a unique index exists on the 'url' field."
            )
