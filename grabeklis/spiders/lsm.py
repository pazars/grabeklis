import re
import sys
import json
import pytz
import traceback

from datetime import datetime, timedelta

import scrapy
from scrapy import signals
from scrapy.spiders import Spider, SitemapSpider

from grabeklis import utils
from grabeklis.items import LSMArticle
from grabeklis.handlers import ScrapedDataHandler


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


def tidy_string(s: str) -> str:
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


def prepare_item_from_response(response, dt_start: datetime):
    """Extract and parse any relevant information from an article."""

    try:
        # First check article category.
        # Some contain mostly audio, video or pictures. Those are excluded.
        # Paid articles are also excluded.
        category = response.xpath('//div[@class="info-item category"]/a/text()').get()
        category = tidy_string(category)

        if category in IGNORE_ARTICLE_CATEGORIES:
            raise ValueError(f"Article category '{category}' in ignore list")

        publish_date = response.xpath('//div[@class="info-item time"]/text()').get()
        publish_date = tidy_string(publish_date)

        # This year's dates don't have year, yesterday's date say yesterday etc.
        publish_date = utils.parse_datetime(publish_date, dt_start)
        publish_date = publish_date.strftime("%Y-%m-%d %H:%M")

        # Main article <div>
        article_div = response.xpath('//div[@class="article__body"]')

        # Select text from <p> or <blockqoute> elements in article <div>
        article_as_list = article_div.xpath(
            "./p/text()|./blockquote/p/text()"
        ).extract()

        # Convert from list of strings to a single string
        article = ""
        for paragraph in article_as_list:
            # Whitespace after end of sentence
            article += " " + paragraph

        article = tidy_string(article)

        # Sometimes there is a <p> element inside <h2> with the text
        lead_div = response.xpath('//h2[@class="article-lead"]')
        
        lead = lead_div.xpath("./text()|./following-sibling::p/text()").get()
        if lead is None or len(lead) < 2:  # can be ' '
            lead_parts = lead_div.xpath(".//text()|./following-sibling::p//text()").extract()
            lead = " ".join(lead_parts).replace("  ", " ").strip()

        lead = tidy_string(lead)

        title = response.xpath('//h1[@class="article-title"]/text()').get()
        title = tidy_string(title)

        url = response.url

        item = LSMArticle(
            url=url,
            datums=publish_date,
            kategorija=category,
            virsraksts=title,
            kopsavilkums=lead,
            raksts=article,
        )

    except Exception:
        err = traceback.format_exc()
        return LSMArticle(url=response.url, error=err)

    return item


class LSMTestSpider(Spider):
    """Spider that crawls a single page. Great for testing."""

    name = "lsmtest"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        return cls(crawler, *args, **kwargs)

    def __init__(self, crawler, *args, **kwargs):
        super(LSMTestSpider, self).__init__(*args, **kwargs)

        # Connect a signal that triggers after spider closed
        # but process still running. Used for file output and logging.
        self.crawler = crawler
        self.crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)
        self.settings = crawler.settings

        self.dt_start = datetime.now()

    async def start(self):
        urls = [
            "https://www.lsm.lv/raksts/laika-zinas/laika-zinas/28.08.2023-pirmdien-visa-latvija-lis-daudzviet-stipri-bus-ari-brazmains-vejs.a521709/",
            "https://www.lsm.lv/raksts/kultura/muzika/25.08.2023-dzezs-pieskandina-latgali-luznavas-muiza-pulcejas-entuziasti-no-visas-baltijas.a521557/",
            "https://www.lsm.lv/raksts/sports/hokejs/skudras-un-daugavina-torpedo-spejusi-norekinaties-ar-hokejistiem-par-augustu.a256646/",
            "https://www.lsm.lv/raksts/dzive--stils/vecaki-un-berni/17.04.2023-atskirigas-vertibas-un-prioritates-nespeja-risinat-nesaskanas-biezakie-iemesli-neveiksmigam-paru-attiecibam.a505180/",
            "https://www.lsm.lv/raksts/zinas/latvija/11.10.2023-policija-masveida-draudu-vestulu-avots-ir-darbojies-ari-polija-un-asv.a527393/?utm_source=lsm&utm_medium=article-right&utm_campaign=popular",
            "https://www.lsm.lv/raksts/arpus-etera/komiksi/09.08.2023-cuku-komikss-apartamenti-ar-naves-smaku.a519434/",
            "https://www.lsm.lv/raksts/arpus-etera/komiksi/11.08.2023-karikaturista-skats-tuvojas-kadastralas-vertibas.a519622/",
            "https://www.lsm.lv/raksts/kas-notiek-latvija/raidijumi/10.08.2023-video-kas-notiek-ar-karina-valdibu-un-ministru-prezidenta-ultimatu-koalicijas-partneriem.a519636/",
            "https://www.lsm.lv/raksts/zinas/arzemes/07.08.2023-ukraina-aiztureta-sieviete-par-uzbrukuma-planosanu-zelenskim.a519253/",
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        now = datetime.now()

        # if now > self.dt_start:
        #     self.crawler.engine.close_spider(self, "Approaching midnight")

        download_time = timedelta(seconds=response.meta.get("download_latency"))
        dt_start = now - download_time

        item = prepare_item_from_response(response, dt_start)

        yield item

    def spider_closed(self, spider, reason):
        pass


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

    # User option: save scraped items
    save_scraped = True

    # User option: earliest publish dates to scrape
    dt_from = datetime(1900, 1, 1, 0, 0)

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

    # RAM threshold for saving files in bytes
    # Too high: Risk of losing more, slower pc
    # Too low: Too many files for long runs
    batch_file_size = 1024 * 20

    # Data handler here used to get directory names
    # In general used to test and join multiple run data
    data_handler = ScrapedDataHandler(name, mode="test")

    data_dir = data_handler.data_dir
    spider_dir = data_handler.spider_data_dir

    batch_prefix = data_handler.batch_prefix
    archive_name = data_handler.ok_archive_name
    history_name = data_handler.ok_history_name

    run_fail_name = data_handler.fail_run_name
    fail_archive_name = data_handler.fail_archive_name
    fail_history_name = data_handler.fail_history_name

    this_run = {}
    articles_ok = []
    articles_failed = []
    history_ok = set()
    history_failed = set()

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

        # Connect a signal that triggers after spider closed
        # but process still running. Used for file output and logging.
        self.crawler = crawler
        self.crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)
        self.settings = crawler.settings

        # Spider run data dir is its start time parsed
        self.tstart = datetime.now(tz=self.tz_info)
        self.run_dir_name = self.tstart.strftime("%Y%m%d%H%M%S")

        # This is where any output besides logs ends up
        self.spider_run_dir = self.spider_dir / self.run_dir_name

        self.archive_path = self.spider_dir / self.archive_name
        self.url_history_path = self.spider_dir / self.history_name
        self.failed_url_history_path = self.spider_dir / self.fail_history_name
        self.failed_articles_path = self.spider_run_dir / self.run_fail_name

        if self.url_history_path.exists():
            with open(self.url_history_path, "r") as f:
                self.history_ok = set(json.load(f))

        if self.failed_url_history_path.exists():
            with open(self.failed_url_history_path, "r") as f:
                self.history_failed = set(json.load(f))

        if "dt-from" in kwargs:
            # User-specified earliest datetime scraped
            self.dt_from = datetime.strptime(kwargs["dt-from"], "%Y%m%d%H%M%S")

        # Add timezone info because scraped articles with timezone
        # Otherwise can't compare dates (naive vs. aware)
        self.dt_from = self.tz_info.localize(self.dt_from)

        if "save" in kwargs:
            self.save_scraped = kwargs["save"].lower() == "false"
            self.logger.info(self.save_scraped)

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
                if url in self.history_ok:
                    continue

                # Article urls
                entry_dtime = datetime.strptime(entry["lastmod"], self.fmt)

            if entry_dtime > self.dt_from:
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

        self.logger.info(f"Scraping: {response.url}")

        if response.url in self.history_ok:
            self.logger.info(f"Already scraped: {response.url}")
            return
        elif response.url in self.history_failed:
            self.logger.info(f"Already failed to scrape: {response.url}")
            return

        item = prepare_item_from_response(response, dt_start)

        if item.check_if_failed():
            self.articles_failed.append(dict(item))
        else:
            self.articles_ok.append(dict(item))
            self.history_ok.add(response.url)

        # Save results as an intermediate file when size is getting bigger
        # Avoids memory issues and large info loss in case of errors
        if self.save_scraped:
            result_size = sys.getsizeof(self.articles_ok)
            if result_size >= self.batch_file_size:
                self.save_articles()

        yield item

    def save_articles(self):
        # Create output directory if it doesn't exist already
        if not self.spider_run_dir.exists():
            self.spider_run_dir.mkdir(parents=True)

        time_str = datetime.now().strftime("%Y%m%d%H%M%S")
        file_path = self.spider_run_dir / f"{self.batch_prefix}_{time_str}.json"
        with open(file_path, "w") as file:
            json.dump(list(self.articles_ok), file)

        self.articles_ok = []

    def spider_closed(self, spider, reason):
        """
        A function that is called when the spider is closed.

        Args:
            spider (object): The spider object that is being closed.
            reason (str): The reason why the spider is being closed.

        Returns:
            None
        """
        tfinish = datetime.now().astimezone().isoformat()
        self.crawler.stats.set_value("finish_time_tz", tfinish)

        if not self.save_scraped:
            return

        # Save ok items
        if len(self.articles_ok) > 0:
            self.save_articles()

        # Save failed items
        if len(self.articles_failed) > 0:
            with open(self.failed_articles_path, "a", encoding="utf-8") as file:
                json.dump(self.articles_failed, file, indent=4)

        info = self.data_handler.add_scraped_data_to_archives(self.run_dir_name)
        self.data_handler.make_archive_summaries()

        for key, value in info.items():
            self.crawler.stats.set_value(key, value)
