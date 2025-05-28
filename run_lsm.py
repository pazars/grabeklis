from __future__ import annotations

import os
import sys
import json
import dotenv
import loguru
from loguru import logger
from datetime import datetime, timedelta
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


logger.remove()
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()


def cloud_logging_sink(message: loguru.Message):
    record = message.record

    gcp_severity_map = {
        "TRACE": "DEBUG",
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "SUCCESS": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR",
        "CRITICAL": "CRITICAL",
    }

    log_entry = {
        "message": record["message"],
        "severity": gcp_severity_map.get(record["level"].name, "INFO"),
        "timestamp": record["time"].isoformat(),
        "file": record["file"].name,
        "line": record["line"],
        "function": record["function"],
        **record["extra"],
    }

    print(json.dumps(log_entry), file=sys.stdout)


logger.add(cloud_logging_sink, level=LOG_LEVEL, enqueue=True)


# For development
# Can't load .env in settings.py
if os.getenv("USE_DOTENV", "false").lower() == "true":
    dotenv.load_dotenv()


mongo_uri = os.environ.get("MONGO_URI")
mongo_db = os.environ.get("MONGO_DB")
mongo_collection = os.environ.get("MONGO_COLLECTION")

scrape_days = int(os.getenv("SCRAPE_DAYS", 0))
if scrape_days == 0:
    logger.warning("SCRAPE_DAYS not set. Defaulting to 2 days.")
    scrape_days = 2

if "DT_FROM" in os.environ:
    dt_from = os.environ.get("DT_FROM")
else:
    fmt = "%Y%m%d%H%M%S"
    days_ago = datetime.now() - timedelta(days=scrape_days)
    days_ago_midnight = days_ago.replace(hour=0, minute=0, second=0)
    dt_from = days_ago_midnight.strftime(fmt)


kwargs = {
    "mongo_uri": mongo_uri,
    "mongo_db": mongo_db,
    "mongo_collection": mongo_collection,
    "dt_from": dt_from,
}

# Load default settings
settings = get_project_settings()

# Override/add specific settings
if "MAX_ITEMS" in os.environ:
    max_items = int(os.environ["MAX_ITEMS"])
    settings.set("CLOSESPIDER_ITEMCOUNT", max_items)

process = CrawlerProcess(settings)
process.crawl("lsmsitemap", **kwargs)
process.start()
