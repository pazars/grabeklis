import os
import dotenv
from datetime import datetime, timedelta
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


# For development
# Can't load .env in settings.py
if os.getenv("USE_DOTENV", "true") == "true":
    dotenv.load_dotenv()


mongo_uri = os.environ.get("MONGO_URI")
mongo_db = os.environ.get("MONGO_DB")
mongo_collection = os.environ.get("MONGO_COLLECTION")

fmt = "%Y%m%d%H%M%S"
two_days_ago = datetime.now() - timedelta(days=2)
two_days_ago_midnight = two_days_ago.replace(hour=0, minute=0, second=0)

dt_from = two_days_ago_midnight.strftime(fmt)
if "DT_FROM" in os.environ:
    dt_from = os.environ.get("DT_FROM")

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
