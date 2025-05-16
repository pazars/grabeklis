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

two_days_ago = datetime.now() - timedelta(days=2)
two_days_ago_midnight = two_days_ago.replace(hour=0, minute=0, second=0)
dt_from = two_days_ago_midnight.strftime("%Y%m%d%H%M%S")
if "DT_FROM" in os.environ:
    dt_from = os.environ.get("DTFROM", "19900101000000")

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
    settings.set("CLOSESPIDER_ITEMCOUNT", os.environ["MAX_ITEMS"])


process = CrawlerProcess(settings)
process.crawl("lsmsitemap", **kwargs)
process.start()
