import os
import dotenv
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


# For development
# Can't load .env in settings.py
if os.getenv("USE_DOTENV", "true") == "true":
    dotenv.load_dotenv()


mongo_uri = os.environ.get("MONGO_URI")
mongo_db = os.environ.get("MONGO_DB")
mongo_collection = os.environ.get("MONGO_COLLECTION")

dt_from = os.environ.get("DT_FROM", "19900101000000")

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
