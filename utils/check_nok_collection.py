import os
from dotenv import load_dotenv
from tqdm import tqdm
from loguru import logger
from pymongo import MongoClient

class Check:
    def __init__(self):
        load_dotenv()
        self.mongo_uri = os.getenv("MONGO_URI")
        self.mongo_db = os.getenv("MONGO_DB")
        self.mongo_collection = os.getenv("MONGO_COLLECTION")

        if self.mongo_uri:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.db = self.mongo_client[self.mongo_db]

            self.collection_ok = self.db[self.mongo_collection]
            self.collection_nok = self.db[self.mongo_collection + "_failed"]

    def check_nok(self):
        count_before = self.collection_nok.count_documents({})
        logger.info(f"Beginning of check: {count_before} entries in NOK")

        docs = self.collection_nok.find({})
        for document in tqdm(docs, total=count_before):
            url = document.get('url')
            url_in_ok = self.collection_ok.find_one({"url": url})
            if url_in_ok:
                logger.info(f"Found nok {url} in ok")
                result = self.collection_nok.delete_one({"url": url})
                if result.deleted_count > 0:
                    logger.info(f"Removed {url} from nok")
        
        count_after = self.collection_nok.count_documents({})
        logger.info(f"After check: {count_after} entries in NOK")
        logger.info(f"Removed {count_before - count_after} entries")


if __name__ == "__main__":
    check = Check()
    check.check_nok()