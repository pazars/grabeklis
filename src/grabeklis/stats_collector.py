import os

from datetime import datetime

from scrapy.statscollectors import StatsCollector
from scrapy.utils.serialize import ScrapyJSONEncoder

from grabeklis import settings


class DefaultStatsCollector(StatsCollector):
    def _persist_stats(self, stats, spider):
        date = datetime.now().strftime("%Y%m%d%H%M%S")

        encoder = ScrapyJSONEncoder()

        prj_dir = settings.PROJECT_DIR
        dumps_dir = os.path.join(prj_dir, "dumps")
        if not os.path.exists(dumps_dir):
            os.mkdir(dumps_dir)

        fpath = os.path.join(dumps_dir, f"stats_{spider.name}_{date}.json")
        fpath_last = os.path.join(dumps_dir, f"stats_{spider.name}_last.json")

        with open(fpath, "w") as file:
            data = encoder.encode(stats)
            file.write(data)

        with open(fpath_last, "w") as file:
            data = encoder.encode(stats)
            file.write(data)
