from datetime import datetime

from scrapy.statscollectors import StatsCollector
from scrapy.utils.serialize import ScrapyJSONEncoder

from pathlib import Path
from grabeklis import settings


class DefaultStatsCollector(StatsCollector):
    def _persist_stats(self, stats, spider):
        date = datetime.now().strftime("%Y%m%d%H%M%S")

        encoder = ScrapyJSONEncoder()

        prj_dir = Path(settings.PROJECT_DIR)
        logs_dir = prj_dir / "logs" / spider.name

        if not logs_dir.exists():
            logs_dir.mkdir(parents=True)

        # TODO: Log date doesn't match data date
        fpath = logs_dir / f"{date}.json"
        fpath_last = logs_dir / "last.json"

        with open(fpath, "w") as file:
            data = encoder.encode(stats)
            file.write(data)

        with open(fpath_last, "w") as file:
            data = encoder.encode(stats)
            file.write(data)
