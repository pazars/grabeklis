import os
import json
import glob

from pathlib import Path

try:
    from grabeklis import settings
except ModuleNotFoundError:
    import settings


class ScrapedDataHandler:
    """Collects and processes spider output data."""

    def __init__(self, spider_name: str, mode: str = "test") -> None:
        self.spider_name = spider_name
        self.mode = mode

        prj_dir = Path(settings.PROJECT_DIR)

        # Set test or production data directory
        self.data_dir = prj_dir / "data_test"
        if self.mode == "production":
            self.data_dir = prj_dir / "data"

        self.spider_data_dir = self.data_dir / self.spider_name

        # File name where run-specific failed scrapes are stored
        self.fail_run_name = "run_failed_items.json"
        # File where all failed scrapes are stored
        self.fail_archive_name = "archive_failed.json"
        # File where just the urls of all failed scrapes are stored
        self.fail_history_name = "_history_failed.json"

        # Same naming strategy, except run name is now a pattern,
        # because there can be multiple files in a single run
        self.batch_prefix = "batch_articles"
        self.ok_run_name_pattern = "batch_articles_*.json"
        self.ok_archive_name = "archive_ok.json"
        self.ok_history_name = "_history_ok.json"

        # File name with summary info of the final dataset
        self.summary_name = "summary.json"
        self.summary_path = self.spider_data_dir / self.summary_name

    def run_batch_tests(self, run_dir: str | None = None):
        if run_dir:
            cmd = f"pytest --spider={self.spider_name} --dir={run_dir}"
        else:
            cmd = f"pytest --spider={self.spider_name}"

        return os.system(cmd)

    def archive_failed_run_items(self, run_name: str):
        run_failed_name = self.spider_data_dir / run_name / self.fail_run_name

        if not run_failed_name.exists():
            # No file means nothing failed to scrape
            return (0, 0)

        combined_data = []

        # Load the existing archive if it already exists
        fail_archive_data = []
        fail_archive = self.spider_data_dir / self.fail_archive_name
        if fail_archive.exists():
            with open(fail_archive, "r", encoding="utf-8") as file:
                fail_archive_data = json.load(file)

        combined_data += fail_archive_data

        fail_run_data = []
        with open(run_failed_name, "r", encoding="utf-8") as file:
            fail_run_data = json.load(file)

        for failed_item in fail_run_data:
            if failed_item not in combined_data:
                combined_data.append(failed_item)

        size_existing = len(fail_archive_data)
        size_all = len(fail_run_data) + size_existing
        size_new_unique = len(combined_data)

        num_new = size_all - size_existing
        num_dupes = size_all - size_new_unique
        num_new_added = num_new - num_dupes

        # Save combined file
        with open(fail_archive, "w") as fail_archive_file:
            json.dump(combined_data, fail_archive_file, indent=4)

        return (num_new_added, num_dupes)

    def archive_ok_run_items(self, run_name: str):
        run_dir = self.spider_data_dir / run_name
        path_pattern = run_dir / self.ok_run_name_pattern

        # Use glob to find all JSON files in a directory
        files = glob.glob(path_pattern.as_posix())

        if len(files) == 0:
            # No articles scraped
            # Can happen if all scrapes failed
            return (0, 0)

        # Initialize an empty list to store the combined data
        combined_data = []

        # Load the existing archive if it already exists
        archive = self.spider_data_dir / self.ok_archive_name
        archive_data = []
        if archive.exists():
            with open(archive, "r", encoding="utf-8") as file:
                archive_data = json.load(file)

        combined_data += archive_data

        # Read and merge JSON files
        for fpath in files:
            print(f"Merging content from: {fpath}")
            with open(fpath, "r") as file:
                data = json.load(file)
                combined_data.extend(data)

        size_existing = len(archive_data)
        size_new = len(combined_data)

        num_new = size_new - size_existing

        # Remove duplicates
        # Duplicates should only exist if this function called twice in a row
        combined_data = [dict(t) for t in {tuple(d.items()) for d in combined_data}]

        size_no_duplicates = len(combined_data)
        num_dupes = size_new - size_no_duplicates
        num_new_added = num_new - num_dupes

        # Serialize combined data to a new JSON file
        with open(archive, "w") as archive_file:
            json.dump(combined_data, archive_file, ensure_ascii=False, indent=4)

        return (num_new_added, num_dupes)

    def make_history_file(self, archive: str):
        if archive == "ok":
            archive_path = self.spider_data_dir / self.ok_archive_name
            history_path = self.spider_data_dir / self.ok_history_name
        elif archive == "failed":
            archive_path = self.spider_data_dir / self.fail_archive_name
            history_path = self.spider_data_dir / self.fail_history_name
        else:
            raise RuntimeError("Invalid value for argument 'archive'")

        if not archive_path.exists():
            return

        with open(archive_path, "r") as file:
            data = json.load(file)

        urls = []
        for value in data:
            url = value["url"]
            urls.append(url)

        with open(history_path, "w") as file:
            json.dump(urls, file, indent=4)

        return len(urls)

    def add_scraped_data_to_archives(self, run_name: str) -> dict:
        if self.mode != "test":
            raise RuntimeError("Function call only allowed in test mode")

        exit_code = self.run_batch_tests(run_name)
        if exit_code != 0:
            raise RuntimeError(f"Pytest exit code: {exit_code}")

        info = {
            "new_in_ok_archive": 0,
            "skipped_ok_duplicates": 0,
            "new_in_failed_archive": 0,
            "skipped_fail_duplicates": 0,
        }
        # Copy successfully scraped articles to archive
        new_ok, dupe_ok = self.archive_ok_run_items(run_name)

        info["new_in_ok_archive"] = new_ok
        info["skipped_ok_duplicates"] = dupe_ok

        # Copy failed to scrape articles to archive
        new_fail, dupe_fail = self.archive_failed_run_items(run_name)

        info["new_in_failed_archive"] = new_fail
        info["skipped_fail_duplicates"] = dupe_fail

        return info

    def make_archive_summaries(self):
        # Update successfully scraped article url history
        num_ok = self.make_history_file("ok")

        # Update failed to scrape article url history
        num_fail = self.make_history_file("failed")

        summary = {
            "num_articles_ok": num_ok,
            "num_articles_failed": num_fail,
        }

        with open(self.summary_path, "w", encoding="utf-8") as file:
            json.dump(summary, file, indent=4)

    def create_archives_from_scraped_data(self):
        for obj in self.spider_data_dir.glob("*"):
            if obj.is_dir():
                print(f"Archiving {obj.name}")
                info = self.add_scraped_data_to_archives(obj.name)
                print(info)


if __name__ == "__main__":
    handler = ScrapedDataHandler("lsmsitemap")
    handler.make_archive_summaries()
