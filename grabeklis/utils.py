import json
import glob

from datetime import datetime

try:
    from grabeklis import settings
except ModuleNotFoundError:
    import settings

from pathlib import Path


def join_jsons(
    pattern: str,
    spider_run_dir: Path,
    archive_fname: str = "items_all.json",
):
    # Initialize an empty list to store the combined data
    combined_data = []

    data_dir = spider_run_dir.parent

    # Load the existing archive if it already exists
    archive = data_dir / archive_fname
    archive_data = []
    if archive.exists():
        with open(archive, "r", encoding="utf-8") as file:
            archive_data = json.load(file)

    combined_data += archive_data

    pattern = (spider_run_dir / pattern).as_posix()

    print(f"Search files matching pattern: {pattern}")

    # Use glob to find all JSON files in a directory
    files = glob.glob(pattern)

    print(f"Found {len(files)} files")

    # Read and merge JSON files
    for fpath in files:
        print(f"Merging content from: {fpath}")
        with open(fpath, "r") as file:
            data = json.load(file)
            combined_data.extend(data)

    # Remove duplicates
    # Can occur when calling the function more than once
    combined_data = [dict(t) for t in {tuple(d.items()) for d in combined_data}]

    # Serialize combined data to a new JSON file
    with open(archive, "w") as archive_file:
        json.dump(combined_data, archive_file)


def make_history_file(
    spider_name: str,
    archive_fname: str = "items_all.json",
    history_fname: str = "history.json",
):
    project_dir = Path(settings.PROJECT_DIR)
    data_dir = project_dir / "data" / spider_name

    # Load the existing archive if it already exists
    archive = data_dir / archive_fname

    with open(archive, "r") as file:
        data = json.load(file)

    urls = []

    for value in data:
        url = value["url"]
        urls.append(url)

    history_fpath = data_dir / history_fname

    with open(history_fpath, "w") as file:
        json.dump(urls, file, indent=4)


def find_last_article_date(fp: Path) -> datetime:
    if not fp.exists():
        return datetime(1990, 1, 1, 0, 0)

    months_lv_to_en = {
        "janvāris": 1,
        "februāris": 2,
        "marts": 3,
        "aprīlis": 4,
        "maijs": 5,
        "jūnijs": 6,
        "jūlijs": 7,
        "augusts": 8,
        "septembris": 9,
        "oktobris": 10,
        "novembris": 11,
        "decembris": 12,
    }

    with open(fp, "r") as file:
        data = json.load(file)

    num_articles = len(data)
    parsed_dates = 0
    yesterday_dates = 0
    unknown_dates = []

    todays_date = datetime.now()
    latest_article_date = datetime(1990, 1, 1)

    for value in data:
        date_string_lv = value["publish_date"]
        try:
            str_parts = date_string_lv.split(",")
        except:
            print(f"Error parsing date: {date_string_lv}")
            continue

        if len(str_parts) == 3:
            day, month_str = str_parts[0].split(". ")
            month = months_lv_to_en[month_str]
            year = str_parts[1]
            hour, min = str_parts[2].replace(" ", "").split(":")

            date = datetime(
                year=int(year),
                month=int(month),
                day=int(day),
                hour=int(hour),
                minute=int(min),
            )

            parsed_dates += 1

            if date > latest_article_date:
                latest_article_date = date

        elif len(str_parts) == 2:
            hour, min = str_parts[1].replace(" ", "").split(":")

            if str_parts[0].lower() == "vakar":
                # Yesterday is a bit ambigouous
                # Rescraping 1 day's worth of articles isn't that bad
                yesterday_dates += 1

            else:
                day, month_str = str_parts[0].split(". ")
                month = months_lv_to_en[month_str]
                year = todays_date.year

                date = datetime(
                    year=int(year),
                    month=int(month),
                    day=int(day),
                    hour=int(hour),
                    minute=int(min),
                )

                parsed_dates += 1

                # date < todays_date in case running just after New Year's
                # In that case todays_date.year might be next year already
                if date < todays_date and date > latest_article_date:
                    latest_article_date = date
        else:
            unknown_dates.append(date_string_lv)

    assert parsed_dates == num_articles - yesterday_dates

    return latest_article_date


if __name__ == "__main__":
    # # Test: Join recent batch with archive
    join_jsons(
        pattern="batch_articles_*",
        spider_run_dir=Path("../data/lsmsitemap/20231010092804").resolve(),
    )

    # Test: Make history file from archive
    make_history_file(
        spider_name="lsmsitemap",
    )

    # Test: Find latest article date in scraped archive
    fp = Path("../data/lsmsitemap/items_all.json").resolve()
    print("Reading from %s" % fp.as_posix())
    date = find_last_article_date(fp)
    print(date)
