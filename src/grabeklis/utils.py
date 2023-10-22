import json
import glob
import hashlib

from datetime import datetime, timedelta

try:
    from grabeklis import settings
except ModuleNotFoundError:
    import settings

from pathlib import Path


def generate_unique_id(url):
    # Create a hashlib object
    hasher = hashlib.sha256()

    # Encode the URL as bytes (UTF-8 encoding)
    url_bytes = url.encode("utf-8")

    # Update the hasher with the URL bytes
    hasher.update(url_bytes)

    # Get the hexadecimal representation of the hash
    unique_id = hasher.hexdigest()

    return unique_id


def join_run_fails_with_fail_archive(
    spider_run_dir: Path,
    fail_run_fname: str = "run_failed_items.json",
    fail_archive_fname: str = "failed_item_history.json",
):
    # Initialize an empty list to store the combined data
    combined_data = []

    data_dir = spider_run_dir.parent

    # Load the existing archive if it already exists
    fail_archive_data = []
    fail_archive = data_dir / fail_archive_fname
    print("Looking for failed item archive in:")
    print(fail_archive.as_posix())
    if fail_archive.exists():
        with open(fail_archive, "r", encoding="utf-8") as file:
            fail_archive_data = json.load(file)

    combined_data += fail_archive_data

    fail_run_data = []
    fail_run = spider_run_dir / fail_run_fname
    print("Looking for failed run items in:")
    print(fail_run.as_posix())
    if fail_run.exists():
        with open(fail_run, "r", encoding="utf-8") as file:
            fail_run_data = json.load(file)

    for failed_item in fail_run_data:
        if failed_item not in combined_data:
            combined_data.append(failed_item)

    # Serialize combined data to a new JSON file
    print("Saving combined file in:")
    print(fail_archive.as_posix())
    with open(fail_archive, "w") as fail_archive_file:
        json.dump(combined_data, fail_archive_file, indent=4)


def join_run_items_with_archive(
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

    size_existing = len(archive_data)

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

    size_new = len(combined_data)
    num_new = size_new - size_existing

    # Remove duplicates
    # Can occur when calling the function more than once
    combined_data = [dict(t) for t in {tuple(d.items()) for d in combined_data}]

    size_no_duplicates = len(combined_data)
    num_dupes = size_new - size_no_duplicates
    num_new_added = num_new - num_dupes

    # Serialize combined data to a new JSON file
    with open(archive, "w") as archive_file:
        json.dump(combined_data, archive_file)

    return (num_new_added, num_dupes)


def make_history_file(
    spider_name: str,
    archive_fname: str = "items_all.json",
    history_fname: str = "history.json",
):
    project_dir = Path(settings.PROJECT_DIR)
    data_dir = project_dir / "data" / spider_name

    if not data_dir.exists():
        data_dir.mkdir(parents=True)

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
        date_string_lv = value["datums"]
        try:
            str_parts = date_string_lv.split(",")
        except Exception:
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


def parse_datetime(datums: str, dt: datetime):
    lv_month_numbers = {
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

    str_parts = datums.split(",")

    if len(str_parts) == 3:
        day, month_str = str_parts[0].split(". ")
        month = lv_month_numbers[month_str]
        year = str_parts[1]
        hour, min = str_parts[2].replace(" ", "").split(":")

    else:  # len is 2
        hour, min = str_parts[1].replace(" ", "").split(":")

        if str_parts[0].lower() == "vakar":
            yesterday = dt - timedelta(days=1)
            day = yesterday.day
            month = yesterday.month
            year = yesterday.year

        elif str_parts[0].lower() == "šodien":
            day = dt.day
            month = dt.month
            year = dt.year

        else:
            # Date without year -> current year
            day, month_str = str_parts[0].split(". ")
            month = lv_month_numbers[month_str]
            year = dt.year

    date = datetime(
        year=int(year),
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(min),
    )

    return date


def check_if_history_unique(
    spider_name: str,
    history_fname: str = "history.json",
):
    project_dir = Path(settings.PROJECT_DIR)
    spider_dir = project_dir / "data" / spider_name
    history_path = spider_dir / history_fname

    with open(history_path, encoding="utf-8") as file:
        data = json.load(file)

    data_unique = list(set(data))
    if len(data) == len(data_unique):
        return True
    return False


def load_items(
    spider_name: str,
    fname: str = "items_all.json",
):
    project_dir = Path(settings.PROJECT_DIR)
    spider_dir = project_dir / "data" / spider_name
    fpath = spider_dir / fname

    with open(fpath, encoding="utf-8") as file:
        items = json.load(file)

    return items


def load_failed_history(
    spider_name: str,
    fname: str = "failed_history.json",
):
    project_dir = Path(settings.PROJECT_DIR)
    spider_dir = project_dir / "data" / spider_name
    fpath = spider_dir / fname

    with open(fpath, encoding="utf-8") as file:
        failed_items = json.load(file)

    return failed_items


def remove_failed_from_items(
    spider_name: str,
    fail_fname: str = "failed_item_history.json",
    item_fname: str = "items_all.json",
):
    # Normally fails are in failed_history.json
    # This is just in case something gets messed up.
    items = load_items(spider_name, item_fname)
    failed_history = load_failed_history(spider_name, fail_fname)

    new_fails = []
    for idx, item in enumerate(items):
        if "error" in item:
            failed_item = items.pop(idx)
            new_fails.append(failed_item)

    # Add new fails and remove duplicates
    failed_history += new_fails
    failed_history = [dict(t) for t in {tuple(d.items()) for d in failed_history}]

    project_dir = Path(settings.PROJECT_DIR)
    spider_dir = project_dir / "data" / spider_name

    fpath = spider_dir / fail_fname
    with open(fpath, "w", encoding="utf-8") as file:
        json.dump(failed_history, file, indent=4)

    fpath = spider_dir / item_fname
    with open(fpath, "w", encoding="utf-8") as file:
        json.dump(items, file)

    return new_fails


if __name__ == "__main__":
    # Test: Join recent batch with archive
    # join_run_items_with_archive(
    #     pattern="batch_articles_*",
    #     spider_run_dir=Path("./data/lsmsitemap/20231015203132").resolve(),
    # )

    # Test: Join all batches with archive
    # spider_name = "lsmsitemap"

    # # Define a regular expression pattern for 14-digit directory names
    # pattern = re.compile(r"^\d{14}$")

    # # Specify the directory path
    # directory_path = Path(f"./data/{spider_name}").resolve()

    # # List all items (files and directories) in the specified directory
    # run_dirs = []
    # for item in directory_path.iterdir():
    #     if item.is_dir() and pattern.match(item.name):
    #         run_dirs.append(item)

    # # Print the list of 14-digit directory names
    # for path in run_dirs:
    #     join_run_items_with_archive(
    #         pattern="batch_articles_*",
    #         spider_run_dir=path,
    #     )

    # # Test: Make history file from archive
    # make_history_file(
    #     spider_name="lsmsitemap",
    # )

    # # Test: Find latest article date in scraped archive
    # fp = Path("./data/lsmsitemap/items_all.json").resolve()
    # print("Reading from %s" % fp.as_posix())
    # date = find_last_article_date(fp)
    # print(date)

    # Test: Check if all entries in history are unique
    # print(check_if_history_unique("lsmsitemap"))

    # Test: Check if removes errors
    failed_items = remove_failed_from_items("lsmsitemap")
    print(len(failed_items))

    # Test: Check if joining fail histories works
    # spider_run_dir = Path("./data/lsmsitemap/20231015203132").resolve()
    # join_run_fails_with_fail_archive(
    #     spider_run_dir=spider_run_dir,
    #     fail_run_fname="run_failed_items_dummy.json",
    #     fail_archive_fname="failed_item_history_copy.json",
    # )

    print("Done")