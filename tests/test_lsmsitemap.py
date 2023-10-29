import json
import pytest

import pandas as pd

from pathlib import Path
from datetime import datetime

from grabeklis import settings
from tests.conftest import spider_dir, run_dir


OK_KEYS = set(
    (
        "url",
        "datums",
        "kategorija",
        "virsraksts",
        "kopsavilkums",
        "raksts",
    )
)

FAILED_KEYS = set(("url", "error"))

KNOWN_CATEGORIES = set(
    (
        "Cilvēkstāsti",
        "Laika ziņas",
        "Latvijā",
        "Izklaide",
        "Futbols",
        "Mūzika",
        "Māksla",
        "Pasaulē",
        "Tautas māksla",
        "Ekonomika",
        "Kilograms kultūras",
        "Basketbols",
        "Vide un dzīvnieki",
        "Volejbols",
        "Pilsētvide",
        "Motoru sports",
        "Ziņu analīze",
        "Ziņas vieglajā valodā",
        "Paraolimpiskais sports",
        "Tehnoloģijas un zinātne",
        "Hokejs",
        "Ārpus ētera",
        "Sports",
        "Ekrāns",
        "Skatuve",
        "Literatūra",
        "Vecāki un bērni",
        "Teniss",
        "Veselība",
        "Cīņas",
        "Bobslejs",
        "Peldēšana",
        "Dizains un arhitektūra",
        "Ziemas sports",
        "Dziesmu un deju svētki",
        "Sarunas",
        "Motori",
        "Kultūrtelpa",
        "Virtuve",
        "Šī diena vēsturē",
        "Olimpiskā kustība",
        "Vēsture",
        "Ikdienai",
        "Skeletons",
        "Kamanas",
        "Vieglatlētika",
        "Burāšana",
        "Riteņbraukšana",
        "Podkāsti",
        "Ceļošana",
        "Sporta politika",
        "Tautas sports",
        "Kas notiek Latvijā?",
        "Handbols",
        "Golfs",
        "Regbijs",
        "Biatlons",
        "Florbols",
        "Komiksi un karikatūras",
        "Esejas",
        "Airēšana",
        "Dārzs un mājas",
        "Eiropā",
        "Vaļasprieki",
        "Sociālo mediju apskati",
        "Balvas",
        "Pareizais viedoklis",
        "Medijpratība",
        "Animācijas",
        "Infografikas",
        "Raidījumi",
        "Spēles",
        "Apmaksāta informācija*",
        "Eiropas spēles",
        "Audio",
        "Jāšanas sports",
        "Spilgtākie video",
        "Mans treneris",
        "informācija",
        "YouTube apskats",
    )
)

PROJECT_DIR = Path(settings.PROJECT_DIR)
DATA_DIR = PROJECT_DIR / "data_test"


def find_ok(path: Path):
    return [f for f in path.glob("batch_articles_*.json")]


def find_fail(path: Path):
    return [f for f in path.glob("run_failed_items.json")]


batch_ok_paths = []
batch_fail_paths = []

if spider_dir:
    if run_dir:
        # Specific dir of a specific spider
        path = DATA_DIR / spider_dir / run_dir
        batch_ok_paths = find_ok(path)
        batch_fail_paths = find_fail(path)
    else:
        # Specific spider
        path = DATA_DIR / spider_dir
        for obj in path.glob("*"):
            if not obj.is_dir():
                continue

            batch_ok_paths += find_ok(obj)
            batch_fail_paths += find_fail(obj)
elif not run_dir:
    # Return file paths from all dirs in all spiders
    for obj in DATA_DIR.rglob("*"):
        if not obj.is_dir():
            continue
        batch_ok_paths += find_ok(obj)
        batch_fail_paths += find_fail(obj)
else:
    # Just dir without spider ambigous
    raise RuntimeError("'dir' argument also needs 'spider'")


@pytest.mark.parametrize("path", batch_fail_paths)
def test_check_fail_keys(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    for item in data:
        assert set(item.keys()) == FAILED_KEYS


@pytest.mark.parametrize("path", batch_ok_paths)
def test_check_ok_keys(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    for item in data:
        assert set(item.keys()) == OK_KEYS


@pytest.mark.parametrize("path", batch_ok_paths)
def test_check_batch_has_nan(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    df = pd.DataFrame.from_dict(data)

    # Check if there are any missing values
    missing_info = df.isna()
    has_missing = bool(missing_info.any().any())

    assert has_missing is False


@pytest.mark.parametrize("path", batch_ok_paths)
def test_check_batch_dates(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    for item in data:
        date = item["datums"]
        try:
            datetime.strptime(date, "%Y-%m-%d %H:%M")
        except ValueError:
            assert False


@pytest.mark.parametrize("path", batch_ok_paths)
def test_check_batch_categories(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    for item in data:
        assert item["kategorija"] in KNOWN_CATEGORIES


@pytest.mark.parametrize("path", batch_ok_paths)
def test_check_batch_titles(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    for item in data:
        assert len(item["virsraksts"]) > 0


@pytest.mark.parametrize("path", batch_ok_paths)
def test_check_batch_summaries(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    for item in data:
        assert len(item["kopsavilkums"]) > 0


@pytest.mark.parametrize("path", batch_ok_paths)
def test_check_batch_articles(path):
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    for item in data:
        assert len(item["raksts"]) > 0
