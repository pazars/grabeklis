from langchain.document_loaders import JSONLoader

import json
from pathlib import Path
from pprint import pprint
import utils


proj_root_dir = utils.get_project_root_dir()
data_dir = proj_root_dir / "data"
data_path = data_dir / "lsm_articles_all_20230924.json"

loader = JSONLoader(
    file_path=data_path,
    jq_schema=".[]",
)

data = loader.load()


pprint(data[:5])
