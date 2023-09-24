import json
import glob

from pathlib import Path


def join_jsons(pattern: str, out_fname: str):
    # Initialize an empty list to store the combined data
    combined_data = []

    # Get the directory path of the running script
    script_dir = Path(__file__).resolve().parent
    data_dir = script_dir / "data"

    pattern = (data_dir / pattern).as_posix()

    print(f"Search files matching pattern: {pattern}")

    # Use glob to find all JSON files in a directory
    files = glob.glob(pattern)

    print(f"Found {len(files)} files")

    # Read and merge JSON files
    for file_name in files:
        with open(file_name, "r") as file:
            data = json.load(file)
            combined_data.extend(data)

    # Serialize combined data to a new JSON file
    out_fpath = data_dir / out_fname
    with open(out_fpath, "w") as combined_file:
        json.dump(combined_data, combined_file, indent=4)
