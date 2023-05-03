import requests
import io
import zipfile
import polars as pl
import logging
import os
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)

ZIP_URL = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
ROOT_DATA_FOLDER = "data"

EXTRACT_PATH = f"{ROOT_DATA_FOLDER}/raw"
OUTPUT_PATH = f"{ROOT_DATA_FOLDER}/openipf-latest.parquet"


def load_data(url) -> str:
    """
    Returns file path of the CSV file
    """
    logging.info(f"Downloading data from {url}")
    response = requests.get(url)
    zip_data = io.BytesIO(response.content)

    # Unzip the data
    logging.info("Unzipping data")
    with zipfile.ZipFile(zip_data) as zip_ref:
        zip_ref.extractall(EXTRACT_PATH)

    # Find the CSV file in the data directory
    logging.info("Searching for CSV file in the data directory")
    csv_file = ""
    for file in zip_ref.namelist():
        if file.endswith(".csv"):
            csv_file = f"{EXTRACT_PATH}/{file}"
            break

    if not csv_file:
        logging.error("CSV file not found")
        return

    logging.info(f"CSV file found: {csv_file}")

    return csv_file


if __name__ == "__main__":
    csv_file_path = load_data(ZIP_URL)
    if csv_file_path:
        # Convert CSV to Parquet
        logging.info("Converting CSV to Parquet")
        df = pl.read_csv(csv_file_path, infer_schema_length=None)
        df.write_parquet(OUTPUT_PATH)

        # Clean up
        logging.info("Removing the CSV file and its parent folder")
        os.remove(csv_file_path)
        logging.info(f"CSV file '{csv_file_path}' removed")

        shutil.rmtree(EXTRACT_PATH)
        logging.info(f"Parent folder '{EXTRACT_PATH}' removed")
