import requests
import io
import zipfile
import polars as pl
import logging
import datetime
import shutil
import re
import boto3
import conf

# Configure logging
logging.basicConfig(level=logging.INFO)

def update_readme_date():
    with open("README.md", "r") as readme_file:
        contents = readme_file.read()

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    updated_contents = re.sub(r"Last updated: \d{4}-\d{2}-\d{2}", f"Last updated: {current_date}", contents)

    with open("README.md", "w") as readme_file:
        readme_file.write(updated_contents)

    logging.info("README.md updated with the current date")


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
        zip_ref.extractall(conf.extract_path)

    # Find the CSV file in the data directory
    logging.info("Searching for CSV file in the data directory")
    csv_file = ""
    for file in zip_ref.namelist():
        if file.endswith(".csv"):
            csv_file = f"{conf.extract_path}/{file}"
            break

    if not csv_file:
        logging.error("CSV file not found")
        return

    logging.info(f"CSV file found: {csv_file}")

    return csv_file


if __name__ == "__main__":
    csv_file_path = load_data(conf.zip_url)
    if csv_file_path:
        s3_key = conf.output_path.split('/')[-1]

        # Convert CSV to Parquet
        logging.info("Converting CSV to Parquet")
        df = pl.read_csv(csv_file_path, infer_schema_length=None)
        df.write_parquet(conf.output_path)

        s3_client = boto3.client('s3')
        s3_client.upload_file(conf.output_path, conf.bucket_name, s3_key)
        logging.info("Parquet file uploaded to S3 successfully")

        # Clean up
        logging.info("Cleaning extracted files")
        shutil.rmtree(conf.root_data_folder)
        logging.info(f"Files from '{conf.root_data_folder}' removed")

        update_readme_date()