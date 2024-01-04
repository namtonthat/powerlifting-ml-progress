import conf
import datetime
import common_io
import io
import logging
import requests
import zipfile

import polars as pl
from jinja2 import Environment, FileSystemLoader


logging.basicConfig(level=logging.INFO)


def update_readme():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d").replace("-", "--")

    # Set up the Jinja2 environment
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("templates/README.md")

    # Render the template with the variables
    updated_contents = template.render(
        last_updated=current_date,
        bucket_name=conf.bucket_name,
        s3_key=conf.landing_s3_key,
    )

    # Save the rendered contents to README.md
    with open("README.md", "w") as readme_file:
        readme_file.write(updated_contents)

    logging.info("README.md updated")
    return


def download_and_extract_csv(url) -> str:
    """
    Downloads a zip file from a given URL and extracts the CSV file from it.
    """

    logging.info(f"Downloading data from {url}")
    with requests.get(url) as response:
        response.raise_for_status()  # This will raise an error for bad requests
        zip_data = io.BytesIO(response.content)

    logging.info("Unzipping data")
    with zipfile.ZipFile(zip_data, "r") as zip_ref:
        zip_ref.extractall(conf.extract_path)

    logging.info("Searching for CSV file")
    csv_files = [f"{conf.extract_path}/{file}" for file in zip_ref.namelist() if file.lower().endswith(".csv")]

    if not csv_files:
        logging.error("CSV file not found")
        return

    logging.info(f"CSV file(s) found: {csv_files}, returning first file")

    return csv_files[0]


if __name__ == "__main__":
    csv_file_path = download_and_extract_csv(conf.zip_url)
    if csv_file_path:
        # Convert CSV to Parquet
        logging.info("Converting CSV to Parquet")
        df = pl.read_csv(csv_file_path, infer_schema_length=None)

        # Rename columns before saving
        remapping_cols: dict = {col: conf.camel_to_snake(col) for col in df.columns}
        logging.info("Renaming columns")
        landing_df = df.rename(mapping=remapping_cols)

        common_io.io_write_from_local_to_s3(
            landing_df,
            conf.landing_local_file_path,
            conf.landing_s3_key,
        )

        update_readme()
