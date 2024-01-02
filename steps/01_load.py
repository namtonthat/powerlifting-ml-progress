import boto3
import conf
import datetime
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
        # Convert CSV to Parquet
        logging.info("Converting CSV to Parquet")
        df = pl.read_csv(csv_file_path, infer_schema_length=None)

        # Rename columns before saving
        remapping_cols: dict = {col: conf.camel_to_snake(col) for col in df.columns}
        logging.info("Renaming columns")
        renamed_df = df.rename(mapping=remapping_cols)
        renamed_df.write_parquet(conf.landing_local_path)

        s3_client = boto3.client("s3")
        s3_client.upload_file(
            conf.landing_local_path,
            conf.bucket_name,
            conf.landing_s3_key,
            ExtraArgs={"ACL": "public-read"},
        )
        logging.info("Parquet file uploaded to S3 successfully")

        conf.clean_up_root_data_folder()

        update_readme()
