import re
import os
import logging
import shutil
from enum import Enum
import polars as pl


class OutputPathType(Enum):
    LANDING = "landing"
    RAW = "raw"
    BASE = "base"
    SEMANTIC = "semantic"


class FileLocation(Enum):
    LOCAL = "local"
    S3 = "s3"


# Global Variables
zip_url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
root_data_folder = "data"
bucket_name = "powerlifting-ml-progress"
parquet_file = "openpowerlifting-latest.parquet"

extract_path = f"{root_data_folder}/raw"

DAYS_IN_YEAR = 365.25


# Functions
def camel_to_snake(camel_str):
    snake_str = re.sub(r"(?<!^)(?=[A-Z])", "_", camel_str).lower()
    return snake_str


def create_output_path(
    output_path_type: OutputPathType,
    file_location: FileLocation,
    as_http: bool = False,
    file_name: str = parquet_file,
) -> str:
    "Generates the output path for a given file location and output path type"
    if file_location == FileLocation.LOCAL:
        return f"{root_data_folder}/{output_path_type.value}/{file_name}"
    elif file_location == FileLocation.S3 and not as_http:
        return f"{output_path_type.value}/{file_name}"
    elif file_location == FileLocation.S3 and as_http:
        return f"https://{bucket_name}.s3.amazonaws.com/{output_path_type.value}/{file_name}"
    else:
        raise ValueError("Invalid file location")


# IO functions
def io_create_root_data_folder() -> None:
    if not os.path.exists(root_data_folder):
        logging.info(f"Creating root data folder: {root_data_folder}")
    else:
        logging.info(f"Root data folder already exists: {root_data_folder}")

    for folder in OutputPathType:
        logging.info(f"Creating {folder.value} folder")
        folder_path = os.path.join(root_data_folder, folder.value)
        os.makedirs(folder_path, exist_ok=True)


def io_clean_up_root_data_folder() -> None:
    logging.info("Cleaning files")
    shutil.rmtree(root_data_folder)
    logging.info(f"Files from '{root_data_folder}' removed")


def debug(func):
    def wrapper(*args, **kwargs):
        result_df: pl.DataFrame = func(*args, **kwargs)
        logging.info(result_df.head(2))

        logging.info(f"Row count: {len(result_df)}")
        return result_df

    return wrapper


# Local
landing_local_path = create_output_path(
    OutputPathType.LANDING,
    FileLocation.LOCAL,
)
raw_local_path = create_output_path(
    OutputPathType.RAW,
    FileLocation.LOCAL,
)
base_local_path = create_output_path(
    OutputPathType.BASE,
    FileLocation.LOCAL,
)

# S3 keys
landing_s3_key = create_output_path(
    OutputPathType.LANDING,
    FileLocation.S3,
)
raw_s3_key = create_output_path(
    OutputPathType.RAW,
    FileLocation.S3,
)
base_s3_key = create_output_path(
    OutputPathType.BASE,
    FileLocation.S3,
)
semantic_s3_key = create_output_path(
    OutputPathType.SEMANTIC,
    FileLocation.S3,
)

landing_s3_http = create_output_path(
    OutputPathType.LANDING, FileLocation.S3, as_http=True
)

raw_s3_http = create_output_path(OutputPathType.RAW, FileLocation.S3, as_http=True)


# Columns
# Landing
_required_landing_column_names = [
    "Date",
    "Name",
    "Sex",
    "Place",
    "Age",
    "AgeClass",
    "BodyweightKg",
    "Event",
    "MeetCountry",
    "Equipment",
    "Best3SquatKg",
    "Best3BenchKg",
    "Best3DeadliftKg",
    "TotalKg",
    "Wilks",
    "Tested",
    "Federation",
    "MeetName",
    "Country",
    "State",
    "ParentFederation",
]

renamed_landing_column_names = {
    "Best3SquatKg": "squat",
    "Best3BenchKg": "bench",
    "Best3DeadliftKg": "deadlift",
    "TotalKg": "total",
    "BodyweightKg": "bodyweight",
}

landing_column_names = [camel_to_snake(col) for col in _required_landing_column_names]

# TODO: Remove this from the raw layer as it is not best practice
# raw columns created from transformations
additional_raw_columns = ["origin_country", "primary_key", "year_of_birth"]

# Base layer
_base_column_names = _required_landing_column_names + additional_raw_columns
base_columns = [camel_to_snake(col) for col in _base_column_names]

base_renamed_columns = {
    camel_to_snake(key): camel_to_snake(value)
    for key, value in renamed_landing_column_names.items()
}
