import re
import os
import logging
import shutil
from enum import Enum


# functions
def camel_to_snake(camel_str):
    snake_str = re.sub(r"(?<!^)(?=[A-Z])", "_", camel_str).lower()
    return snake_str


# commonly used config
zip_url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
root_data_folder = "data"
bucket_name = "powerlifting-ml-progress"
parquet_file = "openpowerlifting-latest.parquet"

extract_path = f"{root_data_folder}/raw"


class OutputPathType(Enum):
    LANDING = "landing"
    RAW = "raw"
    BASE = "base"
    SEMANTIC = "semantic"


class FileLocation(Enum):
    LOCAL = "local"
    S3 = "s3"


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


# Local
landing_local_path = create_output_path(
    OutputPathType.LANDING,
    FileLocation.LOCAL,
)
raw_local_path = create_output_path(
    OutputPathType.RAW,
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


def create_root_data_folder() -> None:
    if not os.path.exists(root_data_folder):
        logging.info(f"Creating root data folder: {root_data_folder}")
    else:
        logging.info(f"Root data folder already exists: {root_data_folder}")

    for folder in OutputPathType:
        logging.info(f"Creating {folder.value} folder")
        folder_path = os.path.join(root_data_folder, folder.value)
        os.makedirs(folder_path, exist_ok=True)


def clean_up_root_data_folder() -> None:
    logging.info("Cleaning files")
    shutil.rmtree(root_data_folder)
    logging.info(f"Files from '{root_data_folder}' removed")


# data specific config
required_cols = [
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
]

op_cols_rename = {
    "Best3SquatKg": "Squat",
    "Best3BenchKg": "Bench",
    "Best3DeadliftKg": "Deadlift",
    "TotalKg": "Total",
    "BodyweightKg": "Bodyweight",
}

required_columns = [camel_to_snake(col) for col in required_cols]
