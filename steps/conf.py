import re
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
    output_path_type: OutputPathType, file_location: FileLocation, file_name: str
) -> str:
    if file_location == FileLocation.LOCAL:
        return f"{root_data_folder}/{output_path_type.value}/{file_name}"
    elif file_location == FileLocation.S3:
        return f"{output_path_type.value}/{file_name}"
    else:
        raise ValueError("Invalid file location")


# Local
landing_local_path = create_output_path(
    OutputPathType.LANDING, FileLocation.LOCAL, parquet_file
)
raw_local_path = create_output_path(
    OutputPathType.RAW, FileLocation.LOCAL, parquet_file
)

# S3 keys
landing_s3_key = create_output_path(
    OutputPathType.LANDING, FileLocation.S3, parquet_file
)
raw_s3_key = create_output_path(OutputPathType.RAW, FileLocation.S3, parquet_file)
base_s3_key = create_output_path(OutputPathType.BASE, FileLocation.S3, parquet_file)
semantic_s3_key = create_output_path(
    OutputPathType.SEMANTIC, FileLocation.S3, parquet_file
)


# data specific config
op_cols = [
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
