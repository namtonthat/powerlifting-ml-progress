import re


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
output_path = f"{root_data_folder}/{parquet_file}"

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
