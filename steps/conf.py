zip_url ="https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
root_data_folder = "data"
bucket_name = "powerlifting-ml-progress"
parquet_file = "openpowerlifting-latest.parquet"

extract_path = f"{root_data_folder}/raw"
output_path = f"{root_data_folder}/{parquet_file}"

# data specific config
op_cols = [
    "Name",
    "Sex",
    "Event",
    "Date",
    "MeetCountry",
    "MeetState",
    "Equipment",
    "Best3SquatKg",
    "Best3BenchKg",
    "Best3DeadliftKg",
    "TotalKg",
    "Wilks",
    "Tested",
    "Federation",
    "MeetName"
]