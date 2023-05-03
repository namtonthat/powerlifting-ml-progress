import polars as pl
import zipfile


POWERLIFTING_DATA_URL = "https://openpowerlifting.gitlab.io/opl-csv/files/openipf-latest.zip"

with zipfile.ZipFile("file.zip", "r") as zip_ref:
    zip_ref.extractall(".")
