import logging
from pathlib import Path
from typing import Iterator

import boto3
import common_io
import conf
import polars as pl


def convert_reference_tables_to_parquet() -> Iterator[str]:
    logging.info("Build reference tables")

    reference_table_dir = Path(conf.reference_tables_local_folder_name)
    reference_table_file_paths = [str(f) for f in reference_table_dir.iterdir() if f.suffix == ".csv"]
    common_io.io_create_root_data_folder()

    for file in reference_table_file_paths:
        logging.info(f"Converting {file} to parquet")
        file_name = Path(file).stem
        output_file_path = conf.create_output_file_path(
            conf.OutputPathType.REFERENCE,
            conf.FileLocation.LOCAL,
            file_name=f"{file_name}.parquet",
        )

        reference_df = pl.read_csv(file)
        reference_df.write_parquet(output_file_path)
        yield output_file_path


def upload_reference_tables_to_s3(s3_client: boto3.client, parquet_files: list[str]) -> None:
    logging.info(f"Uploading {len(parquet_files)} reference tables to S3")

    for file in parquet_files:
        paruqet_file_name = file.split("/")[-1]
        reference_s3_key = conf.create_output_file_path(conf.OutputPathType.REFERENCE, conf.FileLocation.S3, file_name=paruqet_file_name)

        s3_client.upload_file(
            Filename=file,
            Bucket=conf.bucket_name,
            Key=reference_s3_key,
        )

        logging.info(f"Uploaded {paruqet_file_name} to s3://{conf.bucket_name}/{reference_s3_key}")


if __name__ == "__main__":
    reference_table_dir = Path(conf.reference_tables_local_folder_name)
    reference_table_files = [f for f in reference_table_dir.iterdir() if f.suffix == ".csv"]

    logging.info("Load reference tables into S3")
    s3 = boto3.client("s3")

    logging.info(f"Uploading {len(reference_table_files)} reference tables to S3")
    for file in reference_table_files:
        s3.upload_file(
            Filename=str(file),
            Bucket=conf.bucket_name,
            Key=f"{conf.reference_tables_local_folder_name}/{file.name}",
        )

    upload_reference_tables_to_s3(s3, list(convert_reference_tables_to_parquet()))
