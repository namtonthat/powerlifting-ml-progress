import os
import conf
import logging
import boto3


if __name__ == "__main__":
    reference_table_files = [file for file in os.listdir(conf.reference_tables_local_folder_name) if file.endswith(".csv")]

    logging.info("Load reference tables into S3")
    s3 = boto3.client("s3")

    logging.info(f"Uploading {len(reference_table_files)} reference tables to S3")
    for file in reference_table_files:
        s3.upload_file(
            Filename=f"{conf.reference_tables_local_folder_name}/{file}",
            Bucket=conf.bucket_name,
            Key=f"{conf.reference_tables_local_folder_name}/{file}",
        )
