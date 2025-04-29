"""
A set of common IO functions to be used across all steps.
"""

import logging
import shutil
from pathlib import Path

import boto3
import conf
import polars as pl


# IO functions
def io_create_root_data_folder() -> bool:
    """
    Create root data folder and subfolders as defined in OutputPathType.

    Returns:
        bool: True if all folders are created successfully, False otherwise.
    """
    try:
        if not Path(conf.root_data_folder).exists():
            Path(conf.root_data_folder).mkdir(exist_ok=True)
            logging.info(f"Created root data folder: {conf.root_data_folder}")
        else:
            logging.info(f"Root data folder already exists: {conf.root_data_folder}")

        for folder in conf.OutputPathType:
            folder_path = Path(conf.root_data_folder) / Path(folder.value)
            # folder_path = os.path.join(conf.root_data_folder, folder.value)
            Path(folder_path).mkdir(exist_ok=True)
            logging.info(f"Created {folder.value} folder")

        return True
    except Exception as e:
        logging.error(f"Error in creating root data folder: {e}")
        return False


def io_remove_root_data_folder() -> bool:
    """
    Clean up the root data folder by deleting it.

    Returns:
        bool: True if the folder is deleted successfully, False otherwise.
    """
    try:
        if Path(conf.root_data_folder).exists():
            shutil.rmtree(conf.root_data_folder)
            logging.info(f"Files from '{conf.root_data_folder}' removed")
            return True
        else:
            logging.warning(f"Root data folder '{conf.root_data_folder}' does not exist.")
            return False
    except Exception as e:
        logging.error(f"Error in cleaning up root data folder: {e}")
        return False


def io_write_from_local_to_s3(df: pl.DataFrame, local_path: str, s3_key: str, debug: bool = True) -> None:
    """
    Saves a dataframe to local and then uploads it to S3.
    Performs the clean up of the local file after the upload.
    """
    logging.info("Writing to parquet")
    # Need to generate root data folder to ensure it can be written out to
    io_create_root_data_folder()
    df.write_parquet(local_path)

    # logging.info("Writing to S3")
    # s3_client = boto3.client("s3")
    # s3_client.upload_file(
    #     local_path,
    #     conf.bucket_name,
    #     s3_key,
    #     ExtraArgs={"ACL": "public-read"},
    # )
    # logging.info("Parquet file uploaded to S3 successfully")

    if debug:
        logging.info(f"Parquet file can be found at: {local_path}")
    else:
        logging.info("Cleaning up local files")
        io_remove_root_data_folder()


def upload_reference_tables_to_s3(s3_client: boto3.client, parquet_files: list[str]) -> None:
    """
    Upload Parquet reference tables to the configured S3 bucket.

    Args:
        s3_client (boto3.client): Boto3 S3 client instance.
        parquet_files (list[str]): List of local Parquet file paths to upload.

    Returns:
        None
    """
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
