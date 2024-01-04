"""
A set of common IO functions to be used across all steps.
"""

import boto3
import os
import logging
import shutil
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
        if not os.path.exists(conf.root_data_folder):
            os.makedirs(conf.root_data_folder, exist_ok=True)
            logging.info(f"Created root data folder: {conf.root_data_folder}")
        else:
            logging.info(f"Root data folder already exists: {conf.root_data_folder}")

        for folder in conf.OutputPathType:
            folder_path = os.path.join(conf.root_data_folder, folder.value)
            os.makedirs(folder_path, exist_ok=True)
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
        if os.path.exists(conf.root_data_folder):
            shutil.rmtree(conf.root_data_folder)
            logging.info(f"Files from '{conf.root_data_folder}' removed")
            return True
        else:
            logging.warning(f"Root data folder '{conf.root_data_folder}' does not exist.")
            return False
    except Exception as e:
        logging.error(f"Error in cleaning up root data folder: {e}")
        return False


def io_write_from_local_to_s3(df: pl.DataFrame, local_path: str, s3_key: str) -> None:
    """
    Saves a dataframe to local and then uploads it to S3.
    Performs the clean up of the local file after the upload.
    """
    logging.info("Writing to parquet")
    # Need to generate root data folder to ensure it can be written out to
    io_create_root_data_folder()
    df.write_parquet(local_path)

    logging.info("Writing to S3")
    s3_client = boto3.client("s3")
    s3_client.upload_file(
        local_path,
        conf.bucket_name,
        s3_key,
        ExtraArgs={"ACL": "public-read"},
    )
    logging.info("Parquet file uploaded to S3 successfully")

    io_remove_root_data_folder()


# A debug decorator to log the head of the dataframe and the row count
def debug(func):
    def wrapper(*args, **kwargs):
        result_df: pl.DataFrame = func(*args, **kwargs)
        logging.info(result_df.head(2))

        logging.info(f"Row count: {len(result_df)}")
        return result_df

    return wrapper
