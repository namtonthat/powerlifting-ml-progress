import logging
from pathlib import Path
from typing import Iterator

import common_io
import conf
import polars as pl


def test_counts(df_1: pl.DataFrame, df_2: pl.DataFrame) -> bool | None:
    logging.info(f"Row count (df_1): {len(df_1)}")
    logging.info(f"Row count (df_2): {len(df_2)}")
    assert len(df_1) == len(df_2)


def convert_reference_tables_to_parquet() -> Iterator[str]:
    """
    Convert all CSV reference tables in the configured local folder to Parquet format,
    saving them to the local output folder.

    Yields:
        Iterator[str]: Paths of the converted Parquet files.
    """
    logging.info("Build reference tables")

    reference_table_file_paths = [f"{conf.reference_tables_local_folder_name}/{file}" for file in Path(conf.reference_tables_local_folder_name).iterdir() if file.is_file() and file.suffix == ".csv"]
    common_io.io_create_root_data_folder()

    for file in reference_table_file_paths:
        logging.info(f"Converting {file} to parquet")
        file_name = file.split(".csv")[0].split("/")[-1]
        output_file_path = conf.create_output_file_path(
            conf.OutputPathType.REFERENCE,
            conf.FileLocation.LOCAL,
            file_name=f"{file_name}.parquet",
        )

        reference_df = pl.read_csv(file)
        reference_df.write_parquet(output_file_path)
        yield output_file_path
