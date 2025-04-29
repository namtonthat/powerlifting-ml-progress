import logging
from pathlib import Path
from typing import Iterator

import boto3
import common_io
import conf
import polars as pl


# Transformations
@conf.debug
def add_powerlifting_progress(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the rate of progress for powerlifting metrics (squat, bench, deadlift, total, wilks)
    per year since the last competition for each lifter identified by 'primary_key'.

    Args:
        df (pl.DataFrame): Input dataframe containing powerlifting data with columns for lifts and time since last competition.

    Returns:
        pl.DataFrame: DataFrame with additional columns for progress rates of each lift and wilks score.
    """
    progress_df = df.with_columns(
        ((pl.col("squat") - pl.col("squat").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("squat_progress"),
        ((pl.col("bench") - pl.col("bench").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("bench_progress"),
        ((pl.col("deadlift") - pl.col("deadlift").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("deadlift_progress"),
        ((pl.col("total") - pl.col("total").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("total_progress"),
        ((pl.col("wilks") - pl.col("wilks").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("wilks_progress"),
    )

    return progress_df


@conf.debug
def filter_for_raw_events(df: pl.DataFrame) -> pl.DataFrame:
    """
    Filter the dataframe to include only raw, tested powerlifting events of type 'SBD' (Squat, Bench, Deadlift).

    Args:
        df (pl.DataFrame): Input dataframe containing powerlifting event data.

    Returns:
        pl.DataFrame: Filtered dataframe containing only raw, tested SBD events.
    """
    return df.filter((pl.col("event") == "SBD") & (pl.col("tested") == "Yes") & (pl.col("equipment") == "Raw"))


@conf.debug
def add_time_since_last_comp(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the time elapsed since the last competition for each lifter.

    Args:
        df (pl.DataFrame): Input dataframe with a 'date' column and 'primary_key' identifying lifters.

    Returns:
        pl.DataFrame: DataFrame with additional columns for time since last competition in days and years.
    """
    time_since_last_comp_df = df.with_columns(((pl.col("date") - pl.col("date").shift(1)).over("primary_key")).alias("time_since_last_comp")).with_columns(
        pl.col("time_since_last_comp").dt.total_days().alias("time_since_last_comp_days"),
        (pl.col("time_since_last_comp").dt.total_days() / conf.DAYS_IN_YEAR).alias("time_since_last_comp_years"),
    )
    return time_since_last_comp_df


@conf.debug
def add_meet_type(df: pl.DataFrame) -> pl.DataFrame:
    """
    Assign a numeric meet type based on the meet name indicating the level of the competition:
    1 - International
    2 - National
    3 - State
    4 - Local (default)

    Args:
        df (pl.DataFrame): Input dataframe with a 'meet_name' column.

    Returns:
        pl.DataFrame: DataFrame with an added 'meet_type' column representing competition level.
    """
    meet_type_df = df.with_columns(df.with_columns(pl.col("meet_name").fill_null("local"))).with_columns(
        pl.when(pl.col("meet_name").str.contains("national"))
        .then(2)
        .when(pl.col("meet_name").str.contains("international|world|commonwealth"))
        .then(1)
        .when(pl.col("meet_name").str.contains("state"))
        .then(3)
        .otherwise(4)
        .alias("meet_type"),
    )

    return meet_type_df


@conf.debug
def add_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add temporal features to the dataframe:
    - cumulative_comps: cumulative count of competitions per lifter.
    - tenure: cumulative sum of days since last competition per lifter.

    Args:
        df (pl.DataFrame): Input dataframe with 'primary_key' and 'time_since_last_comp_days' columns.

    Returns:
        pl.DataFrame: DataFrame with added temporal feature columns.
    """
    return df.with_columns(
        pl.col("primary_key").cum_count().over("primary_key").alias("cumulative_comps"),
        pl.col("time_since_last_comp_days").cum_sum().over("primary_key").alias("tenure"),
    )


@conf.debug
def add_generic_feature_engineering_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add generic feature engineering columns, such as whether the meet country matches the lifter's origin country.

    Args:
        df (pl.DataFrame): Input dataframe with 'meet_country' and 'origin_country' columns.

    Returns:
        pl.DataFrame: DataFrame with added boolean column 'is_origin_country'.
    """
    return df.with_columns(
        (pl.col("meet_country") == pl.col("origin_country")).alias("is_origin_country"),
    )


@conf.debug
def add_previous_powerlifting_records(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add columns for previous competition's powerlifting records for each lifter.

    Args:
        df (pl.DataFrame): Input dataframe with powerlifting lift columns and 'primary_key'.

    Returns:
        pl.DataFrame: DataFrame with added columns for previous squat, bench, deadlift, and total.
    """
    return df.with_columns(
        (pl.col("squat").shift(1)).over("primary_key").alias("previous_squat"),
        (pl.col("bench").shift(1)).over("primary_key").alias("previous_bench"),
        (pl.col("deadlift").shift(1)).over("primary_key").alias("previous_deadlift"),
        (pl.col("total").shift(1)).over("primary_key").alias("previous_total"),
    )


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


if __name__ == "__main__":
    logging.info("Loading %s S3", conf.raw_s3_http)
    s3 = boto3.client("s3")
    upload_reference_tables_to_s3(s3, list(convert_reference_tables_to_parquet()))

    df = pl.read_parquet(source=conf.raw_s3_http)

    logging.info("Performing base transformations")
    renamed_df = df.select(conf.base_columns).rename(conf.base_renamed_columns)
    ordered_df = renamed_df.sort(by=["primary_key", "date"], descending=[False, False])
    cleansed_df = filter_for_raw_events(ordered_df)

    logging.info("Performing feature engineering transformations")

    # Temporal
    time_since_last_comp_df = add_time_since_last_comp(ordered_df)
    temporal_df = add_temporal_features(time_since_last_comp_df)

    # Meet
    meet_type_df = add_meet_type(temporal_df)
    generic_feature_engineering_df = add_generic_feature_engineering_columns(meet_type_df)

    # Numerical
    progress_df = add_powerlifting_progress(generic_feature_engineering_df)
    fe_df = add_previous_powerlifting_records(progress_df)

    common_io.io_write_from_local_to_s3(
        fe_df,
        conf.base_local_file_path,
        conf.base_s3_key,
    )
