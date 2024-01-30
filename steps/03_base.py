import boto3
import conf
import common_io
import polars as pl
import logging
import os
from typing import Iterator


# Transformations
@conf.debug
def add_powerlifting_progress(df: pl.DataFrame) -> pl.DataFrame:
    progress_df = df.with_columns(
        ((pl.col("squat") - pl.col("squat").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("squat_progress"),
        ((pl.col("bench") - pl.col("bench").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("bench_progress"),
        ((pl.col("deadlift") - pl.col("deadlift").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("deadlift_progress"),
        ((pl.col("total") - pl.col("total").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("total_progress"),
        ((pl.col("wilks") - pl.col("wilks").shift(1)) / pl.col("time_since_last_comp_years")).over("primary_key").alias("wilks_progress"),
    )

    return progress_df


@conf.debug
def order_by_primary_key_and_date(df: pl.DataFrame) -> pl.DataFrame:
    return df.sort(by=["primary_key", "date"], descending=[False, False])


@conf.debug
def filter_for_raw_events(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter((pl.col("event") == "SBD") & (pl.col("tested") == "Yes") & (pl.col("equipment") == "Raw"))


@conf.debug
def add_time_since_last_comp(df: pl.DataFrame) -> pl.DataFrame:
    time_since_last_comp_df = df.with_columns(((pl.col("date") - pl.col("date").shift(1)).over("primary_key")).alias("time_since_last_comp")).with_columns(
        pl.col("time_since_last_comp").dt.total_days().alias("time_since_last_comp_days"),
        (pl.col("time_since_last_comp").dt.total_days() / conf.DAYS_IN_YEAR).alias("time_since_last_comp_years"),
    )
    return time_since_last_comp_df


@conf.debug
def add_meet_type(df: pl.DataFrame) -> pl.DataFrame:
    "Set meet type to local, state, national or international based on meet name."
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
    return df.with_columns(
        pl.col("primary_key").cum_count().over("primary_key").alias("cumulative_comps"),
        pl.col("time_since_last_comp_days").cum_sum().over("primary_key").alias("tenure"),
    )


@conf.debug
def add_generic_feature_engineering_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("meet_country") == pl.col("origin_country")).alias("is_origin_country"),
    )


@conf.debug
def add_previous_powerlifting_records(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("squat").shift(1)).over("primary_key").alias("previous_squat"),
        (pl.col("bench").shift(1)).over("primary_key").alias("previous_bench"),
        (pl.col("deadlift").shift(1)).over("primary_key").alias("previous_deadlift"),
        (pl.col("total").shift(1)).over("primary_key").alias("previous_total"),
    )


def convert_reference_tables_to_parquet() -> Iterator[str]:
    logging.info("Build reference tables")

    reference_table_file_paths = [f"{conf.reference_tables_local_folder_name}/{file}" for file in os.listdir(conf.reference_tables_local_folder_name) if file.endswith("csv")]
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
    logging.info("Loading data from S3")
    s3 = boto3.client("s3")
    upload_reference_tables_to_s3(s3, list(convert_reference_tables_to_parquet()))

    df = pl.read_parquet(source=conf.raw_s3_http)

    logging.info("Performing base transformations")
    renamed_df = df.select(conf.base_columns).rename(conf.base_renamed_columns)
    ordered_df = order_by_primary_key_and_date(renamed_df)
    # cleansed_df = filter_for_raw_events(ordered_df)

    logging.info("Performing feature engineering transformations")

    # # Temporal
    # time_since_last_comp_df = add_time_since_last_comp(ordered_df)
    # temporal_df = add_temporal_features(time_since_last_comp_df)

    # # Meet
    # meet_type_df = add_meet_type(temporal_df)
    # generic_feature_engineering_df = add_generic_feature_engineering_columns(meet_type_df)

    # # Numerical
    # progress_df = add_powerlifting_progress(generic_feature_engineering_df)
    # fe_df = add_previous_powerlifting_records(progress_df)

    # common_io.io_write_from_local_to_s3(
    #     fe_df,
    #     conf.base_local_file_path,
    #     conf.base_s3_key,
    # )
