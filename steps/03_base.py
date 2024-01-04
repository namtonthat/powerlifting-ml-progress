import boto3
import conf
import common_io
import polars as pl

import logging

logging.basicConfig(level=logging.INFO)


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


# @conf.debug
# def filter_for_raw_events(df: pl.DataFrame) -> pl.DataFrame:
#     return df.filter((pl.col("event") == "SBD") & (pl.col("tested") == "Yes") & (pl.col("equipment") == "Raw"))


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


if __name__ == "__main__":
    logging.info("Loading data from S3")
    s3 = boto3.client("s3")
    df = pl.read_parquet(source=conf.raw_s3_http)

    logging.info("Performing base transformations")
    renamed_df = df.select(conf.base_columns).rename(conf.base_renamed_columns)
    ordered_df = order_by_primary_key_and_date(renamed_df)

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
