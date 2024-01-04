import boto3
import conf
import polars as pl

import logging

logging.basicConfig(level=logging.INFO)


# Transformations
@conf.debug
def add_powerlifting_progress(df: pl.DataFrame) -> pl.DataFrame:
    progress_df = df.with_columns(
        ((pl.col("squat") - pl.col("squat").shift(1)) / pl.col("years_since_last_comp"))
        .over("primary_key")
        .alias("squat_progress"),
        ((pl.col("bench") - pl.col("bench").shift(1)) / pl.col("years_since_last_comp"))
        .over("primary_key")
        .alias("bench_progress"),
        (
            (pl.col("deadlift") - pl.col("deadlift").shift(1))
            / pl.col("years_since_last_comp")
        )
        .over("primary_key")
        .alias("deadlift_progress"),
        ((pl.col("total") - pl.col("total").shift(1)) / pl.col("years_since_last_comp"))
        .over("primary_key")
        .alias("total_progress"),
        ((pl.col("wilks") - pl.col("wilks").shift(1)) / pl.col("years_since_last_comp"))
        .over("primary_key")
        .alias("wilks_progress"),
    ).filter(pl.col("total_progress").is_not_null())

    return progress_df


@conf.debug
def filter_for_raw_events(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(
        (pl.col("event") == "SBD")
        & (pl.col("tested") == "Yes")
        & (pl.col("equipment") == "Raw")
    )


@conf.debug
def add_time_since_last_comp(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("date") - pl.col("date").shift(-1))
        .over("primary_key")
        .alias("time_since_last_comp_days")
    ).with_columns(
        pl.col("time_since_last_comp_days").dt.days(),
        (pl.col("time_since_last_comp_days") / conf.DAYS_IN_YEAR).alias(
            "years_since_last_comp"
        ),
    )


@conf.debug
def add_meet_type(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("meet_name").str.contains("national"))
        .then("national")
        .otherwise(
            pl.when(
                pl.col("meet_name").str.contains("International|World|Commonwealth")
            )
            .then("international")
            .otherwise("local")
        )
        .alias("meet_type"),
    )


@conf.debug
def add_generic_feature_engineering_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("meet_country") == pl.col("origin_country")).alias("is_origin_country"),
        pl.col("date").apply(lambda x: x.toordinal()).alias("date_as_ordinal"),
        pl.col("name").cumcount().over("primary_key").alias("cumulative_comps"),
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
    cleansed_df = filter_for_raw_events(renamed_df)

    # Create feature engineered columns
    logging.info("Performing feature engineering transformations")
    time_since_last_comp_df = add_time_since_last_comp(cleansed_df)
    meet_type_df = add_meet_type(time_since_last_comp_df)
    generic_feature_engineering_df = add_generic_feature_engineering_columns(
        meet_type_df
    )
    progress_df = add_powerlifting_progress(generic_feature_engineering_df)
    fe_df = add_previous_powerlifting_records(progress_df)

    fe_df.write_parquet(conf.base_local_path)

    logging.info("Writing to S3")
    s3_client = boto3.client("s3")
    s3_client.upload_file(
        conf.base_local_path,
        conf.bucket_name,
        conf.base_s3_key,
        ExtraArgs={"ACL": "public-read"},
    )
