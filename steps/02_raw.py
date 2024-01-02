import boto3
import conf
import polars as pl
import os

import logging

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    logging.info("Loading data from S3")
    s3 = boto3.client("s3")
    df = pl.read_parquet(source=conf.landing_s3_http)
    logging.info(f"Loaded {conf.landing_s3_http}")

    logging.info("Performing raw transformations")
    # Filter out rows where the place is not numeric
    filtered_df = (
        df.filter(
            pl.col("place").apply(lambda x: x.isnumeric(), return_dtype=pl.Boolean)
        )
        .drop_nulls()
        .unique()
        .sort("date", descending=True)
    )

    # Perform type casting in place
    filtered_with_type_df = filtered_df.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("date"),
        pl.col("place").cast(pl.Int64).alias("place"),
    )

    # Create a year of birth to identify lifters
    year_of_birth_df = filtered_with_type_df.with_columns(
        (pl.col("date").dt.strftime("%Y").cast(pl.Int32) - pl.col("age").cast(pl.Int32))
        .cast(pl.Utf8)
        .alias("year_of_birth")
    )

    # Primary key column is snake case of name plus year of birth
    # Deduplicate on primary key, date, and meet name
    logging.info("Creating primary key")
    primary_key_df = year_of_birth_df.with_columns(
        pl.concat_str(
            [
                pl.col("name").str.to_lowercase().str.replace(" ", "-"),
                pl.col("sex"),
                pl.col("year_of_birth"),
            ],
            separator="-",
        ).alias("primary_key")
    ).unique(subset=["primary_key", "date", "meet_name"])

    # Find the first country that the powerlifter competed in and assume that is their country of origin
    lifter_country_df = primary_key_df.groupby(["primary_key"]).agg(
        pl.first("meet_country").alias("origin_country")
    )

    # Add the origin country to the primary key dataframe
    raw_df = primary_key_df.join(lifter_country_df, on=["primary_key"])

    # Test counts
    logging.info("Testing counts")
    duplicates_on_primary_key: bool = len(raw_df) != len(primary_key_df)
    logging.info(f"Duplicate primary key: {duplicates_on_primary_key}")

    # Write to parquet to s3
    logging.info("Writing to parquet")
    # Need to generate root data folder to ensure it can be written out to
    conf.create_root_data_folder()
    raw_df.write_parquet(conf.raw_local_path)

    logging.info("Writing to S3")
    s3_client = boto3.client("s3")
    s3_client.upload_file(
        conf.raw_local_path,
        conf.bucket_name,
        conf.raw_s3_key,
        ExtraArgs={"ACL": "public-read"},
    )

    conf.clean_up_root_data_folder()
