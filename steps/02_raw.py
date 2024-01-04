import boto3
import conf
import polars as pl
import common_io

import logging

logging.basicConfig(level=logging.INFO)


@conf.debug
def filter_non_numeric_place(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("place").apply(lambda x: x.isnumeric(), return_dtype=pl.Boolean))


@conf.debug
def type_cast(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("date"),
        pl.col("place").cast(pl.Int64).alias("place"),
    )


def add_age_rounded_up_half(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rounds ages ending in .0 to up to the next 0.5 year.
    Used that to calculate year of birth
    """
    return df.with_columns(pl.when(pl.col("age").apply(lambda x: x % 1 == 0)).then(pl.col("age") + 0.5).otherwise(pl.col("age")).alias("age_rounded_up_half"))


@conf.debug
def add_event_year(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns((pl.col("date").dt.strftime("%Y").cast(pl.Int32)).alias("event_year"))


@conf.debug
def add_year_of_birth(df: pl.DataFrame) -> pl.DataFrame:
    """
    Edge cases exist when the age ends in .0, so we round down to the nearest 0.5 year
    Test cases:
    - Joshua Luu for the AGE_TOLERANCE
    - John Paul Cauchi for the age rounding to the nearest half issue
    """

    min_max_birth_year_df = df.with_columns(
        (pl.col("event_year") - (pl.col("age_rounded_up_half") + conf.AGE_TOLERANCE_YEARS)).alias("min_birth_year"),
        (pl.col("event_year") - (pl.col("age_rounded_up_half") - conf.AGE_TOLERANCE_YEARS)).alias("max_birth_year"),
    )

    # Use the average of the min and max birth year as a likely birth year
    year_of_birth_df = min_max_birth_year_df.with_columns(((pl.col("min_birth_year") + pl.col("max_birth_year")) / 2).floor().cast(pl.Int32).alias("year_of_birth"))

    return year_of_birth_df


@conf.debug
def add_primary_key(df: pl.DataFrame) -> pl.DataFrame:
    """
    Primary key column is snake case of name plus year of birth
    Deduplicate on primary key, date, and meet name

    Test case:
    - Elizabeth Nguyen for three different lifters
    """
    logging.info("Creating primary key")
    primary_key_df = df.with_columns(
        pl.concat_str(
            [
                pl.col("name").str.to_lowercase().str.replace(" ", "-"),
                pl.col("sex").str.to_lowercase(),
                pl.col("year_of_birth").cast(pl.Utf8),
            ],
            separator="-",
        ).alias("primary_key")
    )

    # Sort by primary key and date descending
    # Records need to be sorted to ensure that the origin country is the first country that the lifter competed in
    primary_key_df_sorted = primary_key_df.sort(by=["primary_key", "date"], descending=[False, False])

    return primary_key_df_sorted


def filter_for_unique_primary_key(df: pl.DataFrame) -> pl.DataFrame:
    return df.unique(subset=["primary_key", "date", "meet_name"])


@conf.debug
def create_origin_country_df(df: pl.DataFrame) -> pl.DataFrame:
    lifter_country_df = df.groupby(["primary_key"]).agg(pl.first("meet_country").alias("origin_country"))
    return lifter_country_df


@conf.debug
def add_origin_country(df: pl.DataFrame, lifter_country_df: pl.DataFrame) -> pl.DataFrame:
    logging.info(f"Row count (df): {len(df)}")
    logging.info(f"Row count (lifter_country_df): {len(lifter_country_df)}")
    return df.join(lifter_country_df, on="primary_key", how="inner")


def test_counts(df_1, df_2):
    logging.info(f"Row count (df_1): {len(df_1)}")
    logging.info(f"Row count (df_2): {len(df_2)}")
    assert len(df_1) == len(df_2)


if __name__ == "__main__":
    logging.info("Loading data from S3")
    s3 = boto3.client("s3")
    df = pl.read_parquet(source=conf.landing_s3_http)
    logging.info(f"Loaded {conf.landing_s3_http}")

    logging.info("Performing raw transformations")
    # Filter out rows where the place is not numeric
    filtered_df = filter_non_numeric_place(df)
    type_cast_df = type_cast(filtered_df)

    # Add columns
    event_year_df = add_event_year(type_cast_df)
    age_rounded_up_half_df = add_age_rounded_up_half(event_year_df)
    year_of_birth_df = add_year_of_birth(age_rounded_up_half_df)

    # Create primary key
    all_primary_key_df = add_primary_key(year_of_birth_df)
    primary_key_df = filter_for_unique_primary_key(all_primary_key_df)

    # Find the first country that the powerlifter competed in and assume that is their country of origin
    lifter_country_df = create_origin_country_df(primary_key_df)

    # Add the origin country to the primary key dataframe
    raw_df = add_origin_country(primary_key_df, lifter_country_df)

    test_counts(raw_df, primary_key_df)

    # Write to parquet to s3
    common_io.io_write_from_local_to_s3(
        raw_df,
        conf.raw_local_file_path,
        conf.raw_s3_key,
    )
