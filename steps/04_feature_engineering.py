import logging

import boto3
import common_io
import conf
import polars as pl
import utils


@conf.debug
def add_tenure(df: pl.DataFrame) -> pl.DataFrame:
    first_competition_df = df.group_by("primary_key").agg(pl.min("date").alias("first_competition_date"))
    df = df.join(first_competition_df, on="primary_key", how="left")
    return df.with_columns((pl.col("date") - pl.col("first_competition_date")).dt.day().alias("tenure"))


@conf.debug
def add_weight_change(df: pl.DataFrame) -> pl.DataFrame:
    df = df.sort(by=["primary_key", "date"])
    df = df.with_columns(
        (pl.col("bodyweight") - pl.col("bodyweight").shift(1)).alias("weight_change_since_last_comp"),
        (pl.col("bodyweight") - pl.first("bodyweight").over("primary_key")).alias("overall_weight_change"),
    )
    return df


@conf.debug
def add_proximity_to_top_lifter(df: pl.DataFrame) -> pl.DataFrame:
    top_lifter_df = df.group_by(["weight_class", "sex"]).agg(pl.max("elio_rating").alias("top_elio_rating"))
    df = df.join(top_lifter_df, on=["weight_class", "sex"], how="left")
    return df.with_columns((pl.col("elio_rating") / pl.col("top_elio_rating")).alias("proximity_to_top_lifter"))


def add_percentile_ranks(df: pl.DataFrame, score_col: str, group_cols: list[str]) -> pl.DataFrame:
    """
    Add percentile rank column for the given score column grouped by group_cols.
    """
    percentile_col = f"{score_col}_percentile"

    # Calculate percentile rank within groups
    df = df.with_columns(
        pl.col(score_col).rank("average", descending=False).over(group_cols).alias(f"{score_col}_rank"),
        pl.count().over(group_cols).alias(f"{score_col}_count"),
    )

    df = df.with_columns(((pl.col(f"{score_col}_rank") - 1) / (pl.col(f"{score_col}_count") - 1)).fill_null(0).alias(percentile_col))

    # Drop intermediate columns
    df = df.drop([f"{score_col}_rank", f"{score_col}_count"])

    return df


def add_temporal_smoothing(df: pl.DataFrame, percentile_col: str, group_col: str, date_col: str, window_size: int = 3) -> pl.DataFrame:
    """
    Add temporal smoothing (rolling mean) of percentile ranks per lifter over time.
    """
    smoothed_col = f"{percentile_col}_smoothed"

    df = df.sort([group_col, date_col])

    # Rolling mean with window size, min_periods=1 to handle start of series
    df = df.with_columns(pl.col(percentile_col).rolling_mean(window_size, min_periods=1).over(group_col).alias(smoothed_col))

    return df


def add_squat_deadlift_similarity(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add a metric for similarity between squat and deadlift.
    We use ratio of min to max of squat and deadlift to get a value between 0 and 1.
    Closer to 1 means more similar.
    """
    df = df.with_columns((pl.min_horizontal([pl.col("squat"), pl.col("deadlift")]) / pl.max_horizontal([pl.col("squat"), pl.col("deadlift")])).alias("squat_deadlift_similarity"))
    return df


if __name__ == "__main__":
    logging.info("Loading %s S3", conf.raw_s3_http)
    s3 = boto3.client("s3")
    raw_df = pl.read_parquet(source=conf.raw_s3_http)
    logging.info(f"Loaded {conf.raw_s3_http}")

    # Add additional features
    tenure_df = add_tenure(raw_df)
    weight_change_df = add_weight_change(tenure_df)
    squat_deadlift_df = add_squat_deadlift_similarity(weight_change_df)
    base_df = add_proximity_to_top_lifter(squat_deadlift_df)
    # raw_df = add_percentile_ranks(raw_df)
    # raw_df = add_temporal_smoothing(raw_df)

    utils.test_counts(raw_df, base_df)

    common_io.io_write_from_local_to_s3(
        raw_df,
        conf.base_local_file_path,
        conf.base_s3,
    )
