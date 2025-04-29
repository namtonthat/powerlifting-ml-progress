import logging

import boto3
import common_io
import conf
import polars as pl
import utils


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
def add_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add temporal features to the dataframe:
    - time_since_last_comp_days: days since last competition per lifter.
    - cumulative_comps: cumulative count of competitions per lifter.
    - tenure: cumulative sum of days since last competition per lifter.

    Args:
        df (pl.DataFrame): Input dataframe with 'primary_key' and 'time_since_last_comp_days' columns.

    Returns:
        pl.DataFrame: DataFrame with added temporal feature columns.
    """
    time_since_last_comp_df = add_time_since_last_comp(df)

    return time_since_last_comp_df.with_columns(
        pl.col("primary_key").cum_count().over("primary_key").alias("cumulative_comps"),
        pl.col("time_since_last_comp_days").cum_sum().over("primary_key").alias("tenure"),
    )


@conf.debug
def add_generic_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add generic feature engineering columns, such as whether the meet country matches the lifter's origin country.

    Args:
        df (pl.DataFrame): Input dataframe with 'meet_country' and 'origin_country' columns.

    Returns:
        pl.DataFrame: DataFrame with added boolean column 'is_origin_country'.
    """
    meet_type_df = add_meet_type(df)
    return meet_type_df.with_columns(
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


@conf.debug
def add_numerical_features(df: pl.DataFrame) -> pl.DataFrame:
    base_df = add_proximity_to_top_lifter(df)
    progress_df = add_powerlifting_progress(base_df)
    proximity_df = add_proximity_to_top_lifter(progress_df)
    numeric_df = add_squat_deadlift_similarity(proximity_df)

    return numeric_df


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
    df = df.with_columns(pl.col(percentile_col).rolling_mean(window_size, min_samples=1).over(group_col).alias(smoothed_col))

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
    common_io.upload_reference_tables_to_s3(s3, list(utils.convert_reference_tables_to_parquet()))

    df = pl.read_parquet(source=conf.raw_s3_http)

    logging.info("Performing base transformations")
    renamed_df = df.select(conf.base_columns).rename(conf.base_renamed_columns)
    ordered_df = renamed_df.sort(by=["primary_key", "date"], descending=[False, False])
    cleansed_df = filter_for_raw_events(ordered_df)

    logging.info("Performing feature engineering transformations")
    # Generic
    generic_df = add_generic_features(cleansed_df)

    # Temporal
    temporal_df = add_temporal_features(cleansed_df)

    # Numerical
    fe_df = add_numerical_features(temporal_df)

    # raw_df = add_percentile_ranks(raw_df)
    # raw_df = add_temporal_smoothing(raw_df)

    common_io.io_write_from_local_to_s3(
        fe_df,
        conf.base_local_file_path,
        conf.base_s3_key,
    )
