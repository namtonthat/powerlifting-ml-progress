"""
Descriptive statistics on the base layer.
Produces summary tables that answer: "How much strength can a powerlifter gain on average?"
"""

import logging

import common_io
import conf
import polars as pl

logging.basicConfig(level=logging.INFO)

LIFT_PROGRESS_COLS = ["squat_progress", "bench_progress", "deadlift_progress", "total_progress", "wilks_progress"]


def _progress_aggs(cols: list[str]) -> list[pl.Expr]:
    """Generate p25 / median / p75 aggregations for each progress column."""
    aggs = []
    for c in cols:
        aggs.extend(
            [
                pl.col(c).quantile(0.25).alias(f"{c}_p25"),
                pl.col(c).median().alias(f"{c}_median"),
                pl.col(c).quantile(0.75).alias(f"{c}_p75"),
            ]
        )
    aggs.append(pl.col(cols[0]).count().alias("sample_size"))
    return aggs


def _valid_progress_filter() -> pl.Expr:
    return pl.col("total_progress").is_not_null() & pl.col("total_progress").is_finite()


def _add_experience_level(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("cumulative_comps") <= 3)
        .then(pl.lit("novice (1-3)"))
        .when(pl.col("cumulative_comps") <= 8)
        .then(pl.lit("intermediate (4-8)"))
        .when(pl.col("cumulative_comps") <= 15)
        .then(pl.lit("experienced (9-15)"))
        .otherwise(pl.lit("veteran (16+)"))
        .alias("experience_level"),
    )


def progress_by_sex_and_weight_class(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.filter(_valid_progress_filter() & pl.col("sex").is_in(["M", "F"]) & (pl.col("ipf_weight_class") != "unknown"))
        .group_by(["sex", "ipf_weight_class"])
        .agg(*_progress_aggs(LIFT_PROGRESS_COLS))
        .sort(["sex", "total_progress_median"], descending=[False, True])
    )


def progress_by_experience_level(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(_valid_progress_filter()).pipe(_add_experience_level).group_by("experience_level").agg(*_progress_aggs(LIFT_PROGRESS_COLS)).sort("experience_level")


def progress_by_sex_weight_class_and_experience(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.filter(_valid_progress_filter() & pl.col("sex").is_in(["M", "F"]) & (pl.col("ipf_weight_class") != "unknown"))
        .pipe(_add_experience_level)
        .group_by(["sex", "ipf_weight_class", "experience_level"])
        .agg(*_progress_aggs(LIFT_PROGRESS_COLS))
        .sort(["sex", "ipf_weight_class", "experience_level"])
    )


def data_quality_report(df: pl.DataFrame) -> pl.DataFrame:
    total_records = len(df)
    unique_lifters = df["primary_key"].n_unique()
    date_min = df["date"].min()
    date_max = df["date"].max()
    mean_comps = df.group_by("primary_key").len().select(pl.col("len").mean().alias("mean_comps_per_lifter"))

    null_rates = {col: df[col].null_count() / total_records for col in df.columns}
    high_null_cols = {k: round(v, 4) for k, v in null_rates.items() if v > 0.01}

    report = pl.DataFrame(
        {
            "metric": [
                "total_records",
                "unique_lifters",
                "date_range_start",
                "date_range_end",
                "mean_comps_per_lifter",
                "columns_with_gt_1pct_nulls",
            ],
            "value": [
                str(total_records),
                str(unique_lifters),
                str(date_min),
                str(date_max),
                str(round(mean_comps.item(0, 0), 2)),
                str(high_null_cols),
            ],
        }
    )
    return report


if __name__ == "__main__":
    logging.info("Loading base data")
    df = pl.read_parquet(conf.base_local_file_path)

    logging.info("=== Data Quality Report ===")
    quality = data_quality_report(df)
    logging.info("\n%s", quality)

    logging.info("=== Progress by Sex & Weight Class ===")
    by_weight = progress_by_sex_and_weight_class(df)
    logging.info("\n%s", by_weight)

    logging.info("=== Progress by Experience Level ===")
    by_experience = progress_by_experience_level(df)
    logging.info("\n%s", by_experience)

    logging.info("=== Progress by Sex, Weight Class & Experience ===")
    by_sex_wc_exp = progress_by_sex_weight_class_and_experience(df)
    logging.info("\n%s", by_sex_wc_exp)

    # Write summary tables locally + upload to S3
    common_io.io_write_from_local_to_s3(by_weight, conf.describe_by_weight_class_local, conf.describe_by_weight_class_s3_key)
    common_io.io_write_from_local_to_s3(by_experience, conf.describe_by_experience_local, conf.describe_by_experience_s3_key)
    common_io.io_write_from_local_to_s3(by_sex_wc_exp, conf.describe_by_sex_wc_exp_local, conf.describe_by_sex_wc_exp_s3_key)
    common_io.io_write_from_local_to_s3(quality, conf.describe_quality_local, conf.describe_quality_s3_key)

    logging.info("Descriptive statistics written to %s", conf.base_local_file_path.rsplit("/", 1)[0])
