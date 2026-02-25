"""
Advanced analytics on the base layer.
Produces 9 parquet files covering 8 analysis perspectives on powerlifting progress.
"""

import logging

import common_io
import conf
import numpy as np
import polars as pl
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO)


def _base_filter(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("sex").is_in(["M", "F"]) & (pl.col("ipf_weight_class") != "unknown"))


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


def career_trajectory(df: pl.DataFrame) -> pl.DataFrame:
    """Bucket tenure into ranges and compute median total with IQR per group."""
    breaks = conf.CAREER_TRAJECTORY_TENURE_BUCKETS
    labels = conf.CAREER_TRAJECTORY_TENURE_LABELS
    filtered = _base_filter(df).filter(pl.col("tenure").is_not_null())
    cut_labels = [*labels, "10yr+"]
    bucketed = filtered.with_columns(pl.col("tenure").cut(breaks[1:], labels=cut_labels).alias("tenure_bucket"))
    return (
        bucketed.group_by(["sex", "ipf_weight_class", "tenure_bucket"])
        .agg(
            pl.col("total").quantile(0.25).alias("total_p25"),
            pl.col("total").median().alias("median_total"),
            pl.col("total").quantile(0.75).alias("total_p75"),
            pl.col("total").count().alias("sample_size"),
        )
        .sort(["sex", "ipf_weight_class", "tenure_bucket"])
    )


def comp_indexed_growth(df: pl.DataFrame) -> pl.DataFrame:
    """Median lifts by competition number (1-20) per sex/weight class."""
    filtered = _base_filter(df).filter(pl.col("cumulative_comps") <= conf.MAX_CUMULATIVE_COMPS_FOR_INDEXING)
    return (
        filtered.group_by(["sex", "ipf_weight_class", "cumulative_comps"])
        .agg(
            pl.col("total").median().alias("median_total"),
            pl.col("squat").median().alias("median_squat"),
            pl.col("bench").median().alias("median_bench"),
            pl.col("deadlift").median().alias("median_deadlift"),
            pl.col("total").count().alias("sample_size"),
        )
        .sort(["sex", "ipf_weight_class", "cumulative_comps"])
    )


def percentile_movement(df: pl.DataFrame) -> pl.DataFrame:
    """How percentile rank shifts over competition count."""
    filtered = _base_filter(df).filter((pl.col("cumulative_comps") <= conf.MAX_CUMULATIVE_COMPS_FOR_INDEXING) & pl.col("total_percentile_rank").is_not_null())
    return (
        filtered.group_by(["sex", "ipf_weight_class", "cumulative_comps"])
        .agg(
            pl.col("total_percentile_rank").quantile(0.25).alias("percentile_rank_p25"),
            pl.col("total_percentile_rank").median().alias("median_percentile_rank"),
            pl.col("total_percentile_rank").quantile(0.75).alias("percentile_rank_p75"),
            pl.col("total_percentile_rank").count().alias("sample_size"),
        )
        .sort(["sex", "ipf_weight_class", "cumulative_comps"])
    )


def rolling_average_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Median rolling averages by competition number."""
    filtered = _base_filter(df).filter((pl.col("cumulative_comps") <= conf.MAX_CUMULATIVE_COMPS_FOR_INDEXING) & pl.col("rolling_avg_total_3").is_not_null())
    return (
        filtered.group_by(["sex", "ipf_weight_class", "cumulative_comps"])
        .agg(
            pl.col("rolling_avg_total_3").median().alias("median_rolling_total"),
            pl.col("rolling_avg_squat_3").median().alias("median_rolling_squat"),
            pl.col("rolling_avg_bench_3").median().alias("median_rolling_bench"),
            pl.col("rolling_avg_deadlift_3").median().alias("median_rolling_deadlift"),
            pl.col("rolling_avg_total_3").count().alias("sample_size"),
        )
        .sort(["sex", "ipf_weight_class", "cumulative_comps"])
    )


def lift_ratio_analysis(df: pl.DataFrame) -> pl.DataFrame:
    """Median lift ratios by experience level."""
    filtered = _base_filter(df).filter(pl.col("squat_ratio").is_not_null())
    return (
        filtered.pipe(_add_experience_level)
        .group_by(["sex", "ipf_weight_class", "experience_level"])
        .agg(
            pl.col("squat_ratio").median().alias("median_squat_ratio"),
            pl.col("bench_ratio").median().alias("median_bench_ratio"),
            pl.col("deadlift_ratio").median().alias("median_deadlift_ratio"),
            pl.col("squat_ratio").count().alias("sample_size"),
        )
        .sort(["sex", "ipf_weight_class", "experience_level"])
    )


def wilks_trajectory(df: pl.DataFrame) -> pl.DataFrame:
    """Wilks score progression by competition number."""
    filtered = _base_filter(df).filter((pl.col("cumulative_comps") <= conf.MAX_CUMULATIVE_COMPS_FOR_INDEXING) & pl.col("wilks").is_not_null())
    return (
        filtered.group_by(["sex", "ipf_weight_class", "cumulative_comps"])
        .agg(
            pl.col("wilks").quantile(0.25).alias("wilks_p25"),
            pl.col("wilks").median().alias("median_wilks"),
            pl.col("wilks").quantile(0.75).alias("wilks_p75"),
            pl.col("wilks").count().alias("sample_size"),
        )
        .sort(["sex", "ipf_weight_class", "cumulative_comps"])
    )


def survival_analysis(df: pl.DataFrame) -> pl.DataFrame:
    """Fraction of lifters reaching total milestones by competition N."""
    filtered = _base_filter(df).filter(pl.col("cumulative_comps") <= conf.MAX_CUMULATIVE_COMPS_FOR_INDEXING)
    filtered = filtered.with_columns(pl.col("total").cum_max().over("primary_key").alias("best_total_so_far"))

    results = []
    for sex, milestones in [("M", conf.SURVIVAL_MILESTONES_M), ("F", conf.SURVIVAL_MILESTONES_F)]:
        sex_df = filtered.filter(pl.col("sex") == sex)
        total_lifters = sex_df.group_by("ipf_weight_class").agg(pl.col("primary_key").n_unique().alias("total_lifters"))

        for milestone in milestones:
            reached = sex_df.filter(pl.col("best_total_so_far") >= milestone).group_by(["ipf_weight_class", "cumulative_comps"]).agg(pl.col("primary_key").n_unique().alias("lifters_reached"))
            # Cumulative: at comp N, count all who reached by comp N or earlier
            reached = reached.sort(["ipf_weight_class", "cumulative_comps"]).with_columns(pl.col("lifters_reached").cum_max().over("ipf_weight_class").alias("lifters_reached"))
            reached = reached.join(total_lifters, on="ipf_weight_class").with_columns(
                pl.lit(sex).alias("sex"),
                pl.lit(milestone).alias("milestone_kg"),
                (pl.col("lifters_reached") / pl.col("total_lifters")).alias("fraction_reached"),
            )
            results.append(reached.select("sex", "ipf_weight_class", "milestone_kg", "cumulative_comps", "fraction_reached", "total_lifters"))

    return pl.concat(results).sort(["sex", "ipf_weight_class", "milestone_kg", "cumulative_comps"])


def archetype_clustering(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Cluster lifters into archetypes based on career summary features."""
    filtered = _base_filter(df)
    career = (
        filtered.group_by("primary_key")
        .agg(
            pl.col("sex").first().alias("sex"),
            pl.col("cumulative_comps").max().alias("total_comps"),
            pl.col("tenure").max().alias("career_tenure"),
            pl.col("total").max().alias("max_total"),
            pl.col("wilks").max().alias("max_wilks"),
            pl.col("total_progress").median().alias("median_progress"),
        )
        .drop_nulls()
    )

    feature_cols = ["total_comps", "career_tenure", "max_total", "max_wilks", "median_progress"]
    X = career.select(feature_cols).to_numpy()

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=conf.ARCHETYPE_N_CLUSTERS, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_scaled)

    archetypes = career.with_columns(pl.Series("cluster", labels))

    centroids_original = scaler.inverse_transform(kmeans.cluster_centers_)
    centroids_df = pl.DataFrame(
        np.column_stack([np.arange(conf.ARCHETYPE_N_CLUSTERS), centroids_original]),
        schema=["cluster", *feature_cols],
        orient="row",
    ).cast({"cluster": pl.Int32})

    return archetypes, centroids_df


if __name__ == "__main__":
    logging.info("Loading base data")
    df = pl.read_parquet(conf.base_local_file_path)

    logging.info("=== Career Trajectory ===")
    ct = career_trajectory(df)
    common_io.io_write_from_local_to_s3(ct, conf.analysis_career_trajectory_local, conf.analysis_career_trajectory_s3_key)

    logging.info("=== Competition-Indexed Growth ===")
    cig = comp_indexed_growth(df)
    common_io.io_write_from_local_to_s3(cig, conf.analysis_comp_indexed_growth_local, conf.analysis_comp_indexed_growth_s3_key)

    logging.info("=== Percentile Rank Movement ===")
    pm = percentile_movement(df)
    common_io.io_write_from_local_to_s3(pm, conf.analysis_percentile_movement_local, conf.analysis_percentile_movement_s3_key)

    logging.info("=== Rolling Average Summary ===")
    ras = rolling_average_summary(df)
    common_io.io_write_from_local_to_s3(ras, conf.analysis_rolling_averages_local, conf.analysis_rolling_averages_s3_key)

    logging.info("=== Lift Ratio Analysis ===")
    lra = lift_ratio_analysis(df)
    common_io.io_write_from_local_to_s3(lra, conf.analysis_lift_ratios_local, conf.analysis_lift_ratios_s3_key)

    logging.info("=== Wilks Trajectory ===")
    wt = wilks_trajectory(df)
    common_io.io_write_from_local_to_s3(wt, conf.analysis_wilks_trajectory_local, conf.analysis_wilks_trajectory_s3_key)

    logging.info("=== Survival Analysis ===")
    sa = survival_analysis(df)
    common_io.io_write_from_local_to_s3(sa, conf.analysis_survival_local, conf.analysis_survival_s3_key)

    logging.info("=== Lifter Archetype Clustering ===")
    archetypes, centroids = archetype_clustering(df)
    common_io.io_write_from_local_to_s3(archetypes, conf.analysis_archetypes_local, conf.analysis_archetypes_s3_key)
    common_io.io_write_from_local_to_s3(centroids, conf.analysis_archetype_centroids_local, conf.analysis_archetype_centroids_s3_key)

    logging.info("All analyses written successfully")
