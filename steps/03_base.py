import bisect
import logging

import common_io
import conf
import polars as pl


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


@conf.debug
def filter_minimum_competitions(df: pl.DataFrame, min_comps: int = conf.MIN_COMPETITIONS) -> pl.DataFrame:
    return df.filter(pl.col("primary_key").count().over("primary_key") >= min_comps)


@conf.debug
def remove_duplicate_names(df: pl.DataFrame) -> pl.DataFrame:
    """Detect cases where the same lowercase name maps to multiple primary keys
    with similar birth years (within 3 years). Keep the primary key with the most records."""
    lifter_info = df.select("primary_key", "name", "year_of_birth").unique(subset=["primary_key"])
    lifter_info = lifter_info.with_columns(pl.col("name").str.to_lowercase().alias("name_lower"))

    # Count records per primary_key
    counts = df.group_by("primary_key").len().rename({"len": "record_count"})
    lifter_info = lifter_info.join(counts, on="primary_key")

    # Self-join on lowercase name to find potential duplicates
    dupes = lifter_info.join(lifter_info, on="name_lower", suffix="_other").filter(
        (pl.col("primary_key") != pl.col("primary_key_other")) & ((pl.col("year_of_birth") - pl.col("year_of_birth_other")).abs() <= 3)
    )

    if len(dupes) == 0:
        return df

    # For each duplicate pair, keep the primary key with more records
    keys_to_drop = dupes.filter(pl.col("record_count") < pl.col("record_count_other")).select("primary_key").unique()

    logging.info(f"Removing {len(keys_to_drop)} duplicate-name primary keys")
    return df.filter(~pl.col("primary_key").is_in(keys_to_drop["primary_key"]))


@conf.debug
def add_ipf_weight_class(df: pl.DataFrame) -> pl.DataFrame:
    def classify_weight(sex: str | None, bw: float | None) -> str:
        if sex is None or bw is None:
            return "unknown"
        boundaries = conf.IPF_WEIGHT_CLASSES.get(sex[0].upper(), [])
        if not boundaries:
            return "unknown"
        idx = bisect.bisect_left(boundaries, bw)
        if idx >= len(boundaries):
            idx = len(boundaries) - 1
        upper = boundaries[idx]
        if upper == float("inf"):
            prev = boundaries[idx - 1] if idx > 0 else 0
            return f"{prev}+"
        return str(int(upper))

    weight_classes = [classify_weight(s, b) for s, b in zip(df["sex"].to_list(), df["bodyweight"].to_list())]
    return df.with_columns(pl.Series("ipf_weight_class", weight_classes))


@conf.debug
def clean_federation_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("parent_federation").fill_null("Independent"),
    )


@conf.debug
def add_segment_averages(df: pl.DataFrame) -> pl.DataFrame:
    segment_stats = df.group_by(["sex", "ipf_weight_class"]).agg(
        pl.col("total").mean().alias("segment_mean_total"),
    )
    return df.join(segment_stats, on=["sex", "ipf_weight_class"], how="left").with_columns(
        (pl.col("total") / pl.col("segment_mean_total")).alias("total_vs_segment_mean"),
    )


@conf.debug
def add_relative_strength(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("total") / pl.col("bodyweight")).alias("total_per_bw"),
    )


@conf.debug
def add_previous_wilks(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("wilks").shift(1).over("primary_key").alias("previous_wilks"),
    )


@conf.debug
def add_bodyweight_change(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns((pl.col("bodyweight") - pl.col("bodyweight").shift(1)).over("primary_key").alias("bodyweight_change"))


@conf.debug
def add_lift_ratios(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("squat") / pl.col("total")).alias("squat_ratio"),
        (pl.col("bench") / pl.col("total")).alias("bench_ratio"),
        (pl.col("deadlift") / pl.col("total")).alias("deadlift_ratio"),
    )


@conf.debug
def add_rolling_averages(df: pl.DataFrame, window: int = 3) -> pl.DataFrame:
    return df.with_columns(
        pl.col("total").rolling_mean(window_size=window, min_samples=window).over("primary_key").alias(f"rolling_avg_total_{window}"),
        pl.col("squat").rolling_mean(window_size=window, min_samples=window).over("primary_key").alias(f"rolling_avg_squat_{window}"),
        pl.col("bench").rolling_mean(window_size=window, min_samples=window).over("primary_key").alias(f"rolling_avg_bench_{window}"),
        pl.col("deadlift").rolling_mean(window_size=window, min_samples=window).over("primary_key").alias(f"rolling_avg_deadlift_{window}"),
    )


@conf.debug
def add_percentile_rank(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("total").rank("average").over("sex", "ipf_weight_class") / pl.col("total").count().over("sex", "ipf_weight_class") * 100).alias("total_percentile_rank"),
    )


# --- ML-safe lagged features (fix data leakage) ---


@conf.debug
def add_prev_lift_ratios(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("previous_squat") / pl.col("previous_total")).alias("prev_squat_ratio"),
        (pl.col("previous_bench") / pl.col("previous_total")).alias("prev_bench_ratio"),
        (pl.col("previous_deadlift") / pl.col("previous_total")).alias("prev_deadlift_ratio"),
    )


@conf.debug
def add_prev_rolling_averages(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("rolling_avg_total_3").shift(1).over("primary_key").alias("prev_rolling_avg_total_3"),
        pl.col("rolling_avg_squat_3").shift(1).over("primary_key").alias("prev_rolling_avg_squat_3"),
        pl.col("rolling_avg_bench_3").shift(1).over("primary_key").alias("prev_rolling_avg_bench_3"),
        pl.col("rolling_avg_deadlift_3").shift(1).over("primary_key").alias("prev_rolling_avg_deadlift_3"),
    )


@conf.debug
def add_prev_percentile_rank(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("previous_total").rank("average").over("sex", "ipf_weight_class") / pl.col("previous_total").count().over("sex", "ipf_weight_class") * 100).alias("prev_total_percentile_rank"),
    )


@conf.debug
def add_prev_relative_strength(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("previous_total") / pl.col("bodyweight")).alias("prev_total_per_bw"),
    )


@conf.debug
def add_prev_segment_comparison(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("previous_total") / pl.col("segment_mean_total")).alias("prev_total_vs_segment_mean"),
    )


# --- Elo rating system ---


@conf.debug
def add_elo_rating(df: pl.DataFrame) -> pl.DataFrame:
    """Field-based Elo rating adapted from tennis.

    At each meet, lifters are rated against peers in their segment (sex + weight class).
    The feature stored is the pre-meet Elo (no leakage).
    """
    elo_ratings: dict[str, float] = {}  # primary_key -> current elo

    # Pre-allocate output lists
    n = len(df)
    out_elo = [None] * n
    out_change = [None] * n
    out_field = [None] * n

    # Build a row-index map keyed by (meet_name, date)
    meet_names = df["meet_name"].to_list()
    dates = df["date"].to_list()
    pkeys = df["primary_key"].to_list()
    sexes = df["sex"].to_list()
    weight_classes = df["ipf_weight_class"].to_list()
    totals = df["total"].to_list()
    meet_types = df["meet_type"].to_list()

    # Group row indices by (meet_name, date), preserving chronological order
    from collections import OrderedDict

    meet_groups: OrderedDict[tuple, list[int]] = OrderedDict()
    for i in range(n):
        key = (meet_names[i], dates[i])
        if key not in meet_groups:
            meet_groups[key] = []
        meet_groups[key].append(i)

    # Sort meet groups chronologically
    sorted_meets = sorted(meet_groups.items(), key=lambda x: x[0][1])

    for (_meet_name, _date), row_indices in sorted_meets:
        # Sub-group by segment (sex, ipf_weight_class)
        segments: dict[tuple, list[int]] = {}
        for i in row_indices:
            seg_key = (sexes[i], weight_classes[i])
            if seg_key not in segments:
                segments[seg_key] = []
            segments[seg_key].append(i)

        for _seg_key, seg_indices in segments.items():
            if len(seg_indices) < 2:
                # Record pre-meet Elo but skip update for trivial segments
                for i in seg_indices:
                    pk = pkeys[i]
                    pre_elo = elo_ratings.get(pk, conf.ELO_INITIAL)
                    out_elo[i] = pre_elo
                    out_field[i] = pre_elo  # field of 1
                continue

            # Collect pre-meet Elos
            pre_elos = {}
            for i in seg_indices:
                pk = pkeys[i]
                pre_elos[i] = elo_ratings.get(pk, conf.ELO_INITIAL)

            field_avg = sum(pre_elos.values()) / len(pre_elos)

            # Rank lifters by total within this meet+segment (percentile 0-1)
            seg_totals = [(i, totals[i] if totals[i] is not None else 0.0) for i in seg_indices]
            seg_totals.sort(key=lambda x: x[1])
            num_lifters = len(seg_totals)
            for rank_pos, (i, _total) in enumerate(seg_totals):
                actual_score = rank_pos / (num_lifters - 1) if num_lifters > 1 else 0.5

                lifter_elo = pre_elos[i]
                expected_score = 1.0 / (1.0 + 10.0 ** ((field_avg - lifter_elo) / 400.0))

                mt = meet_types[i] if meet_types[i] is not None else 4
                k = conf.ELO_BASE_K * conf.ELO_MEET_TYPE_MULTIPLIER.get(mt, 0.6)
                elo_change = k * (actual_score - expected_score)

                out_elo[i] = lifter_elo
                out_change[i] = elo_change
                out_field[i] = field_avg

                # Update Elo for future meets
                pk = pkeys[i]
                elo_ratings[pk] = lifter_elo + elo_change

    return df.with_columns(
        pl.Series("elo_rating", out_elo, dtype=pl.Float64),
        pl.Series("elo_change", out_change, dtype=pl.Float64),
        pl.Series("meet_field_elo", out_field, dtype=pl.Float64),
    )


if __name__ == "__main__":
    df = (
        pl.read_parquet(source=conf.raw_s3_http)
        .select(conf.base_columns)
        .rename(conf.base_renamed_columns)
        .pipe(clean_federation_columns)
        .pipe(order_by_primary_key_and_date)
        .pipe(filter_for_raw_events)
        .pipe(remove_duplicate_names)
        .pipe(filter_minimum_competitions)
        .pipe(add_time_since_last_comp)
        .pipe(add_temporal_features)
        .pipe(add_meet_type)
        .pipe(add_ipf_weight_class)
        .pipe(add_segment_averages)
        .pipe(add_generic_feature_engineering_columns)
        .pipe(add_bodyweight_change)
        .pipe(add_relative_strength)
        .pipe(add_lift_ratios)
        .pipe(add_rolling_averages)
        .pipe(add_percentile_rank)
        .pipe(add_previous_powerlifting_records)
        .pipe(add_previous_wilks)
        .pipe(add_prev_lift_ratios)
        .pipe(add_prev_rolling_averages)
        .pipe(add_prev_percentile_rank)
        .pipe(add_prev_relative_strength)
        .pipe(add_prev_segment_comparison)
        .pipe(add_elo_rating)
        .pipe(add_powerlifting_progress)
    )

    # Guard: null out unreliable progress rates from back-to-back meets
    progress_cols = ["squat_progress", "bench_progress", "deadlift_progress", "total_progress", "wilks_progress"]
    df = df.with_columns(pl.when(pl.col("time_since_last_comp_days") < conf.MIN_DAYS_BETWEEN_COMPS).then(None).otherwise(pl.col(c)).alias(c) for c in progress_cols)

    common_io.io_write_from_local_to_s3(
        df,
        conf.base_local_file_path,
        conf.base_s3_key,
    )
