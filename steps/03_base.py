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
    """Expanding mean within segment (sex + weight class) sorted by date.
    Uses shift(1) so current row's total is excluded (no data leakage)."""
    df = df.sort(["sex", "ipf_weight_class", "date"])
    df = df.with_columns(
        (pl.col("total").cum_sum().shift(1).over("sex", "ipf_weight_class") / pl.col("total").cum_count().shift(1).over("sex", "ipf_weight_class")).alias("segment_mean_total"),
    ).with_columns(
        (pl.col("total") / pl.col("segment_mean_total")).alias("total_vs_segment_mean"),
    )
    return df.sort(["primary_key", "date"])


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
def add_previous_dots(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("dots").shift(1).over("primary_key").alias("previous_dots"),
    )


@conf.debug
def add_dots_progress(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        ((pl.col("dots") - pl.col("previous_dots")) / pl.col("time_since_last_comp_years")).alias("dots_progress"),
    )


@conf.debug
def add_pct_change_dots(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("previous_dots") > 0).then((pl.col("dots") - pl.col("previous_dots")) / pl.col("previous_dots") * 100).otherwise(None).alias("pct_change_dots"),
    )


@conf.debug
def add_prev_pct_change_dots(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("pct_change_dots").shift(1).over("primary_key").alias("prev_pct_change_dots"),
    )


@conf.debug
def add_rolling_avg_dots(df: pl.DataFrame, window: int = 3) -> pl.DataFrame:
    return df.with_columns(
        pl.col("dots").rolling_mean(window_size=window, min_samples=window).over("primary_key").alias(f"rolling_avg_dots_{window}"),
    )


@conf.debug
def add_prev_rolling_avg_dots(df: pl.DataFrame, window: int = 3) -> pl.DataFrame:
    return df.with_columns(
        pl.col(f"rolling_avg_dots_{window}").shift(1).over("primary_key").alias(f"prev_rolling_avg_dots_{window}"),
    )


@conf.debug
def add_dots_personal_best(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("previous_dots").cum_max().over("primary_key").alias("prev_dots_personal_best"),
    ).with_columns(
        (pl.col("previous_dots") / pl.col("prev_dots_personal_best")).alias("prev_dots_vs_pb"),
        (pl.col("prev_dots_personal_best") - pl.col("previous_dots")).alias("prev_distance_from_pb_dots"),
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


# --- Iteration 2: High-impact features ---


@conf.debug
def add_personal_best_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("previous_total").cum_max().over("primary_key").alias("prev_personal_best_total"),
    ).with_columns(
        (pl.col("previous_total") / pl.col("prev_personal_best_total")).alias("prev_total_vs_pb"),
        (pl.col("prev_personal_best_total") - pl.col("previous_total")).alias("prev_distance_from_pb"),
    )


@conf.debug
def add_prev_progress_rate(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("total_progress").shift(1).over("primary_key").alias("prev_total_progress"),
        pl.col("total_progress").rolling_mean(window_size=3, min_samples=2).shift(1).over("primary_key").alias("prev_avg_progress_3"),
    )


@conf.debug
def add_competition_density(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("tenure") > 0).then(pl.col("cumulative_comps") / (pl.col("tenure") / conf.DAYS_IN_YEAR)).otherwise(None).alias("comps_per_year"),
    )


@conf.debug
def add_rolling_variability(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("total").rolling_std(window_size=3, min_samples=2).shift(1).over("primary_key").alias("prev_rolling_std_total_3"),
    )


@conf.debug
def add_age_lifecycle_features(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            (pl.col("date").dt.year() - pl.col("year_of_birth")).alias("approx_age"),
        )
        .with_columns(
            (pl.col("approx_age") - conf.PEAK_POWERLIFTING_AGE).alias("years_from_peak_age"),
        )
        .with_columns(
            pl.col("years_from_peak_age").abs().alias("abs_years_from_peak_age"),
        )
    )


@conf.debug
def add_first_comp_features(df: pl.DataFrame) -> pl.DataFrame:
    """Broadcast each lifter's first-comp snapshot as constant columns.

    These are 'birth certificate' features — they never change for a given
    lifter, encoding starting strength and age.

    Precondition: input sorted by (primary_key, date); `approx_age` column
    already present (produced by add_age_lifecycle_features) if first_comp_age
    is needed.
    """
    cols_to_add = {
        "first_comp_dots": pl.col("dots").first().over("primary_key"),
        "_first_comp_total": pl.col("total").first().over("primary_key"),
        "_first_comp_bw": pl.col("bodyweight").first().over("primary_key"),
    }
    if "approx_age" in df.columns:
        cols_to_add["first_comp_age"] = pl.col("approx_age").first().over("primary_key")

    df = df.with_columns(**cols_to_add)
    df = df.with_columns(
        (pl.col("_first_comp_total") / pl.col("_first_comp_bw")).alias("first_comp_total_per_bw"),
    )
    return df.drop(["_first_comp_total", "_first_comp_bw"])


@conf.debug
def add_first_comp_percentile(df: pl.DataFrame) -> pl.DataFrame:
    """Percentile of first_comp_dots within each (sex, ipf_weight_class) cohort.

    Note: uses the full dataset distribution (matches existing
    `prev_total_percentile_rank` convention). For stricter temporal purity,
    switch to an expanding-by-date percentile — deferred as future work.
    """
    return df.with_columns(
        (pl.col("first_comp_dots").rank("average").over("sex", "ipf_weight_class") / pl.col("first_comp_dots").count().over("sex", "ipf_weight_class") * 100).alias("first_comp_percentile_vs_sex_wc"),
    )


@conf.debug
def add_rolling_career_features(df: pl.DataFrame) -> pl.DataFrame:
    """Career-so-far features: max DOTS, best growth rate.

    Both use inputs that are already shifted (`previous_dots`) or explicitly
    scoped inside the `over()` partition so the current comp is strictly
    excluded.

    Precondition: input sorted by (primary_key, date).
    """
    # previous_dots is already shift(1) → cum_max.over(pk) gives max of comps 1..k-1.
    df = df.with_columns(
        pl.col("previous_dots").cum_max().over("primary_key").alias("max_dots_so_far"),
    )

    # For best_growth_rate_so_far, shift and cum_max must BOTH be scoped to primary_key
    # to avoid leaking across lifter boundaries. Since we can't chain window operations
    # in Polars, we do this in two steps: first shift within partition, then cum_max.
    if "pct_change_dots" in df.columns:
        df = df.with_columns(
            pl.col("pct_change_dots").shift(1).over("primary_key").alias("_shifted_pct_change"),
        )
        df = df.with_columns(
            pl.col("_shifted_pct_change").cum_max().over("primary_key").alias("best_growth_rate_so_far"),
        )
        df = df.drop("_shifted_pct_change")

    return df


@conf.debug
def add_dots_growth_trend(df: pl.DataFrame) -> pl.DataFrame:
    """Linear-regression slope of previous_dots vs. days-since-lifter's-first-comp.

    Uses all prior (non-null previous_dots) points for each lifter. Null until
    at least 2 prior points exist (comp 3 onward).

    Precondition: input sorted by (primary_key, date).
    """
    # Stage 1: materialise days-from-first-comp as an honest column.
    df = df.with_columns(
        (pl.col("date") - pl.col("date").first().over("primary_key")).dt.total_days().alias("_days_from_first"),
    )

    # Stage 2: masked x/y, counts, and expanding sums — one call, all scoped to primary_key.
    df = df.with_columns(
        pl.when(pl.col("previous_dots").is_not_null()).then(pl.col("_days_from_first")).otherwise(None).alias("_x_masked"),
        pl.when(pl.col("previous_dots").is_not_null()).then(pl.col("previous_dots")).otherwise(None).alias("_y_masked"),
    )
    df = df.with_columns(
        pl.col("previous_dots").is_not_null().cast(pl.Int64).cum_sum().over("primary_key").alias("_n"),
        pl.col("_x_masked").cum_sum().over("primary_key").alias("_x_sum"),
        pl.col("_y_masked").cum_sum().over("primary_key").alias("_y_sum"),
        (pl.col("_x_masked") * pl.col("_y_masked")).cum_sum().over("primary_key").alias("_xy_sum"),
        (pl.col("_x_masked") * pl.col("_x_masked")).cum_sum().over("primary_key").alias("_xx_sum"),
    )

    # Stage 3: compute means → cov/var → slope, with the n>=2 AND var>0 gate.
    df = df.with_columns(
        (pl.col("_x_sum") / pl.col("_n")).alias("_x_mean"),
        (pl.col("_y_sum") / pl.col("_n")).alias("_y_mean"),
    )
    df = df.with_columns(
        (pl.col("_xy_sum") - pl.col("_n") * pl.col("_x_mean") * pl.col("_y_mean")).alias("_cov"),
        (pl.col("_xx_sum") - pl.col("_n") * pl.col("_x_mean") * pl.col("_x_mean")).alias("_var"),
    )
    df = df.with_columns(
        pl.when((pl.col("_n") >= 2) & (pl.col("_var") > 0)).then(pl.col("_cov") / pl.col("_var")).otherwise(None).alias("dots_growth_trend"),
    )

    # Drop scratch columns
    return df.drop(["_days_from_first", "_x_masked", "_y_masked", "_n", "_x_sum", "_y_sum", "_xy_sum", "_xx_sum", "_x_mean", "_y_mean", "_cov", "_var"])


@conf.debug
def add_early_growth_rate(df: pl.DataFrame) -> pl.DataFrame:
    """(dots at 3rd comp - dots at 1st comp) / years between them.

    Broadcast as constant from cumulative_comps == 4 onward. Null for comps 1-3.
    Critical: uses dots *at* comp 3, so must be unavailable until comp 4 to avoid
    leaking into the comp-3 target prediction.

    Guards: null when years_between <= 0 (same-day comp 1 and comp 3, which
    technically shouldn't happen after MIN_DAYS_BETWEEN_COMPS filter but is
    defensive).

    Precondition: input sorted by (primary_key, date).
    """
    df = df.with_columns(pl.col("cumulative_comps").alias("_cc"))

    df = df.with_columns(
        pl.when(pl.col("_cc") == 1).then(pl.col("date")).otherwise(None).max().over("primary_key").alias("_c1_date"),
        pl.when(pl.col("_cc") == 1).then(pl.col("dots")).otherwise(None).max().over("primary_key").alias("_c1_dots"),
        pl.when(pl.col("_cc") == 3).then(pl.col("date")).otherwise(None).max().over("primary_key").alias("_c3_date"),
        pl.when(pl.col("_cc") == 3).then(pl.col("dots")).otherwise(None).max().over("primary_key").alias("_c3_dots"),
    )

    df = df.with_columns(
        ((pl.col("_c3_date") - pl.col("_c1_date")).dt.total_days() / conf.DAYS_IN_YEAR).alias("_years_between"),
    )

    raw_rate = (
        pl.when(pl.col("_years_between") > 0)
        .then(
            (pl.col("_c3_dots") - pl.col("_c1_dots")) / pl.col("_years_between"),
        )
        .otherwise(None)
    )

    df = df.with_columns(
        pl.when(pl.col("_cc") >= 4).then(raw_rate).otherwise(None).alias("early_growth_rate_dots_per_year"),
    )

    return df.drop(["_cc", "_c1_date", "_c1_dots", "_c3_date", "_c3_dots", "_years_between"])


def _score_to_ordinal(score_col: pl.Expr) -> pl.Expr:
    """Translate a DOTS score column into its ordinal tier (0-4).

    Uses conf.DOTS_TIERS with half-open intervals.
    """
    expr = pl.when(score_col < 200.0).then(0)
    expr = expr.when(score_col < 300.0).then(1)
    expr = expr.when(score_col < 400.0).then(2)
    expr = expr.when(score_col < 500.0).then(3)
    return expr.when(score_col >= 500.0).then(4).otherwise(None)


@conf.debug
def add_tier_features(df: pl.DataFrame) -> pl.DataFrame:
    """DOTS-tier ordinals derived from first_comp_dots and previous_dots.

    Four features:
    - starting_tier: tier at first comp (constant per lifter, 0-4)
    - prev_tier: tier at comp k-1 (null at comp 1)
    - max_tier_so_far: max prev_tier across comps 1..k-1
    - tiers_climbed_so_far: max_tier_so_far - starting_tier (>= 0)
    """
    df = df.with_columns(
        _score_to_ordinal(pl.col("first_comp_dots")).alias("starting_tier"),
        _score_to_ordinal(pl.col("previous_dots")).alias("prev_tier"),
    )
    df = df.with_columns(
        pl.col("prev_tier").cum_max().over("primary_key").alias("max_tier_so_far"),
    )
    df = df.with_columns(
        (pl.col("max_tier_so_far") - pl.col("starting_tier")).alias("tiers_climbed_so_far"),
    )
    return df


@conf.debug
def add_comps_above_tier(df: pl.DataFrame) -> pl.DataFrame:
    """One column per tier lower bound: count of prior comps at or above it.

    Uses previous_dots (shift(1)) so strictly comps 1..k-1.
    """
    thresholds = {
        "comps_above_intermediate_so_far": 200.0,
        "comps_above_advanced_so_far": 300.0,
        "comps_above_elite_so_far": 400.0,
        "comps_above_world_class_so_far": 500.0,
    }
    return df.with_columns([pl.col("previous_dots").ge(thr).cast(pl.Int64).fill_null(0).cum_sum().over("primary_key").alias(name) for name, thr in thresholds.items()])


@conf.debug
def add_potential_interactions(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("starting_tier") * pl.col("years_competing")).alias("starting_tier_x_years_competing"),
        (pl.col("max_tier_so_far") * pl.col("time_since_last_comp_years")).alias("max_tier_x_time_gap"),
    )


@conf.debug
def add_prev_absolute_change(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("previous_total") - pl.col("previous_total").shift(1).over("primary_key")).alias("prev_total_change_kg"),
    )


@conf.debug
def add_interaction_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("previous_total") * pl.col("time_since_last_comp_years")).alias("prev_total_x_time_gap"),
        (pl.col("previous_total") / pl.col("cumulative_comps")).alias("prev_total_per_comp"),
        (pl.col("elo_rating") - pl.col("meet_field_elo")).alias("elo_gap_vs_field"),
    )


# --- v8 features ---


@conf.debug
def add_time_gap_category(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("time_since_last_comp_days") <= conf.TIME_GAP_SHORT_UPPER)
        .then(conf.TIME_GAP_CATEGORIES["short"])
        .when(pl.col("time_since_last_comp_days") <= conf.TIME_GAP_MEDIUM_UPPER)
        .then(conf.TIME_GAP_CATEGORIES["medium"])
        .otherwise(conf.TIME_GAP_CATEGORIES["long"])
        .alias("time_gap_category"),
    )


@conf.debug
def add_log_experience(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("cumulative_comps").log().alias("log_cumulative_comps"),
    )


@conf.debug
def add_years_competing(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        (pl.col("tenure") / conf.DAYS_IN_YEAR).alias("years_competing"),
    )


@conf.debug
def add_pct_change_total(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col("previous_total") > 0).then((pl.col("total") - pl.col("previous_total")) / pl.col("previous_total") * 100).otherwise(None).alias("pct_change_total"),
    )


@conf.debug
def add_prev_pct_change(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("pct_change_total").shift(1).over("primary_key").alias("prev_pct_change"),
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
        .pipe(add_time_gap_category)
        .pipe(add_temporal_features)
        .pipe(add_log_experience)
        .pipe(add_years_competing)
        .pipe(add_meet_type)
        .pipe(add_ipf_weight_class)
        .pipe(add_segment_averages)
        .pipe(add_generic_feature_engineering_columns)
        .pipe(add_bodyweight_change)
        .pipe(add_relative_strength)
        .pipe(add_lift_ratios)
        .pipe(add_rolling_averages)
        .pipe(add_rolling_variability)
        .pipe(add_percentile_rank)
        .pipe(add_previous_powerlifting_records)
        .pipe(add_previous_wilks)
        .pipe(add_previous_dots)
        .pipe(add_dots_progress)
        .pipe(add_pct_change_dots)
        .pipe(add_rolling_avg_dots)
        .pipe(add_prev_rolling_avg_dots)
        .pipe(add_dots_personal_best)
        .pipe(add_personal_best_features)
        .pipe(add_prev_lift_ratios)
        .pipe(add_prev_rolling_averages)
        .pipe(add_prev_percentile_rank)
        .pipe(add_prev_relative_strength)
        .pipe(add_prev_segment_comparison)
        .pipe(add_prev_absolute_change)
        .pipe(add_elo_rating)
        .pipe(add_age_lifecycle_features)
        .pipe(add_competition_density)
        .pipe(add_interaction_features)
        .pipe(add_powerlifting_progress)
        .pipe(add_pct_change_total)
    )

    # Guard: null out unreliable progress rates from back-to-back meets
    progress_cols = ["squat_progress", "bench_progress", "deadlift_progress", "total_progress", "wilks_progress", "dots_progress", "pct_change_total", "pct_change_dots"]
    df = df.with_columns(pl.when(pl.col("time_since_last_comp_days") < conf.MIN_DAYS_BETWEEN_COMPS).then(None).otherwise(pl.col(c)).alias(c) for c in progress_cols)

    # Features that depend on guarded progress values
    df = df.pipe(add_prev_progress_rate)
    df = df.pipe(add_prev_pct_change)
    df = df.pipe(add_prev_pct_change_dots)

    common_io.io_write_from_local_to_s3(
        df,
        conf.base_local_file_path,
        conf.base_s3_key,
    )
