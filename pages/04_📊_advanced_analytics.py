import sys
from pathlib import Path

import altair as alt
import polars as pl
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

st.set_page_config(page_title="Advanced Analytics", page_icon="📊", layout="wide")


def _load(s3_http: str, local: str) -> pl.DataFrame:
    try:
        return pl.read_parquet(s3_http)
    except Exception:
        return pl.read_parquet(local)


@st.cache_data
def load_career_trajectory() -> pl.DataFrame:
    return _load(conf.analysis_career_trajectory_s3_http, conf.analysis_career_trajectory_local)


@st.cache_data
def load_comp_indexed_growth() -> pl.DataFrame:
    return _load(conf.analysis_comp_indexed_growth_s3_http, conf.analysis_comp_indexed_growth_local)


@st.cache_data
def load_percentile_movement() -> pl.DataFrame:
    return _load(conf.analysis_percentile_movement_s3_http, conf.analysis_percentile_movement_local)


@st.cache_data
def load_rolling_averages() -> pl.DataFrame:
    return _load(conf.analysis_rolling_averages_s3_http, conf.analysis_rolling_averages_local)


@st.cache_data
def load_lift_ratios() -> pl.DataFrame:
    return _load(conf.analysis_lift_ratios_s3_http, conf.analysis_lift_ratios_local)


@st.cache_data
def load_wilks_trajectory() -> pl.DataFrame:
    return _load(conf.analysis_wilks_trajectory_s3_http, conf.analysis_wilks_trajectory_local)


@st.cache_data
def load_survival() -> pl.DataFrame:
    return _load(conf.analysis_survival_s3_http, conf.analysis_survival_local)


@st.cache_data
def load_archetypes() -> pl.DataFrame:
    return _load(conf.analysis_archetypes_s3_http, conf.analysis_archetypes_local)


@st.cache_data
def load_archetype_centroids() -> pl.DataFrame:
    return _load(conf.analysis_archetype_centroids_s3_http, conf.analysis_archetype_centroids_local)


# --- Load all data ---
ct_df = load_career_trajectory()
cig_df = load_comp_indexed_growth()
pm_df = load_percentile_movement()
ra_df = load_rolling_averages()
lr_df = load_lift_ratios()
wt_df = load_wilks_trajectory()
surv_df = load_survival()
arch_df = load_archetypes()
cent_df = load_archetype_centroids()

# --- Sidebar ---
with st.sidebar:
    st.header("Filters")
    st.markdown("---")
    sex = st.radio("Sex", options=["M", "F"], index=0)

    weight_classes = sorted(
        ct_df.filter(pl.col("sex") == sex)["ipf_weight_class"].unique().to_list(),
        key=lambda x: float(x.replace("+", "")) if x.replace("+", "").replace(".", "").isdigit() else 999,
    )
    weight_class = st.selectbox("Weight Class (kg)", options=weight_classes)

# --- Main ---
st.write("# Advanced Analytics")
st.caption("8 analytical perspectives on powerlifting progress for tested, raw, SBD lifters.")

tab_names = [
    "Career Trajectory",
    "Comp-Indexed Growth",
    "Percentile Rank",
    "Rolling Averages",
    "Lift Ratios",
    "Wilks Trajectory",
    "Time to Milestone",
    "Lifter Archetypes",
]
tabs = st.tabs(tab_names)

tenure_order = conf.CAREER_TRAJECTORY_TENURE_LABELS

# --- Tab 1: Career Trajectory ---
with tabs[0]:
    st.subheader("Career Trajectory Curves")
    st.caption("Median total (kg) by career tenure with IQR band. Shows diminishing returns over time.")

    filt = ct_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class)).to_pandas()

    if len(filt) > 0:
        line = (
            alt.Chart(filt)
            .mark_line(point=True)
            .encode(
                x=alt.X("tenure_bucket:N", sort=tenure_order, title="Career Tenure"),
                y=alt.Y("median_total:Q", title="Median Total (kg)"),
                tooltip=["tenure_bucket", "median_total", "total_p25", "total_p75", "sample_size"],
            )
        )
        band = (
            alt.Chart(filt)
            .mark_area(opacity=0.2)
            .encode(
                x=alt.X("tenure_bucket:N", sort=tenure_order),
                y="total_p25:Q",
                y2="total_p75:Q",
            )
        )
        st.altair_chart(band + line, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# --- Tab 2: Competition-Indexed Growth ---
with tabs[1]:
    st.subheader("Competition-Indexed Growth")
    st.caption("Median lift (kg) by competition number. See how lifters progress across their first 20 meets.")

    filt = cig_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class))

    if len(filt) > 0:
        melted = (
            filt.unpivot(
                on=["median_total", "median_squat", "median_bench", "median_deadlift"],
                index=["cumulative_comps"],
                variable_name="lift",
                value_name="median_kg",
            )
            .with_columns(pl.col("lift").str.replace("median_", ""))
            .to_pandas()
        )

        chart = (
            alt.Chart(melted)
            .mark_line(point=True)
            .encode(
                x=alt.X("cumulative_comps:Q", title="Competition #", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("median_kg:Q", title="Median (kg)"),
                color=alt.Color("lift:N", title="Lift"),
                tooltip=["cumulative_comps", "lift", "median_kg"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# --- Tab 3: Percentile Rank ---
with tabs[2]:
    st.subheader("Percentile Rank Movement")
    st.caption("How a lifter's percentile rank within their weight class shifts over competitions.")

    filt = pm_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class)).to_pandas()

    if len(filt) > 0:
        line = (
            alt.Chart(filt)
            .mark_line(point=True)
            .encode(
                x=alt.X("cumulative_comps:Q", title="Competition #", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("median_percentile_rank:Q", title="Median Percentile Rank"),
                tooltip=["cumulative_comps", "median_percentile_rank", "percentile_rank_p25", "percentile_rank_p75", "sample_size"],
            )
        )
        band = (
            alt.Chart(filt)
            .mark_area(opacity=0.2)
            .encode(
                x=alt.X("cumulative_comps:Q", axis=alt.Axis(tickMinStep=1)),
                y="percentile_rank_p25:Q",
                y2="percentile_rank_p75:Q",
            )
        )
        st.altair_chart(band + line, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# --- Tab 4: Rolling Averages ---
with tabs[3]:
    st.subheader("Rolling Averages (3-Competition Window)")
    st.caption("Smoothed lift progression using a 3-competition rolling mean.")

    filt = ra_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class))

    if len(filt) > 0:
        melted = (
            filt.unpivot(
                on=["median_rolling_total", "median_rolling_squat", "median_rolling_bench", "median_rolling_deadlift"],
                index=["cumulative_comps"],
                variable_name="lift",
                value_name="median_rolling_avg",
            )
            .with_columns(pl.col("lift").str.replace("median_rolling_", ""))
            .to_pandas()
        )

        chart = (
            alt.Chart(melted)
            .mark_line(point=True)
            .encode(
                x=alt.X("cumulative_comps:Q", title="Competition #", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("median_rolling_avg:Q", title="Median Rolling Avg (kg)"),
                color=alt.Color("lift:N", title="Lift"),
                tooltip=["cumulative_comps", "lift", "median_rolling_avg"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# --- Tab 5: Lift Ratios ---
with tabs[4]:
    st.subheader("Lift Ratio Analysis")
    st.caption("How the proportion of squat/bench/deadlift in the total shifts with experience.")

    filt = lr_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class))

    exp_order = ["novice (1-3)", "intermediate (4-8)", "experienced (9-15)", "veteran (16+)"]

    if len(filt) > 0:
        melted = (
            filt.unpivot(
                on=["median_squat_ratio", "median_bench_ratio", "median_deadlift_ratio"],
                index=["experience_level"],
                variable_name="lift",
                value_name="ratio",
            )
            .with_columns(pl.col("lift").str.replace("median_", "").str.replace("_ratio", ""))
            .to_pandas()
        )

        chart = (
            alt.Chart(melted)
            .mark_bar()
            .encode(
                x=alt.X("experience_level:N", sort=exp_order, title="Experience Level"),
                y=alt.Y("ratio:Q", title="Ratio of Total", stack="normalize"),
                color=alt.Color("lift:N", title="Lift"),
                tooltip=["experience_level", "lift", "ratio"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# --- Tab 6: Wilks Trajectory ---
with tabs[5]:
    st.subheader("Wilks Score Trajectory")
    st.caption("Bodyweight-normalized strength progression over competitions.")

    filt = wt_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class)).to_pandas()

    if len(filt) > 0:
        line = (
            alt.Chart(filt)
            .mark_line(point=True)
            .encode(
                x=alt.X("cumulative_comps:Q", title="Competition #", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("median_wilks:Q", title="Median Wilks"),
                tooltip=["cumulative_comps", "median_wilks", "wilks_p25", "wilks_p75", "sample_size"],
            )
        )
        band = (
            alt.Chart(filt)
            .mark_area(opacity=0.2)
            .encode(
                x=alt.X("cumulative_comps:Q", axis=alt.Axis(tickMinStep=1)),
                y="wilks_p25:Q",
                y2="wilks_p75:Q",
            )
        )
        st.altair_chart(band + line, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# --- Tab 7: Time to Milestone ---
with tabs[6]:
    st.subheader("Time to Milestone (Survival Analysis)")
    st.caption("Fraction of lifters reaching a total milestone by competition N.")

    milestones_for_sex = conf.SURVIVAL_MILESTONES_M if sex == "M" else conf.SURVIVAL_MILESTONES_F
    selected_milestones = st.multiselect("Milestones (kg)", options=milestones_for_sex, default=milestones_for_sex[:3])

    filt = surv_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class) & pl.col("milestone_kg").is_in(selected_milestones))

    if len(filt) > 0:
        filt_pd = filt.with_columns(pl.col("milestone_kg").cast(pl.Utf8).alias("milestone")).to_pandas()
        chart = (
            alt.Chart(filt_pd)
            .mark_line(interpolate="step-after")
            .encode(
                x=alt.X("cumulative_comps:Q", title="Competition #", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("fraction_reached:Q", title="Fraction Reached", scale=alt.Scale(domain=[0, 1])),
                color=alt.Color("milestone:N", title="Milestone (kg)"),
                tooltip=["cumulative_comps", "milestone", "fraction_reached", "total_lifters"],
            )
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No data for selected filters.")

# --- Tab 8: Lifter Archetypes ---
with tabs[7]:
    st.subheader("Lifter Archetype Clustering")
    st.caption("Lifters clustered into archetypes based on career summary features (KMeans, k=5).")

    # Label centroids by sorting on max_total descending
    sorted_cents = cent_df.sort("max_total", descending=True)
    archetype_names = ["Elite Veteran", "Strong Experienced", "Solid Intermediate", "Developing Regular", "Casual Newcomer"]
    # Ensure we have enough names for clusters
    names = archetype_names[: len(sorted_cents)]
    label_map = dict(zip(sorted_cents["cluster"].to_list(), names))

    arch_labeled = arch_df.with_columns(pl.col("cluster").replace_strict(label_map, default="Other").alias("archetype"))

    filt = arch_labeled.filter(pl.col("sex") == sex).to_pandas()

    if len(filt) > 0:
        scatter = (
            alt.Chart(filt)
            .mark_circle(size=20, opacity=0.4)
            .encode(
                x=alt.X("career_tenure:Q", title="Career Tenure (days)"),
                y=alt.Y("max_total:Q", title="Max Total (kg)"),
                color=alt.Color("archetype:N", title="Archetype"),
                tooltip=["archetype", "total_comps", "career_tenure", "max_total", "max_wilks"],
            )
        )
        st.altair_chart(scatter, use_container_width=True)

        st.markdown("**Archetype Centroids**")
        centroid_labeled = cent_df.with_columns(pl.col("cluster").replace_strict(label_map, default="Other").alias("archetype"))
        st.dataframe(
            centroid_labeled.select("archetype", "total_comps", "career_tenure", "max_total", "max_wilks", "median_progress").to_pandas().round(1),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No data for selected filters.")

# --- Methodology ---
with st.expander("Methodology"):
    st.markdown(
        """
**Data source:** [OpenPowerlifting](https://www.openpowerlifting.org/) — tested, raw, SBD events only.

**Filters applied across all tabs:**
- Only M/F sex categories; unknown weight classes excluded
- Minimum {min_comps} competitions per lifter
- Competition-indexed tabs capped at first {max_comps} competitions

**Career Trajectory:** Tenure bucketed into 7 ranges (0-6mo through 7-10yr). Median total with p25-p75 band.

**Rolling Averages:** 3-competition rolling mean applied per lifter before aggregation.

**Percentile Rank:** Rank of total within sex + weight class, converted to 0-100 scale.

**Lift Ratios:** Squat/bench/deadlift as proportion of total, grouped by experience level.

**Wilks Trajectory:** Bodyweight-normalized score progression. Useful for comparing across weight classes.

**Survival Analysis:** Fraction of lifters whose cumulative best total reaches each milestone by competition N.

**Archetypes:** KMeans (k=5) on career summary features (total comps, tenure, max total, max Wilks, median progress rate). StandardScaler applied before clustering. Labels assigned by sorting centroids on max total.
""".format(min_comps=conf.MIN_COMPETITIONS, max_comps=conf.MAX_CUMULATIVE_COMPS_FOR_INDEXING)
    )
