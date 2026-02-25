import sys
from pathlib import Path

import altair as alt
import polars as pl
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

st.set_page_config(page_title="Strength Progress", page_icon="💪", layout="wide")


@st.cache_data
def load_by_weight_class() -> pl.DataFrame:
    try:
        return pl.read_parquet(conf.describe_by_weight_class_s3_http)
    except Exception:
        return pl.read_parquet(conf.describe_by_weight_class_local)


@st.cache_data
def load_by_experience() -> pl.DataFrame:
    try:
        return pl.read_parquet(conf.describe_by_experience_s3_http)
    except Exception:
        return pl.read_parquet(conf.describe_by_experience_local)


@st.cache_data
def load_by_sex_wc_exp() -> pl.DataFrame:
    try:
        return pl.read_parquet(conf.describe_by_sex_wc_exp_s3_http)
    except Exception:
        return pl.read_parquet(conf.describe_by_sex_wc_exp_local)


wc_df = load_by_weight_class()
exp_df = load_by_experience()
swc_exp_df = load_by_sex_wc_exp()

# --- Sidebar ---
with st.sidebar:
    st.header("Filters")
    st.markdown("---")

    sex = st.radio("Sex", options=["M", "F"], index=0)

    weight_classes = sorted(
        wc_df.filter(pl.col("sex") == sex)["ipf_weight_class"].unique().to_list(), key=lambda x: float(x.replace("+", "")) if x.replace("+", "").replace(".", "").isdigit() else 999
    )
    weight_class = st.selectbox("Weight Class (kg)", options=weight_classes)

# --- Main ---
st.write("# Expected Strength Progress")
st.caption("Median progress rates (kg/year) for tested, raw, SBD powerlifters.")

# Metric cards for selected weight class
row = wc_df.filter((pl.col("sex") == sex) & (pl.col("ipf_weight_class") == weight_class))

if len(row) > 0:
    r = row.row(0, named=True)
    cols = st.columns(5)
    lift_labels = [
        ("Total", "total_progress_median"),
        ("Squat", "squat_progress_median"),
        ("Bench", "bench_progress_median"),
        ("Deadlift", "deadlift_progress_median"),
        ("Wilks", "wilks_progress_median"),
    ]
    for col, (label, key) in zip(cols, lift_labels):
        val = r.get(key)
        unit = "pts/yr" if "wilks" in key else "kg/yr"
        col.metric(label, f"{val:+.1f} {unit}" if val is not None else "N/A")

    st.markdown(f"**Sample size:** {r['sample_size']:,} observations")

st.markdown("---")

# Bar chart: all weight classes for selected sex
st.subheader(f"Total Progress by Weight Class — {sex}")
sex_df = wc_df.filter(pl.col("sex") == sex).to_pandas()

bar_base = alt.Chart(sex_df).encode(
    x=alt.X("ipf_weight_class:N", sort=weight_classes, title="Weight Class (kg)"),
    y=alt.Y("total_progress_median:Q", title="Median Total Progress (kg/yr)"),
    color=alt.condition(
        alt.datum.ipf_weight_class == weight_class,
        alt.value("#ff4b4b"),
        alt.value("#4b8bff"),
    ),
    tooltip=["ipf_weight_class", "total_progress_median", "sample_size"],
)

st.altair_chart(bar_base.mark_bar(), use_container_width=True)

# Grouped bars: per-lift breakdown with p25-p75 error bars
st.subheader(f"Per-Lift Progress — {sex}, {weight_class} kg")

lift_names = ["squat", "bench", "deadlift", "total"]
if len(row) > 0:
    r = row.row(0, named=True)
    records = []
    for lift in lift_names:
        records.append(
            {
                "Lift": lift.title(),
                "p25": r.get(f"{lift}_progress_p25"),
                "Median": r.get(f"{lift}_progress_median"),
                "p75": r.get(f"{lift}_progress_p75"),
            }
        )
    lift_df = pl.DataFrame(records).to_pandas()

    bars = (
        alt.Chart(lift_df)
        .mark_bar()
        .encode(
            x=alt.X("Lift:N", sort=[name.title() for name in lift_names]),
            y=alt.Y("Median:Q", title="Progress (kg/yr)"),
            color=alt.Color("Lift:N", legend=None),
            tooltip=["Lift", "p25", "Median", "p75"],
        )
    )

    error_bars = (
        alt.Chart(lift_df)
        .mark_rule(strokeWidth=2)
        .encode(
            x=alt.X("Lift:N", sort=[name.title() for name in lift_names]),
            y="p25:Q",
            y2="p75:Q",
        )
    )

    st.altair_chart(bars + error_bars, use_container_width=True)
else:
    st.info("No data available for the selected weight class.")

# Progress by experience level
st.markdown("---")
st.subheader("Progress by Experience Level")

exp_order = ["novice (1-3)", "intermediate (4-8)", "experienced (9-15)", "veteran (16+)"]
exp_pd = exp_df.to_pandas()

exp_chart = (
    alt.Chart(exp_pd)
    .mark_bar()
    .encode(
        x=alt.X("experience_level:N", sort=exp_order, title="Experience Level"),
        y=alt.Y("total_progress_median:Q", title="Median Total Progress (kg/yr)"),
        color=alt.Color("experience_level:N", sort=exp_order, legend=None),
        tooltip=["experience_level", "total_progress_median", "sample_size"],
    )
)

st.altair_chart(exp_chart, use_container_width=True)

# Methodology
with st.expander("Methodology"):
    st.markdown(
        """
**Data source:** [OpenPowerlifting](https://www.openpowerlifting.org/) — tested, raw, SBD events only.

**Progress rate** = (current lift - previous lift) / time between competitions (in years).

**Filters applied:**
- Minimum {min_comps} competitions per lifter
- Competitions less than {min_days} days apart are excluded (avoids inflated rates from back-to-back meets)
- Only M/F sex categories; unknown weight classes excluded

**Weight classes** follow standard IPF boundaries.

**Experience levels** are based on cumulative competition count:
- Novice: 1-3 comps
- Intermediate: 4-8 comps
- Experienced: 9-15 comps
- Veteran: 16+ comps
""".format(min_comps=conf.MIN_COMPETITIONS, min_days=conf.MIN_DAYS_BETWEEN_COMPS)
    )
