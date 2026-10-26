import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.request import urlopen

import polars as pl
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

st.set_page_config(page_title="Meets", page_icon="📅", layout="wide")

REGIONS = {"Australia": "au", "USA": "us", "Europe": "europe", "Mexico": "mx", "Rest of world": "other"}
FEEDS = {"All meets": "all", "Australia": "au", "USA": "us", "Europe": "europe", "Mexico": "mx", "Open for sign-ups": "open"}
NOT_OPEN = ("SOLD OUT", "CLOSED", "CANCELLED")


@st.cache_data(ttl="1h")
def load_meets() -> pl.DataFrame:
    try:
        # Plain HTTPS: polars turns S3 URLs into S3 API calls that fail without AWS credentials, even on this public bucket
        return pl.read_parquet(urlopen(conf.meets_s3_http, timeout=30).read())
    except Exception:
        return pl.read_parquet(conf.meets_local)


all_meets = load_meets()
meets = all_meets.filter(pl.col("start") >= date.today())
is_open = pl.col("status").str.starts_with("OPEN") | pl.col("status").str.ends_with("entered")
is_shut = pl.any_horizontal([pl.col("status").str.starts_with(s) for s in NOT_OPEN])
new_cutoff = date.today() - timedelta(days=7)
federations = sorted(set(meets["federation"].str.split(", ").explode().drop_nulls().to_list()) - {""})

# --- Sidebar ---
with st.sidebar:
    st.header("Filters")
    st.markdown("---")

    regions = st.multiselect("Region", options=list(REGIONS), placeholder="All regions")
    signups = st.radio("Sign-ups", options=["Any", "Open", "Hide sold out, closed and cancelled"])
    picked_feds = st.multiselect("Federation", options=federations, placeholder="All federations")
    dates = st.date_input("Meet dates", value=(date.today(), meets["start"].max()), min_value=date.today())
    new_only = st.checkbox("Added in the last 7 days")
    search = st.text_input("Search", placeholder="Meet, city or federation")
    sort = st.radio("Sort by", options=["Soonest", "Recently added"], horizontal=True)

view = meets
if regions:
    view = view.filter(pl.col("region").is_in([REGIONS[r] for r in regions]))
if picked_feds:
    view = view.filter(pl.col("federation").str.split(", ").list.eval(pl.element().is_in(picked_feds)).list.any())
if signups == "Open":
    view = view.filter(is_open)
elif signups != "Any":
    view = view.filter(~is_shut.fill_null(False))
if len(dates) == 2:
    view = view.filter(pl.col("start").is_between(*dates))
if new_only:
    view = view.filter(pl.col("added") >= new_cutoff)
if search:
    text = pl.concat_str(["name", "location", "federation"], separator=" ").str.to_lowercase()
    view = view.filter(text.str.contains(search.lower(), literal=True))
view = view.sort(["added", "start"], descending=[True, False]) if sort == "Recently added" else view.sort("start")

# --- Main ---
st.write("# Upcoming Meets")
st.caption(f"Powerlifting meets from powerlifting.com and FEMEPO, with sign-up status where the meet's sign-up page shows it. Updated {all_meets['pulled_at'].max():%d %b %Y %H:%M} UTC.")

col1, col2, col3 = st.columns(3)
col1.metric("Meets shown", f"{view.height:,}")
col2.metric("Open for sign-ups", f"{view.filter(is_open).height:,}")
col3.metric("Added this week", f"{view.filter(pl.col('added') >= new_cutoff).height:,}")

status_icon = pl.when(is_open).then(pl.lit("🟢 ")).when(pl.col("status") == "CANCELLED").then(pl.lit("🔴 ")).when(is_shut).then(pl.lit("⚫ ")).otherwise(pl.lit(""))
table = view.select(
    pl.col("start").alias("Date"),
    pl.col("name").alias("Meet"),
    (status_icon + pl.col("status").fill_null("")).alias("Sign-ups"),
    # Links come from third-party pages; render only http(s) ones, else the listing page.
    pl.when(pl.col("link").str.contains(r"(?i)^https?://")).then(pl.col("link")).otherwise(pl.col("listing")).alias("Link"),
    pl.col("location").alias("Location"),
    pl.col("federation").alias("Federation"),
    pl.col("added").alias("Added"),
)
st.dataframe(
    table,
    hide_index=True,
    width="stretch",
    height=min(700, 35 * (table.height + 1) + 3),
    column_config={
        "Date": st.column_config.DateColumn(format="ddd D MMM YYYY", width=125),
        "Meet": st.column_config.TextColumn(width=340),
        "Sign-ups": st.column_config.TextColumn(width=130),
        "Link": st.column_config.LinkColumn(display_text="Sign up ↗", width=85),
        "Location": st.column_config.TextColumn(width=240),
        "Federation": st.column_config.TextColumn(width=200),
        "Added": st.column_config.DateColumn(format="D MMM YYYY", width=100),
    },
)

with st.expander("Subscribe in your calendar"):
    st.caption("Add one of these URLs as a calendar subscription (Google Calendar: Other calendars → From URL).")
    for label, feed in FEEDS.items():
        url = conf.create_output_file_path(conf.OutputPathType.LANDING, conf.FileLocation.S3, as_http=True, file_name=f"meets-{feed}.ics")
        st.markdown(f"**{label}**: `{url}`")
