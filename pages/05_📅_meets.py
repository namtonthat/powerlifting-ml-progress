import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.request import urlopen

import polars as pl
import pydeck as pdk
import streamlit as st
from pydeck.data_utils import compute_view

sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

st.set_page_config(page_title="Meets", page_icon="📅", layout="wide")

REGIONS = {"Australia": "au", "USA": "us", "Europe": "europe", "Mexico": "mx", "Rest of world": "other"}
FEEDS = {"All meets": "all", "Australia": "au", "USA": "us", "Europe": "europe", "Mexico": "mx", "Open for sign-ups": "open"}
NOT_OPEN = ("SOLD OUT", "CLOSED", "CANCELLED")
# Fixed status palette; the icon labels each colour so it never carries meaning alone
STATUS = {
    "open": ("🟢", "Open for sign-ups", [12, 163, 12]),
    "full": ("🟠", "Sold out or closed", [236, 131, 90]),
    "cancelled": ("🔴", "Cancelled", [208, 59, 59]),
    "unknown": ("⚪", "Sign-up status not known", [137, 135, 129]),
}
EQUIPMENT = ["Raw", "Wraps", "Single-ply", "Multi-ply", "Equipped"]
EVENTS = ["Full power", "Bench only", "Deadlift only", "Squat only", "Push/pull"]


@st.cache_data(ttl="1h")
def load_meets() -> pl.DataFrame:
    try:
        # Plain HTTPS: polars turns S3 URLs into S3 API calls that fail without AWS credentials, even on this public bucket
        return pl.read_parquet(urlopen(conf.meets_s3_http, timeout=30).read())
    except Exception:
        return pl.read_parquet(conf.meets_local)


all_meets = load_meets()
# By end date, so multi-day meets already under way stay listed
meets = all_meets.filter(pl.col("end") >= date.today())
# "N entered" is a roster count from LiftingCast, not proof that sign-ups are open
is_open = pl.col("status").str.starts_with("OPEN")
is_shut = pl.any_horizontal([pl.col("status").str.starts_with(s) for s in NOT_OPEN])
kind = pl.when(is_open).then(pl.lit("open")).when(pl.col("status") == "CANCELLED").then(pl.lit("cancelled")).when(is_shut).then(pl.lit("full")).otherwise(pl.lit("unknown"))
new_cutoff = date.today() - timedelta(days=7)


def values(df: pl.DataFrame, col: str) -> list[str]:
    return sorted(set(df[col].to_list()) - {"", None})


# --- Sidebar ---
# Location narrows step by step, and the federation list only offers federations running meets there
with st.sidebar:
    st.header("Filters")
    st.markdown("---")

    regions = st.multiselect("Region", options=list(REGIONS), placeholder="All regions")
    place = meets.filter(pl.col("region").is_in([REGIONS[r] for r in regions])) if regions else meets
    countries = st.multiselect("Country", options=values(place, "country"), placeholder="All countries")
    place = place.filter(pl.col("country").is_in(countries)) if countries else place
    states = st.multiselect("State", options=values(place, "state"), placeholder="All states")
    place = place.filter(pl.col("state").is_in(states)) if states else place
    federations = sorted(set(place["federation"].str.split(", ").explode().drop_nulls().to_list()) - {""})
    picked_feds = st.multiselect("Federation", options=federations, placeholder="All federations")
    st.markdown("---")
    testing = st.multiselect("Drug testing", options=["Tested", "Untested"], placeholder="Tested or untested")
    equipment = st.multiselect("Equipment", options=EQUIPMENT, placeholder="Any equipment")
    events = st.multiselect("Events", options=EVENTS, placeholder="Any events")
    include_unlisted = st.checkbox("Include meets that don't say", value=True, help="Many listings don't state testing, equipment or events.")
    st.markdown("---")
    signups = st.radio("Sign-ups", options=["Any", "Open", "Hide sold out, closed and cancelled"])
    dates = st.date_input("Meet dates", value=(date.today(), meets["start"].max()), min_value=date.today())
    new_only = st.checkbox("Listed in the last 7 days")
    search = st.text_input("Search", placeholder="Meet, city or federation")
    sort = st.radio("Sort by", options=["Soonest", "Recently listed"], horizontal=True)

view = place
if picked_feds:
    view = view.filter(pl.col("federation").str.split(", ").list.eval(pl.element().is_in(picked_feds)).list.any())
if testing:
    match = pl.col("testing").is_in([*testing, "Both"])
    view = view.filter(match | (pl.col("testing") == "Unknown") if include_unlisted else match)
for col, picked in (("equipment", equipment), ("events", events)):
    if picked:
        match = pl.col(col).list.eval(pl.element().is_in(picked)).list.any()
        view = view.filter(match | (pl.col(col).list.len() == 0) if include_unlisted else match)
if signups == "Open":
    view = view.filter(is_open)
elif signups != "Any":
    view = view.filter(~is_shut.fill_null(False))
if len(dates) == 2:
    view = view.filter(pl.col("start").is_between(*dates))
if new_only:
    view = view.filter(pl.col("added") >= new_cutoff)
if search:
    text = pl.concat_str(["name", "location", "state", "country", "federation"], separator=" ").str.to_lowercase()
    view = view.filter(text.str.contains(search.lower(), literal=True))
view = view.sort(["added", "start"], descending=[True, False]) if sort == "Recently listed" else view.sort("start")

# --- Main ---
st.write("# Upcoming Meets")
st.caption(f"Powerlifting meets from powerlifting.com and FEMEPO, with sign-up status where the meet's sign-up page shows it. Updated {all_meets['pulled_at'].max():%d %b %Y %H:%M} UTC.")

col1, col2, col3 = st.columns(3)
col1.metric("Meets shown", f"{view.height:,}")
col2.metric("Open for sign-ups", f"{view.filter(is_open).height:,}")
col3.metric("Listed this week", f"{view.filter(pl.col('added') >= new_cutoff).height:,}")

list_tab, map_tab = st.tabs(["📋 List", "🗺️ Map"])

with list_tab:
    icon = kind.replace_strict({k: f"{v[0]} " for k, v in STATUS.items() if k != "unknown"}, default="")
    table = view.select(
        pl.col("start").alias("Date"),
        pl.col("name").alias("Meet"),
        (icon + pl.col("status").fill_null("")).alias("Spots"),
        # Links come from third-party pages; render only http(s) ones, else the listing page.
        pl.when(pl.col("link").str.contains(r"(?i)^https?://")).then(pl.col("link")).otherwise(pl.col("listing")).alias("Link"),
        pl.col("testing").replace("Unknown", "").alias("Testing"),
        pl.col("equipment").list.join(", ").alias("Equipment"),
        pl.col("events").list.join(", ").alias("Events"),
        pl.col("location").alias("Location"),
        pl.col("state").alias("State"),
        pl.col("country").alias("Country"),
        pl.col("federation").alias("Federation"),
        pl.col("added").alias("Listed"),
    )
    st.dataframe(
        table,
        hide_index=True,
        width="stretch",
        height=min(700, 35 * (table.height + 1) + 3),
        column_config={
            "Date": st.column_config.DateColumn(format="ddd D MMM YYYY", width=125),
            "Meet": st.column_config.TextColumn(width=300),
            "Spots": st.column_config.TextColumn(width=130, help="Entry cap or entries so far, where the sign-up page shows them"),
            "Link": st.column_config.LinkColumn(display_text="Sign up ↗", width=85),
            "Testing": st.column_config.TextColumn(width=80),
            "Equipment": st.column_config.TextColumn(width=150),
            "Events": st.column_config.TextColumn(width=170),
            "Location": st.column_config.TextColumn(width=200),
            "State": st.column_config.TextColumn(width=130),
            "Country": st.column_config.TextColumn(width=110),
            "Federation": st.column_config.TextColumn(width=200),
            "Listed": st.column_config.DateColumn(format="D MMM YYYY", width=100, help="When the meet appeared in the source listing"),
        },
    )

with map_tab:
    rows = (
        view.drop_nulls(["lat", "lon"])
        .with_columns(kind.alias("kind"), pl.col("start").dt.strftime("%a %-d %b %Y").alias("when"), pl.col("status").fill_null("Sign-up status not known").alias("spots"))
        .select("name", "when", "spots", "testing", "events", "location", "state", "country", "lat", "lon", "kind")
        .to_dicts()
    )
    for r in rows:
        r["color"] = STATUS[r["kind"]][2]
        r["place"] = ", ".join(p for p in (r["location"], r["state"], r["country"]) if p)
        r["details"] = " · ".join(p for p in (r["testing"] if r["testing"] != "Unknown" else "", ", ".join(r["events"])) if p)

    counts = {k: sum(r["kind"] == k for r in rows) for k in STATUS}
    legend = " · ".join(f"{icon} {label} ({counts[k]:,})" for k, (icon, label, _) in STATUS.items() if counts[k])
    st.caption(f"{legend} · {view.height - len(rows):,} without a known location not shown")

    if rows:
        dark = st.context.theme.type == "dark"
        ring = [26, 26, 25] if dark else [252, 252, 251]

        def dots(data: list[dict], radius: int) -> pdk.Layer:
            return pdk.Layer(
                "ScatterplotLayer",
                data=data,
                get_position="[lon, lat]",
                get_fill_color="color",
                get_line_color=ring,
                stroked=True,
                # Pixel units alone still scaled with zoom here; the min/max clamps pin the size
                line_width_min_pixels=2,
                line_width_max_pixels=2,
                radius_min_pixels=radius,
                radius_max_pixels=radius,
                pickable=True,
            )

        # Unknown-status dots underneath so meets with a known status stay visible
        layers = [dots([r for r in rows if r["kind"] == "unknown"], 4), dots([r for r in rows if r["kind"] != "unknown"], 6)]
        # Within a chosen place, fit to the central 90% so a mis-geocoded venue doesn't zoom out to the whole world
        view_state = compute_view([[r["lon"], r["lat"]] for r in rows], view_proportion=0.9 if regions or countries or states else 1)
        view_state.zoom = max(view_state.zoom - 0.5, 0)  # compute_view fits tightly and clips dots at the edges
        st.pydeck_chart(
            pdk.Deck(
                layers=layers,
                initial_view_state=view_state,
                map_provider="carto",
                map_style=pdk.map_styles.CARTO_DARK if dark else pdk.map_styles.CARTO_LIGHT,
                # Plain-text tooltip: meet names come from third-party pages
                tooltip={"text": "{name}\n{when}\n{spots}\n{details}\n{place}"},
            ),
            height=600,
        )

with st.expander("Subscribe in your calendar"):
    st.caption("Add one of these URLs as a calendar subscription (Google Calendar: Other calendars → From URL).")
    for label, feed in FEEDS.items():
        url = conf.create_output_file_path(conf.OutputPathType.LANDING, conf.FileLocation.S3, as_http=True, file_name=f"meets-{feed}.ics")
        st.markdown(f"**{label}**: `{url}`")
