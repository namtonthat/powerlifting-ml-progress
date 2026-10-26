import html
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.request import urlopen

import folium
import polars as pl
import streamlit as st
from streamlit_folium import st_folium

sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

st.set_page_config(page_title="Meets", page_icon="📅", layout="wide")

FEEDS = {"All meets": "all", "Australia": "au", "USA": "us", "Europe": "europe", "Mexico": "mx", "Open for sign-ups": "open"}
NOT_OPEN = ("SOLD OUT", "CLOSED", "CANCELLED")
# Fixed status palette; the icon labels each colour so it never carries meaning alone
STATUS = {
    "open": ("🟢", "Open for sign-ups", "#0ca30c"),
    "full": ("🟠", "Sold out or closed", "#ec835a"),
    "cancelled": ("🔴", "Cancelled", "#d03b3b"),
    "unknown": ("⚪", "Sign-up status not known", "#898781"),
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

    continents = st.multiselect("Continent", options=values(meets, "continent"), placeholder="All continents")
    place = meets.filter(pl.col("continent").is_in(continents)) if continents else meets
    countries = st.multiselect("Country", options=values(place, "country"), placeholder="All countries")
    place = place.filter(pl.col("country").is_in(countries)) if countries else place
    states = st.multiselect("State", options=values(place, "state"), placeholder="All states")
    place = place.filter(pl.col("state").is_in(states)) if states else place
    federations = sorted(set(place["federation"].str.split(", ").explode().drop_nulls().to_list()) - {""})
    picked_feds = st.multiselect("Federation", options=federations, placeholder="All federations")
    st.markdown("---")
    testing = st.segmented_control("Drug testing", options=["Tested", "Untested"], help="Leave both off for any. Meets running both count as either.")
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
    match = pl.col("testing").is_in([testing, "Both"])
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

metrics = st.container()
# Map on top, list below; both rendered map-first so the list can use the map's bounds
map_box, list_box = st.container(), st.container()

# The map draws every meet that passes the sidebar filters; panning or zooming only narrows the list.
# Its markers must not depend on the view, or each move would redraw the map and reset it.
with map_box:
    rows = (
        view.drop_nulls(["lat", "lon"])
        .with_columns(kind.alias("kind"), pl.col("start").dt.strftime("%a %-d %b %Y").alias("when"), pl.col("status").fill_null("Sign-up status not known").alias("spots"))
        .select("name", "when", "spots", "testing", "events", "location", "state", "country", "lat", "lon", "kind")
        .to_dicts()
    )
    counts = {k: sum(r["kind"] == k for r in rows) for k in STATUS}
    st.caption(" · ".join(f"{icon} {label} ({counts[k]:,})" for k, (icon, label, _) in STATUS.items() if counts[k]))

    dark = st.context.theme.type == "dark"
    # Esri's grey canvas basemaps need no key (Carto's raster tiles now return an "API key required" image)
    canvas = "World_Dark_Gray_Base" if dark else "World_Light_Gray_Base"
    fmap = folium.Map(
        location=(20, 0),
        zoom_start=2,
        tiles=f"https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/{canvas}/MapServer/tile/{{z}}/{{y}}/{{x}}",
        attr="Tiles &copy; Esri &mdash; Esri, HERE, Garmin, &copy; OpenStreetMap contributors",
        max_zoom=16,
        prefer_canvas=True,
        world_copy_jump=True,
    )
    # Unknown-status dots first, so meets with a known status draw on top
    for r in sorted(rows, key=lambda r: r["kind"] != "unknown"):
        details = " · ".join(p for p in (r["testing"] if r["testing"] != "Unknown" else "", ", ".join(r["events"])) if p)
        place = ", ".join(p for p in (r["location"], r["state"], r["country"]) if p)
        # Tooltips render HTML and meet names come from third-party pages, so escape everything
        tip = "<br>".join(html.escape(t) for t in (r["name"], r["when"], r["spots"], details, place) if t)
        folium.CircleMarker(
            location=(r["lat"], r["lon"]),
            radius=4 if r["kind"] == "unknown" else 6,
            color="#1a1a19" if dark else "#fcfcfb",
            weight=2,
            fill=True,
            fill_color=STATUS[r["kind"]][2],
            fill_opacity=1,
            tooltip=tip,
        ).add_to(fmap)
    if rows:
        # Within a chosen place, fit to the central 90% so a mis-geocoded venue doesn't zoom out to the whole world
        lo, hi = (0.05, 0.95) if continents or countries or states else (0.0, 1.0)
        located = view.drop_nulls(["lat", "lon"])
        fmap.fit_bounds([[located["lat"].quantile(lo), located["lon"].quantile(lo)], [located["lat"].quantile(hi), located["lon"].quantile(hi)]])
    map_state = st_folium(fmap, key="meets_map", height=450, use_container_width=True, returned_objects=["bounds"])

in_view = view
bounds = (map_state or {}).get("bounds") or {}
sw, ne = bounds.get("_southWest") or {}, bounds.get("_northEast") or {}
if sw.get("lat") is not None and ne.get("lat") is not None:
    in_box = pl.col("lat").is_between(sw["lat"], ne["lat"])
    width = ne["lng"] - sw["lng"]
    if width < 360:
        # Leaflet longitudes run past ±180 when panning across the date line; compare them modulo 360
        in_box &= (((pl.col("lon") - sw["lng"]) % 360 + 360) % 360) <= width
    in_view = view.filter(in_box.fill_null(False))

with metrics:
    col1, col2, col3 = st.columns(3)
    col1.metric("Meets in view", f"{in_view.height:,}", help="Pan or zoom the map to narrow the list")
    col2.metric("Open for sign-ups", f"{in_view.filter(is_open).height:,}")
    col3.metric("Listed this week", f"{in_view.filter(pl.col('added') >= new_cutoff).height:,}")

with list_box:
    if outside := view.height - in_view.height:
        st.caption(f"{outside:,} meets match the filters but are outside the map view or have no known location.")
    icon = kind.replace_strict({k: f"{v[0]} " for k, v in STATUS.items() if k != "unknown"}, default="")
    table = in_view.select(
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
        height=min(650, 35 * (table.height + 1) + 3),
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

with st.expander("Subscribe in your calendar"):
    st.caption("Add one of these URLs as a calendar subscription (Google Calendar: Other calendars → From URL).")
    for label, feed in FEEDS.items():
        url = conf.create_output_file_path(conf.OutputPathType.LANDING, conf.FileLocation.S3, as_http=True, file_name=f"meets-{feed}.ics")
        st.markdown(f"**{label}**: `{url}`")
