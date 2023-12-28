import streamlit as st
import polars as pl
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import altair as alt

# Add the parent directory to the path so we can import the steps module
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf  # noqa: E402

s3_file_path = (
    f"https://{conf.bucket_name}.s3.ap-southeast-2.amazonaws.com/{conf.parquet_file}"
)


@st.cache_data
def load_data(url):
    df = pl.read_parquet(s3_file_path)
    df = df.select(conf.op_cols).unique()

    return df


st.set_page_config(page_title="User Data", page_icon="🏋️")
df = load_data(url=s3_file_path)


@st.cache_data
def calculate_unique_fields(_df):
    unique_fields = {}
    for col in _df.columns:
        unique_values = _df[col].unique().to_list()
        unique_values = [value for value in unique_values if value is not None]
        unique_fields[col] = sorted(unique_values)
    return unique_fields


filter_mapping = calculate_unique_fields(_df=df)
all_users = filter_mapping.get("Name")

# dates
all_dates = filter_mapping.get("Date")
all_dates = [datetime.strptime(date, "%Y-%m-%d") for date in all_dates]
earliest_date = all_dates[0]
latest_date = all_dates[-1]

with st.sidebar:
    st.header("filters")
    st.markdown("""---""")
    st.subheader("📅 date")
    date = st.slider(
        label="Select a date range",
        min_value=earliest_date,
        max_value=latest_date,
        value=latest_date - relativedelta(years=10),
    )

    st.subheader("👱‍♀️ lifter")
    user = st.multiselect(
        label="Select a lifter", options=all_users, default="Taylor Atwood"
    )


st.write("# Single Lifter Data")

user_df = (
    df.filter(pl.col("Name").is_in(user) & pl.col("Date").gt(date))
    .sort(by="Date")
    .drop_nulls()
)

# related events
events = filter_mapping.get("Event")
with st.sidebar:
    st.subheader("🏋️ event")
    event = st.multiselect(label="Select an event", options=events, default="SBD")


wilks_df = user_df.select(pl.col(["Date", "Name", "Wilks"])).to_pandas()

wilks_chart = (
    alt.Chart(wilks_df)
    .mark_line(point=True)
    .encode(
        x="Date:T",
        y="Wilks:Q",
        color=alt.Color("Name:N", legend=alt.Legend(title="Lifter")),
    )
    .properties(width=1200, height=800)
)

st.altair_chart(wilks_chart, use_container_width=True)

with st.expander("Show breakdown"):
    st.write(wilks_df)

# unpivot data for graphing
sbd_df = user_df.melt(
    id_vars=["Date", "Name", "Event"],
    variable_name="Lift",
    value_vars=["Best3SquatKg", "Best3BenchKg", "Best3DeadliftKg"],
    value_name="Weight",
).to_pandas()

sbd_chart = (
    alt.Chart(sbd_df)
    .mark_line(point=True)
    .encode(
        x="Date:T",
        y="Weight:Q",
        color=alt.Color("Lift:N", legend=alt.Legend(title="SBD")),
        tooltip=["Name", "Date", "Event"],
    )
    .properties(width=800, height=600)
)

st.altair_chart(sbd_chart, use_container_width=True)
with st.expander("Show breakdown"):
    st.write(sbd_df)
