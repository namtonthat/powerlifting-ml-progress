import streamlit as st
import numpy as np
import polars as pl
import sys
import altair as alt

# Add the parent directory to the path so we can import the steps module
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

s3_file_path = f"https://{conf.bucket_name}.s3.ap-southeast-2.amazonaws.com/{conf.parquet_file}"

@st.cache_data(persist=True)
def load_data(url):
    df = pl.read_parquet(s3_file_path)
    df = df.select(conf.op_cols)

    return df

st.set_page_config(page_title="User Data", page_icon="🏋️")
df = load_data(url=s3_file_path)


users = df["Name"].unique().to_list()
countries = df["MeetCountry"].unique().to_list()

with st.sidebar:
    st.header("filters")
    st.markdown("""---""")
    # st.subheader("🌎 country")
    # country = st.multiselect(
    #     label="Select a country",
    #     options=countries,
    #     default="Australia"
    # )

    st.subheader("👱‍♀️ user")
    user = st.multiselect(
        label="Select a user",
        options=users,
        default="Taylor Atwood"
    )


st.write("# Single Lifter Data")

user_df = df.filter(
    pl.col("Name").is_in(user)
).sort(by="Date").drop_nulls()

# related events
events = user_df["Event"].unique().to_list()
with st.sidebar:
    st.subheader("🏋️ event")
    event = st.multiselect(
        label="Select an event",
        options=events,
        default=events
    )


wilks_df = user_df.select(
    pl.col(["Date", "Name", "Wilks"])
).to_pandas()

wilks_chart = alt.Chart(wilks_df).mark_line(point=True).encode(
    x="Date:T",
    y="Wilks:Q",
    color=alt.Color("Name:N", legend=alt.Legend(title="Lifter")),
).properties(
    width = 1200,
    height = 800
)

st.altair_chart(wilks_chart, use_container_width=True)

with st.expander("Show breakdown"):
    st.write(wilks_df)

# unpivot data for graphing
sbd_df = user_df.melt(
    id_vars= ["Date", "Name", "Event"],
    variable_name="Lift",
    value_vars=["Best3SquatKg", "Best3BenchKg", "Best3DeadliftKg"],
    value_name="Weight"
).to_pandas()

sbd_chart = alt.Chart(sbd_df).mark_line(point=True).encode(
    x="Date:T",
    y="Weight:Q",
    color=alt.Color("Lift:N", legend=alt.Legend(title="SBD")),
    tooltip=["Name", "Date", "Event"]
).properties(
    width=800,
    height=600
)

st.altair_chart(sbd_chart, use_container_width=True)
with st.expander("Show breakdown"):
    st.write(sbd_df)