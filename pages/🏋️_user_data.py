import streamlit as st
import numpy as np
import polars as pl
import sys
import altair as alt

# Add the parent directory to the path so we can import the steps module
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

st.set_page_config(page_title="User Data", page_icon="📈")

df = pl.read_parquet(f"s3://{conf.bucket_name}/{conf.parquet_file}")

df = df.select(conf.op_cols)
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

user_df = df.select(
    pl.col(["Date", "Name", "TotalKg", "Event", "Best3SquatKg", "Best3BenchKg", "Best3DeadliftKg"])
).filter(
    pl.col("Name").is_in(user)
).sort(by="Date").drop_nulls()


# unpivot data for graphing
user_df = user_df.melt(
    id_vars= ["Date", "Name", "Event"],
    variable_name="Lift",
    value_vars=["Best3SquatKg", "Best3BenchKg", "Best3DeadliftKg"],
    value_name="Weight"
).to_pandas()

st.write(user_df)

user_chart = alt.Chart(user_df).mark_line(point=True).encode(
    x="Date:T",
    y="Weight:Q",
    color=alt.Color("Lift:N", legend=alt.Legend(title="SBD")),
    tooltip=["Name", "Date", "Event"]
).properties(
    width=800,
    height=600
)

st.altair_chart(user_chart, use_container_width=True)
