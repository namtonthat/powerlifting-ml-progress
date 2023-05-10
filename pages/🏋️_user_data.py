import streamlit as st
import time
import numpy as np
import polars as pl
import sys

# Add the parent directory to the path so we can import the steps module
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from steps import conf

st.set_page_config(page_title="User Data", page_icon="📈")

df = pl.read_parquet(f"s3://{conf.bucket_name}/{conf.parquet_file}")

df = df.select(conf.op_cols)
users = df.select("Name").unique().to_list()

st.write("# User Data")
st.write(users)

with st.sidebar:
    st.header("filters")
    st.markdown("""---""")
    st.subheader("👱‍♀️ user")
    user = st.multiselect(
        "Select a user",
        users
    )


# st.markdown("# Plotting Demo")
# st.sidebar.header("Plotting Demo")
# st.write(
#     """This demo illustrates a combination of plotting and animation with
# Streamlit. We're generating a bunch of random numbers in a loop for around
# 5 seconds. Enjoy!"""
# )

# progress_bar = st.sidebar.progress(0)
# status_text = st.sidebar.empty()
# last_rows = np.random.randn(1, 1)
# chart = st.line_chart(last_rows)

# for i in range(1, 101):
#     new_rows = last_rows[-1, :] + np.random.randn(5, 1).cumsum(axis=0)
#     status_text.text("%i%% Complete" % i)
#     chart.add_rows(new_rows)
#     progress_bar.progress(i)
#     last_rows = new_rows
#     time.sleep(0.05)

# progress_bar.empty()

# Streamlit widgets automatically run the script from top to bottom. Since
# this button is not connected to any other logic, it just causes a plain
# rerun.
# st.button("Re-run")