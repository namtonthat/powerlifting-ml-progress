import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="Machine Learning",
    page_icon="🏋️‍♂️",
)

st.write("# Machine Learning 🤖")

etl_markdown = Path("notebooks/etl.md").read_text()
# etl_html = open(Path("notebooks/etl.html"), "r", encoding="utf-8").read()

st.markdown(etl_markdown)
