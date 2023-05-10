import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="About",
    page_icon="👋",
)

st.write("# Welcome to `powerlifting-ml-progress`! 👋")

readme_markdown = Path("README.md").read_text()

st.markdown(
    readme_markdown
)