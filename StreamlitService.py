import streamlit as st
from streamlit import container
from PIL import Image


class StService:

    def __init__(self):
        self.container = container

    def Import_pic(self, name: str, caption=None):
        # insert name of jpeg/png as 'banner_home.jpg'
        with self.container():
            img = Image.open(name)
            st.image(img, caption=caption)

    def text_centered_link(self, text: str, url: str, size: int):
        with self.container():
            c1, c2, c3 = st.columns([3, 1.5, 3])
            with c2:
                st.markdown(
                    f""" <font size="{size}">{text}<a href="https://{url}/">{url.replace("www.", "")}</a></font><br />""",
                    unsafe_allow_html=True)
