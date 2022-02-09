import streamlit as st
import streamlit.components.v1 as components
from Widget import Market_State_Widget
from StreamlitService import StService as C
from BinanceService import BinanceService
from DbService import DbService
from datetime import datetime


def Home_Page(api_key: str = '2mYr1HH1a9O3LR3ogAoO9SowRD0DwFX9nLZRUnGifIPmGfmznoVemAqRVc8JKMoC',
              api_secret: str = 'LvUCcMAe3FecFpY9KVQMOquD8UpYHJfCY1y9EbzMgbSCwhHBmB4CruhsBUzKYsa5'):
    """ Banner Image Jpg """
    bins = BinanceService(api_key=api_key,
                          api_secret=api_secret)
    dbs = DbService()
    C().Import_pic(name='banner_home.jpg', caption="Track your Binance Investments")

    # Quantify Crypto Heatmap
    with st.container():
        components.html(html=Market_State_Widget(), height=305, width=None, scrolling=False)
    C().text_centered_link(text="Offered by ", url="www.coingecko.com", size=1)

    with st.container():
        st.header(f"Trending Crypto of the Day {datetime.today().date()}")

        fiat = dbs.get_all_value_in_column(name_column='distinct quote_asset', name_table='symbols')

        c1, c2, c3 = st.columns([1, 2, 2])
        with c1:
            limit = st.slider(key='slider', label="#N° of Top/Worst", min_value=5, max_value=15, step=1)
            quote = st.selectbox(label="Select a quote currency", options=fiat)
            df = bins.get_PriceChange24H(quote=quote)
        with c2:
            st.subheader(f"Top {limit} Gainers")
            top = bins.get_top_10(df=df, limit=limit)
            st.dataframe(data=top, height=600)
        with c3:
            st.subheader(f"Top {limit} Losers")
            worst = bins.get_top_10(df=df, limit=limit)
            st.dataframe(data=worst, height=600)


Home_Page()
