
import streamlit as st
import pandas as pd
import time
from datetime import datetime as dt
from connection import get_mongodb_connection

st.set_page_config(
    layout="wide",
    page_title="Dashboard",
    page_icon="🚗",
)

@st.cache_resource
def init_connection():
    return get_mongodb_connection()


client = init_connection()


def get_collection():
    simulator_db = client["analysis"]
    collection = simulator_db["aggregations"]
    return collection


@st.cache_resource(ttl=1)
def get_data():
    collection = get_collection()
    df = pd.DataFrame(list(collection.find({})))
    if len(df) > 0:
        df["_id"] = df["_id"].astype(str)

    return df


collection = get_collection()
placeholder = st.empty()


def update(last_update):
    df = get_data()
    if len(df) == 0: return

    doc = df.iloc[-1]

    with placeholder.container():
        st.empty()
        st.title("Dashboard")


        last_update_str = dt.fromtimestamp(last_update).strftime("%d/%m/%Y %H:%M:%S")
        st.markdown(f"**Última atualização**: {last_update_str}")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("Rodovias", doc["total_highways"])
        col2.metric("Veículos", doc["vehicle_count"])
        col3.metric("Veículos acima da velocidade", doc["speeding_count"])
        col4.metric("Veículos em risco de colisão", doc["risky_count"])

        # st.markdown("---")

        # col1, col2 = st.columns(2, gap="medium")

        # with col1:
        #     st.subheader("Veículos acima do limite de velocidade")
        #     st.dataframe(df)
        # with col2:
        #     st.subheader("Veículos em risco de colisão")
        #     st.map(df)


if __name__ == "__main__":
    cooldown_s = 2
    last_update = time.time()
    update(last_update)

    with collection.watch() as stream:
        for change in stream:
            if time.time() - last_update > cooldown_s:
                last_update = time.time()
                update(last_update)
