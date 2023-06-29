
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


@st.cache_resource(ttl=1)
def get_data():
    db = client["analysis"]

    highways_count = db["highways_count"].find_one({})["count"]
    vehicles_count = db["vehicles_count"].find_one({})["count"]

    return {
        "highways_count": highways_count,
        "vehicle_count": vehicles_count,
    }


db = client["analysis"]
placeholder = st.empty()


def update(last_update):
    data = get_data()

    with placeholder.container():
        st.empty()
        st.title("Dashboard")


        last_update_str = dt.fromtimestamp(last_update).strftime("%d/%m/%Y %H:%M:%S")
        st.markdown(f"**Última atualização**: {last_update_str}")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric("Rodovias", data["highways_count"])
        col2.metric("Veículos", data["vehicle_count"])
        # col3.metric("Veículos acima da velocidade", data["speeding_count"])
        # col4.metric("Veículos em risco de colisão", data["risky_count"])

        # st.markdown("---")

        # col1, col2 = st.columns(2, gap="medium")

        # with col1:
        #     st.subheader("Veículos acima do limite de velocidade")
        #     speeding_df = pd.DataFrame({
        #         "plate": doc["speeding_vehicles"],
        #     })

        #     st.table(speeding_df)

        # with col2:
        #     st.subheader("Veículos em risco de colisão")

        #     risky_df = pd.DataFrame({
        #         "plate": doc["risky_vehicles"],
        #     })

        #     st.table(risky_df)



if __name__ == "__main__":
    cooldown_s = 2
    last_update = time.time()
    update(last_update)

    with db.watch() as stream:
        for change in stream:
            if time.time() - last_update > cooldown_s:
                last_update = time.time()
                update(last_update)
