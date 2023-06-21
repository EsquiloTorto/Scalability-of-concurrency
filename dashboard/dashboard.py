import pymongo
import streamlit as st
import pandas as pd
import time
from connection import get_mongodb_connection

st.set_page_config(layout="wide", page_title="Highway Traffic Dashboard", page_icon="🚗")


@st.cache_resource
def init_connection():
    return get_mongodb_connection()


client = init_connection()


def get_collection():
    simulator_db = client["simulator"]
    collection = simulator_db["positions"]
    return collection


@st.cache_resource(ttl=1)
def get_data():
    collection = get_collection()
    df = pd.DataFrame(list(collection.find({})))
    df["_id"] = df["_id"].astype(str)
    return df


def update_numbers_view():
    df = get_data()

    with st.container():
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(label="Total Vehicles", value=df.shape[0])
        with col2:
            st.metric(label="Total Highways", value=df["highway"].nunique())
        with col3:
            st.metric(label="Above speed limit", value=24)
        with col4:
            st.metric(label="With collision risk", value=10)


def update_tables_view():
    df = get_data()

    with st.container():
        col1, col2 = st.columns(2, gap="medium")
        with col1:
            st.subheader("Vehicles above speed limit")
            st.dataframe(df)
        with col2:
            st.subheader("Vehicles with collision risk")
            st.dataframe(df)


collection = get_collection()
placeholder = st.empty()

def update():
    with placeholder.container():
        st.empty()
        st.title("Highway Traffic Dashboard")

        update_numbers_view()
        update_tables_view()


if __name__ == "__main__":
    update()
    cooldown_s = 1
    last_update = time.time()

    with collection.watch() as stream:
        for change in stream:
            if time.time() - last_update > cooldown_s:
                update()
                last_update = time.time()
