import pymongo
import streamlit as st
import pandas as pd
import time
from connection import get_mongodb_connection

# Just sets the title and icon of the page
st.set_page_config(layout="wide", page_title="Highway Traffic Dashboard", page_icon="🚗")

# Runs once, when the app is loaded
@st.cache_resource
def init_connection():
    return get_mongodb_connection()

# Runs everytime the page is refreshed
client = init_connection()
placeholder = st.empty()

def get_data():
    # global df
    simulator_db = client["simulator"]
    collection = simulator_db["positions"]
    # cursor = collection.find({})
    # df = pd.DataFrame(list(cursor))
    return collection

# loads the data
data = get_data()

# gets initial dataframe
df = pd.DataFrame(list(data.find({})))
df["_id"] = df["_id"].astype(str) # convert _id to string

def update_data():
    global df
    change_df = pd.DataFrame([insert_change["fullDocument"]])
    change_df["_id"] = change_df["_id"].astype(str)

    df = pd.concat([df, change_df])

def update_numbers_view():
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
    with st.container():
        col1, col2 = st.columns(2, gap='medium')
        with col1:
            st.subheader("Vehicles above speed limit")
            st.dataframe(df)
        with col2:
            st.subheader("Vehicles with collision risk")
            st.dataframe(df)

if __name__ == "__main__":
    # Everytime data changes, will rerun this.
    with data.watch() as stream:
        # Cooldown is to prevent the page from refreshing too often (but it doesn't throw data away)
        cooldown = 5
        start_time = time.time()

        # For each change in the stream
        for insert_change in stream:
            # Create a container to hold the data
            with placeholder.container():
                # Check if it is still in cooldown and just update the dataframe but don't refresh the page
                if time.time() - start_time < cooldown:
                    # Here we deal with the change in the dataframe
                    update_data()
                    continue
                
                # Otherwise, clear the container and refresh the page
                placeholder.empty()
                start_time = time.time()
                placeholder.container()

                st.title("Highway Traffic Dashboard")
                # Place here the code to update the views
                update_numbers_view()
                update_tables_view()
