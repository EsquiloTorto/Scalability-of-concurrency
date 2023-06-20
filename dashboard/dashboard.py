import pymongo
import streamlit as st
from connection import get_mongodb_connection

@st.cache_resource
def init_connection():
    return get_mongodb_connection()

client = init_connection()
placeholder = st.empty()

# Pull data from the collection.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=15)
def get_data():
    global placeholder
    placeholder = st.empty()
    simulator_db = client["simulator"]
    return simulator_db["positions"].count_documents({})


data = get_data()

# Everytime data changes, will rerun this.
with placeholder.container():
    st.markdown("# Highway Traffic Data")
    st.markdown(f"Number of records in collection: **{data}**")






