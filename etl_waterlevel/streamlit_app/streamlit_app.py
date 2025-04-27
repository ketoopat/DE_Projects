import psycopg2
import pandas as pd
import streamlit as st

# PostgreSQL connection function
def get_waterlevel_data():
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",        # IMPORTANT: service name inside Docker network
        port="5432"
    )
    
    query = "SELECT * FROM fact.fact_waterlevel"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

st.title("📊 Water Level Dashboard")

# Pull data
df = get_waterlevel_data()

# Add Date Range Picker widget
start_date, end_date = st.date_input(
    "Select Date Range:",
    value=[df['last_update'].min(), df['last_update'].max()]
)

# Filter the dataframe based on selected date range
filtered_df = df[
    (df['last_update'] >= pd.to_datetime(start_date)) &
    (df['last_update'] <= pd.to_datetime(end_date))
]

# Show filtered data
st.dataframe(df)

# Chart
if 'waterlevel' in filtered_df.columns:
    st.line_chart(filtered_df[['last_update', 'waterlevel']].set_index('last_update'))