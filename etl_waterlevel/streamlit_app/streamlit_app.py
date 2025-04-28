import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import folium
from streamlit_folium import folium_static
import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="Selangor Water Levels",
    page_icon="💧",
    layout="wide",
)

# --- DATA LOADING (cached) ---
@st.cache_data(ttl=600)
def load_data():
    try:
        conn = psycopg2.connect(
            dbname="airflow", user="airflow", password="airflow",
            host="postgres", port="5432"
        )
        df = pd.read_sql("SELECT * FROM fact.fact_waterlevel", conn)
        conn.close()
    except:
        df = pd.read_csv("2025-04-28T23-10_export.csv")
    df['last_update'] = pd.to_datetime(df['last_update'])
    return df

df = load_data()

# --- SIDEBAR FILTERS ---
st.sidebar.header("Filters")
min_date = df.last_update.min().date()
max_date = df.last_update.max().date()
start_date, end_date = st.sidebar.date_input(
    "Date range",
    [max_date - datetime.timedelta(days=7), max_date],
    min_value=min_date, max_value=max_date
)
districts = ["All"] + sorted(df.districtName.unique())
sel_dist = st.sidebar.selectbox("District", districts)

mask = (
    (df.last_update.dt.date >= start_date) &
    (df.last_update.dt.date <= end_date)
)
if sel_dist != "All":
    mask &= (df.districtName == sel_dist)
filtered = df[mask]

# --- TABS LAYOUT ---
tab_overview, tab_trends, tab_map, tab_recs = st.tabs([
    "🏠 Overview", "📈 Trends", "🗺 Map", "🎯 Actions"
])

# --- OVERVIEW TAB ---
with tab_overview:
    st.header("💧 Current Status")
    latest = (
        filtered.sort_values("last_update")
                .drop_duplicates("station_key", keep="last")
    )
    counts = latest.waterlevel_status.value_counts().to_dict()
    total = len(latest)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total", total)
    c2.metric("🚨 Danger", counts.get(3, 0))
    c3.metric("⚠️ Warning", counts.get(2, 0))
    c4.metric("🔶 Alert", counts.get(1, 0))
    c5.metric("✅ Safe", counts.get(0,0) + counts.get(-1,0))

    st.subheader("Status Distribution")
    dist = latest.waterlevel_condition.value_counts().reset_index()
    dist.columns = ["Condition", "Count"]
    fig = px.pie(dist, names="Condition", values="Count", hole=0.4)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Avg Level by District")
    avg = (
        latest.groupby("districtName")
              .water_level.mean()
              .sort_values(ascending=False)
              .reset_index()
    )
    fig2 = px.bar(
        avg, x="districtName", y="water_level",
        labels={"water_level":"Level (m)","districtName":"District"}
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- TRENDS TAB ---
with tab_trends:
    st.header("📈 Daily Avg Water Level")
    daily = (
        filtered.set_index("last_update")
                .resample("D")
                .water_level.mean()
                .reset_index()
    )
    fig = px.line(
        daily, x="last_update", y="water_level",
        labels={"last_update":"Date","water_level":"Avg Level (m)"}
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Status Over Time")
    status_ts = (
        filtered.assign(date=filtered.last_update.dt.date)
                .groupby(["date","waterlevel_condition"])
                .size()
                .reset_index(name="count")
    )
    fig2 = px.area(
        status_ts, x="date", y="count",
        color="waterlevel_condition",
        labels={"waterlevel_condition":"Condition","count":"Stations"}
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- MAP TAB ---
with tab_map:
    st.header("🗺️ Station Map")
    m = folium.Map(location=[3.0738,101.5183], zoom_start=9)
    coords = {
        "SABAK BERNAM":[3.8083,100.9901], "KUALA SELANGOR":[3.3404,101.2501],
        "HULU SELANGOR":[3.6031,101.6604], "GOMBAK":[3.2679,101.5865],
        "PETALING":[3.0977,101.5865], "HULU LANGAT":[3.0738,101.8156],
        "KLANG":[3.0449,101.4455], "KUALA LANGAT":[2.8271,101.5000],
        "SEPANG":[2.6899,101.7414]
    }
    for _, row in latest.iterrows():
        loc = coords.get(row.districtName)
        if not loc: continue
        color = {3:"red",2:"orange",1:"gold",0:"green",-1:"green"}[row.waterlevel_status]
        folium.CircleMarker(
            location=loc, radius=7, color=color,
            popup=f"{row.stationName}: {row.water_level:.2f} m"
        ).add_to(m)
    folium_static(m)

# --- ACTIONS TAB ---
with tab_recs:
    st.header("🎯 Recommendations")
    recs = []
    if counts.get(3,0):
        recs.append(f"🚨 Evacuate areas near DANGER stations ({counts[3]})")
    if counts.get(2,0):
        recs.append(f"⚠️ Issue Warning to affected stations ({counts[2]})")
    recs.append("✅ Continue daily monitoring at 11 PM.")
    for r in recs:
        st.write("- " + r)

# --- FOOTER ---
last = filtered.last_update.max().strftime("%Y-%m-%d %H:%M")
st.markdown(
    f"<div style='text-align:center; color:gray; font-size:0.8rem;'>"
    f"Data updated: {last}</div>",
    unsafe_allow_html=True
)