import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
from datetime import datetime

# =====================================================
# PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="Food Delivery Real-Time KPI Dashboard",
    layout="wide",
)

# =====================================================
# DATABASE CONFIG
# =====================================================
MONGO_URI = "mongodb://root:password123@mongo:27017/?authSource=admin"
DB_NAME = "food_delivery"

# =====================================================
# DATA LOADER
# =====================================================
@st.cache_data(ttl=30)
def load_collection(collection_name: str) -> pd.DataFrame:
    client = MongoClient(MONGO_URI)
    data = list(client[DB_NAME][collection_name].find({}, {"_id": 0}))
    return pd.DataFrame(data)

# =====================================================
# HEADER
# =====================================================
st.title("🍔 Food Delivery Real-Time KPI Dashboard")
st.caption("Updated every minute using **Kafka → MongoDB → Spark → Airflow**")

st.markdown("---")

# =====================================================
# KPI: ZONE DEMAND (GLOBAL METRICS)
# =====================================================
zone_df = load_collection("kpi_zone_demand")

st.subheader("📍 Overall Platform KPIs")

if zone_df.empty:
    st.warning("No KPI data available yet. Spark job may still be running.")
    st.stop()

# Use latest computation
zone_df["computed_at"] = pd.to_datetime(zone_df["computed_at"])
latest_zone = zone_df.sort_values("computed_at").iloc[-1]

c1, c2, c3 = st.columns(3)
c1.metric("Total Orders", int(latest_zone["order_count"]))
c2.metric("Total Revenue", f"Rs. {latest_zone['total_revenue']:,.0f}")
c3.metric("Avg Delivery Delay", f"{latest_zone['avg_delay']:.1f} min")

st.markdown("---")

# =====================================================
# ZONE DEMAND VISUAL
# =====================================================
st.subheader("📊 Orders by Zone")

fig_zone = px.bar(
    zone_df,
    x="zone_name",
    y="order_count",
    color="zone_name",
    title="Order Volume per Zone",
)
st.plotly_chart(fig_zone, use_container_width=True)

# =====================================================
# KPI: RIDER EFFICIENCY
# =====================================================
st.markdown("---")
st.subheader("🚴 Rider Efficiency KPIs")

rider_df = load_collection("kpi_rider_efficiency")

if rider_df.empty:
    st.info("Rider KPIs not available yet.")
else:
    rider_df["computed_at"] = pd.to_datetime(rider_df["computed_at"])

    fig_rider = px.scatter(
        rider_df,
        x="avg_idle_time",
        y="total_trips",
        color="vehicle_type",
        size="avg_traffic_delay",
        hover_data=["rider_id", "zone_name"],
        title="Rider Productivity vs Idle Time",
    )
    st.plotly_chart(fig_rider, use_container_width=True)

# =====================================================
# KPI: KITCHEN LOAD
# =====================================================
st.markdown("---")
st.subheader("🍳 Kitchen Load Analysis")

kitchen_df = load_collection("kpi_kitchen_load")

if kitchen_df.empty:
    st.info("Kitchen KPIs not available yet.")
else:
    kitchen_df["computed_at"] = pd.to_datetime(kitchen_df["computed_at"])

    fig_kitchen = px.bar(
        kitchen_df,
        x="restaurant_name",
        y="load_percentage",
        color="zone_id",
        title="Kitchen Load Percentage by Restaurant",
    )
    fig_kitchen.update_layout(xaxis_tickangle=-45)

    st.plotly_chart(fig_kitchen, use_container_width=True)

# =====================================================
# FOOTER
# =====================================================
st.markdown("---")
st.caption(
    f"Last refreshed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
)
