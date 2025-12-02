import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------
# Databricks SQL Query
# -------------------------------
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

def getData():
    return sqlQuery("SELECT * FROM samples.nyctaxi.trips LIMIT 5000")

data = getData()

# -------------------------------
# Streamlit Page Config
# -------------------------------
st.set_page_config(layout="wide", page_title="NYC Taxi Fare Explorer", page_icon="🚕")
st.title("🚕 NYC Taxi Fare Explorer")
st.markdown("Explore taxi fares and trip data interactively.")

# -------------------------------
# Sidebar Filters
# -------------------------------
with st.sidebar:
    st.header("Filter Trips")

    # Fare range
    min_fare, max_fare = float(data['fare_amount'].min()), float(data['fare_amount'].max())
    fare_range = st.slider("Fare amount ($)", min_value=min_fare, max_value=max_fare,
                           value=(min_fare, max_fare), step=1.0)

    # Trip distance range
    min_dist, max_dist = float(data['trip_distance'].min()), float(data['trip_distance'].max())
    dist_range = st.slider("Trip distance (miles)", min_value=min_dist, max_value=max_dist,
                           value=(min_dist, max_dist), step=0.1)

# -------------------------------
# Apply Filters (without ZIP filters)
# -------------------------------
filtered = data[
    (data['fare_amount'] >= fare_range[0]) &
    (data['fare_amount'] <= fare_range[1]) &
    (data['trip_distance'] >= dist_range[0]) &
    (data['trip_distance'] <= dist_range[1])
]

# -------------------------------
# Scatter Chart: Fare vs Distance
# -------------------------------
st.header("Trip Fare vs. Distance")
col1, col2 = st.columns([3, 1])

with col1:
    st.scatter_chart(
        data=filtered,
        x="trip_distance",
        y="fare_amount",
        height=400,
        width=700
    )

# -------------------------------
# Predict Fare
# -------------------------------
with col2:
    st.subheader("Predict fare")
    pickup = st.text_input("From (zipcode)", value="10003")
    dropoff = st.text_input("To (zipcode)", value="11238")

    if pickup.isdigit() and dropoff.isdigit():
        p = int(pickup)
        d = int(dropoff)

        # 1. Exact match
        exact = data[(data['pickup_zip'] == p) & (data['dropoff_zip'] == d)]
        pickup_only = data[data['pickup_zip'] == p]
        dropoff_only = data[data['dropoff_zip'] == d]

        if len(exact) > 0:
            pred = exact['fare_amount'].mean()
            st.metric("Predicted Fare", f"${pred:.2f}")
            st.caption("Based on matching pickup/dropoff ZIP codes.")
        elif len(pickup_only) > 0:
            pred = pickup_only['fare_amount'].mean()
            st.metric("Predicted Fare", f"${pred:.2f}")
            st.caption("Based on pickup ZIP only (no exact matches).")
        elif len(dropoff_only) > 0:
            pred = dropoff_only['fare_amount'].mean()
            st.metric("Predicted Fare", f"${pred:.2f}")
            st.caption("Based on dropoff ZIP only (no exact matches).")
        else:
            pred = data['fare_amount'].mean()
            st.metric("Predicted Fare", f"${pred:.2f}")
            st.caption("Fallback: average of all trips (ZIPs not found).")
    else:
        st.metric("Predicted Fare", "N/A")
        st.caption("Enter valid numeric zip codes.")

# -------------------------------
# Distributions
# -------------------------------
st.markdown("### Trip Fare Distribution")
st.bar_chart(filtered['fare_amount'], height=200)

st.markdown("### Trip Distance Distribution")
fig, ax = plt.subplots()
ax.hist(filtered['trip_distance'], bins=30, color='skyblue', edgecolor='black')
ax.set_xlabel("Trip Distance (miles)")
ax.set_ylabel("Number of Trips")
st.pyplot(fig)

# -------------------------------
# Display Filtered Data
# -------------------------------
st.markdown("### Filtered Trip Data")
st.dataframe(filtered, height=600, use_container_width=True)
