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

# add helper datetime and ZIP string columns for filtering
data['_pickup_dt'] = pd.to_datetime(data['tpep_pickup_datetime'], errors='coerce')
data['_dropoff_dt'] = pd.to_datetime(data['tpep_dropoff_datetime'], errors='coerce')
data['_pickup_date'] = data['_pickup_dt'].dt.date
data['_dropoff_date'] = data['_dropoff_dt'].dt.date
# keep original ZIP columns intact; add string versions for safe multiselect comparisons
data['_pickup_zip_str'] = data['pickup_zip'].astype(str)
data['_dropoff_zip_str'] = data['dropoff_zip'].astype(str)

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

    # Pickup date range
    min_pickup_date = data['_pickup_date'].min()
    max_pickup_date = data['_pickup_date'].max()
    pickup_date_range = st.date_input("Pickup date range", value=(min_pickup_date, max_pickup_date))

    # Dropoff date range
    min_dropoff_date = data['_dropoff_date'].min()
    max_dropoff_date = data['_dropoff_date'].max()
    dropoff_date_range = st.date_input("Dropoff date range", value=(min_dropoff_date, max_dropoff_date))

    # Pickup ZIP(s)
    pickup_zip_options = sorted(data['_pickup_zip_str'].dropna().unique().tolist())
    pickup_zip_sel = st.multiselect("Pickup ZIP(s)", options=pickup_zip_options, default=[])

    # Dropoff ZIP(s)
    dropoff_zip_options = sorted(data['_dropoff_zip_str'].dropna().unique().tolist())
    dropoff_zip_sel = st.multiselect("Dropoff ZIP(s)", options=dropoff_zip_options, default=[])

# -------------------------------
# Apply Filters (with new date and ZIP filters)
# -------------------------------
filtered = data[
    (data['fare_amount'] >= fare_range[0]) &
    (data['fare_amount'] <= fare_range[1]) &
    (data['trip_distance'] >= dist_range[0]) &
    (data['trip_distance'] <= dist_range[1]) &
    (data['_pickup_date'] >= pickup_date_range[0]) &
    (data['_pickup_date'] <= pickup_date_range[1]) &
    (data['_dropoff_date'] >= dropoff_date_range[0]) &
    (data['_dropoff_date'] <= dropoff_date_range[1])
]

# apply ZIP filters if any selected
if pickup_zip_sel:
    filtered = filtered[filtered['_pickup_zip_str'].isin(pickup_zip_sel)]
if dropoff_zip_sel:
    filtered = filtered[filtered['_dropoff_zip_str'].isin(dropoff_zip_sel)]

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
