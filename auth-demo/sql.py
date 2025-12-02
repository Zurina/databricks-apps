import pandas as pd

from auth import w


def fetch_warehouses():
    warehouse_options = []
    warehouse_options_initial = None
    print("DEBUG: fetch_warehouses called")
    print(f"DEBUG: w = {w}")
    if w:
        try:
            warehouses = w.warehouses.list()
            print(f"DEBUG: warehouses = {warehouses}")
            warehouse_list = sorted(
                [wh for wh in warehouses if wh.odbc_params and wh.odbc_params.path],
                key=lambda x: x.name,
            )
            print(f"DEBUG: warehouse_list = {warehouse_list}")
            if warehouse_list:
                warehouse_options = [
                    {"label": wh.name, "value": wh.odbc_params.path}
                    for wh in warehouse_list
                ]
                warehouse_options_initial = warehouse_options[0]["value"]
            else:
                warehouse_options = [
                    {"label": "No warehouses found", "value": "", "disabled": True}
                ]
        except Exception as e:
            print(f"Error fetching warehouses: {e}")
            warehouse_options = [
                {"label": f"Error fetching: {e}", "value": "", "disabled": True}
            ]
    else:
        print("DEBUG: w is not set or SDK not configured")
        warehouse_options = [
            {"label": "SDK Not Configured", "value": "", "disabled": True}
        ]

    print(f"DEBUG: warehouse_options = {warehouse_options}, warehouse_options_initial = {warehouse_options_initial}")
    return warehouse_options, warehouse_options_initial


def run_query(table_name, conn):
    if not table_name or not conn:
        return pd.DataFrame()

    query = f"SELECT * FROM {table_name} LIMIT 1000"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            df = cursor.fetchall_arrow().to_pandas()
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(
                    df[col]
                ) or pd.api.types.is_timedelta64_dtype(df[col]):
                    try:
                        df[col] = pd.to_datetime(df[col]).dt.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    except Exception:
                        df[col] = df[col].astype(str)
                elif isinstance(df[col].dtype, (pd.ArrowDtype)):
                    df[col] = df[col].astype(str)
                elif not pd.api.types.is_numeric_dtype(
                    df[col]
                ) and not pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].astype(str)
            return df
    except Exception as e:
        print(f"Error running query '{query}': {e}")
        raise


def insert_nyctaxi_row(conn):
    """
    Inserts a dummy row into amace_cdr_bronze_dev.oqmb_test.trips table.
    Adjust the columns/values as needed to match the schema.
    """
    query = """
    INSERT INTO amace_cdr_bronze_dev.oqmb_test.trips (
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        trip_distance,
        fare_amount,
        pickup_zip,
        dropoff_zip
    )
    VALUES (
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        0.1,
        5.0,
        '10001',
        '10002'
    )
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
    except Exception as e:
        print(f"Error inserting row: {e}")
        raise
