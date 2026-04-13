import pandas as pd
from pathlib import Path


BRONZE_PATH = Path("data/processed/bronze")
SILVER_PATH = Path("data/processed/silver")


def load_table(name: str) -> pd.DataFrame:
    path = BRONZE_PATH / f"{name}.parquet"
    return pd.read_parquet(path)


# -----------------------------
# CLEANING FUNCTIONS
# -----------------------------

def clean_trips(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # remove duplicates
    df = df.drop_duplicates(subset=["trip_id"])

    # enforce types
    df["route_id"] = df["route_id"].astype(str)
    df["trip_id"] = df["trip_id"].astype(str)

    # fill missing optional fields
    df["trip_short_name"] = df["trip_short_name"].fillna("")

    return df


def clean_routes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.drop_duplicates(subset=["route_id"])
    df["route_id"] = df["route_id"].astype(str)

    return df


def clean_stops(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.drop_duplicates(subset=["stop_id"])
    df["stop_id"] = df["stop_id"].astype(str)

    return df


def clean_stop_times(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # remove bad rows
    df = df.dropna(subset=["trip_id", "stop_id"])

    # enforce types
    df["trip_id"] = df["trip_id"].astype(str)
    df["stop_id"] = df["stop_id"].astype(str)

    return df


# -----------------------------
# SAVE FUNCTION
# -----------------------------

def save_table(df: pd.DataFrame, name: str):
    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    path = SILVER_PATH / f"{name}.parquet"
    df.to_parquet(path, index=False)


# -----------------------------
# MAIN SILVER RUN
# -----------------------------

def run_silver():
    print("Running Silver Layer...")

    trips = clean_trips(load_table("trips"))
    routes = clean_routes(load_table("routes"))
    stops = clean_stops(load_table("stops"))
    stop_times = clean_stop_times(load_table("stop_times"))
    calendar = load_table("calendar")  # keep raw for now

    save_table(trips, "trips")
    save_table(routes, "routes")
    save_table(stops, "stops")
    save_table(stop_times, "stop_times")
    save_table(calendar, "calendar")

    print("Silver Layer Complete")


if __name__ == "__main__":
    run_silver()