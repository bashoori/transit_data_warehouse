import pandas as pd
from pathlib import Path


BRONZE_PATH = Path("data/processed/bronze")
SILVER_PATH = Path("data/processed/silver")


def load_table(name: str) -> pd.DataFrame:
    path = BRONZE_PATH / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing bronze table: {path}")
    return pd.read_parquet(path)


def save_table(df: pd.DataFrame, name: str) -> None:
    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    path = SILVER_PATH / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"Saved {name} -> {path}")


def clean_agency(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "agency_id" not in df.columns:
        df["agency_id"] = "TRANSLINK"

    df["agency_id"] = df["agency_id"].astype(str)
    df = df.drop_duplicates(subset=["agency_id"])

    return df


def clean_trips(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.drop_duplicates(subset=["trip_id"])
    df["route_id"] = df["route_id"].astype(str)
    df["trip_id"] = df["trip_id"].astype(str)
    df["service_id"] = df["service_id"].astype(str)

    if "trip_short_name" in df.columns:
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

    df = df.dropna(subset=["trip_id", "stop_id"])
    df["trip_id"] = df["trip_id"].astype(str)
    df["stop_id"] = df["stop_id"].astype(str)

    return df


def clean_calendar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.drop_duplicates(subset=["service_id"])
    df["service_id"] = df["service_id"].astype(str)
    return df


def run_silver() -> None:
    print("Running Silver Layer...")

    agency = clean_agency(load_table("agency"))
    trips = clean_trips(load_table("trips"))
    routes = clean_routes(load_table("routes"))
    stops = clean_stops(load_table("stops"))
    stop_times = clean_stop_times(load_table("stop_times"))
    calendar = clean_calendar(load_table("calendar"))

    save_table(agency, "agency")
    save_table(trips, "trips")
    save_table(routes, "routes")
    save_table(stops, "stops")
    save_table(stop_times, "stop_times")
    save_table(calendar, "calendar")

    print("Silver Layer Complete")


if __name__ == "__main__":
    run_silver()