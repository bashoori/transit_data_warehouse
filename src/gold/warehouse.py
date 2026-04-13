from pathlib import Path

import pandas as pd


SILVER_PATH = Path("data/processed/silver")
GOLD_PATH = Path("data/processed/gold")


def load_table(name: str) -> pd.DataFrame:
    path = SILVER_PATH / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing silver table: {path}")
    return pd.read_parquet(path)


def save_table(df: pd.DataFrame, name: str) -> None:
    GOLD_PATH.mkdir(parents=True, exist_ok=True)
    path = GOLD_PATH / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"Saved {name} -> {path}")


# -----------------------------
# DIMENSIONS
# -----------------------------

def build_dim_agency(agency: pd.DataFrame) -> pd.DataFrame:
    dim_agency = agency.copy()

    if "agency_id" not in dim_agency.columns:
        dim_agency["agency_id"] = "TRANSLINK"

    dim_agency = dim_agency.drop_duplicates(subset=["agency_id"])
    dim_agency["agency_id"] = dim_agency["agency_id"].astype(str)

    return dim_agency


def build_dim_route(routes: pd.DataFrame) -> pd.DataFrame:
    dim_route = routes.copy()

    dim_route = dim_route.drop_duplicates(subset=["route_id"])
    dim_route["route_id"] = dim_route["route_id"].astype(str)

    return dim_route


def build_dim_stop(stops: pd.DataFrame) -> pd.DataFrame:
    dim_stop = stops.copy()

    dim_stop = dim_stop.drop_duplicates(subset=["stop_id"])
    dim_stop["stop_id"] = dim_stop["stop_id"].astype(str)

    return dim_stop


def build_dim_service(calendar: pd.DataFrame) -> pd.DataFrame:
    dim_service = calendar.copy()

    dim_service = dim_service.drop_duplicates(subset=["service_id"])
    dim_service["service_id"] = dim_service["service_id"].astype(str)

    return dim_service


def build_dim_trip(trips: pd.DataFrame) -> pd.DataFrame:
    dim_trip = trips.copy()

    dim_trip = dim_trip.drop_duplicates(subset=["trip_id"])
    dim_trip["trip_id"] = dim_trip["trip_id"].astype(str)
    dim_trip["route_id"] = dim_trip["route_id"].astype(str)
    dim_trip["service_id"] = dim_trip["service_id"].astype(str)

    return dim_trip


# -----------------------------
# FACTS
# -----------------------------

def build_fact_trip_summary(stop_times: pd.DataFrame, trips: pd.DataFrame) -> pd.DataFrame:
    st = stop_times.copy()
    st["trip_id"] = st["trip_id"].astype(str)

    trip_summary = (
        st.groupby("trip_id", as_index=False)
        .agg(
            stop_count=("stop_id", "count"),
            first_stop_sequence=("stop_sequence", "min"),
            last_stop_sequence=("stop_sequence", "max"),
        )
    )

    fact_trip_summary = trip_summary.merge(
        trips[["trip_id", "route_id", "service_id"]].drop_duplicates(),
        on="trip_id",
        how="left",
    )

    fact_trip_summary["route_id"] = fact_trip_summary["route_id"].astype(str)
    fact_trip_summary["service_id"] = fact_trip_summary["service_id"].astype(str)

    return fact_trip_summary


def build_fact_stop_time(stop_times: pd.DataFrame, trips: pd.DataFrame) -> pd.DataFrame:
    fact_stop_time = stop_times.copy()

    fact_stop_time["trip_id"] = fact_stop_time["trip_id"].astype(str)
    fact_stop_time["stop_id"] = fact_stop_time["stop_id"].astype(str)

    fact_stop_time = fact_stop_time.merge(
        trips[["trip_id", "route_id", "service_id"]].drop_duplicates(),
        on="trip_id",
        how="left",
    )

    fact_stop_time["route_id"] = fact_stop_time["route_id"].astype(str)
    fact_stop_time["service_id"] = fact_stop_time["service_id"].astype(str)

    return fact_stop_time


# -----------------------------
# MAIN
# -----------------------------

def run_gold() -> None:
    print("Running Gold Layer...")

    agency = load_table("agency")
    routes = load_table("routes")
    stops = load_table("stops")
    trips = load_table("trips")
    stop_times = load_table("stop_times")
    calendar = load_table("calendar")

    dim_agency = build_dim_agency(agency)
    dim_route = build_dim_route(routes)
    dim_stop = build_dim_stop(stops)
    dim_service = build_dim_service(calendar)
    dim_trip = build_dim_trip(trips)

    fact_trip_summary = build_fact_trip_summary(stop_times, trips)
    fact_stop_time = build_fact_stop_time(stop_times, trips)

    save_table(dim_agency, "dim_agency")
    save_table(dim_route, "dim_route")
    save_table(dim_stop, "dim_stop")
    save_table(dim_service, "dim_service")
    save_table(dim_trip, "dim_trip")
    save_table(fact_trip_summary, "fact_trip_summary")
    save_table(fact_stop_time, "fact_stop_time")

    print("Gold Layer Complete")


if __name__ == "__main__":
    run_gold()