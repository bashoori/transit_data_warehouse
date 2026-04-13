from pathlib import Path

import pandas as pd


SILVER_PATH = Path("data/processed/silver")
GOLD_PATH = Path("data/processed/gold_v2")


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


def gtfs_time_to_seconds(value: str) -> int | None:
    """
    Convert GTFS time string like '05:30:00' or '25:14:00'
    into seconds from start of service day.
    """
    if pd.isna(value):
        return None

    value = str(value).strip()
    if not value:
        return None

    parts = value.split(":")
    if len(parts) != 3:
        return None

    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
    except ValueError:
        return None

    return hours * 3600 + minutes * 60 + seconds


def seconds_to_hour(value: int | None) -> int | None:
    """
    Convert seconds from start of day into hour bucket.
    """
    if value is None or pd.isna(value):
        return None

    return int(value) // 3600


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


def build_dim_date(calendar: pd.DataFrame) -> pd.DataFrame:
    """
    Expand service calendar into one row per service_id per valid date.
    """
    records: list[dict] = []

    weekday_columns = {
        "monday": "monday",
        "tuesday": "tuesday",
        "wednesday": "wednesday",
        "thursday": "thursday",
        "friday": "friday",
        "saturday": "saturday",
        "sunday": "sunday",
    }

    for _, row in calendar.iterrows():
        service_id = str(row["service_id"])

        start_date = pd.to_datetime(str(row["start_date"]), format="%Y%m%d", errors="coerce")
        end_date = pd.to_datetime(str(row["end_date"]), format="%Y%m%d", errors="coerce")

        if pd.isna(start_date) or pd.isna(end_date):
            continue

        for current_date in pd.date_range(start=start_date, end=end_date, freq="D"):
            weekday_name = current_date.day_name().lower()
            weekday_col = weekday_columns.get(weekday_name)

            if weekday_col is None:
                continue

            service_runs = row.get(weekday_col, 0)

            if pd.isna(service_runs):
                service_runs = 0

            if int(service_runs) == 1:
                records.append(
                    {
                        "date_key": int(current_date.strftime("%Y%m%d")),
                        "full_date": current_date.date(),
                        "year": current_date.year,
                        "quarter": current_date.quarter,
                        "month": current_date.month,
                        "day": current_date.day,
                        "day_name": current_date.day_name(),
                        "day_of_week": current_date.weekday() + 1,
                        "is_weekend": current_date.weekday() >= 5,
                        "service_id": service_id,
                    }
                )

    dim_date = pd.DataFrame(records)

    if not dim_date.empty:
        dim_date = dim_date.drop_duplicates(subset=["date_key", "service_id"])

    return dim_date


# -----------------------------
# FACTS
# -----------------------------

def build_fact_trip_summary_v2(stop_times: pd.DataFrame, trips: pd.DataFrame) -> pd.DataFrame:
    st = stop_times.copy()
    st["trip_id"] = st["trip_id"].astype(str)

    st["arrival_seconds"] = st["arrival_time"].apply(gtfs_time_to_seconds)
    st["departure_seconds"] = st["departure_time"].apply(gtfs_time_to_seconds)

    trip_summary = (
        st.groupby("trip_id", as_index=False)
        .agg(
            stop_count=("stop_id", "count"),
            first_stop_sequence=("stop_sequence", "min"),
            last_stop_sequence=("stop_sequence", "max"),
            first_departure_seconds=("departure_seconds", "min"),
            last_arrival_seconds=("arrival_seconds", "max"),
        )
    )

    trip_summary["trip_duration_minutes"] = (
        trip_summary["last_arrival_seconds"] - trip_summary["first_departure_seconds"]
    ) / 60

    fact_trip_summary = trip_summary.merge(
        trips[["trip_id", "route_id", "service_id"]].drop_duplicates(),
        on="trip_id",
        how="left",
    )

    fact_trip_summary["route_id"] = fact_trip_summary["route_id"].astype(str)
    fact_trip_summary["service_id"] = fact_trip_summary["service_id"].astype(str)

    return fact_trip_summary


def build_fact_stop_time_v2(stop_times: pd.DataFrame, trips: pd.DataFrame) -> pd.DataFrame:
    fact_stop_time = stop_times.copy()

    fact_stop_time["trip_id"] = fact_stop_time["trip_id"].astype(str)
    fact_stop_time["stop_id"] = fact_stop_time["stop_id"].astype(str)

    fact_stop_time["arrival_seconds"] = fact_stop_time["arrival_time"].apply(gtfs_time_to_seconds)
    fact_stop_time["departure_seconds"] = fact_stop_time["departure_time"].apply(gtfs_time_to_seconds)

    fact_stop_time["arrival_hour"] = fact_stop_time["arrival_seconds"].apply(seconds_to_hour)
    fact_stop_time["departure_hour"] = fact_stop_time["departure_seconds"].apply(seconds_to_hour)

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

def run_gold_v2() -> None:
    print("Running Gold Layer V2...")

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
    dim_date = build_dim_date(calendar)

    fact_trip_summary = build_fact_trip_summary_v2(stop_times, trips)
    fact_stop_time = build_fact_stop_time_v2(stop_times, trips)

    save_table(dim_agency, "dim_agency")
    save_table(dim_route, "dim_route")
    save_table(dim_stop, "dim_stop")
    save_table(dim_service, "dim_service")
    save_table(dim_trip, "dim_trip")
    save_table(dim_date, "dim_date")
    save_table(fact_trip_summary, "fact_trip_summary")
    save_table(fact_stop_time, "fact_stop_time")

    print("Gold Layer V2 Complete")


if __name__ == "__main__":
    run_gold_v2()