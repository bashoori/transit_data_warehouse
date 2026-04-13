from pathlib import Path

import pandas as pd


GOLD_V2_PATH = Path("data/processed/gold_v2")
OUTPUT_PATH = Path("analysis/output")


def load_table(name: str) -> pd.DataFrame:
    path = GOLD_V2_PATH / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing table: {path}")
    return pd.read_parquet(path)


def save_csv(df: pd.DataFrame, name: str) -> None:
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_PATH / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"Saved {name} -> {path}")


def top_routes_by_trip_count(dim_trip: pd.DataFrame, dim_route: pd.DataFrame) -> pd.DataFrame:
    result = (
        dim_trip.groupby("route_id", as_index=False)
        .agg(trip_count=("trip_id", "count"))
        .merge(dim_route, on="route_id", how="left")
        .sort_values("trip_count", ascending=False)
    )
    return result.head(10)


def average_trip_duration_by_route(
    fact_trip_summary: pd.DataFrame,
    dim_route: pd.DataFrame,
) -> pd.DataFrame:
    result = (
        fact_trip_summary.groupby("route_id", as_index=False)
        .agg(
            avg_trip_duration_minutes=("trip_duration_minutes", "mean"),
            trip_count=("trip_id", "count"),
        )
        .merge(dim_route, on="route_id", how="left")
        .sort_values("avg_trip_duration_minutes", ascending=False)
    )
    return result.head(10)


def busiest_departure_hours(fact_stop_time: pd.DataFrame) -> pd.DataFrame:
    result = (
        fact_stop_time.groupby("departure_hour", as_index=False)
        .agg(stop_events=("trip_id", "count"))
        .sort_values("stop_events", ascending=False)
    )
    return result


def weekday_vs_weekend_service(
    dim_date: pd.DataFrame,
    fact_trip_summary: pd.DataFrame,
) -> pd.DataFrame:
    service_dates = dim_date[["service_id", "is_weekend"]].drop_duplicates()

    merged = fact_trip_summary.merge(service_dates, on="service_id", how="inner")

    result = (
        merged.groupby("is_weekend", as_index=False)
        .agg(
            trip_count=("trip_id", "count"),
            avg_trip_duration_minutes=("trip_duration_minutes", "mean"),
        )
        .sort_values("is_weekend")
    )

    result["day_type"] = result["is_weekend"].map({False: "Weekday", True: "Weekend"})
    return result[["day_type", "trip_count", "avg_trip_duration_minutes"]]


def main() -> None:
    print("Loading Gold V2 tables...")

    dim_route = load_table("dim_route")
    dim_trip = load_table("dim_trip")
    dim_date = load_table("dim_date")
    fact_trip_summary = load_table("fact_trip_summary")
    fact_stop_time = load_table("fact_stop_time")

    print("\n1. Top routes by trip count")
    routes_by_trip_count = top_routes_by_trip_count(dim_trip, dim_route)
    print(routes_by_trip_count.head(10))
    save_csv(routes_by_trip_count, "top_routes_by_trip_count")

    print("\n2. Average trip duration by route")
    avg_duration_by_route = average_trip_duration_by_route(fact_trip_summary, dim_route)
    print(avg_duration_by_route.head(10))
    save_csv(avg_duration_by_route, "average_trip_duration_by_route")

    print("\n3. Busiest departure hours")
    departure_hours = busiest_departure_hours(fact_stop_time)
    print(departure_hours.head(10))
    save_csv(departure_hours, "busiest_departure_hours")

    print("\n4. Weekday vs weekend service")
    weekday_weekend = weekday_vs_weekend_service(dim_date, fact_trip_summary)
    print(weekday_weekend)
    save_csv(weekday_weekend, "weekday_vs_weekend_service")

    print("\nAnalysis complete.")


if __name__ == "__main__":
    main()