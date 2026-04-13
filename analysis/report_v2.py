from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


BASE = Path("data/processed/gold_v2")
OUT = Path("analysis/output")
OUT.mkdir(parents=True, exist_ok=True)


def load(name: str) -> pd.DataFrame:
    return pd.read_parquet(BASE / f"{name}.parquet")


def plot_top_routes(dim_trip):
    df = (
        dim_trip.groupby("route_id")
        .size()
        .reset_index(name="trip_count")
        .sort_values("trip_count", ascending=False)
        .head(10)
    )

    plt.figure()
    plt.bar(df["route_id"].astype(str), df["trip_count"])
    plt.title("Top Routes by Trip Count")
    plt.xlabel("Route ID")
    plt.ylabel("Trips")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(OUT / "top_routes.png")
    plt.close()


def plot_trip_duration(fact_trip):
    df = (
        fact_trip.groupby("route_id")["trip_duration_minutes"]
        .mean()
        .reset_index()
        .sort_values("trip_duration_minutes", ascending=False)
        .head(10)
    )

    plt.figure()
    plt.bar(df["route_id"].astype(str), df["trip_duration_minutes"])
    plt.title("Average Trip Duration by Route")
    plt.xlabel("Route ID")
    plt.ylabel("Minutes")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(OUT / "trip_duration.png")
    plt.close()


def plot_departure_hours(fact_stop):
    df = (
        fact_stop.groupby("departure_hour")
        .size()
        .reset_index(name="events")
        .sort_values("departure_hour")
    )

    plt.figure()
    plt.plot(df["departure_hour"], df["events"])
    plt.title("Departure Activity by Hour")
    plt.xlabel("Hour")
    plt.ylabel("Events")
    plt.tight_layout()
    plt.savefig(OUT / "departure_hours.png")
    plt.close()


def main():
    print("Loading data...")

    dim_trip = load("dim_trip")
    fact_trip = load("fact_trip_summary")
    fact_stop = load("fact_stop_time")

    print("Generating charts...")

    plot_top_routes(dim_trip)
    plot_trip_duration(fact_trip)
    plot_departure_hours(fact_stop)

    print("Charts saved in analysis/output/")


if __name__ == "__main__":
    main()