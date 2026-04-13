from pathlib import Path

import pandas as pd


SILVER_PATH = Path("data/processed/silver")


def load_table(name: str) -> pd.DataFrame:
    path = SILVER_PATH / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing silver table: {path}")
    return pd.read_parquet(path)


def check_file_exists(name: str) -> None:
    path = SILVER_PATH / f"{name}.parquet"
    print(f"\nChecking file: {path}")
    print("Exists:", path.exists())


def check_basic_info(name: str) -> None:
    df = load_table(name)

    print(f"\n=== {name.upper()} ===")
    print("Shape:", df.shape)
    print("Columns:", list(df.columns))
    print("\nDtypes:")
    print(df.dtypes)
    print("\nSample rows:")
    print(df.head(5))


def check_nulls(df: pd.DataFrame, columns: list[str]) -> None:
    print("\nNull check:")
    for col in columns:
        if col in df.columns:
            print(f"{col}: {df[col].isna().sum()}")
        else:
            print(f"{col}: column not found")


def check_duplicates(df: pd.DataFrame, columns: list[str]) -> None:
    print("\nDuplicate check:")
    for col in columns:
        if col in df.columns:
            print(f"{col}: {df[col].duplicated().sum()}")
        else:
            print(f"{col}: column not found")


def run_silver_tests() -> None:
    required_tables = ["trips", "routes", "stops", "stop_times", "calendar"]

    for table in required_tables:
        check_file_exists(table)

    trips = load_table("trips")
    routes = load_table("routes")
    stops = load_table("stops")
    stop_times = load_table("stop_times")
    calendar = load_table("calendar")

    check_basic_info("trips")
    check_nulls(trips, ["trip_id", "route_id", "service_id"])
    check_duplicates(trips, ["trip_id"])

    check_basic_info("routes")
    check_nulls(routes, ["route_id"])
    check_duplicates(routes, ["route_id"])

    check_basic_info("stops")
    check_nulls(stops, ["stop_id"])
    check_duplicates(stops, ["stop_id"])

    check_basic_info("stop_times")
    check_nulls(stop_times, ["trip_id", "stop_id", "stop_sequence"])
    check_duplicates(stop_times, [])

    check_basic_info("calendar")
    check_nulls(calendar, ["service_id"])
    check_duplicates(calendar, ["service_id"])

    print("\nSilver tests complete.")


if __name__ == "__main__":
    run_silver_tests()