from pathlib import Path

import pandas as pd


SILVER_PATH = Path("data/processed/silver")


def load_table(name: str) -> pd.DataFrame:
    path = SILVER_PATH / f"{name}.parquet"
    assert path.exists(), f"Missing silver table: {path}"
    return pd.read_parquet(path)


def assert_table_not_empty(df: pd.DataFrame, name: str) -> None:
    assert not df.empty, f"{name} is empty"


def assert_columns_exist(df: pd.DataFrame, expected_columns: list[str], name: str) -> None:
    missing = [col for col in expected_columns if col not in df.columns]
    assert not missing, f"{name} is missing columns: {missing}"


def assert_no_nulls(df: pd.DataFrame, columns: list[str], name: str) -> None:
    for col in columns:
        assert col in df.columns, f"{name} is missing required column: {col}"
        null_count = df[col].isna().sum()
        assert null_count == 0, f"{name}.{col} has {null_count} null values"


def assert_no_duplicates(df: pd.DataFrame, columns: list[str], name: str) -> None:
    duplicate_count = df.duplicated(subset=columns).sum()
    assert duplicate_count == 0, f"{name} has {duplicate_count} duplicate rows on {columns}"


def run_silver_tests() -> None:
    trips = load_table("trips")
    routes = load_table("routes")
    stops = load_table("stops")
    stop_times = load_table("stop_times")
    calendar = load_table("calendar")

    assert_table_not_empty(trips, "trips")
    assert_columns_exist(trips, ["trip_id", "route_id", "service_id"], "trips")
    assert_no_nulls(trips, ["trip_id", "route_id", "service_id"], "trips")
    assert_no_duplicates(trips, ["trip_id"], "trips")

    assert_table_not_empty(routes, "routes")
    assert_columns_exist(routes, ["route_id"], "routes")
    assert_no_nulls(routes, ["route_id"], "routes")
    assert_no_duplicates(routes, ["route_id"], "routes")

    assert_table_not_empty(stops, "stops")
    assert_columns_exist(stops, ["stop_id"], "stops")
    assert_no_nulls(stops, ["stop_id"], "stops")
    assert_no_duplicates(stops, ["stop_id"], "stops")

    assert_table_not_empty(stop_times, "stop_times")
    assert_columns_exist(stop_times, ["trip_id", "stop_id", "stop_sequence"], "stop_times")
    assert_no_nulls(stop_times, ["trip_id", "stop_id", "stop_sequence"], "stop_times")

    assert_table_not_empty(calendar, "calendar")
    assert_columns_exist(calendar, ["service_id"], "calendar")
    assert_no_nulls(calendar, ["service_id"], "calendar")
    assert_no_duplicates(calendar, ["service_id"], "calendar")

    print("PASS: Silver tests passed.")


if __name__ == "__main__":
    run_silver_tests()