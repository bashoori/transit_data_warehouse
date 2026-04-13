from pathlib import Path

import pandas as pd


GOLD_PATH = Path("data/processed/gold")


def load_table(name: str) -> pd.DataFrame:
    path = GOLD_PATH / f"{name}.parquet"
    assert path.exists(), f"Missing gold table: {path}"
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


def run_gold_tests() -> None:
    dim_agency = load_table("dim_agency")
    dim_route = load_table("dim_route")
    dim_stop = load_table("dim_stop")
    dim_service = load_table("dim_service")
    dim_trip = load_table("dim_trip")
    fact_trip_summary = load_table("fact_trip_summary")
    fact_stop_time = load_table("fact_stop_time")

    assert_table_not_empty(dim_agency, "dim_agency")
    assert_columns_exist(dim_agency, ["agency_id"], "dim_agency")
    assert_no_nulls(dim_agency, ["agency_id"], "dim_agency")
    assert_no_duplicates(dim_agency, ["agency_id"], "dim_agency")

    assert_table_not_empty(dim_route, "dim_route")
    assert_columns_exist(dim_route, ["route_id"], "dim_route")
    assert_no_nulls(dim_route, ["route_id"], "dim_route")
    assert_no_duplicates(dim_route, ["route_id"], "dim_route")

    assert_table_not_empty(dim_stop, "dim_stop")
    assert_columns_exist(dim_stop, ["stop_id"], "dim_stop")
    assert_no_nulls(dim_stop, ["stop_id"], "dim_stop")
    assert_no_duplicates(dim_stop, ["stop_id"], "dim_stop")

    assert_table_not_empty(dim_service, "dim_service")
    assert_columns_exist(dim_service, ["service_id"], "dim_service")
    assert_no_nulls(dim_service, ["service_id"], "dim_service")
    assert_no_duplicates(dim_service, ["service_id"], "dim_service")

    assert_table_not_empty(dim_trip, "dim_trip")
    assert_columns_exist(dim_trip, ["trip_id", "route_id", "service_id"], "dim_trip")
    assert_no_nulls(dim_trip, ["trip_id", "route_id", "service_id"], "dim_trip")
    assert_no_duplicates(dim_trip, ["trip_id"], "dim_trip")

    assert_table_not_empty(fact_trip_summary, "fact_trip_summary")
    assert_columns_exist(
        fact_trip_summary,
        ["trip_id", "route_id", "service_id", "stop_count"],
        "fact_trip_summary",
    )
    assert_no_nulls(fact_trip_summary, ["trip_id", "route_id", "service_id"], "fact_trip_summary")
    assert_no_duplicates(fact_trip_summary, ["trip_id"], "fact_trip_summary")

    assert_table_not_empty(fact_stop_time, "fact_stop_time")
    assert_columns_exist(
        fact_stop_time,
        ["trip_id", "stop_id", "stop_sequence", "route_id", "service_id"],
        "fact_stop_time",
    )
    assert_no_nulls(
        fact_stop_time,
        ["trip_id", "stop_id", "stop_sequence", "route_id", "service_id"],
        "fact_stop_time",
    )

    print("PASS: Gold tests passed.")


if __name__ == "__main__":
    run_gold_tests()