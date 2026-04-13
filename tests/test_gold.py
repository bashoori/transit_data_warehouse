from pathlib import Path

import pandas as pd


GOLD_PATH = Path("data/processed/gold")


def load_table(name: str) -> pd.DataFrame:
    path = GOLD_PATH / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing gold table: {path}")
    return pd.read_parquet(path)


def check_file_exists(name: str) -> None:
    path = GOLD_PATH / f"{name}.parquet"
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


def run_gold_tests() -> None:
    required_tables = [
        "dim_agency",
        "dim_route",
        "dim_stop",
        "dim_service",
        "dim_trip",
        "fact_trip_summary",
        "fact_stop_time",
    ]

    for table in required_tables:
        check_file_exists(table)

    dim_agency = load_table("dim_agency")
    dim_route = load_table("dim_route")
    dim_stop = load_table("dim_stop")
    dim_service = load_table("dim_service")
    dim_trip = load_table("dim_trip")
    fact_trip_summary = load_table("fact_trip_summary")
    fact_stop_time = load_table("fact_stop_time")

    check_basic_info("dim_agency")
    check_nulls(dim_agency, ["agency_id"])
    check_duplicates(dim_agency, ["agency_id"])

    check_basic_info("dim_route")
    check_nulls(dim_route, ["route_id"])
    check_duplicates(dim_route, ["route_id"])

    check_basic_info("dim_stop")
    check_nulls(dim_stop, ["stop_id"])
    check_duplicates(dim_stop, ["stop_id"])

    check_basic_info("dim_service")
    check_nulls(dim_service, ["service_id"])
    check_duplicates(dim_service, ["service_id"])

    check_basic_info("dim_trip")
    check_nulls(dim_trip, ["trip_id", "route_id", "service_id"])
    check_duplicates(dim_trip, ["trip_id"])

    check_basic_info("fact_trip_summary")
    check_nulls(fact_trip_summary, ["trip_id"])
    check_duplicates(fact_trip_summary, ["trip_id"])

    check_basic_info("fact_stop_time")
    check_nulls(fact_stop_time, ["trip_id", "stop_id", "stop_sequence"])

    print("\nGold tests complete.")


if __name__ == "__main__":
    run_gold_tests()