from pathlib import Path

import pandas as pd


BRONZE_PATH = Path("data/processed/bronze")


def load_table(name: str) -> pd.DataFrame:
    path = BRONZE_PATH / f"{name}.parquet"
    assert path.exists(), f"Missing bronze table: {path}"
    return pd.read_parquet(path)


def assert_table_not_empty(df: pd.DataFrame, name: str) -> None:
    assert not df.empty, f"{name} is empty"


def assert_columns_exist(df: pd.DataFrame, expected_columns: list[str], name: str) -> None:
    missing = [col for col in expected_columns if col not in df.columns]
    assert not missing, f"{name} is missing columns: {missing}"


def run_bronze_tests() -> None:
    required_tables = {
        "agency": ["agency_name"],
        "routes": ["route_id"],
        "trips": ["trip_id", "route_id", "service_id"],
        "stops": ["stop_id"],
        "stop_times": ["trip_id", "stop_id", "stop_sequence"],
        "calendar": ["service_id"],
    }

    for table_name, expected_columns in required_tables.items():
        df = load_table(table_name)
        assert_table_not_empty(df, table_name)
        assert_columns_exist(df, expected_columns, table_name)

    print("PASS: Bronze tests passed.")


if __name__ == "__main__":
    run_bronze_tests()