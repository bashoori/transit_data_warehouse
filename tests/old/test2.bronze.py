from pathlib import Path

import pandas as pd


BRONZE_PATH = Path("data/processed/bronze")


def load_table(name: str) -> pd.DataFrame:
    path = BRONZE_PATH / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing bronze table: {path}")
    return pd.read_parquet(path)


def check_file_exists(name: str) -> None:
    path = BRONZE_PATH / f"{name}.parquet"
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


def run_bronze_tests() -> None:
    required_tables = [
        "agency",
        "routes",
        "trips",
        "stops",
        "stop_times",
        "calendar",
    ]

    for table in required_tables:
        check_file_exists(table)

    for table in required_tables:
        check_basic_info(table)

    print("\nBronze tests complete.")


if __name__ == "__main__":
    run_bronze_tests()