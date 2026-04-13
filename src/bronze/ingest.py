import zipfile
import pandas as pd
from pathlib import Path


RAW_PATH = Path("data/raw/google_transit.zip")
BRONZE_PATH = Path("data/processed/bronze")


def extract_gtfs():
    """
    Extract GTFS zip and load key tables into pandas DataFrames
    """
    tables = {}

    with zipfile.ZipFile(RAW_PATH, 'r') as z:
        for file in z.namelist():
            if file.endswith(".txt"):
                with z.open(file) as f:
                    df = pd.read_csv(f)
                    table_name = file.replace(".txt", "")
                    tables[table_name] = df

    return tables


def save_bronze(tables: dict):
    """
    Save raw tables as parquet (no transformation)
    """
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    for name, df in tables.items():
        output_path = BRONZE_PATH / f"{name}.parquet"
        df.to_parquet(output_path, index=False)


def run_bronze():
    print("Running Bronze Layer...")
    tables = extract_gtfs()
    save_bronze(tables)
    print("Bronze Layer Complete")


if __name__ == "__main__":
    run_bronze()