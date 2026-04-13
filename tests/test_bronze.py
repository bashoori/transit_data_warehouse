import pandas as pd

df = pd.read_parquet("data/processed/bronze/trips.parquet")
print(df.head())
print(df.columns)