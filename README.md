# TransLink GTFS Data Warehouse

A medallion-style data engineering project built on the real TransLink GTFS feed, designed to move from raw transit data to a structured, analysis-ready warehouse.

---

## Goal

Build a reliable, end-to-end data pipeline that transforms raw GTFS data into a dimensional model that supports real analytical use cases.

This project focuses on:

- preserving raw data fidelity at ingestion  
- structuring and validating data for consistency  
- modeling data into fact and dimension tables  
- evolving the warehouse to support time-based analysis  

---

## Architecture

### Bronze

Raw ingestion layer.

- Stores GTFS tables exactly as received from the source zip  
- No transformation or filtering  
- Acts as the source of truth  

Source: https://www.transit.land/feeds

---

### Silver

Cleaning and standardization layer.

- normalize column types and formats  
- enforce required keys (`trip_id`, `route_id`, `stop_id`, `service_id`)  
- remove duplicates  
- handle null values  
- prepare data for downstream modeling  

---

### Gold

Warehouse layer for analytics.

Two versions are implemented to show design evolution.

---

## Warehouse Versions

### V1: Core Warehouse

Initial dimensional model built from cleaned GTFS data.

**Dimensions**
- `dim_agency`
- `dim_route`
- `dim_stop`
- `dim_service`
- `dim_trip`

**Facts**
- `fact_trip_summary`
- `fact_stop_time`

Focus:
- clean medallion flow  
- basic dimensional modeling  
- stable, testable pipeline  

---

### V2: Time-Aware Warehouse

Extended model to support real transit analysis.

Adds:

- `dim_date` generated from service calendar  
- GTFS time normalization (`HH:MM:SS` including values beyond 24:00:00)  
- derived metrics such as:
  - trip duration (minutes)
  - arrival and departure seconds
  - hourly service buckets  

New table:
- `dim_date`

Enhanced facts:
- `fact_trip_summary` → includes trip duration  
- `fact_stop_time` → includes time-based features  

Focus:
- making the warehouse usable for real questions  
- handling domain-specific complexity (GTFS time format)  

---

## Source Data

Real TransLink GTFS static feed.

Expected location:

data/raw/google_transit.zip

---

## Project Structure
```
transit_data_warehouse/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/
│   ├── processed/
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │   └── gold_v2/
├── src/
│   ├── pipeline.py
│   ├── bronze/
│   │   └── ingest.py
│   ├── silver/
│   │   └── transform.py
│   ├── gold/
│   │   ├── warehouse.py        # V1
│   │   └── warehouse_v2.py     # V2
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   ├── test_gold.py
│   ├── test_gold_v2.py
│   └── run_all_tests.py
```
---

## Data Quality

Each layer is validated using assertion-based tests.

Checks include:

- required files exist  
- tables are not empty  
- required columns exist  
- no null values in key fields  
- no duplicate primary keys  

Run all tests:

python tests/run_all_tests.py

Run V2 tests:

python tests/run_all_tests_v2.py

---

## How to Run

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

### Run full pipeline (V1)

python src/pipeline.py

### Run Gold V2

python src/gold/warehouse_v2.py

---

## Example Analytical Questions

The V2 model enables questions such as:

- How many trips run per route per day?
- What are the busiest hours of service?
- What is the average trip duration by route?
- How does service differ between weekdays and weekends?

---

## Design Notes

- GTFS time values can exceed 24:00:00 and are normalized into seconds  
- Calendar data is expanded into a proper date dimension  
- The warehouse separates structure (V1) from analytical usability (V2)  
- Each layer only depends on the layer before it  

---

## Next Steps

- integrate `calendar_dates.txt` for service exceptions  
- build route-level and time-based aggregates  
- add orchestration (Airflow or Fabric pipelines)  
- migrate to PySpark for larger-scale processing  
- expose data through Power BI or SQL endpoints  

---

## Why this project matters

This is not just a data pipeline. It shows:

- how to structure data across layers  
- how to enforce data quality  
- how to evolve a warehouse toward real analytical use  
- how to handle domain-specific data challenges  
