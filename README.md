# Databricks Declarative Pipeline

A Databricks Delta Live Tables (DLT) pipeline for processing real-estate listings from **Rabat**, **Casablanca**, and **Tangier**. The project also includes notebooks for training a synthetic-data model and continuously writing test data to Delta landing locations.

## What this project does

The pipeline follows a medallion-style architecture:

```text
Synthetic data generator
        |
        v
Delta landing volumes
        |
        v
Bronze: raw streaming tables
        |
        v
Silver: typed and validated tables
        |
        v
Gold: unified real-estate dataset
```

The pipeline is implemented with Databricks notebooks and DLT decorators. It is intended to run in a Databricks workspace with Unity Catalog volumes.

## Repository contents

| File | Purpose |
| --- | --- |
| `real_estate_data_gen.ipynb` | Trains and saves an SDV Gaussian Copula synthesizer from a source CSV. |
| `data_generation.ipynb` | Uses the saved synthesizer to append synthetic records to three Delta landing locations. |
| `01_bronze_processing.ipynb` | Reads the three landing locations as streaming Delta sources. |
| `02_silver_processing.ipynb` | Casts columns to usable types, adds agency IDs, and drops invalid records. |
| `03_gold_processing.ipynb` | Creates agency metadata, property-type links, and a unified Gold table. |
| `real_estate_management.sample_data.synthetic_data_generator` | Serialized SDV model artifact used by `data_generation.ipynb`. |

## Prerequisites

- A Databricks workspace with DLT/Lakeflow Declarative Pipelines enabled.
- Unity Catalog access to the `real_estate_management` catalog.
- A schema named `00_landing` for the landing volumes.
- A Python environment or Databricks cluster with:
  - `pyspark`
  - `delta`
  - `sdv`
  - `pandas`
- A source file at:

  ```text
  /Volumes/real_estate_management/sample_data/data_source/houses_data_eng.csv
  ```

- Write access to the following Delta locations:

  ```text
  /Volumes/real_estate_management/00_landing/streaming_data/RE_Rabat/
  /Volumes/real_estate_management/00_landing/streaming_data/RE_casablanca/
  /Volumes/real_estate_management/00_landing/streaming_data/RE_tanger/
  ```

The notebooks use the relative artifact path `real_estate_management.sample_data.synthetic_data_generator`. In Databricks, make sure the working directory or workspace file location resolves this path consistently, or update the path in both data-generation notebooks.

## Recommended execution order

### 1. Train the synthetic-data model

Open `real_estate_data_gen.ipynb` and run the notebook. It:

1. Loads `houses_data_eng.csv`.
2. Removes the first column.
3. Normalizes the third column name by replacing spaces with underscores.
4. Detects metadata with SDV.
5. Trains a `GaussianCopulaSynthesizer`.
6. Saves the trained model as `real_estate_management.sample_data.synthetic_data_generator`.

Run this step again whenever the source data distribution changes significantly.

### 2. Start the synthetic streams

Open `data_generation.ipynb` after the model artifact exists. The notebook starts one worker per city and continuously appends batches of 1–30 rows to Delta. The interval between batches is randomly selected between 1 and 60 seconds.

This notebook contains an intentional infinite loop. Stop the notebook or its attached cluster when the test stream is no longer needed.

### 3. Configure and run the DLT pipeline

Add these notebooks to the same DLT pipeline in this order:

1. `01_bronze_processing.ipynb`
2. `02_silver_processing.ipynb`
3. `03_gold_processing.ipynb`

The Bronze notebooks use `spark.readStream` and the Silver/Gold notebooks use DLT streaming reads, so the pipeline should be configured for continuous or triggered incremental processing according to the workspace workload.

## Tables produced

### Bronze

| Table | Source |
| --- | --- |
| `01_bronze.Real_estate_rabat` | `RE_Rabat` Delta volume |
| `01_bronze.Real_estate_casablanca` | `RE_casablanca` Delta volume |
| `01_bronze.Real_estate_tanger` | `RE_tanger` Delta volume |

Bronze tables preserve the incoming records and are intended to represent raw readings.

### Silver

| Table | Transformations |
| --- | --- |
| `02_silver.realestate_rabat` | Casts price, room, bathroom, surface, and floor fields; adds agency ID `3`. |
| `02_silver.realestate_casablanca` | Casts price, room, bathroom, surface, and floor fields; adds agency ID `1`. |
| `02_silver.realestate_tanger` | Casts price, room, bathroom, surface, and floor fields; adds agency ID `2`. |

The Silver tables use `expect_or_drop` quality rules:

- `new_price` must be non-null and greater than zero.
- `city` must be non-null.
- `surface` must be greater than zero.

Records that fail these rules are dropped rather than published to Silver.

### Gold and reference tables

| Table | Purpose |
| --- | --- |
| `03_gold.Unified_RealEstate_data` | Unions the three Silver streaming tables into one dataset. |
| `02_silver.agencies_metadata` | Static agency ID, name, manager, and city metadata. |
| `links` | Static links for property types such as apartment, studio, office, and villa. |

The Gold union requires the three Silver inputs to have compatible schemas. If the source CSV changes, update the Silver casts and validate the resulting schemas before running the pipeline.

## Data-generation details

The synthetic generator adds the following fields before writing each batch:

- `timestamp`, generated from the current time.
- `city`, set to `rabat`, `casablanca`, or `tanger`.
- Normalized column names with spaces replaced by underscores.

The generator writes Delta in append mode. It is designed for development and demonstration data, not for production ingestion or reproducible test fixtures.

## Operational notes

- The source and landing paths are hard-coded in the notebooks. Change them if the Unity Catalog layout differs.
- The serialized model artifact is binary and environment-dependent; retrain it if it cannot be loaded by the target Databricks runtime.
- The data generator catches write errors, prints the error, waits, and retries. Monitor notebook output for persistent failures.
- The city-to-agency IDs are currently encoded directly in `02_silver_processing.ipynb`; keep those values aligned with `agencies_metadata` when changing the mapping.
- The property-type links are static demonstration data and should be replaced with governed reference data for production use.

