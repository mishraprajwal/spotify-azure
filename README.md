# Spotify streaming analytics on Azure

An end-to-end medallion pipeline on Azure with synthetic Spotify-style data.

```
Azure SQL DB ──ADF (watermark copy)──> ADLS Gen2 bronze/raw (parquet)
        └─ Databricks: bronze (Auto Loader) ─> silver (clean + merge) ─> gold (SCD2, enriched fact, aggregates)
```

## Layout

| Path | Purpose |
|------|---------|
| `data_generator/generate_data.py` | Creates reproducible synthetic data as T-SQL scripts |
| `sql/schema.sql` | Source tables for Azure SQL Database |
| `sql/seed_initial.sql`, `sql/seed_incremental.sql` | Generated initial and incremental loads |
| `adf/` | Data Factory linked services, datasets and `pl_incremental_ingest` pipeline |
| `config/tables.json` | Tables and cursor columns driving the incremental copy |
| `config/watermarks/` | Initial watermark files to upload to `bronze/_watermarks/` |
| `databricks/` | Notebooks: `01_bronze_ingest`, `02_silver_transform`, `03_gold_models` |
| `tests/` | Generator tests |

## How incremental loading works

For each table the pipeline reads the last watermark from `bronze/_watermarks/<table>.json`, looks up the current
`MAX(cursor_column)`, copies rows in `(old, new]` to `bronze/raw/<table>/` as parquet, and only then saves the new
watermark. Fixing the upper bound first avoids missing rows written while a copy is running, and a failed copy
leaves the watermark untouched so the run can be retried.

## Setup

1. **Generate data**: `python data_generator/generate_data.py` (options: `--seed`, `--users`, `--streams`, ...).
2. **Azure resources**: resource group, Azure SQL Database, ADLS Gen2 account (hierarchical namespace) with
   containers `bronze`, `silver`, `gold`, Key Vault, Data Factory, Databricks workspace (Unity Catalog).
3. **Source database**: run `sql/schema.sql`, then `sql/seed_initial.sql`.
4. **Key Vault**: store the SQL connection string as secret `sql-connection-string`; give the Data Factory managed
   identity *Get/List* on secrets.
5. **Storage**: give the Data Factory managed identity *Storage Blob Data Contributor*, and upload
   `config/watermarks/*.json` to `bronze/_watermarks/`.
6. **Data Factory**: replace the `<placeholders>` in `adf/linkedService/*.json`, import the linked services,
   datasets and pipeline, and run `pl_incremental_ingest`.
7. **Databricks**: import the notebooks, grant the workspace access to the storage account (external location),
   then run them in order with widgets `storage_account=<account>` and `catalog=spotify`
   (`03_gold_models` only needs `catalog`).
8. **Incremental run**: execute `sql/seed_incremental.sql`, rerun the pipeline and the notebooks. New rows and the
   user tier changes flow through; gold `dim_user_history` gains new versions.

## Gold outputs

* `gold.dim_user_history` - SCD type 2 user dimension (`valid_from`, `valid_to`, `is_current`)
* `gold.fact_stream_enriched` - streams with track, artist, date and the user tier at stream time
* `gold.daily_track_streams`, `gold.top_artists` - reporting aggregates

## Tests

```
pip install pytest
pytest
```
