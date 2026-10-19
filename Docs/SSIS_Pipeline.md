# SSIS Pipeline

This document explains the **logic** of the SSIS pipeline that orchestrates the data warehouse. SSIS only orchestrates: the business logic lives in the stored procedures of the `etl` schema (see the scripts in `Scripts/`).

The SSIS project files and the connection settings are **not published** in this repository. Everything needed to understand the pipeline is here.

![SSIS pipeline – logical flow](../Images/SSIS_Pipeline_Flow.png)

---

## Order of execution

| Step | Task                    | What it does                                                                                       |
|-----:|-------------------------|----------------------------------------------------------------------------------------------------|
| 1    | **Start Batch**         | Creates the run in `etl.batch` and stores its number in the variable `batch_id`                    |
| 2    | **Get Config Objects**  | Reads the active objects of `etl.config`, ordered by `load_order`, into the variable `object_list`  |
| 3    | **Bronze** (loop)       | For each object: loads its Bronze table with `etl.usp_load_bronze`                                 |
| 4    | **Silver** (loop)       | For each object: checks the data quality and loads its Silver table (details below)               |
| 5    | **Gold**                | Loads the fact table with `etl.usp_load_gold_fact_sales_order` (Release 1: always a full reload)                                     |

Steps 1 and 2 are independent; Bronze starts when both have finished. Each step starts only when the previous one succeeded: a failure stops the pipeline.

## What happens for each object in the Silver loop

| Task                          | What it does                                                                                              |
|-------------------------------|-----------------------------------------------------------------------------------------------------------|
| **Get Last Watermark Value**  | Reads the watermark of the object from `etl.config`. For a `full` object it uses `1990-01-01`, so everything is read |
| **Count New Records**         | Counts the Bronze rows newer than the watermark and stores the number in `new_rows_count`                 |
| **Bronze Data Quality**       | Runs `etl.usp_check_bronze_quality_load_<object>`. Executed only when `new_rows_count > 0`                |
| **Load Silver**               | Runs `etl.usp_load_silver_<object>`                                                                       |

The path depends on `new_rows_count`:

| Condition               | Path                                                       |
|-------------------------|------------------------------------------------------------|
| `new_rows_count > 0`    | Bronze Data Quality, then Load Silver                      |
| `new_rows_count = 0`    | Load Silver only (no new rows to validate)                 |

## Variables

| Variable                                | Type     | Meaning                                                              |
|-----------------------------------------|----------|----------------------------------------------------------------------|
| `batch_id`                              | Int64    | Run created by Start Batch, passed to every procedure                |
| `object_list`                           | Object   | Active objects read from `etl.config`                                |
| `current_object_name`                   | String   | Object of the current loop iteration                                 |
| `last_watermark_value`                  | DateTime | Watermark of the current object                                      |
| `check_query`                           | String   | Query that counts the new Bronze rows of the current object          |
| `new_rows_count`                        | Int32    | Result of Count New Records                                          |
| `current_data_quality_stored_procedure` | String   | `EXEC etl.usp_check_bronze_quality_load_<object>`, built from the object name |
| `current_silver_load_stored_procedure`  | String   | `EXEC etl.usp_load_silver_<object>`, built from the object name      |
| `current_sp_gold_fact_sales_order`      | String   | `EXEC etl.usp_load_gold_fact_sales_order`                            |

The procedure names are built from `current_object_name`, so **a new object is added with one row in `etl.config` and no change to the package**.

## Design notes

- **Release 1**: the pipeline is intentionally simple. Next steps are error handling, closing the run in `etl.batch` and the incremental load of the Gold fact.
- **Metadata-driven**: the list of objects, their order and their load type come from `etl.config`.
- **One batch per run**: every object of the run shares the same `batch_id`, so the rows, the quality findings and the log can be traced to one execution.
- **The load type is decided by the procedure**, not by the package: the Silver procedure reads `full` or `incremental` from `etl.config`.
- **Refresh**: the package is started by SQL Server Agent, outside business hours (see the [Source System Analysis](Source_System_Analysis.md)).
