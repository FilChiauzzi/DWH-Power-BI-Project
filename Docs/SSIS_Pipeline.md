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
| 6    | **End Batch**           | Closes the run: `EXEC etl.usp_end_batch ?, 'succeeded'` (parameter: `batch_id`)                     |

Steps 1 and 2 are independent; Bronze starts when both have finished. Each step starts only when the previous one succeeded: a failure stops the pipeline.

**OnError event handler** (package level): `EXEC etl.usp_end_batch ?, 'failed'`, with the same `batch_id`. When any task fails, the run is closed as `failed` at once.

## Run log: who writes what

The package opens and closes the **run**; the procedures open and close their own **steps**.

| Who                         | Procedure         | Table              | When                                         |
|-----------------------------|-------------------|--------------------|----------------------------------------------|
| SSIS, Start Batch           | `usp_start_batch` | `etl.batch`        | First task: new run `running`; runs left open become `aborted` |
| every load and quality check | `usp_audit_start` | `etl.load_audit`   | Start of the procedure: step `running`       |
| every load and quality check | `usp_audit_end`   | `etl.load_audit`   | End of the procedure: `succeeded` or `failed`, counts, error |
| SSIS, End Batch / OnError   | `usp_end_batch`   | `etl.batch`        | Last task or failure: run `succeeded` or `failed` |

`usp_end_batch` does not trust the package alone: the run is `succeeded` only if SSIS says so **and** no step of `etl.load_audit` failed. Steps still `running` (a procedure stopped by a timeout) are closed as `failed`.

A query on `etl.load_audit` for one `batch_id` shows the whole path of every object: Bronze load, quality check, Silver load, Gold load.

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

`batch_id` is also the parameter of End Batch and of the OnError event handler.

The procedure names are built from `current_object_name`, so **a new object is added with one row in `etl.config` and no change to the package**.

## Design notes

- **Release 1**: the pipeline is intentionally simple. Next steps are notifications on failure and the incremental load of the Gold fact.
- **Run log**: every run is closed in `etl.batch` and every step is logged in `etl.load_audit`, so a failure is found with one query, without opening the SSIS logs.
- **Metadata-driven**: the list of objects, their order and their load type come from `etl.config`.
- **One batch per run**: every object of the run shares the same `batch_id`, so the rows, the quality findings and the run log can be traced to one execution.
- **One run at a time**: Start Batch closes the runs still open as `aborted`, so two runs must never overlap.
- **The load type is decided by the procedure**, not by the package: the Silver procedure reads `full` or `incremental` from `etl.config`.
- **Refresh**: the package is started by SQL Server Agent, outside business hours (see the [Source System Analysis](Source_System_Analysis.md)).
