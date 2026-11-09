# Installation Order

How to build the data warehouse on an **empty** SQL Server, with the `AdventureWorks2025` database available and SQL Server Agent running (needed by CDC).

Run the scripts **one at a time**, in this order, and check the Messages tab after each one.

| Prefix | Content |
|---|---|
| `DDL_` | Structure: tables, views, sequences |
| `usp_` | One stored procedure per file |

---

## 1. Database and configuration

| # | Script | Creates |
|---:|---|---|
| 1 | `Scripts/ETL/DDL_ETL_Database.sql` | The database and the schemas `etl`, `bronze`, `silver`, `gold` |
| 2 | `Scripts/ETL/DDL_ETL_Config.sql` | `etl.config`: the 15 objects, their load type and `is_cdc_enabled` |
| 3 | `Scripts/ETL/CDC/Enable_CDC_Source.sql` | CDC on the source tables with `is_cdc_enabled = 1` (runs on `AdventureWorks2025`) |

## 2. Run log

| # | Script | Creates |
|---:|---|---|
| 4 | `Scripts/ETL/Audit/DDL_ETL_Batch.sql` | `etl.batch`: one row per run |
| 5 | `Scripts/ETL/Audit/DDL_ETL_Load_Audit.sql` | `etl.load_audit`: one row per step; `etl.vw_last_successful_load` |
| 6 | `Scripts/ETL/Audit/usp_start_batch.sql` | `etl.usp_start_batch` |
| 7 | `Scripts/ETL/Audit/usp_end_batch.sql` | `etl.usp_end_batch` |
| 8 | `Scripts/ETL/Audit/usp_audit_start.sql` | `etl.usp_audit_start` |
| 9 | `Scripts/ETL/Audit/usp_audit_end.sql` | `etl.usp_audit_end` |

## 3. Data quality and profiling

| # | Script | Creates |
|---:|---|---|
| 10 | `Scripts/ETL/Error_Event/DDL_ETL_Rejected.sql` | `etl.rejected`: the quarantine |
| 11 | `Scripts/ETL/Error_Event/DDL_ETL_Error_Event.sql` | The error event schema (Kimball subsystem 5) and `etl.vw_error_event_daily` |
| 12 | `Scripts/ETL/Error_Event/usp_save_error_events.sql` | `etl.usp_save_error_events` |
| 13 | `Scripts/ETL/Profiling/DDL_ETL_Data_Profile.sql` | `etl.data_profile` |
| 14 | `Scripts/ETL/Profiling/usp_profile_source.sql` | `etl.usp_profile_source` |

## 4. Tables of the three layers

| # | Script | Creates |
|---:|---|---|
| 15 | `Scripts/Bronze/DDL_Bronze.sql` | The 15 Bronze tables |
| 16 | `Scripts/Silver/DDL_Silver.sql` | The 15 Silver tables and their sequences |
| 17 | `Scripts/Gold/DDL_Gold.sql` | Status and channel dimensions (filled), calendar, `gold.dim_audit`, `gold.fact_sales_order` |
| 18 | `Scripts/Gold/DDL_View_Dim_*.sql` (5 files) | The dimension views on Silver |

## 5. Load procedures

| # | Script | Creates |
|---:|---|---|
| 19 | `Scripts/Bronze/usp_load_bronze_cdc.sql` | `etl.usp_load_bronze_cdc` |
| 20 | `Scripts/Bronze/usp_load_bronze.sql` | `etl.usp_load_bronze` (calls the CDC load) |
| 21 | `Scripts/Silver/Data_Quality/*.sql` (15 files) | One quality check per entity |
| 22 | `Scripts/Silver/Load/*.sql` (15 files) | One Silver load per entity |
| 23 | `Scripts/Gold/usp_load_gold_calendar.sql` | `etl.usp_load_gold_calendar` |
| 24 | `Scripts/Gold/usp_load_gold_fact_sales_order.sql` | `etl.usp_load_gold_fact_sales_order` (also rebuilds the calendar) |

## 6. First load

| # | Action | Result |
|---:|---|---|
| 25 | Run the SSIS package | Bronze, Silver and Gold loaded; the run is `succeeded` in `etl.batch` |
| 26 | Optional: `EXEC etl.usp_profile_source;` | The profile of the source in `etl.data_profile` |

## Checks after the first load

```sql
SELECT TOP 1 batch_id, status FROM etl.batch ORDER BY batch_id DESC;                 -- succeeded
SELECT layer, status, COUNT(*) AS steps FROM etl.load_audit
WHERE batch_id = (SELECT MAX(batch_id) FROM etl.batch) GROUP BY layer, status;       -- all succeeded
SELECT COUNT(*) AS fact_rows FROM gold.fact_sales_order;                             -- > 0
SELECT COUNT(*) AS calendar_days FROM gold.dwh_dim_calendar;                         -- > 0
```
