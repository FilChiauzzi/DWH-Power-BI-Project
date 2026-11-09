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
| 21 | `Scripts/Silver/Data_Quality/` (15 files, order below) | One quality check per entity |
| 22 | `Scripts/Silver/Load/` (15 files, order below) | One Silver load per entity |
| 23 | `Scripts/Gold/usp_load_gold_calendar.sql` | `etl.usp_load_gold_calendar` |
| 24 | `Scripts/Gold/usp_load_gold_fact_sales_order.sql` | `etl.usp_load_gold_fact_sales_order` (also rebuilds the calendar) |

### The Silver files, one by one

Run them in the `load_order` of `etl.config`: **parents first, children after**. A quality check looks for the parent in Silver (structure screens: "the category exists?"), and a Silver load looks up the surrogate key of the parent (`category_sk`, `customer_sk`…).

| Parent | Must come before | Why |
|---|---|---|
| category | subcategory | A subcategory belongs to a category |
| subcategory | product | A product belongs to a subcategory |
| store, person, sales_territory | customer | A customer is a person or a store, in a territory |
| person | employee | An employee is a person |
| employee, sales_territory | sales_person | A sales person is an employee, in a territory |
| country_region, sales_territory | state_province | A state belongs to a country and a territory |
| state_province | person_address | An address is in a state |
| customer, sales_person, sales_territory, ship_method | sales_order_header | An order has a customer, a sales person, a territory, a ship method |
| sales_order_header, product | sales_order_detail | An order line belongs to an order and has a product |

**Step 21 – `Scripts/Silver/Data_Quality/`**

| # | load_order | File |
|---:|---:|---|
| 21.1 | 10 | `usp_check_bronze_quality_load_category.sql` |
| 21.2 | 20 | `usp_check_bronze_quality_load_subcategory.sql` |
| 21.3 | 30 | `usp_check_bronze_quality_load_product.sql` |
| 21.4 | 40 | `usp_check_bronze_quality_load_store.sql` |
| 21.5 | 50 | `usp_check_bronze_quality_load_country_region.sql` |
| 21.6 | 60 | `usp_check_bronze_quality_load_person.sql` |
| 21.7 | 70 | `usp_check_bronze_quality_load_sales_territory.sql` |
| 21.8 | 80 | `usp_check_bronze_quality_load_customer.sql` |
| 21.9 | 90 | `usp_check_bronze_quality_load_employee.sql` |
| 21.10 | 100 | `usp_check_bronze_quality_load_sales_person.sql` |
| 21.11 | 110 | `usp_check_bronze_quality_load_ship_method.sql` |
| 21.12 | 120 | `usp_check_bronze_quality_load_state_province.sql` |
| 21.13 | 130 | `usp_check_bronze_quality_load_person_address.sql` |
| 21.14 | 140 | `usp_check_bronze_quality_load_sales_order_header.sql` |
| 21.15 | 150 | `usp_check_bronze_quality_load_sales_order_detail.sql` |

**Step 22 – `Scripts/Silver/Load/`**

| # | load_order | File |
|---:|---:|---|
| 22.1 | 10 | `usp_load_silver_category.sql` |
| 22.2 | 20 | `usp_load_silver_subcategory.sql` |
| 22.3 | 30 | `usp_load_silver_product.sql` |
| 22.4 | 40 | `usp_load_silver_store.sql` |
| 22.5 | 50 | `usp_load_silver_country_region.sql` |
| 22.6 | 60 | `usp_load_silver_person.sql` |
| 22.7 | 70 | `usp_load_silver_sales_territory.sql` |
| 22.8 | 80 | `usp_load_silver_customer.sql` |
| 22.9 | 90 | `usp_load_silver_employee.sql` |
| 22.10 | 100 | `usp_load_silver_sales_person.sql` |
| 22.11 | 110 | `usp_load_silver_ship_method.sql` |
| 22.12 | 120 | `usp_load_silver_state_province.sql` |
| 22.13 | 130 | `usp_load_silver_person_address.sql` |
| 22.14 | 140 | `usp_load_silver_sales_order_header.sql` |
| 22.15 | 150 | `usp_load_silver_sales_order_detail.sql` |

Creating the procedures in another order would not give an error (SQL Server checks the objects a procedure uses only when it runs), but following `load_order` keeps one rule for everything: creating, running, reading the code.

### The Gold files

| # | File | Needs |
|---:|---|---|
| 17 | `DDL_Gold.sql` | Nothing: the Gold tables |
| 18.1 | `DDL_View_Dim_Product.sql` | Silver product, subcategory, category |
| 18.2 | `DDL_View_Dim_Customer.sql` | Silver customer, person, store, territory, country |
| 18.3 | `DDL_View_Dim_Sales_Person.sql` | Silver sales person, employee, person, territory, country |
| 18.4 | `DDL_View_Dim_Geography.sql` | Silver address, state, country, territory |
| 18.5 | `DDL_View_Dim_Ship_Method.sql` | Silver ship method |
| 23 | `usp_load_gold_calendar.sql` | The calendar table |
| 24 | `usp_load_gold_fact_sales_order.sql` | Silver header and detail, `gold.dim_audit`, the calendar procedure |

A view needs its Silver tables **when it is created** (step 16 has created them): unlike a procedure, a view that uses a missing table gives an error.

## 6. First load

| # | Action | Result |
|---:|---|---|
| 25 | Run the SSIS package | Bronze, Silver and Gold loaded; the run is `succeeded` in `etl.batch`. The package follows the same `load_order`: for each object Bronze load → quality check → Silver load, then the fact |
| 26 | Optional: `EXEC etl.usp_profile_source;` | The profile of the source in `etl.data_profile` |

## Checks after the first load

```sql
SELECT TOP 1 batch_id, status FROM etl.batch ORDER BY batch_id DESC;                 -- succeeded
SELECT layer, status, COUNT(*) AS steps FROM etl.load_audit
WHERE batch_id = (SELECT MAX(batch_id) FROM etl.batch) GROUP BY layer, status;       -- all succeeded
SELECT COUNT(*) AS fact_rows FROM gold.fact_sales_order;                             -- > 0
SELECT COUNT(*) AS calendar_days FROM gold.dwh_dim_calendar;                         -- > 0
```
