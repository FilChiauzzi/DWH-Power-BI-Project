# Data Quality

How the data warehouse checks its data and records every problem, following Kimball, *The Data Warehouse Toolkit* (3rd ed.), Chapter 19: subsystem 4 (Data Cleansing System) and subsystem 5 (Error Event Schema).

---

## 1. Quality screens (subsystem 4)

Every object has a quality check, `etl.usp_check_bronze_quality_load_<object>`, that runs **before** its Silver load, on the Bronze rows Silver is about to load. Each rule is a **screen**: a test that does nothing when it passes and records an error when it fails.

| Screen type | Tests | Examples |
|---|---|---|
| **Column** | One column | `ProductID` NULL, `OrderQty <= 0` |
| **Structure** | Relationships | Duplicated key; the parent exists in Silver |
| **Business rule** | Rules between values, aggregate thresholds | `DueDate < OrderDate`; more than 20% of the rows set aside; Bronze reconciliation |

## 2. Response policy

| Severity | When | Response (`exception_action`) |
|---|---|---|
| `critical` | Key NULL or duplicated: the row cannot be identified | `set aside` in `etl.rejected`: it does not reach Silver |
| `error` | The row is identified, a value is wrong | `pass`: the row goes on (a missing parent becomes `-1`) |
| `warning` | Suspicious but plausible | `pass` |
| *threshold* | Too many rows set aside, empty source, mass delete | The pipeline **halts** (THROW) |

## 3. Error event schema (subsystem 5)

Kimball's Figure 19-1:

| Kimball | Data warehouse |
|---|---|
| Date Dimension | `gold.dwh_dim_calendar` (`date_id` = yyyymmdd) |
| Batch Dimension | `etl.batch` |
| Screen Dimension | `etl.dim_screen`: `screen_type`, `etl_module`, `screen_processing_definition`, `severity`, `exception_action` |
| Error Event Fact | `etl.fact_error_event`: every error thrown by a screen (`error_event_date_key`, `screen_key`, `batch_key`, `error_date_time`, `severity_score`) |
| Error Event Detail Fact | `etl.fact_error_event_detail`: every field of the record that took part in the error (`table_key`, `field_key`, `record_identifier_key`, `error_condition`) |
| (tables, fields) | `etl.dim_table`, `etl.dim_field` |

## 4. Metadata-driven: nothing is written by hand

A screen **describes itself** when it writes an error:

```sql
INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
SELECT 'OrderQty invalid', 'column', 'error', 'pass', 'OrderQty',
       CONCAT(b.SalesOrderID, '-', b.SalesOrderDetailID), 'OrderQty <= 0'
FROM #b AS b
WHERE b.OrderQty <= 0;
```

At the end of the check, one call saves everything:

```sql
EXEC etl.usp_save_error_events @batch_id = @batch_id, @layer = 'bronze',
     @object_name = 'sales_order_detail', @etl_module = @proc_name;
```

| Step of `etl.usp_save_error_events` | Result |
|---|---|
| 1 | The table is added to `etl.dim_table` if new |
| 2 | Each screen is added to `etl.dim_screen` if new, updated if known: the code is the truth |
| 3 | Each field named in `fields` is added to `etl.dim_field` if new |
| 4 | One row per error in `etl.fact_error_event` |
| 5 | One row per field of each error in `etl.fact_error_event_detail` (`(whole record)` when no field is named) |

A new screen needs **no** change outside its procedure: it appears in the catalog the first time it finds an error.

Example: a wrong `LineTotal` on line 43659-1 gives one error event and four detail rows (`LineTotal`, `UnitPrice`, `UnitPriceDiscount`, `OrderQty`).

## 5. Measuring quality over time

```sql
SELECT error_date, object_name, screen_type, severity, SUM(error_events) AS events
FROM etl.vw_error_event_daily
GROUP BY error_date, object_name, screen_type, severity
ORDER BY error_date DESC;
```

## 6. Audit dimension (subsystem 6)

Kimball's Figure 19-2: a special dimension attached to **every fact row**, that tells **how it was loaded**. The metadata of the load become real data, filterable in Power BI.

| Attribute of `gold.dim_audit` | Value | Comes from |
|---|---|---|
| `overall_quality_rating` | Perfect / Warning / Error | The worst of the flags below |
| `complete_flag` | Complete / Incomplete | A dimension key of the row is the unknown member `-1` |
| `validation_flag` | Valid / Invalid | A **structure** screen found an error (missing parent) |
| `out_of_bounds_flag` | In bounds / Out of bounds | A **column** or **business rule** screen found an error |
| `screen_failed_flag` | No screen failed / Screen failed | Any screen fired on the order header or line |
| `record_modified_flag` | Not modified / Modified | A **warning** fired: Silver repaired the value |
| `etl_master_version_number` | 2.0 | Constant in `etl.usp_load_gold_fact_sales_order` |
| `allocation_version_number` | 1.0 | Version of the `total_due` allocation rule |
| `batch_id` | The run | The batch that loaded the fact |

The errors are read from the error event schema: the errors of the Silver header and line **in the batch that loaded them**. One `gold.dim_audit` row per **combination** of flags met in a run: a perfect run creates one row, attached to all the fact rows; an order line with a wrong quantity gets one more row.

```sql
SELECT a.overall_quality_rating, a.out_of_bounds_flag, COUNT(*) AS fact_rows, SUM(f.total_due) AS total_due
FROM gold.fact_sales_order AS f
JOIN gold.dim_audit AS a ON a.audit_key = f.audit_key
GROUP BY a.overall_quality_rating, a.out_of_bounds_flag;
```

## 7. Scripts

| Script | Content |
|---|---|
| `Scripts/ETL/Error_Event/ETL_Rejected_Table.sql` | The quarantine |
| `Scripts/ETL/Error_Event/ETL_Error_Event_Schema.sql` | Dimensions, facts, `etl.usp_save_error_events`, `etl.vw_error_event_daily` |
| `Scripts/Silver/Coding/Check_Bronze_Quality/*.sql` | The 15 quality checks |
| `Scripts/Gold/Gold_DDL.sql` | `gold.dim_audit` and the fact with `audit_key` |
| `Scripts/Gold/Load_Fact_Sales_Order.sql` | Computes the flags and attaches the audit row to every fact row |
