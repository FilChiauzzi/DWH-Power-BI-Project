# Data Quality

How the data warehouse checks its data and records every problem, following Kimball, *The Data Warehouse Toolkit* (3rd ed.), Chapter 19: subsystem 4 (Data Cleansing System) and subsystem 5 (Error Event Schema).

---

## 1. Quality screens (subsystem 4)

Every object has a quality check, `etl.usp_check_bronze_quality_load_<object>`, that runs **before** its Silver load, on the Bronze rows that Silver is about to load (batch greater than the Silver register, latest version of each key, deletions excluded).

Each rule is a **screen**: a test that does nothing when it passes and records an error event when it fails. Screens are of three types (Jack Olson's classification, adopted by Kimball):

| Type | Tests | Examples |
|---|---|---|
| **Column** | One column | `ProductID` NULL, `OrderQty <= 0`, empty name |
| **Structure** | Relationships between columns and tables | Duplicated key; the parent row exists in Silver (order line → header, order → customer, product → subcategory) |
| **Business rule** | Rules linking values, and aggregate thresholds | `DueDate < OrderDate`, `TotalDue` ≠ `SubTotal + TaxAmt + Freight`, discount above 100%; more than 20% of the rows set aside; Bronze volume checks |

## 2. Response policy

Kimball lists three responses — halt, suspend, tag — and recommends **tagging** whenever possible. One policy for every screen:

| Severity | When | Response | Kimball |
|---|---|---|---|
| `critical` | The row cannot be identified: business key NULL or duplicated | Recorded **and** set aside in `etl.rejected`: it does not reach Silver | Suspend |
| `error` | The row is identified, a value is wrong | Recorded, the row goes on (a missing parent becomes the Unknown member `-1`) | Tag |
| `warning` | A value is suspicious but plausible | Recorded, the row goes on | Tag |
| *aggregate threshold* | Too many rows set aside (error 51010), empty or dropped source, mass delete | The pipeline **halts** | Halt |

Screens are calibrated on the profiling (subsystem 1): a NULL that is normal for the business (for example the subcategory of a component) is not a screen.

## 3. Error event schema (subsystem 5)

The structure of Kimball's Figure 19-1:

```
gold.dwh_dim_calendar ─┐                      ┌─ etl.dim_screen
                       ├─ etl.fact_error_event ┤
etl.batch ─────────────┘          │           └─ (severity_score)
                                  │ error_event_key
                     etl.fact_error_event_detail ── etl.dim_table, etl.dim_field
```

| Kimball (Figure 19-1) | Data warehouse | Grain / content |
|---|---|---|
| Date Dimension | `gold.dwh_dim_calendar` (`date_id` = yyyymmdd) | One row per day |
| Batch Dimension | `etl.batch` (`batch_id`) | One row per pipeline run |
| Screen Dimension | `etl.dim_screen` | One row per screen: `screen_type`, `etl_module`, `screen_processing_definition`, `severity`, `exception_action` |
| Error Event Fact | `etl.fact_error_event` | **Every error thrown by a screen**: `error_event_key`, `error_event_date_key`, `screen_key`, `batch_key`, `error_date_time`, `severity_score` (3 critical, 2 error, 1 warning) |
| Error Event Detail Fact | `etl.fact_error_event_detail` | **Every field of a record that took part in the error**: `table_key`, `field_key`, `record_identifier_key`, `error_condition` |
| (Table, field) | `etl.dim_table`, `etl.dim_field` | The tables and fields checked by the screens |
| — | `etl.bridge_screen_field` | The fields tested by each screen (one detail row per field) |

Example: a wrong `LineTotal` on order line 43659-1 produces **one** error event and **four** detail rows (`LineTotal`, `UnitPrice`, `UnitPriceDiscount`, `OrderQty`).

Design notes:
- `record_identifier_key` holds the business key of the record as text (`43659-1`): the checked tables have different keys.
- `error_event_date_key` has no foreign key to the calendar: the calendar covers the order dates, not the ETL run dates.
- `etl.dim_screen` is filled by `ETL_Error_Event_Screens.sql`, generated from the code of the quality checks. A screen not yet in the catalog is registered automatically as `unclassified`.

## 4. Compatibility: `etl.data_quality_log`

`etl.data_quality_log` is a **view** on the error event facts, with the same columns as the original table (`dq_log_key`, `batch_id`, `layer`, `object_name`, `check_name`, `record_id`, `description`, `severity`, `detected_at`). An `INSTEAD OF INSERT` trigger turns every insert into one error event and its detail rows: the quality checks and the Bronze loads write to the star without any change, and the existing queries keep working.

## 5. Measuring quality over time

`etl.vw_error_event_daily`: error events per day, object, screen type, screen, severity and response. It answers Kimball's goal *capture all data quality errors and precisely measure data quality metrics over time*.

```sql
SELECT error_date, object_name, screen_type, severity, SUM(error_events) AS events
FROM etl.vw_error_event_daily
GROUP BY error_date, object_name, screen_type, severity
ORDER BY error_date DESC;
```

## 6. Scripts

| Script | Content |
|---|---|
| `Scripts/ETL/Error_Event/ETL_Rejected_Table.sql` | The quarantine |
| `Scripts/ETL/Error_Event/ETL_Error_Event_Schema.sql` | Dimensions, facts, compatibility view and trigger, monitoring view |
| `Scripts/ETL/Error_Event/ETL_Error_Event_Screens.sql` | The screen catalog |
| `Scripts/ETL/Error_Event/Migration_Error_Event.sql` | Moves the rows of the old table into the star (existing databases only) |
| `Scripts/Silver/Coding/Check_Bronze_Quality/*.sql` | The 15 quality checks |
