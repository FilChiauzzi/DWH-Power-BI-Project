/*
=========================================================
Gold DDL – Tables
=========================================================
Purpose:
    Creates the Gold tables:
    - gold.dwh_order_sales_status: status codes 1-6 plus
      Unknown (-1), filled here (fixed list)
    - gold.dwh_order_online_order: order channel 0/1 plus
      Unknown (-1), filled here (fixed list)
    - gold.dwh_dim_calendar: one row per day, filled by
      etl.usp_load_gold_calendar
    - gold.dim_audit: the audit dimension of Kimball
      subsystem 6 (Figure 19-2), one row per combination of
      quality flags met in a run
    - gold.fact_sales_order: order-line grain (sales_order_id
      + sales_order_detail_id), with the keys to the
      dimensions, the measures and audit_key

    The other dimensions are views on Silver
    (DDL_DDL_View_Dim_*.sql).

SSIS Usage:
    Not executed by SSIS. Run once at deployment. The fact
    and the calendar are filled by
    etl.usp_load_gold_fact_sales_order.

Warning:
    Running this script DROPS the Gold tables and recreates
    them. A FULL load is needed afterwards.
*/

USE [DWH_AdventureWorks2025];
GO

-----------------------------------------------------------
-- Create Dim Order Status
-----------------------------------------------------------
IF OBJECT_ID('gold.dwh_order_sales_status', 'U') IS NOT NULL
    DROP TABLE gold.dwh_order_sales_status;

CREATE TABLE gold.dwh_order_sales_status (
    status_id              INT            NOT NULL,   -- business key
    status_description     NVARCHAR (30)  NULL,

    -- technical columns
    dwh_create_date        DATETIME2(3)   NOT NULL
        CONSTRAINT DF_gold_dwh_order_sales_status_create DEFAULT SYSDATETIME(),
    dwh_load_date          DATETIME2(3)   NOT NULL
        CONSTRAINT DF_gold_dwh_order_sales_status_load   DEFAULT SYSDATETIME()
);

ALTER TABLE gold.dwh_order_sales_status
    ADD CONSTRAINT PK_gold_dwh_order_sales_status PRIMARY KEY (status_id);

CREATE INDEX IX_dwh_order_sales_status_status_description
    ON gold.dwh_order_sales_status (status_description);
GO

INSERT INTO gold.dwh_order_sales_status (status_id, status_description )
    VALUES
        ( 1, 'In process' ),
        ( 2, 'Approved' ),
        ( 3, 'Backordered'),
        ( 4, 'Rejected' ),
        ( 5, 'Shipped' ),
        ( 6, 'Cancelled' ),
        (-1, 'Unknown');
GO

-----------------------------------------------------------
-- Create Dim Order Online Order Flag
-----------------------------------------------------------
IF OBJECT_ID('gold.dwh_order_online_order', 'U') IS NOT NULL
    DROP TABLE gold.dwh_order_online_order;

CREATE TABLE gold.dwh_order_online_order (
    online_order_id              INT            NOT NULL,   -- business key
    order_type_description     NVARCHAR (50)  NULL,

    -- technical columns
    dwh_create_date        DATETIME2(3)   NOT NULL
        CONSTRAINT DF_gold_dwh_order_online_order_create DEFAULT SYSDATETIME(),
    dwh_load_date          DATETIME2(3)   NOT NULL
        CONSTRAINT DF_gold_dwh_order_online_order_load   DEFAULT SYSDATETIME()
);

ALTER TABLE gold.dwh_order_online_order
    ADD CONSTRAINT PK_gold_dwh_order_online_order PRIMARY KEY (online_order_id);

CREATE INDEX IX_dwh_order_online_order_description
    ON gold.dwh_order_online_order (order_type_description);
GO

INSERT INTO gold.dwh_order_online_order (online_order_id, order_type_description )
    VALUES
        ( 0, 'Sales Person Ordered ' ),
        ( 1, 'Customer Online Ordered' ),
        (-1, 'Unknown');
GO

------------------------------------------------------------------
-- Create Dim Calendar
------------------------------------------------------------------
IF OBJECT_ID('gold.dwh_dim_calendar', 'U') IS NOT NULL
    DROP TABLE gold.dwh_dim_calendar;

CREATE TABLE gold.dwh_dim_calendar (
    date_id              INT           NOT NULL,
    full_date            DATE          NOT NULL,

    year                 SMALLINT      NOT NULL,
    quarter              TINYINT       NOT NULL,
    quarter_name         NVARCHAR(2)   NOT NULL,
    month                TINYINT       NOT NULL,
    month_name           NVARCHAR(20)  NOT NULL,
    month_short_name     NVARCHAR(3)   NOT NULL,
    day                  TINYINT       NOT NULL,
    day_name             NVARCHAR(20)  NOT NULL,
    day_short_name       NVARCHAR(3)   NOT NULL,
    week_of_year         TINYINT       NOT NULL,
    is_weekend           BIT           NOT NULL,

    dwh_create_date      DATETIME2(3)  NOT NULL
        CONSTRAINT DF_gold_dwh_dim_calendar_create DEFAULT SYSDATETIME(),
    dwh_load_date        DATETIME2(3)  NOT NULL
        CONSTRAINT DF_gold_dwh_dim_calendar_load   DEFAULT SYSDATETIME()
);

ALTER TABLE gold.dwh_dim_calendar
    ADD CONSTRAINT PK_gold_dwh_dim_calendar PRIMARY KEY (date_id);

CREATE INDEX IX_gold_dwh_dim_calendar_full_date
    ON gold.dwh_dim_calendar (full_date);
GO
DROP TABLE IF EXISTS gold.fact_sales_order;
DROP TABLE IF EXISTS gold.dim_audit;
GO

---------------------------------------------------------
-- Audit dimension (Kimball subsystem 6, Figure 19-2)
---------------------------------------------------------
CREATE TABLE gold.dim_audit (
    audit_key                  INT IDENTITY(1,1) NOT NULL,
    batch_id                   BIGINT       NULL,   -- the run that loaded the fact rows
    overall_quality_rating     VARCHAR(20)  NOT NULL,   -- Perfect / Warning / Error
    complete_flag              VARCHAR(20)  NOT NULL,   -- Complete / Incomplete (unknown member used)
    validation_flag            VARCHAR(20)  NOT NULL,   -- Valid / Invalid (structure screen failed)
    out_of_bounds_flag         VARCHAR(20)  NOT NULL,   -- In bounds / Out of bounds (column or business rule error)
    screen_failed_flag         VARCHAR(20)  NOT NULL,   -- No screen failed / Screen failed
    record_modified_flag       VARCHAR(20)  NOT NULL,   -- Not modified / Modified (warning: repaired in Silver)
    etl_master_version_number  VARCHAR(20)  NOT NULL,   -- version of the ETL code
    allocation_version_number  VARCHAR(20)  NOT NULL,   -- version of the total_due allocation rule
    dwh_create_date            DATETIME2(3) NOT NULL
        CONSTRAINT DF_gold_dim_audit_create DEFAULT SYSDATETIME(),
    CONSTRAINT PK_gold_dim_audit PRIMARY KEY (audit_key)
);
GO

-- Unknown member: fact rows loaded without an audit row
SET IDENTITY_INSERT gold.dim_audit ON;
INSERT INTO gold.dim_audit (audit_key, batch_id, overall_quality_rating, complete_flag, validation_flag,
                            out_of_bounds_flag, screen_failed_flag, record_modified_flag,
                            etl_master_version_number, allocation_version_number)
VALUES (-1, NULL, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown');
SET IDENTITY_INSERT gold.dim_audit OFF;
GO

---------------------------------------------------------
-- Fact table
---------------------------------------------------------

CREATE TABLE gold.fact_sales_order (
    -- Business keys
    sales_order_id         INT           NOT NULL,
    sales_order_detail_id  INT           NOT NULL,

    -- Dates (optimized as DATE)
    order_date             DATE          NULL,
    due_date               DATE          NULL,
    ship_date              DATE          NULL,
    modified_date          DATE          NULL,

    -- Dimension SKs
    status_id              INT           NULL,
    online_order_id        INT           NULL,
    customer_sk            INT           NULL,
    person_sk              INT           NULL,
    territory_sk           INT           NULL,
    product_sk             INT           NULL,

    -- Measures
    order_qty              SMALLINT      NULL,
    unit_price             DECIMAL(19,4) NULL,
    unit_price_discount    DECIMAL(5,4)  NULL,
    line_total             DECIMAL(19,4)   NULL,
    total_due              DECIMAL(19,4)   NULL,

    -- Technical
    row_hash               VARBINARY(32) NULL,
    dwh_create_date        DATETIME2(3)  NOT NULL
        CONSTRAINT DF_gold_fact_sales_order_create DEFAULT SYSDATETIME(),
    dwh_load_date          DATETIME2(3)  NOT NULL
        CONSTRAINT DF_gold_fact_sales_order_load   DEFAULT SYSDATETIME(),
    dwh_batch_id           BIGINT        NULL,
    audit_key              INT           NOT NULL
        CONSTRAINT DF_gold_fact_sales_order_audit DEFAULT -1,   -- gold.dim_audit
    CONSTRAINT FK_gold_fact_sales_order_audit FOREIGN KEY (audit_key) REFERENCES gold.dim_audit (audit_key)
);

ALTER TABLE gold.fact_sales_order
    ADD CONSTRAINT PK_gold_fact_sales_order
        PRIMARY KEY (sales_order_id, sales_order_detail_id);
GO
