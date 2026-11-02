/*
=========================================================
Gold DDL – Audit Dimension and Fact Table
=========================================================
Purpose:
    Creates gold.dim_audit, the audit dimension of Kimball
    subsystem 6 (Figure 19-2): one row per combination of
    quality flags met in a run, attached to every fact row.
    It answers "how was this fact row loaded?".

    Creates gold.fact_sales_order at order-line grain: the
    primary key is sales_order_id + sales_order_detail_id.
    The table holds dates, keys to the dimensions, the
    measures (quantity, prices, line_total, total_due) and
    the technical columns (row_hash, dwh_create_date,
    dwh_load_date, dwh_batch_id) and audit_key.

SSIS Usage:
    Not executed by SSIS. Run once at deployment. The table
    is filled by etl.usp_load_gold_fact_sales_order.

Warning:
    Running this script DROPS gold.fact_sales_order and
    gold.dim_audit and recreates them empty. A FULL load is
    needed afterwards.
*/

USE [DWH_AdventureWorks2025];
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
