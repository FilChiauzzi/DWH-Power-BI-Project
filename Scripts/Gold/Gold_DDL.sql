/*
=========================================================
Gold DDL – Fact Table (fact_sales_order)
=========================================================
Purpose:
    Creates gold.fact_sales_order at order-line grain: the
    primary key is sales_order_id + sales_order_detail_id.
    The table holds dates, keys to the dimensions, the
    measures (quantity, prices, line_total, total_due) and
    the technical columns (row_hash, dwh_create_date,
    dwh_load_date, dwh_batch_id).

SSIS Usage:
    Not executed by SSIS. Run once at deployment. The table
    is filled by etl.usp_load_gold_fact_sales_order.

Warning:
    Running this script DROPS gold.fact_sales_order and
    recreates it empty. A FULL load is needed afterwards.
*/

USE [DWH_AdventureWorks2025];
GO

IF OBJECT_ID('gold.fact_sales_order', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales_order;
GO

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
    dwh_batch_id           BIGINT        NULL
);

ALTER TABLE gold.fact_sales_order
    ADD CONSTRAINT PK_gold_fact_sales_order
        PRIMARY KEY (sales_order_id, sales_order_detail_id);
GO
