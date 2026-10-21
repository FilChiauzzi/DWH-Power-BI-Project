/*
=========================================================
Gold DDL – Small Dimensions
=========================================================
Purpose:
    Creates and fills the small Gold dimensions that do not
    come from Silver:

    - gold.dwh_order_sales_status: status codes 1-6 plus
      Unknown (-1)
    - gold.dwh_order_online_order: order channel 0/1 plus
      Unknown (-1)
    - gold.dwh_dim_calendar: one row per day, from the first
      to the last order_date of gold.fact_sales_order

SSIS Usage:
    Not executed by SSIS. Run once at deployment. Re-run the
    calendar part when the fact covers new dates.

Warning:
    Running this script DROPS and recreates the three
    tables. The calendar is built from
    gold.fact_sales_order, so the fact table must already
    contain data.
*/

USE [DWH_AdventureWorks2025];
GO

-----------------------------------------------------------
-- Cteate Dim Order Status
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
        CONSTRAINT DF_gold_dwh_order_sales_status_load   DEFAULT SYSDATETIME(),
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
-- Cteate Dim Order Online Order Flag
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
        CONSTRAINT DF_gold_dwh_order_online_order_load   DEFAULT SYSDATETIME(),
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
-- Create Dim CAlendar
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
-----------------------------------------------------------
-- Populate Dim Calendar based on gold.fact_sales_order
-----------------------------------------------------------

DECLARE @StartDate DATE;
DECLARE @EndDate   DATE;

SELECT 
    @StartDate = MIN(order_date),
    @EndDate   = MAX(order_date)
FROM gold.fact_sales_order;

;WITH cte AS (
    SELECT @StartDate AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM cte
    WHERE d < @EndDate
)
INSERT INTO gold.dwh_dim_calendar (
    date_id,
    full_date,
    year,
    quarter,
    quarter_name,
    month,
    month_name,
    month_short_name,
    day,
    day_name,
    day_short_name,
    week_of_year,
    is_weekend
)
SELECT
    CONVERT(INT, FORMAT(d, 'yyyyMMdd')) AS date_id,
    d AS full_date,
    YEAR(d) AS year,
    DATEPART(QUARTER, d) AS quarter,
    CONCAT('Q', DATEPART(QUARTER, d)) AS quarter_name,
    MONTH(d) AS month,
    DATENAME(MONTH, d) AS month_name,
    LEFT(DATENAME(MONTH, d), 3) AS month_short_name,
    DAY(d) AS day,
    DATENAME(WEEKDAY, d) AS day_name,
    LEFT(DATENAME(WEEKDAY, d), 3) AS day_short_name,
    DATEPART(WEEK, d) AS week_of_year,
    CASE WHEN DATENAME(WEEKDAY, d) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS is_weekend
FROM cte
OPTION (MAXRECURSION 32767);
GO

