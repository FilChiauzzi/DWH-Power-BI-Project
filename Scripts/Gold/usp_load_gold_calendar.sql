/*
=========================================================
Gold Procedure – etl.usp_load_gold_calendar
=========================================================
Purpose:
    Rebuilds gold.dwh_dim_calendar: one row per day, from the
    first to the last order_date of gold.fact_sales_order.
    The calendar follows the fact automatically: new order
    dates mean new calendar days.

SSIS Usage:
    Not called by SSIS directly: etl.usp_load_gold_fact_sales_order
    calls it after the fact is loaded.

Warning:
    Empties and refills the calendar. Does nothing if the
    fact is empty.
*/

USE [DWH_AdventureWorks2025];
GO

CREATE OR ALTER PROCEDURE etl.usp_load_gold_calendar
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM gold.fact_sales_order) RETURN;

    TRUNCATE TABLE gold.dwh_dim_calendar;

    DECLARE @StartDate DATE, @EndDate DATE;

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
END;
GO
