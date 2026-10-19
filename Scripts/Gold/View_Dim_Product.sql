/*
=========================================================
Gold View – Dim Product
=========================================================
Purpose:
    Exposes the Product dimension for reporting
    (gold.vw_dim_product). It reads every version of the
    SCD2 table silver.erp_product and adds the subcategory
    and category names; a missing subcategory or category is
    shown as 'Unknown'. Validity dates are cast to DATE and
    is_active marks the current version.

SSIS Usage:
    Not executed by SSIS. Run once at deployment, after the
    Silver tables exist. The view is read directly by Power
    BI and analytical queries.

Warning:
    None. The script uses CREATE OR ALTER, so it can be re-
    run safely.
*/

USE [DWH_AdventureWorks2025]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER VIEW [gold].[vw_dim_product] AS
SELECT
    p.product_sk,
    p.product_id,
    p.product_name,
    p.product_number,
    p.color,
    p.size,
    p.weight,
    p.product_line_desc,
    p.class_desc,
    p.style_desc,
    COALESCE(sc.subcategory_name, 'Unknown') AS subcategory_name,
    COALESCE(c.category_name, 'Unknown') AS category_name,
    CAST(p.dwh_start_date AS DATE) AS dwh_start_date,
    CAST(p.dwh_end_date AS DATE) AS dwh_end_date,
    p.is_active
FROM silver.erp_product p
LEFT JOIN silver.erp_subcategory sc
    ON p.product_subcategory_sk = sc.subcategory_sk
LEFT JOIN silver.erp_category c
    ON sc.category_sk = c.category_sk;

GO
