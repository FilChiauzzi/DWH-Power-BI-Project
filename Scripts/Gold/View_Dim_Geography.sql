/*
=========================================================
Gold View – Dim Geography
=========================================================
Purpose:
    Exposes the Geography dimension for reporting
    (gold.vw_dim_geography). It starts from
    silver.erp_person_address and adds the state or
    province, the country and the sales territory, together
    with latitude and longitude.

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

CREATE OR ALTER VIEW [gold].[vw_dim_geography] AS
SELECT
    ---------------------------------------------------------
    -- Geography business keys (Address)
    ---------------------------------------------------------
    a.address_id,
    a.address_line1,
    a.address_line2,
    a.city,
    a.postal_code,
    a.latitude,
    a.longitude,

    ---------------------------------------------------------
    -- State/Province
    ---------------------------------------------------------
    sp.name                AS state_province_name,

    ---------------------------------------------------------
    -- Country
    ---------------------------------------------------------
    cr.country_name,

    ---------------------------------------------------------
    -- Territory
    ---------------------------------------------------------
    t.territory_name,

    ---------------------------------------------------------
    -- Technical fields (from Address)
    ---------------------------------------------------------
    a.dwh_create_date,
    a.dwh_load_date

FROM silver.erp_person_address AS a
LEFT JOIN silver.erp_state_province AS sp
    ON a.state_province_sk = sp.state_province_sk

LEFT JOIN silver.erp_country_region AS cr
    ON sp.country_region_sk = cr.country_sk

LEFT JOIN silver.erp_sales_territory AS t
    ON sp.territory_sk = t.territory_sk
GO


