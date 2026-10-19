/*
=========================================================
Gold View – Dim Customer
=========================================================
Purpose:
    Exposes the Customer dimension for reporting
    (gold.vw_dim_customer). It reads every version of the
    SCD2 table silver.erp_customer and adds the person name,
    the store name, the continent and the country of the
    territory. customer_name is the person name for
    individual customers and the store name for all the
    others.

    The validity dates and is_active are kept, so facts can
    be joined to the version valid at the order date.

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


CREATE OR ALTER VIEW [gold].[vw_dim_customer] AS
SELECT 
      cs.customer_sk
    , cs.customer_id
    , cs.customer_type
    , CASE 
        WHEN cs.customer_type = 'individual customer'
            THEN pe.person_name
        ELSE st.store_name
      END AS customer_name
    , pe.person_name AS individual_name
    , st.store_name
    , te.continent_name
    , cr.country_name
    ,cs.dwh_start_date
    ,cs.dwh_end_date
    ,cs.is_active
FROM silver.erp_customer cs
LEFT JOIN silver.erp_person pe
       ON cs.person_sk = pe.person_sk
LEFT JOIN silver.erp_store st
       ON cs.store_sk = st.store_sk
LEFT JOIN silver.erp_sales_territory te
       ON cs.territory_sk = te.territory_sk
LEFT JOIN silver.erp_country_region cr
       ON te.country_sk = cr.country_sk
GO


