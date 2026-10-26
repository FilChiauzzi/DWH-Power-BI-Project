/*
=========================================================
Gold View – Dim Sales Person
=========================================================
Purpose:
    Exposes the Sales Person dimension for reporting
    (gold.vw_dim_sales_person). It reads every version of
    the SCD2 table silver.erp_sales_person and adds the
    person name, the employee attributes (job title, hire
    date, gender, birth date), the territory and the
    country, together with quota, bonus, commission and
    sales figures.

    Deleted members are kept, not filtered: facts of the past
    must still find them. is_deleted = 1 marks a member
    deleted in the source (soft delete in Silver).

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

CREATE OR ALTER VIEW [gold].[vw_dim_sales_person] AS
SELECT
    sp.sales_person_sk,
    sp.person_id,
    sp.employee_sk,
    sp.territory_sk,
    p.person_name,
    e.job_title,
    e.hire_date,
    e.gender,
    e.birth_date,
    st.territory_name,
    st.continent_name,
    c.country_name,
    sp.sales_quota,
    sp.bonus,
    sp.commission_pct,
    sp.sales_ytd,
    sp.sales_last_year,
    sp.dwh_start_date,
    sp.dwh_end_date,
    sp.is_active,
    sp.dwh_is_deleted AS is_deleted         -- 1 = deleted in the source (soft delete)
FROM silver.erp_sales_person sp
LEFT JOIN silver.erp_person p
       ON sp.person_id = p.person_id
LEFT JOIN silver.erp_employee e
       ON sp.employee_sk = e.employee_sk
LEFT JOIN silver.erp_sales_territory st
       ON sp.territory_sk = st.territory_sk
LEFT JOIN silver.erp_country_region c
       ON st.country_sk = c.country_sk;
GO


