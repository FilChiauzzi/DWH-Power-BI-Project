/*
=========================================================
Gold View – Dim Ship Method
=========================================================
Purpose:
    Exposes the Ship Method dimension for reporting
    (gold.vw_dim_ship_method), read from
    silver.erp_ship_method. The creation date is cast to
    DATE.

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

CREATE OR ALTER VIEW [gold].[vw_ship_method] AS
(
SELECT [ship_method_sk]
      ,[ship_method_id]
      ,[ship_name]
      ,[ship_base]
      ,[ship_rate]
      ,CAST( [dwh_create_date] AS DATE ) AS [dwh_create_date]
  FROM [DWH_AdventureWorks2025].[silver].[erp_ship_method]
  )
GO


