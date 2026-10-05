/*
=========================================================
Data Quality Log Table – Bronze/Silver Quality Framework
=========================================================
Purpose:
    This table stores all data quality issues detected
    during the stored procedure " usp_check_bronze_quality_load_* "
    Each stored procedure writes one row per issue,
    including severity, description, and record identifier.
Notes:
    - log_key is an identity primary key.
    - detect_at is automatically populated with GETDATE().
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

---------------------------------------------------------
-- Drop & Create Table
---------------------------------------------------------
IF OBJECT_ID('etl.data_quality_log', 'U') IS NOT NULL
    DROP TABLE etl.data_quality_log;
GO

CREATE TABLE etl.data_quality_log (
    log_key INT IDENTITY(1,1) PRIMARY KEY,
    layer NVARCHAR(50) NULL,                     -- bronze / silver
    object_name NVARCHAR(200) NULL,              -- product / category / subcategory
    check_name NVARCHAR(200) NULL,               -- name of the validation rule
    record_id NVARCHAR(200) NULL,                -- ID of the record that failed
    description NVARCHAR(500) NULL,              -- detailed explanation of the issue
    severity NVARCHAR(20) NULL,                  -- warning / error / critical
    detect_at DATETIME NULL DEFAULT(GETDATE())   -- timestamp of detection
);
GO
