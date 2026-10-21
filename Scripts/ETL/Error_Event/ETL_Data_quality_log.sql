/*
=========================================================
ETL DDL – Data Quality Log Table
=========================================================
Purpose:
    Creates etl.data_quality_log, which stores every finding
    of the Bronze quality checks (warnings included): one
    row per issue, with batch, layer, object, check name,
    record identifier, description and severity. dq_log_key
    is an identity primary key and detected_at is filled
    automatically.

SSIS Usage:
    Not executed by SSIS. Run once at deployment. The
    usp_check_bronze_quality_load_* procedures write into
    this table.

Warning:
    Running this script DROPS and recreates the table: all
    quality history is lost.
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
    dq_log_key BIGINT IDENTITY(1,1) PRIMARY KEY,   -- unique log entry
    batch_id BIGINT NULL,                          -- ETL run identifier
    layer VARCHAR(10) NULL,                        -- bronze / silver
    object_name VARCHAR(50) NULL,                  -- product / category / etc.
    check_name NVARCHAR(200) NULL,                 -- validation rule name
    record_id NVARCHAR(200) NULL,                  -- failing record identifier
    description NVARCHAR(500) NULL,                -- detailed issue description
    severity VARCHAR(20) NULL,                     -- warning / error / critical
    detected_at DATETIME2(3) NOT NULL              -- timestamp of detection
        DEFAULT(SYSDATETIME())
);
GO

