/*
=========================================================
ETL DDL – Source Profiling table (etl.data_profile)
=========================================================
Purpose:
    Creates etl.data_profile: one row per column of a source
    table per profiling run (Kimball subsystem 1). Filled by
    etl.usp_profile_source (usp_profile_source.sql).

SSIS Usage:
    Not executed by SSIS. Run once at deployment.

Warning:
    None. The table is created only if it does not exist,
    so the profiling history is kept.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Table (created only if it does not exist: history is kept)
---------------------------------------------------------
IF OBJECT_ID('etl.data_profile', 'U') IS NULL
BEGIN
    CREATE TABLE etl.data_profile (
        profile_key     BIGINT IDENTITY(1,1) NOT NULL,
        profile_run_id  BIGINT          NOT NULL,   -- one run = one execution of the procedure
        profiled_at     DATETIME2(3)    NOT NULL
            CONSTRAINT DF_etl_data_profile_profiled_at DEFAULT SYSDATETIME(),
        object_name     VARCHAR(50)     NOT NULL,
        source_table    VARCHAR(200)    NOT NULL,
        column_id       INT             NOT NULL,
        column_name     SYSNAME         NOT NULL,
        data_type       SYSNAME         NOT NULL,
        is_nullable     BIT             NOT NULL,   -- as declared in the source
        row_count       BIGINT          NULL,
        null_count      BIGINT          NULL,
        null_pct        DECIMAL(5,2)    NULL,
        distinct_count  BIGINT          NULL,
        min_value       NVARCHAR(400)   NULL,
        max_value       NVARCHAR(400)   NULL,
        max_length      INT             NULL,       -- text columns only
        CONSTRAINT PK_etl_data_profile PRIMARY KEY CLUSTERED (profile_key)
    );

    CREATE INDEX IX_etl_data_profile_run
        ON etl.data_profile (profile_run_id, object_name, column_id);
END;
GO
