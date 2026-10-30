/*
=========================================================
ETL DDL – Batch Tracking Table (etl.batch)
=========================================================
Purpose:
    Creates etl.batch, the table that tracks every ETL
    execution. Each row is one run of the Bronze, Silver and
    Gold pipeline, with its start time, end time, the login
    that started it, optional notes and the final status
    (running, succeeded, failed, aborted). The identity
    batch_id groups all the loads of a run and links them to
    etl.load_audit, the error event schema and etl.rejected.

SSIS Usage:
    Not executed by SSIS. The run is opened by
    etl.usp_start_batch (first task of the pipeline) and
    closed by etl.usp_end_batch (last task, or the OnError
    event handler), which fills ended_at and status.

Warning:
    None. The script never drops anything: it creates the
    table only if it does not exist, and on an existing
    table it adds the column status only if it is missing.
    It can be run again safely.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

---------------------------------------------------------
-- Create Table (new database)
---------------------------------------------------------
IF OBJECT_ID('etl.batch', 'U') IS NULL
BEGIN
    CREATE TABLE [etl].[batch](
        [batch_id]       BIGINT IDENTITY(1,1) NOT NULL,   -- unique batch identifier
        [started_at]     DATETIME2(3) NOT NULL            -- start timestamp
            DEFAULT(sysdatetime()),
        [ended_at]       DATETIME2(3) NULL,               -- end timestamp
        [triggered_by]   NVARCHAR(128) NULL,              -- login: SUSER_SNAME() returns up to 128 chars
        [notes]          NVARCHAR(500) NULL,              -- optional notes
        [status]         VARCHAR(20) NOT NULL             -- running / succeeded / failed / aborted
            CONSTRAINT DF_etl_batch_status DEFAULT ('running'),
        CONSTRAINT [PK_etl_batch] PRIMARY KEY CLUSTERED (
            [batch_id] ASC
        ),
        CONSTRAINT CK_etl_batch_status
            CHECK ([status] IN ('running', 'succeeded', 'failed', 'aborted'))
    ) ON [PRIMARY];
END
GO

---------------------------------------------------------
-- Existing table created before the column status
-- (database already deployed): add it once
---------------------------------------------------------
IF COL_LENGTH('etl.batch', 'status') IS NULL
BEGIN
    ALTER TABLE etl.batch
        ADD [status] VARCHAR(20) NOT NULL
            CONSTRAINT DF_etl_batch_status DEFAULT ('running');

    -- Runs created before this column existed were never
    -- closed: their outcome is unknown, so they are 'aborted'.
    -- Dynamic SQL: the new column is visible only after the
    -- ALTER has run, not when this batch is compiled.
    EXEC sp_executesql
        N'UPDATE etl.batch SET [status] = ''aborted'' WHERE ended_at IS NULL;';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_etl_batch_status')
BEGIN
    ALTER TABLE etl.batch
        ADD CONSTRAINT CK_etl_batch_status
            CHECK ([status] IN ('running', 'succeeded', 'failed', 'aborted'));
END
GO

---------------------------------------------------------
-- Existing table: triggered_by was NVARCHAR(100), but
-- SUSER_SNAME() returns up to 128 characters (sysname)
---------------------------------------------------------
IF COL_LENGTH('etl.batch', 'triggered_by') < 256      -- bytes: NVARCHAR(128) = 256
BEGIN
    ALTER TABLE etl.batch
        ALTER COLUMN [triggered_by] NVARCHAR(128) NULL;
END
GO
