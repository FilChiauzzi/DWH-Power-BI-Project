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
    None. The table is created only if it does not exist,
    so the run history is kept.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

---------------------------------------------------------
-- Create Table (only if it does not exist)
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
    );
END
GO
