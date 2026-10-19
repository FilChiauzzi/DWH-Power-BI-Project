/*
=========================================================
ETL DDL – Batch Tracking Table (etl.batch)
=========================================================
Purpose:
    Creates etl.batch, the table that tracks every ETL
    execution. Each row is one run of the Bronze, Silver and
    Gold pipeline, with its start time, end time, the login
    that started it and optional notes. The identity
    batch_id groups all the loads of a run and links them to
    etl.data_quality_log and etl.rejected.

SSIS Usage:
    SSIS creates a new batch at the beginning of the
    pipeline with etl.usp_start_batch and passes the
    batch_id to every load procedure. Closing the run
    (ended_at) is not implemented yet.

Warning:
    None. The table is created only once: running the script
    again fails because etl.batch already exists.
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

---------------------------------------------------------
-- Create Table
---------------------------------------------------------
CREATE TABLE [etl].[batch](
    [batch_id]       BIGINT IDENTITY(1,1) NOT NULL,   -- unique batch identifier
    [started_at]     DATETIME2(3) NOT NULL            -- start timestamp
        DEFAULT(sysdatetime()),
    [ended_at]       DATETIME2(3) NULL,               -- end timestamp
    [triggered_by]   NVARCHAR(100) NULL,              -- user or agent
    [notes]          NVARCHAR(500) NULL,              -- optional notes
    CONSTRAINT [PK_etl_batch] PRIMARY KEY CLUSTERED (
        [batch_id] ASC
    )
) ON [PRIMARY];
GO
