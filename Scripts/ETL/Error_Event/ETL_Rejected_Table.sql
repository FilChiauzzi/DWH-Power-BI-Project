/*
=========================================================
ETL DDL – Rejected Rows Table (etl.rejected)
=========================================================
Purpose:
    Creates etl.rejected, the quarantine of the blocking
    findings of the quality checks. Each row is one rejected
    record, with its business key, rule code, severity, a
    JSON copy of the raw row and the batch_id of the run.

    The columns replayed_at and replayed_batch_id are
    reserved to mark fixed records that are loaded again.

SSIS Usage:
    Not executed by SSIS. Run once at deployment. The
    usp_check_bronze_quality_load_* procedures insert the
    blocking findings into this table, and the Silver loads
    exclude the rows rejected for the same batch.

Warning:
    None. The table is created only once: running the script
    again fails because etl.rejected already exists.
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
CREATE TABLE [etl].[rejected](
    [rejected_key]        BIGINT IDENTITY(1,1) NOT NULL,   -- unique rejection entry
    [batch_id]            BIGINT NOT NULL,                 -- ETL run identifier
    [layer]               VARCHAR(10) NOT NULL,            -- bronze / silver
    [object_name]         VARCHAR(50) NOT NULL,            -- product / category / etc.
    [business_key]        NVARCHAR(200) NULL,              -- natural key of the rejected row
    [rule_code]           VARCHAR(100) NOT NULL,           -- validation rule identifier
    [rule_description]    NVARCHAR(500) NULL,              -- human-readable rule description
    [severity]            VARCHAR(20) NOT NULL,            -- error / critical
    [raw_row]             NVARCHAR(MAX) NULL,              -- full JSON payload of the rejected row
    [rejected_at]         DATETIME2(3) NOT NULL            -- timestamp of rejection
        DEFAULT(sysdatetime()),
    [replayed_at]         DATETIME2(3) NULL,               -- timestamp of replay (if any)
    [replayed_batch_id]   BIGINT NULL,                     -- batch_id of replay run
    CONSTRAINT [PK_etl_rejected] PRIMARY KEY CLUSTERED (
        [rejected_key] ASC
    )
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
GO

---------------------------------------------------------
-- Default Values
---------------------------------------------------------
ALTER TABLE [etl].[rejected]
    ADD DEFAULT ('error') FOR [severity];
GO
