/*
=========================================================
ETL DDL – Load Audit Table (etl.load_audit)
=========================================================
Purpose:
    Creates etl.load_audit, the run log of every step of
    the pipeline: one row per object, per layer, per step
    (bronze load, quality check, silver load, gold load) and
    per batch. Each row stores the status (running,
    succeeded, failed, skipped), start and end time, the
    duration, the row counts (read, inserted, updated,
    deleted, rejected), the watermark range processed and,
    on failure, the error details.

    etl.batch      -> one row per RUN       (the night)
    etl.load_audit -> one row per STEP      (what happened inside the run)

SSIS Usage:
    Not executed by SSIS. Run once at deployment, after
    ETL_Batch.sql (the foreign key on batch_id needs it).
    The load procedures write into this table through
    etl.usp_audit_start and etl.usp_audit_end
    (ETL_Audit_Procedures.sql).

Warning:
    None. The script never drops anything: it creates the
    table only if it does not exist, so it can be run again
    safely and the audit history is kept.
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
IF OBJECT_ID('etl.load_audit', 'U') IS NULL
BEGIN
    CREATE TABLE etl.load_audit (
        audit_id         BIGINT IDENTITY(1,1) NOT NULL,  -- unique audit entry
        batch_id         BIGINT         NOT NULL,        -- ETL run identifier
        layer            VARCHAR(10)    NOT NULL,        -- bronze / silver / gold
        object_name      VARCHAR(50)    NOT NULL,        -- product / sales_order_header / ...
        step_name        VARCHAR(30)    NOT NULL,        -- load / dq_check
        procedure_name   NVARCHAR(256)  NULL,            -- procedure that wrote the row
        load_type        VARCHAR(20)    NULL,            -- full / incremental

        status           VARCHAR(20)    NOT NULL         -- running / succeeded / failed / skipped
            CONSTRAINT DF_etl_load_audit_status DEFAULT ('running'),
        started_at       DATETIME2(3)   NOT NULL
            CONSTRAINT DF_etl_load_audit_started DEFAULT (SYSDATETIME()),
        ended_at         DATETIME2(3)   NULL,
        duration_ms      AS DATEDIFF(MILLISECOND, started_at, ended_at),  -- computed

        -- row counts (NULL = not applicable to the step)
        rows_read        INT            NULL,
        rows_inserted    INT            NULL,
        rows_updated     INT            NULL,
        rows_deleted     INT            NULL,
        rows_rejected    INT            NULL,

        -- watermark range processed by the step
        watermark_from   DATETIME2(3)   NULL,
        watermark_to     DATETIME2(3)   NULL,

        -- error details (filled only when status = failed)
        error_number     INT            NULL,
        error_line       INT            NULL,
        error_procedure  NVARCHAR(128)  NULL,
        error_message    NVARCHAR(4000) NULL,

        CONSTRAINT PK_etl_load_audit PRIMARY KEY CLUSTERED (audit_id),

        CONSTRAINT FK_etl_load_audit_batch
            FOREIGN KEY (batch_id) REFERENCES etl.batch (batch_id),

        CONSTRAINT CK_etl_load_audit_status
            CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),

        CONSTRAINT CK_etl_load_audit_layer
            CHECK (layer IN ('bronze', 'silver', 'gold')),

        CONSTRAINT CK_etl_load_audit_dates
            CHECK (ended_at IS NULL OR ended_at >= started_at)
    );

    -- "What happened in batch X?"
    CREATE INDEX IX_etl_load_audit_batch
        ON etl.load_audit (batch_id)
        INCLUDE (layer, object_name, step_name, status);

    -- "When did object Y last load successfully, and up to which watermark?"
    CREATE INDEX IX_etl_load_audit_object
        ON etl.load_audit (object_name, layer, step_name, status, ended_at)
        INCLUDE (batch_id, watermark_to);
END
GO

---------------------------------------------------------
-- View: last successful load of every object and step.
-- Useful for monitoring and as a reliable "bookmark" for
-- incremental loads (instead of MAX(dwh_load_date) of the
-- target table).
---------------------------------------------------------
CREATE OR ALTER VIEW etl.vw_last_successful_load
AS
SELECT
    a.layer,
    a.object_name,
    a.step_name,
    a.batch_id,
    a.load_type,
    a.started_at,
    a.ended_at,
    a.duration_ms,
    a.rows_read,
    a.rows_inserted,
    a.rows_updated,
    a.rows_rejected,
    a.watermark_to
FROM (
    SELECT
        la.*,
        ROW_NUMBER() OVER (
            PARTITION BY la.layer, la.object_name, la.step_name
            ORDER BY la.ended_at DESC, la.audit_id DESC
        ) AS rn
    FROM etl.load_audit AS la
    WHERE la.status = 'succeeded'
) AS a
WHERE a.rn = 1;
GO
