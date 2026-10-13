/*
=========================================================
ETL Framework - DDL
=========================================================
Creates the ETL support objects. Run this once, before the
stored procedures: they contain logic only, no DDL.

Objects:
    etl.batch              one row per pipeline run
    etl.rejected           quarantined rows (blocking issues)
    etl.data_quality_log   every check finding (append-only)

Safe to re-run: every object is created only if missing.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Schemas (guarded: SQL Server has no CREATE SCHEMA IF NOT EXISTS)
---------------------------------------------------------
IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl;');
IF SCHEMA_ID('dwh') IS NULL EXEC('CREATE SCHEMA dwh;');
GO

---------------------------------------------------------
-- etl.batch : one row per pipeline run.
-- The batch_id is created ONCE by SSIS at the start of the
-- run, and passed as an input to every stored procedure of
-- that run. So all objects of one night share one number.
---------------------------------------------------------
IF OBJECT_ID('etl.batch', 'U') IS NULL
BEGIN
    CREATE TABLE etl.batch (
        batch_id     BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_etl_batch PRIMARY KEY,
        started_at   DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
        ended_at     DATETIME2(3)  NULL,
        status       VARCHAR(20)   NOT NULL DEFAULT 'running',
        triggered_by NVARCHAR(100) NULL,
        notes        NVARCHAR(500) NULL,

        CONSTRAINT CK_etl_batch_status
            CHECK (status IN ('running', 'completed', 'failed'))
    );
    PRINT 'Table etl.batch created.';
END
ELSE
    PRINT 'Table etl.batch already exists.';
GO

---------------------------------------------------------
-- etl.rejected : the quarantine.
-- One row per blocking problem, with the original row
-- serialized in JSON so it can be seen and fixed.
-- replayed_at / replayed_batch_id mark the rows that have
-- been fixed and re-loaded (the queue is WHERE replayed_at IS NULL).
---------------------------------------------------------
IF OBJECT_ID('etl.rejected', 'U') IS NULL
BEGIN
    CREATE TABLE etl.rejected (
        rejected_key     BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_etl_rejected PRIMARY KEY,
        batch_id         BIGINT        NOT NULL,   -- the run that rejected the row
        layer            VARCHAR(10)   NOT NULL,   -- layer that detected the problem
        object_name      VARCHAR(50)   NOT NULL,
        business_key     NVARCHAR(200) NULL,       -- key of the rejected row, as text
        rule_code        VARCHAR(100)  NOT NULL,   -- short code, e.g. NULL_BK
        rule_description NVARCHAR(500) NULL,
        severity         VARCHAR(20)   NOT NULL DEFAULT 'error',
        raw_row          NVARCHAR(MAX) NULL,       -- original row, JSON
        rejected_at      DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),
        replayed_at      DATETIME2(3)  NULL,
        replayed_batch_id BIGINT       NULL
    );
    PRINT 'Table etl.rejected created.';
END
ELSE
    PRINT 'Table etl.rejected already exists.';
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'ix_etl_rejected_gate'
                 AND object_id = OBJECT_ID('etl.rejected'))
    CREATE INDEX ix_etl_rejected_gate
        ON etl.rejected (layer, object_name, batch_id)
        INCLUDE (business_key, replayed_at);
GO

---------------------------------------------------------
-- etl.data_quality_log : every finding, warnings included.
-- Append-only: never delete, so the quality trend is kept.
---------------------------------------------------------
IF OBJECT_ID('etl.data_quality_log', 'U') IS NULL
BEGIN
    CREATE TABLE etl.data_quality_log (
        dq_log_key   BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_etl_data_quality_log PRIMARY KEY,
        batch_id     BIGINT        NULL,           -- the run that produced the finding
        layer        VARCHAR(10)   NULL,
        object_name  VARCHAR(200)  NULL,
        check_name   NVARCHAR(200) NULL,
        record_id    NVARCHAR(200) NULL,
        description  NVARCHAR(500) NULL,
        severity     VARCHAR(20)   NULL,
        detected_at  DATETIME2(3)  NOT NULL DEFAULT SYSDATETIME(),

        CONSTRAINT CK_etl_dq_severity
            CHECK (severity IN ('warning', 'error', 'critical'))
    );
    PRINT 'Table etl.data_quality_log created.';
END
ELSE
BEGIN
    PRINT 'Table etl.data_quality_log already exists.';
END
GO

-- The table may already exist from an earlier version without
-- batch_id: add it if missing.
IF COL_LENGTH('etl.data_quality_log', 'batch_id') IS NULL
BEGIN
    ALTER TABLE etl.data_quality_log ADD batch_id BIGINT NULL;
    PRINT 'Column etl.data_quality_log.batch_id added.';
END
GO

---------------------------------------------------------
-- Verification
---------------------------------------------------------
SELECT t.name AS object_name, c.name AS column_name, ty.name AS data_type
FROM sys.columns AS c
JOIN sys.tables  AS t  ON t.object_id = c.object_id
JOIN sys.types   AS ty ON ty.user_type_id = c.user_type_id
WHERE SCHEMA_NAME(t.schema_id) = 'etl'
  AND t.name IN ('batch', 'rejected', 'data_quality_log')
ORDER BY t.name, c.column_id;
GO

