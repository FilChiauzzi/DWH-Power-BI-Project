/*
=========================================================
Bronze Data Quality Check - Category  (final)
=========================================================
Purpose:
    Validates the Bronze data that feeds the Silver category
    dimension and records the outcome in the quality framework.

The three steps, two tables:
    1. DETECT      evaluate the rules on bronze.erp_category
    2. LOG         every finding -> etl.data_quality_log
                   (append-only: it is the quality trend;
                   warnings included, they block nothing)
    3. QUARANTINE  the blocking findings -> etl.rejected
                   (row level, with the rule and the original
                   row serialized in JSON; this is what the load
                   procedure reads to exclude the row)

    Why two tables:
       etl.data_quality_log = "how good is the data, over time?"
       etl.rejected         = "which rows are waiting for a fix?"

Changes vs the original version:
    1. The DELETE of the previous logs is gone. Quality history
       is append-only and batch_key keeps the runs apart, so the
       trend is preserved instead of erased at every run.
    2. @batch_id BIGINT = NULL OUTPUT: the same pattern as the
       Bronze load, so findings and loaded rows share one run id.
       Self-registers in etl.batch when it is not passed.
    3. Blocking findings also go to etl.rejected with raw_row in
       JSON. In the original the check logged and stopped there:
       the load went ahead regardless of what was found.
    4. Severity now matches what the load really does:
         ProductCategoryID NULL       -> error   (blocking)
         Duplicate ProductCategoryID  -> error   (blocking)
         Name NULL or empty           -> warning (NOT blocking)
       The load substitutes 'Unknown', so the row is fine.
       A rule the load can repair is a warning, never an error.
    5. DATETIME2(3) / SYSDATETIME() instead of DATETIME / GETDATE().
    6. Threshold guard: if the rejection rate exceeds
       @max_reject_rate %, the procedure fails on purpose.
       Two bad rows are data; most of the table bad means a broken
       source. The findings are COMMITTED before the guard, so the
       evidence survives the alarm.
    7. No silent CATCH: rollback when a transaction is open, then
       re-throw, so SSIS sees the failure.

SSIS Usage:
    EXEC etl.usp_check_bronze_quality_load_category ?, ?
        param 0: batch_id         (Input, from the SSIS variable)
        param 1: max_reject_rate  (Input, e.g. 20)
    Both have defaults, so the one-parameter call still works.
    Wire the precedence constraint "on success" to the load task.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Prerequisites: created only if missing, so this script
-- can be re-run without harm.
---------------------------------------------------------

IF OBJECT_ID('etl.batch', 'U') IS NULL
BEGIN
    CREATE TABLE etl.batch (
        batch_id     BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_etl_batch PRIMARY KEY,
        started_at   DATETIME2(3) NOT NULL DEFAULT SYSDATETIME(),
        ended_at     DATETIME2(3) NULL,
        triggered_by NVARCHAR(100) NULL,
        notes        NVARCHAR(500) NULL
    );
    PRINT 'Table etl.batch created.';
END
GO

IF OBJECT_ID('etl.rejected', 'U') IS NULL
BEGIN
    CREATE TABLE etl.rejected (
        rejected_key       BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_etl_rejected PRIMARY KEY,
        batch_key          BIGINT        NOT NULL,   -- logical FK -> etl.batch.batch_id
        layer              NVARCHAR(10)  NOT NULL,
        object_name        NVARCHAR(50)  NOT NULL,
        business_key       NVARCHAR(200) NULL,
        rule_code          NVARCHAR(100) NOT NULL,
        rule_description   NVARCHAR(500) NULL,
        severity           NVARCHAR(20)  NOT NULL
            CONSTRAINT DF_etl_rejected_severity DEFAULT ('error'),
        raw_row            NVARCHAR(MAX) NULL,       -- original row as JSON
        rejected_at        DATETIME2(3)  NOT NULL
            CONSTRAINT DF_etl_rejected_rejected_at DEFAULT SYSDATETIME(),
        replayed_at        DATETIME2(3)  NULL,
        replayed_batch_key BIGINT        NULL
    );
    PRINT 'Table etl.rejected created.';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'ix_etl_rejected_gate'
                 AND object_id = OBJECT_ID('etl.rejected'))
    CREATE INDEX ix_etl_rejected_gate
        ON etl.rejected (layer, object_name, batch_key)
        INCLUDE (business_key, replayed_at);
GO

IF COL_LENGTH('etl.data_quality_log', 'batch_key') IS NULL
BEGIN
    ALTER TABLE etl.data_quality_log ADD batch_key BIGINT NULL;
    PRINT 'Column etl.data_quality_log.batch_key added.';
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_category]
    @batch_id        BIGINT       = NULL OUTPUT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @batch_start_time DATETIME2(3) = SYSDATETIME(),
            @rows_checked     INT           = 0,
            @rows_rejected    INT           = 0,
            @errors           INT           = 0,
            @reject_rate      DECIMAL(9,4)  = 0;

    BEGIN TRY
        ---------------------------------------------------------
        -- Batch resolution: the same pattern as the Bronze load
        ---------------------------------------------------------
        IF @batch_id IS NULL
        BEGIN
            INSERT INTO etl.batch (triggered_by, notes)
            VALUES (SUSER_SNAME(), 'self-registered DQ check for object: category');

            SET @batch_id = SCOPE_IDENTITY();
        END

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - category | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_category;

            ---------------------------------------------------------
            -- RULE 0: source empty  ->  warning
            -- A load that brought back nothing is worth noticing,
            -- but it is not by itself a data defect.
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_key, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'category', 'Source empty', NULL,
                     'bronze.erp_category contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: business key NULL  ->  error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 1: ProductCategoryID NULL';

            INSERT INTO etl.data_quality_log
                (batch_key, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'category',
                   'ProductCategoryID NULL',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'Business key ProductCategoryID is NULL',
                   'error'
            FROM bronze.erp_category AS b
            WHERE b.ProductCategoryID IS NULL;

            -- Quarantine: the row will not enter Silver.
            INSERT INTO etl.rejected
                (batch_key, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'category',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'NULL_BK',
                   'Business key ProductCategoryID is NULL',
                   'error',
                   JSON_OBJECT('ProductCategoryID': b.ProductCategoryID,
                               'Name':              b.[Name],
                               'rowguid':           b.rowguid,
                               'ModifiedDate':      b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_category AS b
            WHERE b.ProductCategoryID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: duplicated business key  ->  error (blocking)
            -- Every row sharing the duplicated key is quarantined:
            -- with a non unique key there is no way to tell which
            -- version is the right one.
            ---------------------------------------------------------
            PRINT '-- Rule 2: duplicated ProductCategoryID';

            INSERT INTO etl.data_quality_log
                (batch_key, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'category',
                   'Duplicate ProductCategoryID',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'Business key duplicated: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_category AS b
            GROUP BY b.ProductCategoryID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_key, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'category',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Business key duplicated: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT('ProductCategoryID': b.ProductCategoryID,
                               'Name':              b.[Name],
                               'rowguid':           b.rowguid,
                               'ModifiedDate':      b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_category AS b
            INNER JOIN (
                SELECT ProductCategoryID, COUNT(*) AS cnt
                FROM bronze.erp_category
                GROUP BY ProductCategoryID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.ProductCategoryID = b.ProductCategoryID;

            ---------------------------------------------------------
            -- RULE 3: name NULL or empty  ->  warning (not blocking)
            -- The load substitutes 'Unknown': the row is acceptable.
            -- Logged so the issue stays visible over time.
            ---------------------------------------------------------
            PRINT '-- Rule 3: name NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_key, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'category',
                   'Name NULL or empty',
                   CAST(b.ProductCategoryID AS NVARCHAR(200)),
                   'Name is NULL or empty - the load substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_category AS b
            WHERE b.[Name] IS NULL
               OR LTRIM(RTRIM(b.[Name])) = '';

        COMMIT TRANSACTION;
        -- Committed BEFORE the guard below, so the evidence
        -- survives even when the pipeline is stopped.

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_key   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'category'
          AND severity IN ('error', 'critical');   -- warnings excluded

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_key   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'category';

        IF @rows_checked > 0
            SET @reject_rate =
                CAST(@rows_rejected AS DECIMAL(9,4)) * 100.0 / @rows_checked;

        PRINT '>> Rows checked:  ' + CAST(@rows_checked  AS NVARCHAR(20));
        PRINT '>> Errors logged: ' + CAST(@errors        AS NVARCHAR(20));
        PRINT '>> Rows rejected: ' + CAST(@rows_rejected AS NVARCHAR(20)) +
              ' (' + CAST(@reject_rate AS NVARCHAR(10)) + '%)';
        PRINT '>> Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, SYSDATETIME()) AS NVARCHAR(20)) + ' ms';

        ---------------------------------------------------------
        -- Threshold guard
        ---------------------------------------------------------
        IF @rows_checked > 0
           AND @reject_rate > @max_reject_rate
            THROW 51010, 'Rejection rate above the allowed threshold: the source looks broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - category completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        -- Informational only: SSIS reads success/failure, not this.
        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: category';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO

PRINT 'Procedure etl.usp_check_bronze_quality_load_category created.';
GO

