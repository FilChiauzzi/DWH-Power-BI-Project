/*
=========================================================
Bronze Data Quality Check – Person
=========================================================
Purpose:
    Validates the Bronze data that feeds the Silver Person
    dimension and records the outcome in the quality framework.

The three steps, two tables:
    1. DETECT      evaluate the rules on bronze.erp_person
    2. LOG         every finding -> etl.data_quality_log
                   (append-only: quality trend over time;
                   warnings included, they block nothing)
    3. QUARANTINE  the blocking findings -> etl.rejected
                   (row-level, with the original row serialized
                   in JSON; the Silver load excludes these rows)

Why two tables:
    etl.data_quality_log = "how good is the data, over time?"
    etl.rejected         = "which rows are waiting for a fix?"

Changes vs the original version:
    1. No DELETE of previous logs: quality history is append-only.
       batch_id separates runs and preserves the trend.
    2. @batch_id BIGINT (required input): created ONCE by SSIS at
       the start of the run and passed to every procedure.
       The procedure never creates a batch id by itself.
    3. Blocking findings also go to etl.rejected with raw_row in
       JSON. The Silver load excludes these rows.
    4. Severity matches what the Silver load really does:
         BusinessEntityID NULL       -> error   (blocking)
         Duplicate BusinessEntityID  -> error   (blocking)
         FirstName NULL/empty        -> warning (NOT blocking)
         LastName NULL/empty         -> warning (NOT blocking)
       Silver substitutes 'Unknown', so the row is still valid.
    5. DATETIME2(3) / SYSDATETIME() instead of DATETIME / GETDATE().
    6. Threshold guard: if the rejection rate exceeds
       @max_reject_rate %, the procedure fails intentionally.
       Findings are COMMITTED before the guard, so evidence
       survives the alarm.
    7. No silent CATCH: rollback when a transaction is open, then
       re-throw, so SSIS sees the failure.
    8. No DDL and no self-registration: this script only defines
       the procedure (the tables live in 00_etl_ddl.sql).

SSIS Usage:
    EXEC etl.usp_check_bronze_quality_load_person ?, ?
        param 0: batch_id         (Input, from the SSIS variable)
        param 1: max_reject_rate  (Input, e.g. 20)
    Wire the precedence constraint "on success" to the Silver load.

Prerequisite:
    Run 00_etl_ddl.sql first (etl.batch, etl.rejected,
    etl.data_quality_log).
*/

USE [DWH_AdventureWorks2025]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [etl].[usp_check_bronze_quality_load_person]
    @batch_id        BIGINT,
    @max_reject_rate DECIMAL(5,2) = 20.00
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @batch_start_time DATETIME2(3) = SYSDATETIME(),
        @rows_checked     INT           = 0,
        @rows_rejected    INT           = 0,
        @errors           INT           = 0,
        @reject_rate      DECIMAL(9,4)  = 0;

    BEGIN TRY
        PRINT '====================================================';
        PRINT 'Bronze Data Quality - person | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        BEGIN TRANSACTION;

            ---------------------------------------------------------
            -- Count source rows
            ---------------------------------------------------------
            SELECT @rows_checked = COUNT(*)
            FROM bronze.erp_person;

            ---------------------------------------------------------
            -- RULE 0: source empty -> warning
            ---------------------------------------------------------
            IF @rows_checked = 0
            BEGIN
                INSERT INTO etl.data_quality_log
                    (batch_id, layer, object_name, check_name, record_id, description, severity)
                VALUES
                    (@batch_id, 'bronze', 'person', 'Source empty', NULL,
                     'bronze.erp_person contains no rows', 'warning');
            END

            ---------------------------------------------------------
            -- RULE 1: BusinessEntityID NULL -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 1: BusinessEntityID NULL';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'BusinessEntityID NULL',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'BusinessEntityID is NULL',
                   'error'
            FROM bronze.erp_person AS b
            WHERE b.BusinessEntityID IS NULL;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'person',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'NULL_BK',
                   'BusinessEntityID is NULL',
                   'error',
                   JSON_OBJECT('BusinessEntityID': b.BusinessEntityID,
                               'FirstName':        b.FirstName,
                               'LastName':         b.LastName,
                               'PersonType':       b.PersonType,
                               'EmailPromotion':   b.EmailPromotion,
                               'rowguid':          b.rowguid,
                               'ModifiedDate':     b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_person AS b
            WHERE b.BusinessEntityID IS NULL;

            ---------------------------------------------------------
            -- RULE 2: Duplicate BusinessEntityID -> error (blocking)
            ---------------------------------------------------------
            PRINT '-- Rule 2: Duplicate BusinessEntityID';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'Duplicate BusinessEntityID',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'Duplicate BusinessEntityID detected: ' + CAST(COUNT(*) AS VARCHAR(10)) + ' rows',
                   'error'
            FROM bronze.erp_person AS b
            GROUP BY b.BusinessEntityID
            HAVING COUNT(*) > 1;

            INSERT INTO etl.rejected
                (batch_id, layer, object_name, business_key, rule_code,
                 rule_description, severity, raw_row)
            SELECT @batch_id, 'bronze', 'person',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'DUP_BK',
                   'Duplicate BusinessEntityID detected: ' + CAST(d.cnt AS VARCHAR(10)) + ' rows',
                   'error',
                   JSON_OBJECT('BusinessEntityID': b.BusinessEntityID,
                               'FirstName':        b.FirstName,
                               'LastName':         b.LastName,
                               'PersonType':       b.PersonType,
                               'EmailPromotion':   b.EmailPromotion,
                               'rowguid':          b.rowguid,
                               'ModifiedDate':     b.ModifiedDate
                               NULL ON NULL)
            FROM bronze.erp_person AS b
            INNER JOIN (
                SELECT BusinessEntityID, COUNT(*) AS cnt
                FROM bronze.erp_person
                GROUP BY BusinessEntityID
                HAVING COUNT(*) > 1
            ) AS d
                ON d.BusinessEntityID = b.BusinessEntityID;

            ---------------------------------------------------------
            -- RULE 3: FirstName NULL or empty -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 3: FirstName NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'FirstName NULL or empty',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'FirstName is NULL or empty - Silver substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_person AS b
            WHERE b.FirstName IS NULL
               OR LTRIM(RTRIM(b.FirstName)) = '';

            ---------------------------------------------------------
            -- RULE 4: LastName NULL or empty -> warning
            ---------------------------------------------------------
            PRINT '-- Rule 4: LastName NULL or empty (warning)';

            INSERT INTO etl.data_quality_log
                (batch_id, layer, object_name, check_name, record_id, description, severity)
            SELECT @batch_id, 'bronze', 'person',
                   'LastName NULL or empty',
                   CAST(b.BusinessEntityID AS NVARCHAR(200)),
                   'LastName is NULL or empty - Silver substitutes ''Unknown''',
                   'warning'
            FROM bronze.erp_person AS b
            WHERE b.LastName IS NULL
               OR LTRIM(RTRIM(b.LastName)) = '';

        COMMIT TRANSACTION;

        ---------------------------------------------------------
        -- Summary
        ---------------------------------------------------------
        SELECT @errors = COUNT(*)
        FROM etl.data_quality_log
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'person'
          AND severity IN ('error','critical');

        SELECT @rows_rejected = COUNT(*)
        FROM etl.rejected
        WHERE batch_id   = @batch_id
          AND layer       = 'bronze'
          AND object_name = 'person';

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
            THROW 51010, 'Rejection rate above threshold: source likely broken. Pipeline stopped on purpose.', 1;

        PRINT '====================================================';
        PRINT 'Bronze Data Quality - person completed | batch: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        RETURN @errors;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze DQ check for: person';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT 'Line: '    + CAST(ERROR_LINE()   AS NVARCHAR);
        PRINT '====================================================';

        THROW;
    END CATCH
END;
GO
