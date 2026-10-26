/*
=========================================================
Bronze Load – All Objects (etl.usp_load_bronze)
=========================================================
Purpose:
    Loads one Bronze table from the OLTP source, driven by
    etl.config:

    load_type full
        truncates and reloads the table in one transaction
        (every row dwh_operation = 'F').
    load_type incremental
        delegates to etl.usp_load_bronze_cdc, which reads
        only the changes with SQL Server Change Data Capture
        (rows I / U / D, bookmark last_lsn). The first run,
        or a bookmark older than the CDC retention, is
        loaded there as a full snapshot.

    Every row is stamped with dwh_batch_id and
    dwh_source_row_num. Errors are rolled back and re-thrown
    so that SSIS sees the failure.

    Every execution is logged in etl.load_audit (layer
    bronze, step load): status, duration, rows inserted and
    the load type really executed. On failure the error is
    stored too.

SSIS Usage:
    A Start Batch task creates the run in etl.batch and
    stores the number in User::batch_id. A Foreach Loop
    iterates over the active objects of etl.config and runs,
    for each one: EXEC etl.usp_load_bronze ?, ?
    (object_name, batch_id).

Warning:
    A FULL load truncates the Bronze table. Run the ETL
    framework scripts first (etl.batch, etl.rejected,
    etl.data_quality_log, etl.config, etl.load_audit, the
    audit procedures and etl.usp_load_bronze_cdc).
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_bronze]
    @object_name VARCHAR(50),
    @batch_id    BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @batch_start_time DATETIME2(3),
        @batch_end_time   DATETIME2(3),
        @audit_id         BIGINT,
        @proc_name        NVARCHAR(256) =
            QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' + QUOTENAME(OBJECT_NAME(@@PROCID));

    -- Declared before TRY: the CATCH block needs them for the audit
    DECLARE
        @load_type     VARCHAR(20),
        @rows_inserted INT = 0;

    BEGIN TRY
        SET @batch_start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Audit: open the step (outside any transaction)
        ---------------------------------------------------------
        EXEC etl.usp_audit_start
            @batch_id       = @batch_id,
            @layer          = 'bronze',
            @object_name    = @object_name,
            @step_name      = 'load',
            @procedure_name = @proc_name,
            @audit_id       = @audit_id OUTPUT;

        PRINT '====================================================';
        PRINT 'Retrieve configuration for object: ' + @object_name;
        PRINT 'Batch id: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        DECLARE
            @source_table VARCHAR(200),
            @bronze_table VARCHAR(200),
            @cdc_mode     VARCHAR(10),
            @cols         NVARCHAR(MAX),
            @sql          NVARCHAR(MAX);

        ---------------------------------------------------------
        -- Read metadata from config table
        ---------------------------------------------------------
        SELECT
            @load_type    = load_type,
            @source_table = source_table,
            @bronze_table = bronze_table
        FROM etl.config
        WHERE object_name = @object_name
          AND is_active = 1;

        ---------------------------------------------------------
        -- Validate configuration (fail fast, never silently skip)
        ---------------------------------------------------------
        IF @load_type IS NULL
            THROW 51000, 'No active configuration found for the given object_name. Check etl.config (object_name / is_active).', 1;

        IF @load_type NOT IN ('full', 'incremental')
            THROW 51001, 'Invalid load_type in etl.config. Allowed values: full, incremental.', 1;

        SET @start_time = SYSDATETIME();

        PRINT '====================================================';
        PRINT 'Loading Bronze for object: ' + @object_name + ' (' + @load_type + ')';
        PRINT '====================================================';

        ---------------------------------------------------------
        -- INCREMENTAL: CDC, delegated to etl.usp_load_bronze_cdc
        -- (it decides full / cdc / none and saves last_lsn)
        ---------------------------------------------------------
        IF @load_type = 'incremental'
        BEGIN
            EXEC etl.usp_load_bronze_cdc
                 @object_name   = @object_name,
                 @batch_id      = @batch_id,
                 @rows_inserted = @rows_inserted OUTPUT,
                 @mode          = @cdc_mode      OUTPUT;

            -- The audit records what really happened
            IF @cdc_mode = 'full'
                SET @load_type = 'full';
        END

        ---------------------------------------------------------
        -- FULL: TRUNCATE + INSERT in one transaction
        ---------------------------------------------------------
        ELSE
        BEGIN
            -- Columns of the Bronze table, technical ones excluded
            -- (dwh_load_date and dwh_operation = 'F' come from
            --  their DEFAULT)
            SELECT @cols = STRING_AGG(QUOTENAME(c.name), ', ')
                           WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(@bronze_table)
              AND c.name NOT LIKE 'dwh[_]%';

            IF @cols IS NULL
                THROW 51002, 'Bronze table not found in sys.columns. Check bronze_table in etl.config.', 1;

            BEGIN TRANSACTION;

                SET @sql = 'TRUNCATE TABLE ' + @bronze_table + ';';
                EXEC(@sql);

                SET @sql =
                    'INSERT INTO ' + @bronze_table +
                    ' (' + @cols + ', [dwh_batch_id], [dwh_source_row_num])' +
                    ' SELECT ' + @cols + ', @batch_id, ROW_NUMBER() OVER (ORDER BY (SELECT NULL))' +
                    ' FROM '   + @source_table + ';';
                EXEC sp_executesql @sql, N'@batch_id BIGINT', @batch_id = @batch_id;
                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;
        END

        ---------------------------------------------------------
        -- Audit: close the step as succeeded
        ---------------------------------------------------------
        EXEC etl.usp_audit_end
            @audit_id      = @audit_id,
            @status        = 'succeeded',
            @load_type     = @load_type,
            @rows_read     = @rows_inserted,      -- Bronze: rows read = rows written
            @rows_inserted = @rows_inserted;

        SET @end_time = SYSDATETIME();

        PRINT '>> Batch id: '     + CAST(@batch_id AS NVARCHAR(20));
        PRINT '>> Rows inserted: ' + CAST(ISNULL(@rows_inserted, 0) AS NVARCHAR(20));
        PRINT '>> Load Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @start_time, @end_time) AS NVARCHAR) +
              ' ms';

        SET @batch_end_time = SYSDATETIME();

        PRINT '====================================================';
        PRINT 'Bronze load completed for: ' + @object_name +
              ' | Total Duration: ' +
              CAST(DATEDIFF(MILLISECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +
              ' ms';
        PRINT '====================================================';
    END TRY

    BEGIN CATCH
        -- Roll back the open transaction, if any
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Audit AFTER the rollback, or it would be rolled back too.
        -- usp_audit_end reads ERROR_MESSAGE() & co. by itself.
        IF @audit_id IS NOT NULL
            EXEC etl.usp_audit_end
                @audit_id  = @audit_id,
                @status    = 'failed',
                @load_type = @load_type;

        PRINT '====================================================';
        PRINT 'ERROR during Bronze load for: ' + @object_name;
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Line: '  + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '====================================================';

        -- Re-throw: the caller (SSIS Execute SQL Task) MUST see
        -- the failure. Never end a CATCH silently.
        THROW;
    END CATCH
END;
GO
