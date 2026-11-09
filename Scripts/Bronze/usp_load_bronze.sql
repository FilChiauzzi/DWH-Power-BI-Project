/*
=========================================================
Bronze Load – All Objects (etl.usp_load_bronze)
=========================================================
Purpose:
    Loads one Bronze table from the OLTP source, driven by
    etl.config:

    load_type full
        truncates and reloads the table in one transaction
        (every row dwh_operation = 'F'). Before emptying the
        table a volume check compares the source with the
        last load: an empty source (error 51004) or a drop of
        more than 50% (error 51005) stops the load and leaves
        the Bronze table untouched. After the copy the rows
        copied are compared with the rows counted in the
        source: a difference is logged as a warning in
        the error event schema (the source may change during the
        copy). Rows are numbered by the primary key of the
        source, so dwh_source_row_num is stable between runs.
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
    bronze, step load): status, duration, rows read from the
    source, rows inserted and the load type really executed. On failure the error is
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
    the error event schema, etl.config, etl.load_audit, the
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
        @cdc_enabled   BIT,
        @rows_read     INT = 0,
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
            @sql          NVARCHAR(MAX),
            @src_rows     BIGINT,
            @bronze_rows  BIGINT,
            @max_drop_pct INT = 50,          -- more than 50% fewer rows than the last load = suspicious
            @msg          NVARCHAR(400),
            @key_order    NVARCHAR(MAX);

        ---------------------------------------------------------
        -- Read metadata from config table
        ---------------------------------------------------------
        SELECT
            @load_type    = load_type,
            @cdc_enabled  = is_cdc_enabled,
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

        IF @load_type = 'incremental' AND @cdc_enabled = 0
            THROW 51003, 'Incremental object without CDC: set is_cdc_enabled = 1 in etl.config and run Enable_CDC_Source.sql.', 1;

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
                 @rows_read     = @rows_read     OUTPUT,
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

            -----------------------------------------------------
            -- Volume check, BEFORE emptying the Bronze table:
            -- an empty source, or a source with much fewer rows
            -- than the last load, stops the load and leaves the
            -- Bronze table untouched.
            -----------------------------------------------------
            SET @sql = N'SELECT @n = COUNT_BIG(*) FROM ' + @source_table + N';';
            EXEC sp_executesql @sql, N'@n BIGINT OUTPUT', @n = @src_rows OUTPUT;

            SET @sql = N'SELECT @n = COUNT_BIG(*) FROM ' + @bronze_table + N';';
            EXEC sp_executesql @sql, N'@n BIGINT OUTPUT', @n = @bronze_rows OUTPUT;

            IF @src_rows = 0
            BEGIN
                SET @msg = CONCAT('Source is empty. Last load: ', @bronze_rows, ' rows. Bronze left untouched.');
                THROW 51004, @msg, 1;
            END;

            IF @bronze_rows > 0
               AND @src_rows < @bronze_rows * (100 - @max_drop_pct) / 100.0
            BEGIN
                SET @msg = CONCAT('Source has ', @src_rows, ' rows, last load ', @bronze_rows,
                                  ': drop over ', @max_drop_pct, '%. Bronze left untouched.');
                THROW 51005, @msg, 1;
            END;

            PRINT CONCAT('>> Volume check ok: source ', @src_rows, ', last load ', @bronze_rows);

            SET @rows_read = @src_rows;

            -----------------------------------------------------
            -- Stable row numbers: order by the primary key of
            -- the source (no primary key: any order)
            -----------------------------------------------------
            SET @sql =
                N'SELECT @k = STRING_AGG(CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)), N'', '')' +
                N'            WITHIN GROUP (ORDER BY ic.key_ordinal)' +
                N' FROM ' + QUOTENAME(PARSENAME(@source_table, 3)) + N'.sys.indexes AS i' +
                N' JOIN ' + QUOTENAME(PARSENAME(@source_table, 3)) + N'.sys.index_columns AS ic' +
                N'   ON ic.object_id = i.object_id AND ic.index_id = i.index_id' +
                N' JOIN ' + QUOTENAME(PARSENAME(@source_table, 3)) + N'.sys.columns AS c' +
                N'   ON c.object_id = ic.object_id AND c.column_id = ic.column_id' +
                N' WHERE i.object_id = OBJECT_ID(@t) AND i.is_primary_key = 1;';
            EXEC sp_executesql @sql, N'@t NVARCHAR(400), @k NVARCHAR(MAX) OUTPUT',
                 @t = @source_table, @k = @key_order OUTPUT;

            SET @key_order = ISNULL(@key_order, N'(SELECT NULL)');

            BEGIN TRANSACTION;

                SET @sql = 'TRUNCATE TABLE ' + @bronze_table + ';';
                EXEC(@sql);

                SET @sql =
                    'INSERT INTO ' + @bronze_table +
                    ' (' + @cols + ', [dwh_batch_id], [dwh_source_row_num])' +
                    ' SELECT ' + @cols + ', @batch_id, ROW_NUMBER() OVER (ORDER BY ' + @key_order + ')' +
                    ' FROM '   + @source_table + ';';
                EXEC sp_executesql @sql, N'@batch_id BIGINT', @batch_id = @batch_id;
                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            -----------------------------------------------------
            -- Row count reconciliation (Kimball: verify that all
            -- the data moved). Not blocking: the source may change
            -- between the count and the copy.
            -----------------------------------------------------
            IF @rows_inserted <> @src_rows
            BEGIN
                DROP TABLE IF EXISTS #errors;
                CREATE TABLE #errors (row_id INT IDENTITY(1,1), screen_name NVARCHAR(200), screen_type VARCHAR(20),
                                      severity VARCHAR(20), exception_action VARCHAR(20), fields NVARCHAR(400),
                                      record_id NVARCHAR(200), error_condition NVARCHAR(500));

                INSERT INTO #errors (screen_name, screen_type, severity, exception_action, fields, record_id, error_condition)
                VALUES ('Row count reconciliation', 'business rule', 'warning', 'pass', '', NULL,
                        CONCAT('Source counted ', @src_rows, ' rows before the copy, ',
                            @rows_inserted, ' rows copied into Bronze'));

                EXEC etl.usp_save_error_events @batch_id = @batch_id, @layer = 'bronze',
                     @object_name = @object_name, @etl_module = '[etl].[usp_load_bronze]';

                PRINT CONCAT('>> WARNING row count: source ', @src_rows, ', copied ', @rows_inserted);
            END
            ELSE
                PRINT CONCAT('>> Row count reconciliation ok: ', @rows_inserted, ' rows');
        END

        ---------------------------------------------------------
        -- Audit: close the step as succeeded
        ---------------------------------------------------------
        EXEC etl.usp_audit_end
            @audit_id      = @audit_id,
            @status        = 'succeeded',
            @load_type     = @load_type,
            @rows_read     = @rows_read,          -- counted in the source (CDC: net changes read)
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
