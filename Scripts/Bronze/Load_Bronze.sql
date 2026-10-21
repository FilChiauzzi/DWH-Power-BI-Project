/*
=========================================================
Bronze Load – All Objects
=========================================================
Purpose:
    Loads one Bronze table from the OLTP source, driven by
    the metadata in etl.config. FULL truncates and reloads
    the table in one transaction; INCREMENTAL reads only
    rows newer than the stored watermark.

    Every row is stamped with dwh_batch_id and
    dwh_source_row_num. The watermark is advanced only when
    a new value exists. Errors are rolled back and re-thrown
    so that SSIS sees the failure.

    Every execution is logged in etl.load_audit (layer
    bronze, step load): status, duration, rows inserted and
    the watermark range. On failure the error is stored too.

SSIS Usage:
    A Start Batch task creates the run in etl.batch and
    stores the number in User::batch_id. A Foreach Loop
    iterates over the active objects of etl.config and runs,
    for each one: EXEC etl.usp_load_bronze ?, ?
    (object_name, batch_id).

Warning:
    A FULL load truncates the Bronze table. Run the ETL
    framework scripts first (etl.batch, etl.rejected,
    etl.data_quality_log, etl.config, etl.load_audit and the
    audit procedures).
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
        @load_type            VARCHAR(20),
        @last_watermark_value DATETIME,
        @new_watermark_value  DATETIME,
        @rows_inserted        INT;

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
            @source_table         VARCHAR(200),
            @bronze_table         VARCHAR(200),
            @watermark            VARCHAR(100),
            @cols                 NVARCHAR(MAX),
            @cols_ins             NVARCHAR(MAX),
            @sql                  NVARCHAR(MAX),
            @rownum_expr          NVARCHAR(200);

        ---------------------------------------------------------
        -- Read metadata from config table
        ---------------------------------------------------------
        SELECT
            @load_type            = load_type,
            @source_table         = source_table,
            @bronze_table         = bronze_table,
            @watermark            = watermark,
            @last_watermark_value = last_watermark_value
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

        ---------------------------------------------------------
        -- Explicit column list from the bronze table definition.
        --   @cols_src: source columns only (exclude ALL dwh_%)
        --   @cols_ins: source columns + the two audit columns we
        --              supply here (dwh_load_date is excluded:
        --              its DEFAULT fills it).
        ---------------------------------------------------------
        SELECT @cols = STRING_AGG(QUOTENAME(c.name), ', ')
                       WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(@bronze_table)
          AND c.name NOT LIKE 'dwh[_]%';

        IF @cols IS NULL
            THROW 51002, 'Bronze table not found in sys.columns. Check bronze_table in etl.config.', 1;

        SET @cols_ins = @cols + ', [dwh_batch_id], [dwh_source_row_num]';

        ---------------------------------------------------------
        -- Row number expression: ordered by the watermark column
        -- when available (stable order), otherwise by nothing in
        -- particular (consecutive but unordered).
        ---------------------------------------------------------
        IF @watermark IS NOT NULL
            SET @rownum_expr = 'ROW_NUMBER() OVER (ORDER BY ' + QUOTENAME(@watermark) + ')';
        ELSE
            SET @rownum_expr = 'ROW_NUMBER() OVER (ORDER BY (SELECT NULL))';

        SET @start_time = SYSDATETIME();

        PRINT '====================================================';
        PRINT 'Loading Bronze for object: ' + @object_name;
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD
        ---------------------------------------------------------
        IF @load_type = 'full'
        BEGIN
            -- TRUNCATE + INSERT are atomic: if the INSERT fails,
            -- the TRUNCATE is rolled back as well.
            BEGIN TRANSACTION;

                SET @sql = 'TRUNCATE TABLE ' + @bronze_table + ';';
                EXEC(@sql);

                SET @sql =
                    'INSERT INTO ' + @bronze_table + ' (' + @cols_ins + ')' +
                    ' SELECT ' + @cols + ', @batch_id, ' + @rownum_expr +
                    ' FROM '   + @source_table + ';';
                EXEC sp_executesql @sql, N'@batch_id BIGINT', @batch_id = @batch_id;
                SET @rows_inserted = @@ROWCOUNT;

            COMMIT TRANSACTION;

            -- Compute new watermark from the source
            SET @sql =
                'SELECT @wm = MAX(' + QUOTENAME(@watermark) + ')' +
                ' FROM ' + @source_table + ';';
            EXEC sp_executesql @sql,
                N'@wm DATETIME OUTPUT',
                @wm = @new_watermark_value OUTPUT;
        END

        ---------------------------------------------------------
        -- INCREMENTAL LOAD
        ---------------------------------------------------------
        ELSE
        BEGIN
            IF @last_watermark_value IS NULL
                THROW 51003, 'Incremental load requires a last_watermark_value in etl.config. Run a FULL load first or set the starting watermark.', 1;

            -- The watermark is passed as a DATETIME parameter,
            -- never converted to a string.
            SET @sql =
                'INSERT INTO ' + @bronze_table + ' (' + @cols_ins + ')' +
                ' SELECT ' + @cols + ', @batch_id, ' + @rownum_expr +
                ' FROM '   + @source_table +
                ' WHERE '  + QUOTENAME(@watermark) + ' > @wm;';
            EXEC sp_executesql @sql,
                N'@wm DATETIME, @batch_id BIGINT',
                @wm       = @last_watermark_value,
                @batch_id = @batch_id;
            SET @rows_inserted = @@ROWCOUNT;

            -- Compute new watermark from newly inserted rows
            SET @sql =
                'SELECT @wm = MAX(' + QUOTENAME(@watermark) + ')' +
                ' FROM ' + @bronze_table +
                ' WHERE ' + QUOTENAME(@watermark) + ' > @wm;';
            EXEC sp_executesql @sql,
                N'@wm DATETIME, @new_wm DATETIME OUTPUT',
                @wm     = @last_watermark_value,
                @new_wm = @new_watermark_value OUTPUT;
        END

        ---------------------------------------------------------
        -- Update watermark in config table
        -- ONLY if a new value was computed: with zero new rows
        -- (or before the first full load) the previous watermark
        -- must be preserved, never overwritten with NULL.
        ---------------------------------------------------------
        IF @new_watermark_value IS NOT NULL
        BEGIN
            UPDATE etl.config
            SET last_watermark_value = @new_watermark_value
            WHERE object_name = @object_name;
        END

        ---------------------------------------------------------
        -- Audit: close the step as succeeded
        ---------------------------------------------------------
        -- With zero new rows the watermark did not move: the
        -- audit stores the unchanged value, never NULL.
        SET @new_watermark_value = COALESCE(@new_watermark_value, @last_watermark_value);

        EXEC etl.usp_audit_end
            @audit_id       = @audit_id,
            @status         = 'succeeded',
            @load_type      = @load_type,
            @rows_read      = @rows_inserted,      -- Bronze: rows read = rows written
            @rows_inserted  = @rows_inserted,
            @watermark_from = @last_watermark_value,
            @watermark_to   = @new_watermark_value;

        SET @end_time = SYSDATETIME();

        PRINT '>> Batch id: '     + CAST(@batch_id AS NVARCHAR(20));
        PRINT '>> Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
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
        -- Roll back the FULL-load transaction if it is still open
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Audit AFTER the rollback, or it would be rolled back too.
        -- usp_audit_end reads ERROR_MESSAGE() & co. by itself.
        IF @audit_id IS NOT NULL
            EXEC etl.usp_audit_end
                @audit_id       = @audit_id,
                @status         = 'failed',
                @load_type      = @load_type,
                @watermark_from = @last_watermark_value;

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
