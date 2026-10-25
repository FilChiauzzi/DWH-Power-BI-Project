/*
=========================================================
Bronze Load – All Objects
=========================================================
Purpose:
    Loads one Bronze table from the OLTP source, driven by
    the metadata in etl.config.

    load_type full:
        truncates and reloads the table in one transaction.
    load_type incremental, cdc_method cdc:
        reads the SQL Server Change Data Capture net changes
        between the bookmark last_lsn and the latest captured
        LSN. CDC only says WHICH rows changed and HOW (I/U/D):
        the values are read from the source table, because
        CDC does not store computed columns. Deleted rows keep
        their key, the other columns are NULL.
    load_type incremental, cdc_method watermark:
        reads the rows whose audit column is newer than
        last_watermark_value.

    An incremental object is loaded as FULL (snapshot, F)
    when:
    - it has no bookmark yet (initial load);
    - cdc: the bookmark is older than the oldest change still
      kept by CDC (cleanup job), so changes may be lost.
    The bookmark of a full load is taken BEFORE the copy: a
    change made during the copy is read again the next time,
    never lost.

    Every row is stamped with dwh_batch_id,
    dwh_source_row_num and dwh_operation (F = full snapshot,
    I / U / D = from CDC). Bookmarks are computed from what
    was read, never from the source after the copy, and are
    saved in the same transaction as the rows.

    Every execution is logged in etl.load_audit (layer
    bronze, step load). Errors are rolled back and re-thrown
    so that SSIS sees the failure.

SSIS Usage:
    A Start Batch task creates the run in etl.batch and
    stores the number in User::batch_id. A Foreach Loop
    iterates over the active objects of etl.config and runs,
    for each one: EXEC etl.usp_load_bronze ?, ?
    (object_name, batch_id).

Warning:
    A FULL load truncates the Bronze table. CDC must be
    enabled on the source tables with cdc_method = 'cdc'
    (sys.sp_cdc_enable_table, @supports_net_changes = 1),
    and SQL Server Agent must be running (capture job).
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
        @audit_load_type      VARCHAR(20),
        @last_watermark_value DATETIME,
        @new_watermark_value  DATETIME,
        @rows_inserted        INT = 0;

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
            @cdc_method        VARCHAR(20),
            @last_lsn          BINARY(10),
            @source_table      VARCHAR(200),
            @bronze_table      VARCHAR(200),
            @watermark         VARCHAR(100),
            @mode              VARCHAR(20),       -- full / cdc / watermark / none
            @mode_reason       VARCHAR(100),
            @cols              NVARCHAR(MAX),
            @cols_ins          NVARCHAR(MAX),
            @sql               NVARCHAR(MAX),
            @rownum_expr       NVARCHAR(200),
            -- CDC
            @src_db            SYSNAME,
            @src_exec          NVARCHAR(300),     -- [source db].sys.sp_executesql
            @src_obj           NVARCHAR(300),     -- [schema].[table] in the source
            @capture_instance  SYSNAME,
            @min_lsn           BINARY(10),
            @max_lsn           BINARY(10),
            @from_lsn          BINARY(10),
            @to_lsn            BINARY(10),
            @join              NVARCHAR(MAX),
            @cols_cdc          NVARCHAR(MAX);

        ---------------------------------------------------------
        -- Read metadata from config table
        ---------------------------------------------------------
        SELECT
            @load_type            = load_type,
            @cdc_method           = cdc_method,
            @last_lsn             = last_lsn,
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

        IF @load_type = 'incremental' AND ISNULL(@cdc_method, '') NOT IN ('cdc', 'watermark')
            THROW 51003, 'Incremental objects need cdc_method = cdc or watermark in etl.config.', 1;

        ---------------------------------------------------------
        -- Explicit column list from the bronze table definition.
        --   @cols     : source columns only (exclude ALL dwh_%)
        --   @cols_ins : source columns + the audit columns we
        --               supply here (dwh_load_date and the
        --               dwh_operation of a full load come from
        --               their DEFAULT).
        ---------------------------------------------------------
        SELECT @cols = STRING_AGG(CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)), ', ')
                       WITHIN GROUP (ORDER BY c.column_id)
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(@bronze_table)
          AND c.name NOT LIKE 'dwh[_]%';

        IF @cols IS NULL
            THROW 51002, 'Bronze table not found in sys.columns. Check bronze_table in etl.config.', 1;

        SET @cols_ins = @cols + ', [dwh_batch_id], [dwh_source_row_num]';

        IF @watermark IS NOT NULL
            SET @rownum_expr = 'ROW_NUMBER() OVER (ORDER BY ' + QUOTENAME(@watermark) + ')';
        ELSE
            SET @rownum_expr = 'ROW_NUMBER() OVER (ORDER BY (SELECT NULL))';

        ---------------------------------------------------------
        -- CDC: read the LSN window in the source database
        --   @min_lsn  oldest change still kept for this table
        --   @max_lsn  latest change captured ("now")
        --   @from_lsn the position right after the bookmark
        ---------------------------------------------------------
        IF @load_type = 'incremental' AND @cdc_method = 'cdc'
        BEGIN
            SET @src_db           = PARSENAME(@source_table, 3);
            SET @src_exec         = QUOTENAME(@src_db) + N'.sys.sp_executesql';
            SET @src_obj          = QUOTENAME(PARSENAME(@source_table, 2)) + N'.' + QUOTENAME(PARSENAME(@source_table, 1));
            SET @capture_instance = PARSENAME(@source_table, 2) + N'_' + PARSENAME(@source_table, 1);

            -- sp_executesql prefixed with the database name runs
            -- inside that database: the cdc functions are there.
            EXEC @src_exec
                N'SELECT @min  = sys.fn_cdc_get_min_lsn(@ci),
                         @max  = sys.fn_cdc_get_max_lsn(),
                         @from = sys.fn_cdc_increment_lsn(@last);',
                N'@ci SYSNAME, @last BINARY(10),
                  @min BINARY(10) OUTPUT, @max BINARY(10) OUTPUT, @from BINARY(10) OUTPUT',
                @ci   = @capture_instance,
                @last = @last_lsn,
                @min  = @min_lsn  OUTPUT,
                @max  = @max_lsn  OUTPUT,
                @from = @from_lsn OUTPUT;

            -- min LSN 0x00...: no capture instance with that name
            IF @min_lsn IS NULL OR @min_lsn = 0x00000000000000000000
                THROW 51004, 'CDC is not enabled on the source table (capture instance not found). Run sys.sp_cdc_enable_table.', 1;
        END;

        ---------------------------------------------------------
        -- Decide what to do this run
        ---------------------------------------------------------
        IF @load_type = 'full'
            SELECT @mode = 'full', @mode_reason = 'load_type full';

        ELSE IF @cdc_method = 'cdc'
        BEGIN
            IF @last_lsn IS NULL
                SELECT @mode = 'full', @mode_reason = 'initial load (no last_lsn)';
            ELSE IF @from_lsn < @min_lsn
                SELECT @mode = 'full', @mode_reason = 'last_lsn older than the CDC retention';
            ELSE IF @from_lsn > @max_lsn
                SELECT @mode = 'none', @mode_reason = 'no new changes';
            ELSE
                SELECT @mode = 'cdc',  @mode_reason = 'read CDC net changes';

            -- The full load of a CDC object takes its bookmark NOW,
            -- before the copy
            SET @to_lsn = @max_lsn;
        END

        ELSE  -- watermark
        BEGIN
            IF @last_watermark_value IS NULL
                SELECT @mode = 'full', @mode_reason = 'initial load (no watermark)';
            ELSE
                SELECT @mode = 'watermark', @mode_reason = 'rows newer than the watermark';
        END;

        SET @audit_load_type = CASE WHEN @mode = 'full' THEN 'full' ELSE 'incremental' END;

        SET @start_time = SYSDATETIME();

        PRINT '====================================================';
        PRINT 'Loading Bronze for object: ' + @object_name;
        PRINT '>> Mode: ' + @mode + ' (' + @mode_reason + ')';
        PRINT '====================================================';

        ---------------------------------------------------------
        -- FULL LOAD (also the initial load of incremental objects)
        ---------------------------------------------------------
        IF @mode = 'full'
        BEGIN
            -- TRUNCATE + INSERT + bookmark are atomic
            BEGIN TRANSACTION;

                SET @sql = 'TRUNCATE TABLE ' + @bronze_table + ';';
                EXEC(@sql);

                SET @sql =
                    'INSERT INTO ' + @bronze_table + ' (' + @cols_ins + ')' +
                    ' SELECT ' + @cols + ', @batch_id, ' + @rownum_expr +
                    ' FROM '   + @source_table + ';';
                EXEC sp_executesql @sql, N'@batch_id BIGINT', @batch_id = @batch_id;
                SET @rows_inserted = @@ROWCOUNT;

                -- CDC bookmark: the LSN taken before the copy
                IF @cdc_method = 'cdc' AND @load_type = 'incremental'
                    UPDATE etl.config
                    SET last_lsn = @to_lsn
                    WHERE object_name = @object_name;

            COMMIT TRANSACTION;

            -- Watermark from the rows copied into Bronze
            -- (not from the source: a row modified after the copy
            --  would move the watermark past data never copied)
            IF @watermark IS NOT NULL
            BEGIN
                SET @sql =
                    'SELECT @wm = MAX(' + QUOTENAME(@watermark) + ')' +
                    ' FROM ' + @bronze_table + ';';
                EXEC sp_executesql @sql,
                    N'@wm DATETIME OUTPUT',
                    @wm = @new_watermark_value OUTPUT;
            END
        END

        ---------------------------------------------------------
        -- CDC LOAD: net changes between last_lsn and now
        ---------------------------------------------------------
        ELSE IF @mode = 'cdc'
        BEGIN
            -- 1. Key columns of the table, from the CDC catalog
            DROP TABLE IF EXISTS #keys;
            CREATE TABLE #keys (
                column_name   SYSNAME COLLATE DATABASE_DEFAULT,
                index_ordinal INT
            );

            INSERT INTO #keys (column_name, index_ordinal)
            EXEC @src_exec
                N'SELECT ic.column_name, ic.index_ordinal
                  FROM cdc.index_columns AS ic
                  JOIN cdc.change_tables AS ct ON ct.object_id = ic.object_id
                  WHERE ct.capture_instance = @ci;',
                N'@ci SYSNAME',
                @ci = @capture_instance;

            IF NOT EXISTS (SELECT 1 FROM #keys)
                THROW 51005, 'No key columns found for the CDC capture instance.', 1;

            -- 2. Join condition: s.[k1] = nc.[k1] AND s.[k2] = nc.[k2]
            SELECT @join = STRING_AGG(
                               CAST(N's.' + QUOTENAME(column_name) + N' = nc.' + QUOTENAME(column_name) AS NVARCHAR(MAX)),
                               N' AND ')
                           WITHIN GROUP (ORDER BY index_ordinal)
            FROM #keys;

            -- 3. Column list: keys from CDC (present also for D),
            --    every other column from the source (current values,
            --    computed columns included)
            SELECT @cols_cdc = STRING_AGG(
                                   CAST(CASE WHEN k.column_name IS NOT NULL THEN N'nc.' ELSE N's.' END
                                        + QUOTENAME(c.name) AS NVARCHAR(MAX)),
                                   N', ')
                               WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.columns AS c
            LEFT JOIN #keys  AS k ON k.column_name = c.name
            WHERE c.object_id = OBJECT_ID(@bronze_table)
              AND c.name NOT LIKE 'dwh[_]%';

            -- 4. The insert runs in the source database: the Bronze
            --    table is written with its three-part name
            SET @sql =
                N'INSERT INTO ' + QUOTENAME(DB_NAME()) + N'.' + @bronze_table +
                N' (' + @cols + N', [dwh_batch_id], [dwh_source_row_num], [dwh_operation])' +
                N' SELECT ' + @cols_cdc + N', @batch_id,' +
                N' ROW_NUMBER() OVER (ORDER BY nc.__$start_lsn),' +
                N' CASE nc.__$operation WHEN 1 THEN ''D'' WHEN 2 THEN ''I'' ELSE ''U'' END' +
                N' FROM cdc.' + QUOTENAME(N'fn_cdc_get_net_changes_' + @capture_instance) +
                N'(@from_lsn, @to_lsn, N''all'') AS nc' +
                N' LEFT JOIN ' + @src_obj + N' AS s ON ' + @join + N';' +
                N' SET @rows = @@ROWCOUNT;';

            -- Rows and bookmark are atomic
            BEGIN TRANSACTION;

                EXEC @src_exec @sql,
                    N'@batch_id BIGINT, @from_lsn BINARY(10), @to_lsn BINARY(10), @rows INT OUTPUT',
                    @batch_id = @batch_id,
                    @from_lsn = @from_lsn,
                    @to_lsn   = @to_lsn,
                    @rows     = @rows_inserted OUTPUT;

                UPDATE etl.config
                SET last_lsn = @to_lsn
                WHERE object_name = @object_name;

            COMMIT TRANSACTION;

            PRINT '>> LSN window: ' + CONVERT(VARCHAR(30), @from_lsn, 1) +
                  ' -> ' + CONVERT(VARCHAR(30), @to_lsn, 1);
        END

        ---------------------------------------------------------
        -- WATERMARK LOAD: rows newer than the audit column
        ---------------------------------------------------------
        ELSE IF @mode = 'watermark'
        BEGIN
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

        -- @mode = 'none': nothing to read, bookmark unchanged

        ---------------------------------------------------------
        -- Update watermark in config table
        -- ONLY if a new value was computed: with zero new rows
        -- the previous watermark must be preserved, never
        -- overwritten with NULL.
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
        SET @new_watermark_value = COALESCE(@new_watermark_value, @last_watermark_value);

        EXEC etl.usp_audit_end
            @audit_id       = @audit_id,
            @status         = 'succeeded',
            @load_type      = @audit_load_type,
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
        -- Roll back the open transaction, if any
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Audit AFTER the rollback, or it would be rolled back too.
        -- usp_audit_end reads ERROR_MESSAGE() & co. by itself.
        IF @audit_id IS NOT NULL
            EXEC etl.usp_audit_end
                @audit_id       = @audit_id,
                @status         = 'failed',
                @load_type      = @audit_load_type,
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
