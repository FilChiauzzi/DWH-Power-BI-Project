/*
=========================================================
Bronze Load – CDC objects (etl.usp_load_bronze_cdc)
=========================================================
Purpose:
    Loads one Bronze table from the SQL Server Change Data
    Capture of its source table (load_type = 'incremental'
    in etl.config: every incremental object uses CDC).
    Built step by step in the CDC lessons 2-7.

    1. Reads the bookmark last_lsn from etl.config.
    2. Reads the CDC window inside the source database:
       oldest change kept for the table (min), latest change
       captured (max), position right after the bookmark (from).
    3. Decides:
         full  no bookmark, or bookmark older than the CDC
               retention: snapshot of the source (rows F);
         cdc   net changes between the bookmark and now
               (rows I / U / D);
         none  nothing new.
    4. Volume checks, before writing anything:
         full  an empty source (error 52005) or a source with
               more than 50% fewer rows than the current
               state of the Bronze (error 52006) stops the load;
         cdc   more than 50% of the current rows deleted in
               one night (error 52007) stops the load.
       The current state of the Bronze is the latest version
       of each key, deleted keys excluded. On error nothing is
       written and the bookmark does not move.
    5. Writes the rows and saves the new bookmark in ONE
       transaction: both or neither.

    CDC says WHICH rows changed and HOW; the values are read
    from the source (CDC does not store computed columns).
    The key comes from CDC, so deleted rows keep it.
    The bookmark is "how far the tape was read" (max LSN of
    the database), not the last change of the table.

SSIS Usage:
    Not called by SSIS directly: etl.usp_load_bronze calls it
    for objects with load_type = 'incremental'.
    Can be run by hand:
        EXEC etl.usp_load_bronze_cdc
             @object_name = 'sales_order_detail',
             @batch_id    = 123,
             @debug       = 1;      -- prints the generated SQL
    The thresholds of the volume checks are parameters
    (@max_drop_pct, @max_del_pct, default 50): the pipeline
    uses the defaults, a test can force them.

Warning:
    CDC must be enabled on the source table with
    @supports_net_changes = 1, and SQL Server Agent must be
    running (capture job).
*/

USE [DWH_AdventureWorks2025];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE etl.usp_load_bronze_cdc
    @object_name   VARCHAR(50),
    @batch_id      BIGINT,
    @debug         BIT         = 0,        -- 1 = print the generated SQL
    @max_drop_pct  INT         = 50,       -- full: more than 50% fewer rows than the current state = suspicious
    @max_del_pct   INT         = 50,       -- cdc: more than 50% of the rows deleted in one night = suspicious
    @rows_inserted INT         = NULL OUTPUT,
    @mode          VARCHAR(10) = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @source_table VARCHAR(200),  @bronze_table VARCHAR(200),
        @load_type    VARCHAR(20),   @last_lsn     BINARY(10),
        @min_lsn      BINARY(10),    @max_lsn      BINARY(10),
        @from_lsn     BINARY(10),    @to_lsn       BINARY(10),
        @rows         INT = 0,
        @src_exec     NVARCHAR(300), @src_obj      NVARCHAR(300),
        @ci           SYSNAME,
        @cols         NVARCHAR(MAX), @cols_cdc     NVARCHAR(MAX),
        @join         NVARCHAR(MAX), @key_order    NVARCHAR(MAX),
        @sql          NVARCHAR(MAX),
        @src_rows     BIGINT,        @state_rows   BIGINT,
        @deleted_rows BIGINT,
        @msg          NVARCHAR(400);

    BEGIN TRY
        ---------------------------------------------------------
        -- 1. Configuration
        ---------------------------------------------------------
        SELECT @source_table = source_table,
               @bronze_table = bronze_table,
               @load_type    = load_type,
               @last_lsn     = last_lsn
        FROM etl.config
        WHERE object_name = @object_name
          AND is_active = 1;

        IF @source_table IS NULL
            THROW 52000, 'No active configuration found for the object in etl.config.', 1;

        IF @load_type <> 'incremental'
            THROW 52001, 'The object is not incremental in etl.config: CDC is used only by incremental objects.', 1;

        ---------------------------------------------------------
        -- 2. Names derived from source_table
        ---------------------------------------------------------
        SET @src_exec = QUOTENAME(PARSENAME(@source_table, 3)) + N'.sys.sp_executesql';
        SET @src_obj  = QUOTENAME(PARSENAME(@source_table, 2)) + N'.' + QUOTENAME(PARSENAME(@source_table, 1));
        SET @ci       = PARSENAME(@source_table, 2) + N'_' + PARSENAME(@source_table, 1);

        ---------------------------------------------------------
        -- 3. The window, computed inside the source database
        ---------------------------------------------------------
        EXEC @src_exec
             N'SELECT @min  = sys.fn_cdc_get_min_lsn(@ci),
                      @max  = sys.fn_cdc_get_max_lsn(),
                      @from = sys.fn_cdc_increment_lsn(@last);',
             N'@ci SYSNAME, @last BINARY(10),
               @min BINARY(10) OUTPUT, @max BINARY(10) OUTPUT, @from BINARY(10) OUTPUT',
             @ci = @ci, @last = @last_lsn,
             @min = @min_lsn OUTPUT, @max = @max_lsn OUTPUT, @from = @from_lsn OUTPUT;

        IF @min_lsn IS NULL OR @min_lsn = 0x00000000000000000000
            THROW 52002, 'CDC is not enabled on the source table (capture instance not found).', 1;

        SET @to_lsn = @max_lsn;          -- taken NOW, before any copy

        ---------------------------------------------------------
        -- 4. The decision
        ---------------------------------------------------------
        SET @mode =
            CASE
                WHEN @last_lsn IS NULL    THEN 'full'      -- first time
                WHEN @from_lsn < @min_lsn THEN 'full'      -- bookmark too old
                WHEN @from_lsn > @max_lsn THEN 'none'      -- nothing new
                ELSE                           'cdc'       -- read the changes
            END;

        PRINT '>> ' + @object_name + ' | CDC mode: ' + @mode;

        IF @mode <> 'none'
        BEGIN
            -----------------------------------------------------
            -- 5. Columns and key
            -----------------------------------------------------
            SELECT @cols = STRING_AGG(CAST(QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ')
                           WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(@bronze_table)
              AND c.name NOT LIKE 'dwh[_]%';

            IF @cols IS NULL
                THROW 52003, 'Bronze table not found. Check bronze_table in etl.config.', 1;

            DROP TABLE IF EXISTS #keys;
            CREATE TABLE #keys (column_name SYSNAME COLLATE DATABASE_DEFAULT, index_ordinal INT);

            INSERT INTO #keys (column_name, index_ordinal)
            EXEC @src_exec
                 N'SELECT ic.column_name, ic.index_ordinal
                   FROM cdc.index_columns AS ic
                   JOIN cdc.change_tables AS ct ON ct.object_id = ic.object_id
                   WHERE ct.capture_instance = @ci;',
                 N'@ci SYSNAME',
                 @ci = @ci;

            IF NOT EXISTS (SELECT 1 FROM #keys)
                THROW 52004, 'No key columns found for the CDC capture instance.', 1;

            SELECT @join = STRING_AGG(CAST(N's.' + QUOTENAME(column_name) + N' = nc.' + QUOTENAME(column_name) AS NVARCHAR(MAX)), N' AND ')
                           WITHIN GROUP (ORDER BY index_ordinal),
                   @key_order = STRING_AGG(CAST(QUOTENAME(column_name) AS NVARCHAR(MAX)), N', ')
                           WITHIN GROUP (ORDER BY index_ordinal)
            FROM #keys;

            SELECT @cols_cdc = STRING_AGG(CAST(CASE WHEN k.column_name IS NOT NULL THEN N'nc.' ELSE N's.' END
                                               + QUOTENAME(c.name) AS NVARCHAR(MAX)), N', ')
                               WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.columns AS c
            LEFT JOIN #keys  AS k ON k.column_name = c.name
            WHERE c.object_id = OBJECT_ID(@bronze_table)
              AND c.name NOT LIKE 'dwh[_]%';
        END;

        ---------------------------------------------------------
        -- 5b. Volume checks, BEFORE writing anything
        ---------------------------------------------------------
        IF @mode <> 'none'
        BEGIN
            -- Current state of the Bronze: latest version of each key,
            -- deleted keys excluded (= the source as we last saw it)
            SET @sql =
                N'SELECT @n = COUNT_BIG(*)' +
                N' FROM (SELECT dwh_operation,' +
                N'              ROW_NUMBER() OVER (PARTITION BY ' + @key_order +
                N'                                 ORDER BY dwh_batch_id DESC, dwh_source_row_num DESC) AS rn' +
                N'       FROM ' + @bronze_table + N') AS v' +
                N' WHERE v.rn = 1 AND v.dwh_operation <> ''D'';';
            EXEC sp_executesql @sql, N'@n BIGINT OUTPUT', @n = @state_rows OUTPUT;

            IF @mode = 'full'
            BEGIN
                -- Rows of the source now
                SET @sql = N'SELECT @n = COUNT_BIG(*) FROM ' + @source_table + N';';
                EXEC sp_executesql @sql, N'@n BIGINT OUTPUT', @n = @src_rows OUTPUT;

                IF @src_rows = 0
                BEGIN
                    SET @msg = CONCAT('Source is empty. Current Bronze state: ', @state_rows,
                                      ' rows. Bronze left untouched.');
                    THROW 52005, @msg, 1;
                END;

                IF @state_rows > 0
                   AND @src_rows < @state_rows * (100 - @max_drop_pct) / 100.0
                BEGIN
                    SET @msg = CONCAT('Source has ', @src_rows, ' rows, current Bronze state ', @state_rows,
                                      ': drop over ', @max_drop_pct, '%. Bronze left untouched.');
                    THROW 52006, @msg, 1;
                END;

                PRINT CONCAT('>> Volume check ok: source ', @src_rows, ', current state ', @state_rows);
            END
            ELSE
            BEGIN
                -- Deletions in tonight's window, counted inside the source
                SET @sql =
                    N'SELECT @n = COUNT_BIG(*)' +
                    N' FROM cdc.' + QUOTENAME(N'fn_cdc_get_net_changes_' + @ci) + N'(@from_lsn, @to_lsn, N''all'')' +
                    N' WHERE __$operation = 1;';
                EXEC @src_exec @sql,
                     N'@from_lsn BINARY(10), @to_lsn BINARY(10), @n BIGINT OUTPUT',
                     @from_lsn = @from_lsn, @to_lsn = @to_lsn, @n = @deleted_rows OUTPUT;

                IF @state_rows > 0
                   AND @deleted_rows > @state_rows * @max_del_pct / 100.0
                BEGIN
                    SET @msg = CONCAT(@deleted_rows, ' rows deleted in the source tonight, current Bronze state ',
                                      @state_rows, ': over ', @max_del_pct,
                                      '%. Mass delete? Nothing written, bookmark unchanged.');
                    THROW 52007, @msg, 1;
                END;

                PRINT CONCAT('>> Delete check ok: ', @deleted_rows, ' deletions, current state ', @state_rows);
            END;
        END;

        ---------------------------------------------------------
        -- 6a. FULL: snapshot of the source (dwh_operation = 'F' by default)
        ---------------------------------------------------------
        IF @mode = 'full'
        BEGIN
            SET @sql =
                N'TRUNCATE TABLE ' + @bronze_table + N';' +
                N' INSERT INTO ' + @bronze_table +
                N' (' + @cols + N', [dwh_batch_id], [dwh_source_row_num])' +
                N' SELECT ' + @cols + N', @batch_id, ROW_NUMBER() OVER (ORDER BY ' + @key_order + N')' +
                N' FROM ' + @source_table + N';' +
                N' SET @rows = @@ROWCOUNT;';

            IF @debug = 1 PRINT @sql;

            BEGIN TRANSACTION;

                EXEC sp_executesql @sql,
                     N'@batch_id BIGINT, @rows INT OUTPUT',
                     @batch_id = @batch_id, @rows = @rows OUTPUT;

                UPDATE etl.config SET last_lsn = @to_lsn
                WHERE object_name = @object_name;

            COMMIT TRANSACTION;
        END

        ---------------------------------------------------------
        -- 6b. CDC: net changes + source values, run inside the source
        ---------------------------------------------------------
        ELSE IF @mode = 'cdc'
        BEGIN
            SET @sql =
                N'INSERT INTO ' + QUOTENAME(DB_NAME()) + N'.' + @bronze_table +
                N' (' + @cols + N', [dwh_batch_id], [dwh_source_row_num], [dwh_operation])' +
                N' SELECT ' + @cols_cdc + N', @batch_id,' +
                N' ROW_NUMBER() OVER (ORDER BY nc.__$start_lsn),' +
                N' CASE nc.__$operation WHEN 1 THEN ''D'' WHEN 2 THEN ''I'' ELSE ''U'' END' +
                N' FROM cdc.' + QUOTENAME(N'fn_cdc_get_net_changes_' + @ci) + N'(@from_lsn, @to_lsn, N''all'') AS nc' +
                N' LEFT JOIN ' + @src_obj + N' AS s ON ' + @join + N';' +
                N' SET @rows = @@ROWCOUNT;';

            IF @debug = 1 PRINT @sql;

            BEGIN TRANSACTION;

                EXEC @src_exec @sql,
                     N'@batch_id BIGINT, @from_lsn BINARY(10), @to_lsn BINARY(10), @rows INT OUTPUT',
                     @batch_id = @batch_id, @from_lsn = @from_lsn, @to_lsn = @to_lsn,
                     @rows = @rows OUTPUT;

                UPDATE etl.config SET last_lsn = @to_lsn
                WHERE object_name = @object_name;

            COMMIT TRANSACTION;
        END;

        -- 6c. NONE: nothing to read, bookmark unchanged

        SET @rows_inserted = @rows;     -- returned to the caller (usp_load_bronze)

        PRINT '>> Rows inserted: ' + CAST(@rows AS VARCHAR(20));
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
