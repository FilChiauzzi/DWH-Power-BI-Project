/*
=========================================================
Bronze Load Procedure
=========================================================
Purpose:
    This stored procedure loads Bronze-layer tables based on
    metadata stored in etl.config. It supports both FULL and
    INCREMENTAL loads using a watermark column.

New in v2.1 (why each change was made):
    1. New table etl.batch: one row per pipeline run. Its
       IDENTITY is the batch_id stamped on every bronze row.
    2. New optional parameter @batch_id:
         - SSIS passes it (same value for every object of the
           same pipeline run), OR
         - it is NULL (manual execution) and the procedure
           creates its own batch row, so it works standalone.
    3. dwh_batch_id and dwh_source_row_num are now populated:
         - dwh_batch_id       = the batch key of this run
         - dwh_source_row_num = ROW_NUMBER() of the row within
           THIS extraction (restarts at 1 every load: it traces
           the row's position in the source response, it is not
           a lifetime counter).
       dwh_load_date stays filled by its column DEFAULT.
    4. The column list built from sys.columns now also appends
       the two audit columns to insert; dwh_load_date is still
       excluded (DEFAULT fills it).

Improvements kept from v2 (unchanged):
    - Explicit column list instead of SELECT * (bronze has 3
      audit columns the source does not have).
    - Watermark advanced ONLY when a new value exists.
    - THROW on missing/invalid configuration.
    - FULL load wrapped in a transaction (TRUNCATE + INSERT
      atomic), SET XACT_ABORT ON.
    - Watermark comparison parameterized with sp_executesql,
      identifiers wrapped with QUOTENAME().
    - CATCH logs, rolls back, and RE-THROWS: SSIS must see
      the failure.
    - @@ROWCOUNT captured for logging.

SSIS Usage:
    - The Foreach Loop iterates over an Object variable
      "object_list" containing active object names.
    - Each iteration assigns the current object name to
      "current_object_name".
    - This procedure is executed via an Execute SQL Task:
          EXEC etl.usp_load_bronze ?, @batch_id OUTPUT
      (declare an SSIS variable batch_id, fill it once before
      the loop, pass it to every call of the run).
    - If you do not pass it, the procedure creates its own batch.

Warning:
    This procedure performs TRUNCATE TABLE during FULL loads.
    Ensure downstream dependencies are aware of this behavior.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Batch registry: one row per pipeline run.
-- Idempotent: created only if it does not exist.
---------------------------------------------------------
IF OBJECT_ID('etl.batch', 'U') IS NULL
BEGIN
    CREATE TABLE etl.batch (
        batch_id     BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_etl_batch PRIMARY KEY,
        started_at   DATETIME2(3) NOT NULL DEFAULT SYSDATETIME(),
        ended_at     DATETIME2(3) NULL,
        triggered_by NVARCHAR(100) NULL,   -- who/what started it (SUSER_SNAME(), SSIS, SQL Agent job)
        notes        NVARCHAR(500) NULL
    );
    PRINT 'Table etl.batch created.';
END
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [etl].[usp_load_bronze]
    @object_name VARCHAR(50),
    @batch_id    BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @start_time       DATETIME2(3),
        @end_time         DATETIME2(3),
        @batch_start_time DATETIME2(3),
        @batch_end_time   DATETIME2(3);

    BEGIN TRY
        SET @batch_start_time = SYSDATETIME();

        ---------------------------------------------------------
        -- Batch resolution:
        -- if the caller (SSIS) did not pass a batch_id, this
        -- execution registers itself in etl.batch.
        ---------------------------------------------------------
        IF @batch_id IS NULL
        BEGIN
            INSERT INTO etl.batch (triggered_by, notes)
            VALUES (SUSER_SNAME(), 'self-registered by usp_load_bronze for object: ' + @object_name);

            SET @batch_id = SCOPE_IDENTITY();
        END

        PRINT '====================================================';
        PRINT 'Retrieve configuration for object: ' + @object_name;
        PRINT 'Batch id: ' + CAST(@batch_id AS NVARCHAR(20));
        PRINT '====================================================';

        DECLARE
            @load_type            VARCHAR(20),
            @source_table         VARCHAR(200),
            @bronze_table         VARCHAR(200),
            @watermark            VARCHAR(100),
            @last_watermark_value DATETIME,
            @new_watermark_value  DATETIME,
            @cols                 NVARCHAR(MAX),
            @cols_ins             NVARCHAR(MAX),
            @sql                  NVARCHAR(MAX),
            @rownum_expr          NVARCHAR(200),
            @rows_inserted        INT;

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

PRINT 'Procedure etl.usp_load_bronze created.';
GO

