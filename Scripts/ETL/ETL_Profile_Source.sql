/*
=========================================================
ETL – Source Profiling (etl.data_profile, etl.usp_profile_source)
=========================================================
Purpose:
    Profiles the source tables of the active objects in
    etl.config (Kimball ETL subsystem 1, Data Profiling).
    For every column it stores: row count, NULL count and
    percentage, distinct count, min and max value and, for
    text columns, the max length. Every execution gets a new
    profile_run_id, so two runs can be compared over time.

    The result confirms (or corrects) what IT declared in the
    Source System Analysis: row counts, nullability, candidate
    keys, value ranges.

SSIS Usage:
    Not part of the nightly pipeline. Run on demand:
    - before a new object is added to etl.config;
    - periodically, to see whether the source has changed.
        EXEC etl.usp_profile_source;                         -- all active objects
        EXEC etl.usp_profile_source @object_name = 'product';

Warning:
    Reads the full source tables, one query per column.
    Run outside business hours, as agreed with IT.
    Columns of type xml, geography, geometry, hierarchyid,
    varbinary, image, text, ntext and sql_variant are
    profiled for NULLs only.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Table (created only if it does not exist: history is kept)
---------------------------------------------------------
IF OBJECT_ID('etl.data_profile', 'U') IS NULL
BEGIN
    CREATE TABLE etl.data_profile (
        profile_key     BIGINT IDENTITY(1,1) NOT NULL,
        profile_run_id  BIGINT          NOT NULL,   -- one run = one execution of the procedure
        profiled_at     DATETIME2(3)    NOT NULL
            CONSTRAINT DF_etl_data_profile_profiled_at DEFAULT SYSDATETIME(),
        object_name     VARCHAR(50)     NOT NULL,
        source_table    VARCHAR(200)    NOT NULL,
        column_id       INT             NOT NULL,
        column_name     SYSNAME         NOT NULL,
        data_type       SYSNAME         NOT NULL,
        is_nullable     BIT             NOT NULL,   -- as declared in the source
        row_count       BIGINT          NULL,
        null_count      BIGINT          NULL,
        null_pct        DECIMAL(5,2)    NULL,
        distinct_count  BIGINT          NULL,
        min_value       NVARCHAR(400)   NULL,
        max_value       NVARCHAR(400)   NULL,
        max_length      INT             NULL,       -- text columns only
        CONSTRAINT PK_etl_data_profile PRIMARY KEY CLUSTERED (profile_key)
    );

    CREATE INDEX IX_etl_data_profile_run
        ON etl.data_profile (profile_run_id, object_name, column_id);
END;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

---------------------------------------------------------
-- Procedure
---------------------------------------------------------
CREATE OR ALTER PROCEDURE etl.usp_profile_source
    @object_name VARCHAR(50) = NULL     -- NULL = every active object
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @run_id        BIGINT,
        @obj           VARCHAR(50),
        @source_table  VARCHAR(200),
        @db            SYSNAME,
        @sql           NVARCHAR(MAX),
        @column_id     INT,
        @col           SYSNAME,
        @type          SYSNAME,
        @nullable      BIT,
        @c             NVARCHAR(300),
        @min_expr      NVARCHAR(500),
        @max_expr      NVARCHAR(500),
        @dist_expr     NVARCHAR(300),
        @len_expr      NVARCHAR(300),
        @rows          BIGINT,
        @nulls         BIGINT,
        @distinct      BIGINT,
        @min           NVARCHAR(400),
        @max           NVARCHAR(400),
        @maxlen        INT;

    SELECT @run_id = ISNULL(MAX(profile_run_id), 0) + 1
    FROM etl.data_profile;

    PRINT '====================================================';
    PRINT 'Source profiling | run: ' + CAST(@run_id AS VARCHAR(20));
    PRINT '====================================================';

    ---------------------------------------------------------
    -- Objects to profile: active rows of etl.config
    -- (the Gold fact row is inactive, so it is skipped)
    ---------------------------------------------------------
    DECLARE obj_cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT object_name, source_table
        FROM etl.config
        WHERE is_active = 1
          AND (@object_name IS NULL OR object_name = @object_name)
        ORDER BY load_order;

    OPEN obj_cur;
    FETCH NEXT FROM obj_cur INTO @obj, @source_table;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT '>> Profiling ' + @obj + ' (' + @source_table + ')';

        -----------------------------------------------------
        -- Column list of the source table (other database)
        -----------------------------------------------------
        SET @db = PARSENAME(@source_table, 3);

        IF OBJECT_ID('tempdb..#cols') IS NOT NULL DROP TABLE #cols;
        CREATE TABLE #cols (
            column_id   INT,
            column_name SYSNAME,
            data_type   SYSNAME,
            is_nullable BIT
        );

        -- TYPE_NAME(system_type_id): base type, so the
        -- AdventureWorks user types (Name, Flag...) become
        -- nvarchar, bit...
        SET @sql = N'
            SELECT c.column_id, c.name, TYPE_NAME(c.system_type_id), c.is_nullable
            FROM ' + QUOTENAME(@db) + N'.sys.columns AS c
            WHERE c.object_id = OBJECT_ID(@tbl)
              AND c.is_computed = 0
            ORDER BY c.column_id;';

        INSERT INTO #cols (column_id, column_name, data_type, is_nullable)
        EXEC sp_executesql @sql, N'@tbl NVARCHAR(400)', @tbl = @source_table;

        IF NOT EXISTS (SELECT 1 FROM #cols)
            PRINT '   !! Source table not found or no columns: check source_table in etl.config';

        -----------------------------------------------------
        -- One query per column
        -----------------------------------------------------
        DECLARE col_cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT column_id, column_name, data_type, is_nullable
            FROM #cols
            ORDER BY column_id;

        OPEN col_cur;
        FETCH NEXT FROM col_cur INTO @column_id, @col, @type, @nullable;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @c = QUOTENAME(@col);

            -- Types that cannot be compared or counted distinct
            IF @type IN ('xml', 'geography', 'geometry', 'hierarchyid',
                         'varbinary', 'binary', 'image', 'text', 'ntext',
                         'sql_variant', 'timestamp')
            BEGIN
                SET @dist_expr = N'NULL';
                SET @min_expr  = N'NULL';
                SET @max_expr  = N'NULL';
            END
            ELSE IF @type = 'uniqueidentifier'
            BEGIN
                SET @dist_expr = N'COUNT_BIG(DISTINCT ' + @c + N')';
                SET @min_expr  = N'NULL';
                SET @max_expr  = N'NULL';
            END
            ELSE IF @type = 'bit'
            BEGIN
                -- MIN/MAX do not accept bit: cast to tinyint
                SET @dist_expr = N'COUNT_BIG(DISTINCT ' + @c + N')';
                SET @min_expr  = N'CONVERT(NVARCHAR(400), MIN(CAST(' + @c + N' AS TINYINT)))';
                SET @max_expr  = N'CONVERT(NVARCHAR(400), MAX(CAST(' + @c + N' AS TINYINT)))';
            END
            ELSE IF @type IN ('date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset', 'time')
            BEGIN
                -- Style 121: yyyy-mm-dd hh:mi:ss.mmm
                SET @dist_expr = N'COUNT_BIG(DISTINCT ' + @c + N')';
                SET @min_expr  = N'CONVERT(NVARCHAR(400), MIN(' + @c + N'), 121)';
                SET @max_expr  = N'CONVERT(NVARCHAR(400), MAX(' + @c + N'), 121)';
            END
            ELSE
            BEGIN
                SET @dist_expr = N'COUNT_BIG(DISTINCT ' + @c + N')';
                SET @min_expr  = N'CONVERT(NVARCHAR(400), MIN(' + @c + N'))';
                SET @max_expr  = N'CONVERT(NVARCHAR(400), MAX(' + @c + N'))';
            END;

            SET @len_expr =
                CASE WHEN @type IN ('char', 'varchar', 'nchar', 'nvarchar')
                     THEN N'MAX(LEN(' + @c + N'))'
                     ELSE N'NULL'
                END;

            SET @sql = N'
                SELECT
                    @rows     = COUNT_BIG(*),
                    @nulls    = SUM(CASE WHEN ' + @c + N' IS NULL THEN 1 ELSE 0 END),
                    @distinct = ' + @dist_expr + N',
                    @min      = ' + @min_expr  + N',
                    @max      = ' + @max_expr  + N',
                    @maxlen   = ' + @len_expr  + N'
                FROM ' + @source_table + N';';

            EXEC sp_executesql @sql,
                N'@rows BIGINT OUTPUT, @nulls BIGINT OUTPUT, @distinct BIGINT OUTPUT,
                  @min NVARCHAR(400) OUTPUT, @max NVARCHAR(400) OUTPUT, @maxlen INT OUTPUT',
                @rows = @rows OUTPUT, @nulls = @nulls OUTPUT, @distinct = @distinct OUTPUT,
                @min = @min OUTPUT, @max = @max OUTPUT, @maxlen = @maxlen OUTPUT;

            INSERT INTO etl.data_profile (
                profile_run_id, object_name, source_table,
                column_id, column_name, data_type, is_nullable,
                row_count, null_count, null_pct, distinct_count,
                min_value, max_value, max_length
            )
            VALUES (
                @run_id, @obj, @source_table,
                @column_id, @col, @type, @nullable,
                @rows, ISNULL(@nulls, 0),
                CASE WHEN @rows = 0 THEN NULL
                     ELSE CAST(100.0 * ISNULL(@nulls, 0) / @rows AS DECIMAL(5,2)) END,
                @distinct, @min, @max, @maxlen
            );

            FETCH NEXT FROM col_cur INTO @column_id, @col, @type, @nullable;
        END;

        CLOSE col_cur;
        DEALLOCATE col_cur;

        FETCH NEXT FROM obj_cur INTO @obj, @source_table;
    END;

    CLOSE obj_cur;
    DEALLOCATE obj_cur;

    PRINT '====================================================';
    PRINT 'Profiling completed | run: ' + CAST(@run_id AS VARCHAR(20));
    PRINT '====================================================';
END;
GO

/*
---------------------------------------------------------
Useful queries on the last run
---------------------------------------------------------

-- 1. Columns with NULLs (candidates for "Unknown" or for a DQ rule)
SELECT object_name, column_name, data_type, null_count, null_pct
FROM etl.data_profile
WHERE profile_run_id = (SELECT MAX(profile_run_id) FROM etl.data_profile)
  AND null_count > 0
ORDER BY null_pct DESC;

-- 2. Candidate keys: one distinct value per row, no NULLs
SELECT object_name, column_name, row_count, distinct_count
FROM etl.data_profile
WHERE profile_run_id = (SELECT MAX(profile_run_id) FROM etl.data_profile)
  AND distinct_count = row_count
  AND null_count = 0;

-- 3. Row counts per table (compare with section 4.2 of the Source System Analysis)
SELECT object_name, MAX(row_count) AS row_count
FROM etl.data_profile
WHERE profile_run_id = (SELECT MAX(profile_run_id) FROM etl.data_profile)
GROUP BY object_name
ORDER BY object_name;

-- 4. What changed between the last two runs
WITH r AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY profile_run_id DESC) AS rk
    FROM etl.data_profile
)
SELECT cur.object_name, cur.column_name,
       prev.row_count  AS rows_before, cur.row_count  AS rows_now,
       prev.null_count AS nulls_before, cur.null_count AS nulls_now
FROM r AS cur
JOIN r AS prev
  ON prev.object_name = cur.object_name
 AND prev.column_name = cur.column_name
 AND prev.rk = 2
WHERE cur.rk = 1
  AND (cur.row_count <> prev.row_count OR cur.null_count <> prev.null_count);
*/
