/*
=========================================================
ETL Procedure – etl.usp_save_error_events
=========================================================
Purpose:
    Saves the errors found by the quality screens into the
    error event schema (Kimball subsystem 5). Metadata-driven:
    a screen describes itself (name, type, severity, action,
    fields) when it writes its errors into #errors; this
    procedure adds to the dimensions every screen, table and
    field it does not know yet, then writes the facts.

SSIS Usage:
    Not called by SSIS: the quality checks and the Bronze
    loads call it, after filling #errors.

Warning:
    #errors must exist (created by the caller). It is
    emptied at the end.
*/

USE [DWH_AdventureWorks2025];
GO

/*
    The caller fills #errors, one row per error found:

    CREATE TABLE #errors (
        row_id           INT IDENTITY(1,1),
        screen_name      NVARCHAR(200),  -- 'OrderQty invalid'
        screen_type      VARCHAR(20),    -- column / structure / business rule
        severity         VARCHAR(20),    -- critical / error / warning
        exception_action VARCHAR(20),    -- set aside / pass / halt
        fields           NVARCHAR(400),  -- 'UnitPrice,OrderQty' ('' = whole record)
        record_id        NVARCHAR(200),  -- '43659-1'
        error_condition  NVARCHAR(500)   -- 'OrderQty <= 0'
    );

    then: EXEC etl.usp_save_error_events @batch_id, @layer, @object_name, @etl_module;
*/
CREATE OR ALTER PROCEDURE etl.usp_save_error_events
    @batch_id    BIGINT,
    @layer       VARCHAR(10),
    @object_name VARCHAR(50),
    @etl_module  NVARCHAR(256)
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM #errors) RETURN;

    DECLARE @now DATETIME2(3) = SYSDATETIME(),
            @table_key INT;

    -- 1. The table
    IF NOT EXISTS (SELECT 1 FROM etl.dim_table WHERE object_name = @object_name AND layer = @layer)
        INSERT INTO etl.dim_table (object_name, layer) VALUES (@object_name, @layer);

    SELECT @table_key = table_key FROM etl.dim_table WHERE object_name = @object_name AND layer = @layer;

    -- 2. The screens: new ones added, known ones updated (the code is the truth)
    MERGE etl.dim_screen AS d
    USING (SELECT screen_name, MAX(screen_type) AS screen_type, MAX(severity) AS severity,
                  MAX(exception_action) AS exception_action, MIN(error_condition) AS definition
           FROM #errors GROUP BY screen_name) AS s
       ON d.object_name = @object_name AND d.screen_name = s.screen_name
    WHEN MATCHED THEN
        UPDATE SET screen_type = s.screen_type, severity = s.severity,
                   exception_action = s.exception_action, etl_module = @etl_module
    WHEN NOT MATCHED THEN
        INSERT (screen_name, object_name, screen_type, etl_module, screen_processing_definition, severity, exception_action)
        VALUES (s.screen_name, @object_name, s.screen_type, @etl_module, s.definition, s.severity, s.exception_action);

    -- 3. The fields: one row per field named by a screen
    SELECT DISTINCT e.row_id, ISNULL(NULLIF(LTRIM(f.value), ''), '(whole record)') AS field_name
    INTO #error_fields
    FROM #errors AS e
    CROSS APPLY STRING_SPLIT(CONCAT(e.fields, ''), ',') AS f;

    INSERT INTO etl.dim_field (table_key, field_name)
    SELECT DISTINCT @table_key, x.field_name
    FROM #error_fields AS x
    WHERE NOT EXISTS (SELECT 1 FROM etl.dim_field AS d WHERE d.table_key = @table_key AND d.field_name = x.field_name);

    -- 4. The error events: one per error
    DECLARE @map TABLE (row_id INT, error_event_key BIGINT);

    MERGE etl.fact_error_event AS t
    USING (SELECT e.row_id, s.screen_key,
                  CASE e.severity WHEN 'critical' THEN 3 WHEN 'error' THEN 2 ELSE 1 END AS severity_score
           FROM #errors AS e
           JOIN etl.dim_screen AS s ON s.object_name = @object_name AND s.screen_name = e.screen_name) AS e
       ON 1 = 0
    WHEN NOT MATCHED THEN
        INSERT (error_event_date_key, screen_key, batch_key, error_date_time, severity_score)
        VALUES (CONVERT(INT, FORMAT(@now, 'yyyyMMdd')), e.screen_key, @batch_id, @now, e.severity_score)
    OUTPUT e.row_id, inserted.error_event_key INTO @map (row_id, error_event_key);

    -- 5. The details: one per field of each error
    INSERT INTO etl.fact_error_event_detail
        (error_event_key, error_event_date_key, screen_key, batch_key, error_date_time,
         table_key, field_key, record_identifier_key, error_condition)
    SELECT m.error_event_key, f.error_event_date_key, f.screen_key, f.batch_key, f.error_date_time,
           @table_key, d.field_key, e.record_id, e.error_condition
    FROM @map AS m
    JOIN #errors              AS e ON e.row_id = m.row_id
    JOIN etl.fact_error_event AS f ON f.error_event_key = m.error_event_key
    JOIN #error_fields        AS x ON x.row_id = m.row_id
    JOIN etl.dim_field        AS d ON d.table_key = @table_key AND d.field_name = x.field_name;

    -- The errors are saved: empty the list for the next call
    TRUNCATE TABLE #errors;
END;
GO
