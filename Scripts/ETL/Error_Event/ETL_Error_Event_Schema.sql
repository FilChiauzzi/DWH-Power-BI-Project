/*
=========================================================
Error Event Schema (Kimball subsystem 5)
=========================================================
Purpose:
    The error event schema of Kimball, The Data Warehouse
    Toolkit, 3rd ed., Figure 19-1: one place that records every
    error thrown by a quality screen anywhere in the ETL.

    Dimensions
      etl.dim_screen        one row per quality screen
      etl.dim_table         the tables checked by the screens
      etl.dim_field         the fields of those tables
      etl.batch             Batch Dimension (already existing)
      gold.dwh_dim_calendar Date Dimension (date_id = yyyymmdd)
    Facts
      etl.fact_error_event         every error thrown by a screen
      etl.fact_error_event_detail  every field of the record
                                   that took part in the error

    Metadata-driven: nothing is written by hand. A screen
    describes itself (name, type, severity, action, fields)
    when it writes its errors into #errors; the procedure
    etl.usp_save_error_events adds to the dimensions every
    screen, table and field it does not know yet, then writes
    the facts.

SSIS Usage:
    Not executed by SSIS. The quality checks and the Bronze
    loads call etl.usp_save_error_events.

Warning:
    Drops and recreates the error event objects: the error
    history is lost. Run after ETL_Batch.sql.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- 0. Drop (also the old quality log)
---------------------------------------------------------
DROP VIEW      IF EXISTS etl.vw_error_event_daily;
DROP PROCEDURE IF EXISTS etl.usp_save_error_events;
DROP TABLE     IF EXISTS etl.fact_error_event_detail;
DROP TABLE     IF EXISTS etl.fact_error_event;
DROP TABLE     IF EXISTS etl.dim_field;
DROP TABLE     IF EXISTS etl.dim_screen;
DROP TABLE     IF EXISTS etl.dim_table;
DROP TABLE     IF EXISTS etl.data_quality_log;
GO

---------------------------------------------------------
-- 1. Dimensions
---------------------------------------------------------
CREATE TABLE etl.dim_table (
    table_key    INT IDENTITY(1,1) NOT NULL,
    object_name  VARCHAR(50)       NOT NULL,      -- product, sales_order_detail, ...
    layer        VARCHAR(10)       NOT NULL,      -- layer where the screens run
    CONSTRAINT PK_etl_dim_table PRIMARY KEY (table_key)
);
CREATE UNIQUE INDEX UX_etl_dim_table_object ON etl.dim_table (object_name, layer);

CREATE TABLE etl.dim_field (
    field_key    INT IDENTITY(1,1) NOT NULL,
    table_key    INT               NOT NULL,
    field_name   SYSNAME           NOT NULL,      -- '(whole record)' when no field is named
    CONSTRAINT PK_etl_dim_field PRIMARY KEY (field_key),
    CONSTRAINT FK_etl_dim_field_table FOREIGN KEY (table_key) REFERENCES etl.dim_table (table_key)
);
CREATE UNIQUE INDEX UX_etl_dim_field_name ON etl.dim_field (table_key, field_name);

CREATE TABLE etl.dim_screen (
    screen_key                   INT IDENTITY(1,1) NOT NULL,
    screen_name                  NVARCHAR(200)     NOT NULL,
    object_name                  VARCHAR(50)       NOT NULL,
    screen_type                  VARCHAR(20)       NOT NULL,   -- column / structure / business rule
    etl_module                   NVARCHAR(256)     NULL,       -- the procedure that runs the screen
    screen_processing_definition NVARCHAR(500)     NULL,       -- what the screen tests
    severity                     VARCHAR(20)       NOT NULL,   -- critical / error / warning
    exception_action             VARCHAR(20)       NOT NULL,   -- set aside / pass / halt
    CONSTRAINT PK_etl_dim_screen PRIMARY KEY (screen_key),
    CONSTRAINT CK_etl_dim_screen_type     CHECK (screen_type IN ('column', 'structure', 'business rule')),
    CONSTRAINT CK_etl_dim_screen_severity CHECK (severity IN ('critical', 'error', 'warning')),
    CONSTRAINT CK_etl_dim_screen_action   CHECK (exception_action IN ('set aside', 'pass', 'halt'))
);
CREATE UNIQUE INDEX UX_etl_dim_screen_name ON etl.dim_screen (object_name, screen_name);
GO

---------------------------------------------------------
-- 2. Facts
---------------------------------------------------------
CREATE TABLE etl.fact_error_event (
    error_event_key      BIGINT IDENTITY(1,1) NOT NULL,
    error_event_date_key INT          NOT NULL,   -- yyyymmdd, like gold.dwh_dim_calendar.date_id
    screen_key           INT          NOT NULL,
    batch_key            BIGINT       NULL,       -- etl.batch.batch_id
    error_date_time      DATETIME2(3) NOT NULL,
    severity_score       TINYINT      NOT NULL,   -- 3 critical, 2 error, 1 warning
    CONSTRAINT PK_etl_fact_error_event PRIMARY KEY (error_event_key),
    CONSTRAINT FK_etl_fact_error_event_screen FOREIGN KEY (screen_key) REFERENCES etl.dim_screen (screen_key),
    CONSTRAINT FK_etl_fact_error_event_batch  FOREIGN KEY (batch_key)  REFERENCES etl.batch (batch_id)
);

CREATE TABLE etl.fact_error_event_detail (
    error_event_key        BIGINT        NOT NULL,   -- ties the detail to its event
    error_event_date_key   INT           NOT NULL,
    screen_key             INT           NOT NULL,
    batch_key              BIGINT        NULL,
    error_date_time        DATETIME2(3)  NOT NULL,
    table_key              INT           NOT NULL,
    field_key              INT           NOT NULL,
    record_identifier_key  NVARCHAR(200) NULL,       -- business key of the record, as text
    error_condition        NVARCHAR(500) NULL,
    CONSTRAINT PK_etl_fact_error_event_detail PRIMARY KEY (error_event_key, field_key),
    CONSTRAINT FK_etl_fact_error_event_detail_event FOREIGN KEY (error_event_key) REFERENCES etl.fact_error_event (error_event_key),
    CONSTRAINT FK_etl_fact_error_event_detail_table FOREIGN KEY (table_key)       REFERENCES etl.dim_table (table_key),
    CONSTRAINT FK_etl_fact_error_event_detail_field FOREIGN KEY (field_key)       REFERENCES etl.dim_field (field_key)
);
GO

---------------------------------------------------------
-- 3. The procedure that saves the errors of a screen run
---------------------------------------------------------
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
CREATE PROCEDURE etl.usp_save_error_events
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

---------------------------------------------------------
-- 4. Data quality over time
---------------------------------------------------------
CREATE VIEW etl.vw_error_event_daily
AS
SELECT CAST(e.error_date_time AS DATE) AS error_date,
       s.object_name,
       s.screen_type,
       s.screen_name,
       s.severity,
       s.exception_action,
       COUNT(*)                        AS error_events
FROM etl.fact_error_event AS e
JOIN etl.dim_screen       AS s ON s.screen_key = e.screen_key
GROUP BY CAST(e.error_date_time AS DATE), s.object_name, s.screen_type,
         s.screen_name, s.severity, s.exception_action;
GO
