/*
=========================================================
Error Event Schema (Kimball subsystem 5)
=========================================================
Purpose:
    Creates the error event schema of Kimball, The Data
    Warehouse Toolkit, 3rd ed., Figure 19-1: a centralized
    dimensional schema that records every error thrown by a
    quality screen anywhere in the ETL pipeline.

    Dimensions
      etl.dim_screen   Screen Dimension: one row per quality
                       screen (type, ETL module, definition,
                       severity, exception action)
      etl.dim_table    The tables checked by the screens
      etl.dim_field    The fields of those tables
      etl.batch        Batch Dimension (already existing)
      gold.dwh_dim_calendar  Date Dimension (date_id = yyyymmdd)

    Facts
      etl.fact_error_event         grain: every error thrown by
                                   a screen
      etl.fact_error_event_detail  grain: every field of a record
                                   that took part in the error

    Bridge
      etl.bridge_screen_field      the fields tested by each
                                   screen (not in the figure: it
                                   tells the detail fact which
                                   fields an error involves)

    Compatibility
      etl.data_quality_log  becomes a VIEW on the facts, with
                            the same columns as the old table.
                            An INSTEAD OF INSERT trigger turns
                            every INSERT into one error event
                            and its detail rows, so the quality
                            checks and the Bronze loads write to
                            the star without any change.

SSIS Usage:
    Not executed by SSIS. Run once at deployment, then
    ETL_Error_Event_Screens.sql (the screen catalog). On an
    existing database, then Migration_Error_Event.sql.

Warning:
    If etl.data_quality_log exists as a TABLE, it is renamed
    etl.data_quality_log_legacy: its rows are moved by
    Migration_Error_Event.sql. No foreign key to the calendar:
    gold.dwh_dim_calendar covers the order dates, not the ETL
    run dates.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- 0. The old table becomes the legacy table
---------------------------------------------------------
IF OBJECT_ID('etl.data_quality_log', 'U') IS NOT NULL
    AND OBJECT_ID('etl.data_quality_log_legacy', 'U') IS NULL
    EXEC sp_rename 'etl.data_quality_log', 'data_quality_log_legacy';
GO

IF OBJECT_ID('etl.data_quality_log', 'V') IS NOT NULL DROP VIEW etl.data_quality_log;
IF OBJECT_ID('etl.vw_error_event_daily', 'V') IS NOT NULL DROP VIEW etl.vw_error_event_daily;
IF OBJECT_ID('etl.fact_error_event_detail', 'U') IS NOT NULL DROP TABLE etl.fact_error_event_detail;
IF OBJECT_ID('etl.fact_error_event', 'U') IS NOT NULL DROP TABLE etl.fact_error_event;
IF OBJECT_ID('etl.bridge_screen_field', 'U') IS NOT NULL DROP TABLE etl.bridge_screen_field;
IF OBJECT_ID('etl.dim_field', 'U') IS NOT NULL DROP TABLE etl.dim_field;
IF OBJECT_ID('etl.dim_screen', 'U') IS NOT NULL DROP TABLE etl.dim_screen;
IF OBJECT_ID('etl.dim_table', 'U') IS NOT NULL DROP TABLE etl.dim_table;
GO

---------------------------------------------------------
-- 1. Dimensions
---------------------------------------------------------
CREATE TABLE etl.dim_table (
    table_key    INT           NOT NULL,           -- -1 = unknown
    object_name  VARCHAR(50)   NOT NULL,           -- product, sales_order_detail, ...
    layer        VARCHAR(10)   NOT NULL,           -- layer where the screens run
    table_name   NVARCHAR(200) NOT NULL,           -- bronze.erp_product
    CONSTRAINT PK_etl_dim_table PRIMARY KEY (table_key)
);
CREATE UNIQUE INDEX UX_etl_dim_table_object ON etl.dim_table (object_name, layer);

CREATE TABLE etl.dim_field (
    field_key    INT           NOT NULL,           -- -1 = the whole record
    table_key    INT           NOT NULL,
    field_name   SYSNAME       NOT NULL,
    CONSTRAINT PK_etl_dim_field PRIMARY KEY (field_key),
    CONSTRAINT FK_etl_dim_field_table FOREIGN KEY (table_key) REFERENCES etl.dim_table (table_key)
);
CREATE UNIQUE INDEX UX_etl_dim_field_name ON etl.dim_field (table_key, field_name);

CREATE TABLE etl.dim_screen (
    screen_key                   INT           NOT NULL,   -- -1 = unknown
    screen_name                  NVARCHAR(200) NOT NULL,   -- = check_name of the old log
    object_name                  VARCHAR(50)   NOT NULL,
    screen_type                  VARCHAR(20)   NOT NULL,   -- column / structure / business rule
    etl_module                   NVARCHAR(256) NULL,       -- the procedure that runs the screen
    screen_processing_definition NVARCHAR(500) NULL,       -- what the screen tests
    severity                     VARCHAR(20)   NOT NULL,   -- critical / error / warning
    exception_action             VARCHAR(20)   NOT NULL,   -- set aside / pass / halt
    CONSTRAINT PK_etl_dim_screen PRIMARY KEY (screen_key),
    CONSTRAINT CK_etl_dim_screen_type     CHECK (screen_type IN ('column', 'structure', 'business rule', 'unclassified')),
    CONSTRAINT CK_etl_dim_screen_severity CHECK (severity IN ('critical', 'error', 'warning')),
    CONSTRAINT CK_etl_dim_screen_action   CHECK (exception_action IN ('set aside', 'pass', 'halt'))
);
CREATE UNIQUE INDEX UX_etl_dim_screen_name ON etl.dim_screen (object_name, screen_name);

CREATE TABLE etl.bridge_screen_field (
    screen_key   INT NOT NULL,
    field_key    INT NOT NULL,
    CONSTRAINT PK_etl_bridge_screen_field PRIMARY KEY (screen_key, field_key),
    CONSTRAINT FK_etl_bridge_screen_field_screen FOREIGN KEY (screen_key) REFERENCES etl.dim_screen (screen_key),
    CONSTRAINT FK_etl_bridge_screen_field_field  FOREIGN KEY (field_key)  REFERENCES etl.dim_field (field_key)
);
GO

-- Unknown members
INSERT INTO etl.dim_table  (table_key, object_name, layer, table_name) VALUES (-1, 'unknown', 'unknown', N'unknown');
INSERT INTO etl.dim_field  (field_key, table_key, field_name)          VALUES (-1, -1, N'(whole record)');
INSERT INTO etl.dim_screen (screen_key, screen_name, object_name, screen_type, etl_module,
                            screen_processing_definition, severity, exception_action)
VALUES (-1, N'Unknown screen', 'unknown', 'unclassified', NULL, N'Screen not registered in the catalog', 'warning', 'pass');
GO

---------------------------------------------------------
-- 2. Facts
---------------------------------------------------------
CREATE TABLE etl.fact_error_event (
    error_event_key      BIGINT IDENTITY(1,1) NOT NULL,   -- surrogate key, sequential
    error_event_date_key INT          NOT NULL,           -- yyyymmdd (gold.dwh_dim_calendar.date_id)
    screen_key           INT          NOT NULL,
    batch_key            BIGINT       NULL,               -- etl.batch.batch_id
    error_date_time      DATETIME2(3) NOT NULL
        CONSTRAINT DF_etl_fact_error_event_time DEFAULT SYSDATETIME(),
    severity_score       TINYINT      NOT NULL,           -- 3 critical, 2 error, 1 warning
    CONSTRAINT PK_etl_fact_error_event PRIMARY KEY (error_event_key),
    CONSTRAINT FK_etl_fact_error_event_screen FOREIGN KEY (screen_key) REFERENCES etl.dim_screen (screen_key),
    CONSTRAINT FK_etl_fact_error_event_batch  FOREIGN KEY (batch_key)  REFERENCES etl.batch (batch_id)
);
CREATE INDEX IX_etl_fact_error_event_batch ON etl.fact_error_event (batch_key, screen_key);

CREATE TABLE etl.fact_error_event_detail (
    error_event_detail_key BIGINT IDENTITY(1,1) NOT NULL,
    error_event_key        BIGINT        NOT NULL,         -- ties the detail to its event
    error_event_date_key   INT           NOT NULL,
    screen_key             INT           NOT NULL,
    batch_key              BIGINT        NULL,
    error_date_time        DATETIME2(3)  NOT NULL,
    table_key              INT           NOT NULL,
    field_key              INT           NOT NULL,         -- -1 = the whole record
    record_identifier_key  NVARCHAR(200) NULL,             -- business key of the record (text: tables differ)
    error_condition        NVARCHAR(500) NULL,             -- what was wrong, readable
    CONSTRAINT PK_etl_fact_error_event_detail PRIMARY KEY (error_event_detail_key),
    CONSTRAINT FK_etl_fact_error_event_detail_event  FOREIGN KEY (error_event_key) REFERENCES etl.fact_error_event (error_event_key),
    CONSTRAINT FK_etl_fact_error_event_detail_screen FOREIGN KEY (screen_key)      REFERENCES etl.dim_screen (screen_key),
    CONSTRAINT FK_etl_fact_error_event_detail_table  FOREIGN KEY (table_key)       REFERENCES etl.dim_table (table_key),
    CONSTRAINT FK_etl_fact_error_event_detail_field  FOREIGN KEY (field_key)       REFERENCES etl.dim_field (field_key)
);
CREATE INDEX IX_etl_fact_error_event_detail_event ON etl.fact_error_event_detail (error_event_key);
GO

---------------------------------------------------------
-- 3. Compatibility view: the old etl.data_quality_log
--    (one row per event, as the old table)
---------------------------------------------------------
CREATE VIEW etl.data_quality_log
AS
-- NULLIF(…) keeps every column NULL-able: an INSERT through the
-- INSTEAD OF trigger may omit them (dq_log_key, detected_at)
SELECT NULLIF(e.error_event_key, -1)       AS dq_log_key,
       e.batch_key                         AS batch_id,
       t.layer                             AS layer,
       NULLIF(s.object_name, '')           AS object_name,
       NULLIF(s.screen_name, N'')          AS check_name,
       d.record_identifier_key             AS record_id,
       d.error_condition                   AS [description],
       CASE e.severity_score WHEN 3 THEN 'critical' WHEN 2 THEN 'error' WHEN 1 THEN 'warning' END AS severity,
       NULLIF(e.error_date_time, '19000101') AS detected_at
FROM etl.fact_error_event AS e
JOIN etl.dim_screen       AS s ON s.screen_key = e.screen_key
OUTER APPLY (SELECT TOP (1) dd.record_identifier_key, dd.error_condition, dd.table_key
             FROM etl.fact_error_event_detail AS dd
             WHERE dd.error_event_key = e.error_event_key
             ORDER BY dd.error_event_detail_key) AS d
LEFT JOIN etl.dim_table   AS t ON t.table_key = d.table_key;
GO

---------------------------------------------------------
-- 4. INSTEAD OF INSERT: one INSERT into the view =
--    one error event + its detail rows
---------------------------------------------------------
CREATE TRIGGER etl.trg_data_quality_log_insert
ON etl.data_quality_log
INSTEAD OF INSERT
AS
BEGIN
    SET NOCOUNT ON;

    -- The inserted rows, numbered
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn,
           i.batch_id, ISNULL(i.layer, 'bronze') AS layer, ISNULL(i.object_name, 'unknown') AS object_name,
           ISNULL(i.check_name, N'Unknown screen') AS check_name, i.record_id, i.[description],
           CASE WHEN LOWER(i.severity) IN ('critical', 'error', 'warning') THEN LOWER(i.severity) ELSE 'warning' END AS severity,
           ISNULL(i.detected_at, SYSDATETIME()) AS detected_at
    INTO #ins
    FROM inserted AS i;

    -- Tables not in the catalog yet
    INSERT INTO etl.dim_table (table_key, object_name, layer, table_name)
    SELECT ISNULL((SELECT MAX(table_key) FROM etl.dim_table), 0) + ROW_NUMBER() OVER (ORDER BY n.object_name, n.layer),
           n.object_name, n.layer, CONCAT(n.layer, '.erp_', n.object_name)
    FROM (SELECT DISTINCT object_name, layer FROM #ins) AS n
    WHERE NOT EXISTS (SELECT 1 FROM etl.dim_table AS t WHERE t.object_name = n.object_name AND t.layer = n.layer);

    -- Screens not in the catalog yet: registered as 'unclassified'
    INSERT INTO etl.dim_screen (screen_key, screen_name, object_name, screen_type, etl_module,
                                screen_processing_definition, severity, exception_action)
    SELECT ISNULL((SELECT MAX(screen_key) FROM etl.dim_screen), 0) + ROW_NUMBER() OVER (ORDER BY n.object_name, n.check_name),
           n.check_name, n.object_name, 'unclassified', NULL,
           N'Registered automatically: complete it in ETL_Error_Event_Screens.sql',
           n.severity,
           CASE WHEN n.severity = 'critical' THEN 'set aside' ELSE 'pass' END
    FROM (SELECT object_name, check_name, MAX(severity) AS severity
          FROM #ins GROUP BY object_name, check_name) AS n
    WHERE NOT EXISTS (SELECT 1 FROM etl.dim_screen AS s
                      WHERE s.object_name = n.object_name AND s.screen_name = n.check_name);

    -- One error event per inserted row
    DECLARE @map TABLE (rn BIGINT, error_event_key BIGINT);

    MERGE etl.fact_error_event AS tgt
    USING (SELECT i.rn, s.screen_key, i.batch_id, i.detected_at,
                  CASE i.severity WHEN 'critical' THEN 3 WHEN 'error' THEN 2 ELSE 1 END AS severity_score
           FROM #ins AS i
           JOIN etl.dim_screen AS s ON s.object_name = i.object_name AND s.screen_name = i.check_name) AS src
    ON 1 = 0
    WHEN NOT MATCHED THEN
        INSERT (error_event_date_key, screen_key, batch_key, error_date_time, severity_score)
        VALUES (CONVERT(INT, FORMAT(src.detected_at, 'yyyyMMdd')), src.screen_key, src.batch_id,
                src.detected_at, src.severity_score)
    OUTPUT src.rn, inserted.error_event_key INTO @map (rn, error_event_key);

    -- The detail: one row per field tested by the screen (none = the whole record)
    INSERT INTO etl.fact_error_event_detail
        (error_event_key, error_event_date_key, screen_key, batch_key, error_date_time,
         table_key, field_key, record_identifier_key, error_condition)
    SELECT m.error_event_key, e.error_event_date_key, e.screen_key, e.batch_key, e.error_date_time,
           t.table_key, ISNULL(bf.field_key, -1), i.record_id, i.[description]
    FROM @map AS m
    JOIN #ins AS i                   ON i.rn = m.rn
    JOIN etl.fact_error_event AS e   ON e.error_event_key = m.error_event_key
    JOIN etl.dim_table AS t          ON t.object_name = i.object_name AND t.layer = i.layer
    LEFT JOIN etl.bridge_screen_field AS bf ON bf.screen_key = e.screen_key;
END;
GO

---------------------------------------------------------
-- 5. Monitoring: data quality over time (Kimball subsystem 4,
--    goal: measure data quality metrics over time)
---------------------------------------------------------
CREATE VIEW etl.vw_error_event_daily
AS
SELECT e.error_event_date_key,
       CAST(e.error_date_time AS DATE)  AS error_date,
       s.object_name,
       s.screen_type,
       s.screen_name,
       s.severity,
       s.exception_action,
       COUNT(*)                         AS error_events
FROM etl.fact_error_event AS e
JOIN etl.dim_screen       AS s ON s.screen_key = e.screen_key
GROUP BY e.error_event_date_key, CAST(e.error_date_time AS DATE),
         s.object_name, s.screen_type, s.screen_name, s.severity, s.exception_action;
GO
