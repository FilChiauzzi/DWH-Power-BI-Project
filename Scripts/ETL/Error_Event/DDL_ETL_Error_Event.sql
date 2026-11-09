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

    Metadata-driven: nothing is written by hand. The
    dimensions and facts are filled by
    etl.usp_save_error_events (usp_save_error_events.sql).

SSIS Usage:
    Not executed by SSIS. Run once at deployment.

Warning:
    Drops and recreates the error event objects: the error
    history is lost. Run after DDL_ETL_Batch.sql.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- 0. Drop
---------------------------------------------------------
DROP VIEW      IF EXISTS etl.vw_error_event_daily;
DROP TABLE     IF EXISTS etl.fact_error_event_detail;
DROP TABLE     IF EXISTS etl.fact_error_event;
DROP TABLE     IF EXISTS etl.dim_field;
DROP TABLE     IF EXISTS etl.dim_screen;
DROP TABLE     IF EXISTS etl.dim_table;
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
-- 3. Data quality over time
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
