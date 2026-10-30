/*
=========================================================
Error Event Schema – migration of the old quality log
=========================================================
Purpose:
    Moves the rows of the old table etl.data_quality_log
    (renamed etl.data_quality_log_legacy by
    ETL_Error_Event_Schema.sql) into the error event star,
    through the compatibility view: every old row becomes one
    error event with its detail rows, with its original date
    and time.

SSIS Usage:
    Not executed by SSIS. Run once, on an existing database,
    after ETL_Error_Event_Schema.sql and
    ETL_Error_Event_Screens.sql. A new installation does not
    need it.

Warning:
    The legacy table is NOT dropped: check the counts at the
    end, then drop it by hand.
*/

USE [DWH_AdventureWorks2025];
GO

IF OBJECT_ID('etl.data_quality_log_legacy', 'U') IS NULL
BEGIN
    PRINT 'Nothing to migrate: etl.data_quality_log_legacy does not exist.';
    RETURN;
END;

IF EXISTS (SELECT 1 FROM etl.fact_error_event)
BEGIN
    PRINT 'The error event fact already has rows: migration skipped (already done?).';
    RETURN;
END;

-- Old batches that no longer exist in etl.batch are kept without batch
INSERT INTO etl.data_quality_log
    (batch_id, layer, object_name, check_name, record_id, [description], severity, detected_at)
SELECT CASE WHEN EXISTS (SELECT 1 FROM etl.batch AS b WHERE b.batch_id = l.batch_id) THEN l.batch_id END,
       l.layer, l.object_name, l.check_name, l.record_id, l.[description],
       CASE WHEN LOWER(l.severity) IN ('critical', 'error', 'warning') THEN LOWER(l.severity) ELSE 'warning' END,
       l.detected_at
FROM etl.data_quality_log_legacy AS l
ORDER BY l.dq_log_key;
GO

-- Check: the same number of rows before and after
SELECT (SELECT COUNT(*) FROM etl.data_quality_log_legacy) AS legacy_rows,
       (SELECT COUNT(*) FROM etl.fact_error_event)        AS error_events;

-- When the two numbers match:
-- DROP TABLE etl.data_quality_log_legacy;
GO
