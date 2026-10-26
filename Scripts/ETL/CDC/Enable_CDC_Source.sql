/*
=========================================================
CDC – Enable Change Data Capture on the source
=========================================================
Purpose:
    Enables SQL Server Change Data Capture on the source
    database AdventureWorks2025 and on the 11 source tables
    of the incremental objects (load_type = 'incremental'
    in etl.config). Re-runnable: tables already tracked are
    skipped.

SSIS Usage:
    Not executed by SSIS. Run once at deployment, before the
    first incremental Bronze load.

Warning:
    Needs sysadmin (database) and db_owner (tables). On a real
    source system this is done by the DBA. SQL Server Agent
    must be running: the capture job copies the changes from
    the transaction log; the cleanup job deletes changes older
    than 3 days (default retention).
*/

USE [AdventureWorks2025];
GO

-- 1. Database level
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

-- 2. Table level: the source tables of the incremental objects
DECLARE @tables TABLE (schema_name SYSNAME, table_name SYSNAME);
INSERT INTO @tables VALUES
    ('Production',     'Product'),
    ('Sales',          'Store'),
    ('Person',         'CountryRegion'),
    ('Person',         'Person'),
    ('Sales',          'Customer'),
    ('HumanResources', 'Employee'),
    ('Sales',          'SalesPerson'),
    ('Person',         'StateProvince'),
    ('Person',         'Address'),
    ('Sales',          'SalesOrderHeader'),
    ('Sales',          'SalesOrderDetail');

DECLARE @schema SYSNAME, @table SYSNAME;

DECLARE cur_cdc CURSOR LOCAL FAST_FORWARD FOR
    SELECT schema_name, table_name FROM @tables;

OPEN cur_cdc;
FETCH NEXT FROM cur_cdc INTO @schema, @table;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (SELECT 1
               FROM sys.tables  AS t
               JOIN sys.schemas AS s ON s.schema_id = t.schema_id
               WHERE s.name = @schema AND t.name = @table
                 AND t.is_tracked_by_cdc = 1)
        PRINT '-- already tracked: ' + @schema + '.' + @table;
    ELSE
    BEGIN
        PRINT '>> enabling CDC on: ' + @schema + '.' + @table;
        EXEC sys.sp_cdc_enable_table
             @source_schema        = @schema,
             @source_name          = @table,
             @role_name            = NULL,
             @supports_net_changes = 1;     -- needed by fn_cdc_get_net_changes_*
    END;

    FETCH NEXT FROM cur_cdc INTO @schema, @table;
END;

CLOSE cur_cdc;
DEALLOCATE cur_cdc;

SELECT capture_instance, supports_net_changes, create_date
FROM cdc.change_tables
ORDER BY capture_instance;     -- expected: 11 rows
GO
