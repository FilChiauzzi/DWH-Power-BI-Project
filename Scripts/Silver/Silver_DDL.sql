/*
=========================================================
Silver Layer Tables – v2 (full script)
=========================================================
Purpose:
    These tables represent the refined and standardized
    entities of the Silver layer in the Medallion Architecture.
    Data loaded here has passed Bronze-level quality checks
    and is ready for business rule transformations.

Design rules of the Silver layer:
    - Column names follow snake_case.
    - Surrogate keys (SK) are assigned by the load procedures
      through the dwh sequences, NOT by SSIS: SSIS only
      orchestrates, the logic lives in the stored procedures.
    - Business keys and surrogate keys are NOT NULL with a
      PRIMARY KEY. This is the opposite of Bronze, on purpose:
      Bronze is raw, Silver is clean, so here a constraint
      documents what "a valid row" means and catches a bug in
      our own load code. There are no FOREIGN KEYs: referential
      integrity is enforced by the load procedures (with the
      -1 Unknown member), not by the engine.
    - One standard set of technical columns on EVERY table, so
      any table can be audited with the same query.

Standard technical columns (uniform across the layer):

    SCD Type 1 tables (a change overwrites the row):
        row_hash         VARBINARY(32)  fingerprint of the traced
                                        attributes (SHA2_256)
        dwh_create_date  DATETIME2(3)   when the row was created
                                        in Silver (never updated)
        dwh_load_date    DATETIME2(3)   last time a load touched it
        dwh_batch_id     BIGINT         the run that touched it last

    SCD Type 2 tables (a change closes the old row, adds a new):
        all of the above, plus
        dwh_start_date   DATETIME2(3)   start of validity
        dwh_end_date     DATETIME2(3)   end of validity
                                        (9999-12-31 = current)
        is_active        BIT            exactly ONE row per
                                        business key must be active

    Type conventions:
        - Technical columns we generate: DATETIME2(3), created
          with SYSDATETIME(). The SOURCE dates keep their own
          type (a watermark column is never narrowed: casting
          modified_date to DATE would lose every change made
          during the same day).
        - row_hash is VARBINARY(32): SHA2_256 always returns 32
          bytes; VARBINARY(8000) wastes space and misleads.
        - Surrogate key naming: <entity>_sk, never a bare
          person_sk on a table called erp_sales_person.

Indexes:
    Dimensions stay ROWSTORE here (PK + business key), not
    columnstore: dimensions are small and are updated often,
    the opposite of the Bronze append-only workload.

Unknown member (-1):
    The dimension needs a row with SK = -1, used when a parent
    is missing. The seed is at the end of this script: run it
    after the tables are created.

Warning:
    Running this script will DROP existing tables and recreate
    them. All data in these tables will be permanently lost:
    the Silver layer is rebuilt from Bronze by the load
    procedures.
*/

USE [DWH_AdventureWorks2025];
GO

---------------------------------------------------------
-- Silver Category  (SCD Type 1)
---------------------------------------------------------

IF OBJECT_ID('silver.erp_category', 'U') IS NOT NULL
    DROP TABLE silver.erp_category;

CREATE TABLE silver.erp_category (
    category_sk       INT            NOT NULL,
    category_id       INT            NOT NULL,   -- business key
    category_name     NVARCHAR(50)   NULL,

    -- technical columns
    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_category_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_category_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_category
    ADD CONSTRAINT PK_silver_erp_category PRIMARY KEY (category_sk);

CREATE UNIQUE INDEX UX_silver_erp_category_id
    ON silver.erp_category (category_id);
GO

---------------------------------------------------------
-- Silver Subcategory  (SCD Type 1)
---------------------------------------------------------
-- category_sk is the resolved FK to silver.erp_category; it is
-- loaded after the category dimension (see load_order in
-- etl.config). -1 is the conventional "Unknown member" used
-- when the parent is missing: it must exist as a real row in
-- silver.erp_category (see the seed at the end of this script).

IF OBJECT_ID('silver.erp_subcategory', 'U') IS NOT NULL
    DROP TABLE silver.erp_subcategory;

CREATE TABLE silver.erp_subcategory (
    subcategory_sk    INT            NOT NULL,
    subcategory_id    INT            NOT NULL,   -- business key
    subcategory_name  NVARCHAR(50)   NULL,
    category_sk       INT            NOT NULL
        CONSTRAINT DF_silver_erp_subcategory_category_sk DEFAULT -1,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_subcategory_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_subcategory_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_subcategory
    ADD CONSTRAINT PK_silver_erp_subcategory PRIMARY KEY (subcategory_sk);

CREATE UNIQUE INDEX UX_silver_erp_subcategory_id
    ON silver.erp_subcategory (subcategory_id);

CREATE INDEX IX_silver_erp_subcategory_category_sk
    ON silver.erp_subcategory (category_sk);
GO

---------------------------------------------------------
-- Silver Product  (SCD Type 2)
---------------------------------------------------------
IF OBJECT_ID('silver.erp_product', 'U') IS NOT NULL
    DROP TABLE silver.erp_product;

CREATE TABLE silver.erp_product (
    product_sk                 INT            NOT NULL,
    product_id                 INT            NOT NULL,   -- business key
    product_name               NVARCHAR(50)   NULL,
    product_number             NVARCHAR(25)   NULL,
    make_flag                  BIT            NULL,
    make_flag_desc             NVARCHAR(20)   NULL,       -- NEW

    finished_goods_flag        BIT            NULL,
    finished_goods_flag_desc   NVARCHAR(20)   NULL,       -- NEW

    color                      NVARCHAR(15)   NULL,
    safety_stock_level         SMALLINT       NULL,
    reorder_point              SMALLINT       NULL,
    standard_cost              DECIMAL(19,4)  NULL,
    list_price                 DECIMAL(19,4)  NULL,

    [size]                     NVARCHAR(5)    NULL,
    size_unit_measure_code     NCHAR(3)       NULL,
    size_unit_desc             NVARCHAR(20)   NULL,       -- NEW

    weight_unit_measure_code   NCHAR(3)       NULL,
    weight_unit_desc           NVARCHAR(20)   NULL,       -- NEW

    [weight]                   DECIMAL(10,2)  NULL,
    days_to_manufacture        INT            NULL,

    product_line               NCHAR(2)       NULL,
    product_line_desc          NVARCHAR(20)   NULL,       -- NEW

    [class]                    NCHAR(2)       NULL,
    class_desc                 NVARCHAR(20)   NULL,       -- NEW

    style                      NCHAR(2)       NULL,
    style_desc                 NVARCHAR(20)   NULL,       -- NEW

    sell_start_date            DATE           NULL,
    sell_end_date              DATE           NULL,

    product_subcategory_sk     INT            NULL,

    -- technical columns
    row_hash                   VARBINARY(32)  NULL,
    dwh_create_date            DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_create DEFAULT SYSDATETIME(),
    dwh_start_date             DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_start  DEFAULT SYSDATETIME(),
    dwh_end_date               DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_end    DEFAULT DATEFROMPARTS(9999,12,31),
    is_active                  BIT            NOT NULL
        CONSTRAINT DF_silver_erp_product_active DEFAULT 1,
    dwh_load_date              DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_product_load   DEFAULT SYSDATETIME(),
    dwh_batch_id               BIGINT         NULL
);

ALTER TABLE silver.erp_product
    ADD CONSTRAINT PK_silver_erp_product PRIMARY KEY (product_sk);

-- SCD2: the business key is unique only among the CURRENT rows.
-- A filtered unique index allows any number of historical
-- versions and enforces EXACTLY ONE active row per product.
CREATE UNIQUE INDEX UX_silver_erp_product_active_id
    ON silver.erp_product (product_id)
    WHERE is_active = 1;

CREATE INDEX IX_silver_erp_product_subcategory_sk
    ON silver.erp_product (product_subcategory_sk);
GO
----------------------------------------------------------
/*
	Silver Customer  (SCD Type 2)
*/
---------------------------------------------------------
IF OBJECT_ID('silver.erp_customer', 'U') IS NOT NULL
    DROP TABLE silver.erp_customer;

CREATE TABLE silver.erp_customer (
    customer_sk       INT            NOT NULL,
    customer_id       INT            NOT NULL,   -- business key
    person_id         INT            NULL,
    store_id          INT            NULL,
    territory_id      INT            NULL,
    customer_type     NVARCHAR(50)   NULL,
    person_sk         INT            NULL,
    store_sk          INT            NULL,
    territory_sk      INT            NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_create DEFAULT SYSDATETIME(),
    dwh_start_date    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_start  DEFAULT SYSDATETIME(),
    dwh_end_date      DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_end    DEFAULT DATEFROMPARTS(9999,12,31),
    is_active         BIT            NOT NULL
        CONSTRAINT DF_silver_erp_customer_active DEFAULT 1,
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_customer_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_customer
    ADD CONSTRAINT PK_silver_erp_customer PRIMARY KEY (customer_sk);

CREATE UNIQUE INDEX UX_silver_erp_customer_active_id
    ON silver.erp_customer (customer_id)
    WHERE is_active = 1;

CREATE INDEX IX_silver_erp_customer_person_sk
    ON silver.erp_customer (person_sk);
GO

----------------------------------------------------------
/*
	Silver Store  (SCD Type 1)
*/
---------------------------------------------------------
IF OBJECT_ID('silver.erp_store', 'U') IS NOT NULL
    DROP TABLE silver.erp_store;

CREATE TABLE silver.erp_store (
    store_sk          INT            NOT NULL,
    store_id          INT            NOT NULL,   -- business key
    store_name        NVARCHAR(50)   NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_store_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_store_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_store
    ADD CONSTRAINT PK_silver_erp_store PRIMARY KEY (store_sk);

CREATE UNIQUE INDEX UX_silver_erp_store_id
    ON silver.erp_store (store_id);
GO

----------------------------------------------------------
/*
	Silver Sales Territory  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_sales_territory', 'U') IS NOT NULL
    DROP TABLE silver.erp_sales_territory;

CREATE TABLE silver.erp_sales_territory (
    territory_sk      INT            NOT NULL,
    territory_id      INT            NOT NULL,   -- business key
    territory_name    NVARCHAR(20)   NULL,
    country_code      NVARCHAR(3)    NULL,
    continent_name    NVARCHAR(50)   NULL,
    country_sk        INT            NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_territory_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_territory_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_sales_territory
    ADD CONSTRAINT PK_silver_erp_sales_territory PRIMARY KEY (territory_sk);

CREATE UNIQUE INDEX UX_silver_erp_sales_territory_id
    ON silver.erp_sales_territory (territory_id);
GO

----------------------------------------------------------
/*
	Silver Person  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_person', 'U') IS NOT NULL
    DROP TABLE silver.erp_person;

CREATE TABLE silver.erp_person (
    person_sk         INT            NOT NULL,
    person_id         INT            NOT NULL,   -- business key
    person_name       NVARCHAR(80)   NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_person_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_person_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_person
    ADD CONSTRAINT PK_silver_erp_person PRIMARY KEY (person_sk);

CREATE UNIQUE INDEX UX_silver_erp_person_id
    ON silver.erp_person (person_id);
GO

----------------------------------------------------------
/*
	Silver Country Region  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_country_region', 'U') IS NOT NULL
    DROP TABLE silver.erp_country_region;

CREATE TABLE silver.erp_country_region (
    country_sk        INT            NOT NULL,
    country_code      NVARCHAR(3)    NOT NULL,   -- business key
    country_name      NVARCHAR(50)   NULL,

    row_hash          VARBINARY(32)  NULL,
    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_country_region_create DEFAULT SYSDATETIME(),
    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_country_region_load   DEFAULT SYSDATETIME(),
    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_country_region
    ADD CONSTRAINT PK_silver_erp_country_region PRIMARY KEY (country_sk);

CREATE UNIQUE INDEX UX_silver_erp_country_region_code
    ON silver.erp_country_region (country_code);
GO

----------------------------------------------------------
/*
	Silver Sales Person  (SCD Type 2)
*/
----------------------------------------------------------
-- Naming: the surrogate key is sales_person_sk, not person_sk
-- (that would be ambiguous with silver.erp_person.person_sk).
-- person_id stays as the reference to the person.

IF OBJECT_ID('silver.erp_sales_person', 'U') IS NOT NULL
    DROP TABLE silver.erp_sales_person;

CREATE TABLE silver.erp_sales_person (
    sales_person_sk   INT            NOT NULL,      -- surrogate key SCD2
    person_id         INT            NOT NULL,      -- business key (BusinessEntityID)
    territory_id      INT            NULL,
    sales_quota       MONEY          NULL,
    bonus             MONEY          NULL,
    commission_pct    SMALLMONEY     NULL,
    sales_ytd         MONEY          NULL,
    sales_last_year   MONEY          NULL,

    territory_sk      INT            NULL,          -- FK to silver.erp_sales_territory
    employee_sk       INT            NULL,          -- FK to silver.erp_employee

    row_hash          VARBINARY(32)  NULL,

    dwh_create_date   DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_create DEFAULT SYSDATETIME(),

    dwh_start_date    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_start  DEFAULT SYSDATETIME(),

    dwh_end_date      DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_end    DEFAULT DATEFROMPARTS(9999,12,31),

    is_active         BIT            NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_active DEFAULT 1,

    dwh_load_date     DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_person_load   DEFAULT SYSDATETIME(),

    dwh_batch_id      BIGINT         NULL
);

ALTER TABLE silver.erp_sales_person
    ADD CONSTRAINT PK_silver_erp_sales_person PRIMARY KEY (sales_person_sk);

CREATE UNIQUE INDEX UX_silver_erp_sales_person_active_id
    ON silver.erp_sales_person (person_id)
    WHERE is_active = 1;

CREATE INDEX IX_silver_erp_sales_person_employee_sk
    ON silver.erp_sales_person (employee_sk);

CREATE INDEX IX_silver_erp_sales_person_sales_territory_sk
    ON silver.erp_sales_person (territory_sk);
GO

----------------------------------------------------------
/*
    Silver Employee  (SCD Type 1)
*/
----------------------------------------------------------
IF OBJECT_ID('silver.erp_employee', 'U') IS NOT NULL
    DROP TABLE silver.erp_employee;

CREATE TABLE silver.erp_employee (
    employee_sk        INT            NOT NULL,      -- surrogate key
    business_entity_id INT            NOT NULL,      -- business key (PK in HR.Employee)

    job_title          NVARCHAR(50)   NULL,
    birth_date         DATE           NULL,
    marital_status     NCHAR(1)       NULL,
    gender             NCHAR(1)       NULL,
    hire_date          DATE           NULL,
    salaried_flag      BIT            NULL,
    vacation_hours     SMALLINT       NULL,
    sick_leave_hours   SMALLINT       NULL,
    current_flag       BIT            NULL,

   person_sk INT NULL,
    row_hash           VARBINARY(32)  NULL,
    dwh_create_date    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_employee_create DEFAULT SYSDATETIME(),
    dwh_load_date      DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_employee_load   DEFAULT SYSDATETIME(),
    dwh_batch_id       BIGINT         NULL
);

ALTER TABLE silver.erp_employee
    ADD CONSTRAINT PK_silver_erp_employee PRIMARY KEY (employee_sk);

CREATE UNIQUE INDEX UX_silver_erp_employee_business_entity_id
    ON silver.erp_employee (business_entity_id);

CREATE INDEX IX_silver_erp_employee_person_sk
    ON silver.erp_employee (person_sk);

GO
	
---------------------------------------------------------
-- Silver Person Address  (SCD Type 1)
---------------------------------------------------------

IF OBJECT_ID('silver.erp_person_address', 'U') IS NOT NULL
    DROP TABLE silver.erp_person_address;

CREATE TABLE silver.erp_person_address (
    address_sk          INT             NOT NULL,       -- surrogate key
    address_id          INT             NOT NULL,       -- business key
    address_line1       NVARCHAR(60)    NULL,
    address_line2       NVARCHAR(60)    NULL,
    city                NVARCHAR(30)    NULL,
    state_province_id   INT             NULL,
    postal_code         NVARCHAR(15)    NULL,

    -- coordinate estratte dal tipo geography del Bronze
    latitude            FLOAT           NULL,
    longitude           FLOAT           NULL,

    -- technical columns
    row_hash            VARBINARY(32)   NULL,
    dwh_create_date     DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_person_address_create DEFAULT SYSDATETIME(),
    dwh_load_date       DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_person_address_load   DEFAULT SYSDATETIME(),
    dwh_batch_id        BIGINT          NULL
);

ALTER TABLE silver.erp_person_address
    ADD CONSTRAINT PK_silver_person_address PRIMARY KEY (address_sk);

CREATE INDEX IX_silver_erp_person_addres_sk
    ON silver.erp_person_address (address_sk);

CREATE UNIQUE INDEX UX_silver_person_address_id
    ON silver.erp_person_address (address_id);
GO
	
---------------------------------------------------------
-- Silver State Province  (SCD Type 1)
---------------------------------------------------------

IF OBJECT_ID('silver.erp_state_province', 'U') IS NOT NULL
    DROP TABLE silver.erp_state_province;

CREATE TABLE silver.erp_state_province (
    state_province_sk          INT             NOT NULL,       -- surrogate key
    state_province_id          INT             NOT NULL,       -- business key
    state_province_code        NCHAR(3) 	   NULL,
    country_region_code       NVARCHAR(3)      NULL,
    country_region_sk         INT              NULL,
    [name]                    NVARCHAR(50)     NULL,
    territory_id              INT              NULL,
    territory_sk              INT              NULL,
    -- technical columns
    row_hash            VARBINARY(32)   NULL,
    dwh_create_date     DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_state_province_create DEFAULT SYSDATETIME(),
    dwh_load_date       DATETIME2(3)    NOT NULL
        CONSTRAINT DF_silver_state_province_load   DEFAULT SYSDATETIME(),
    dwh_batch_id        BIGINT          NULL
);

ALTER TABLE silver.erp_state_province
    ADD CONSTRAINT PK_silver_state_province_sk PRIMARY KEY (state_province_sk);

CREATE INDEX IX_silver_erp_state_province_sk
    ON silver.erp_state_province (state_province_sk);

CREATE UNIQUE INDEX UX_silver_state_province_id
    ON silver.erp_state_province (state_province_id);
----------------------------------------------------------
/*
	Silver Sales Order Header
*/
----------------------------------------------------------
-- NOTE: modified_date stays DATETIME, NOT DATE. It is the
--       watermark column of the incremental load: casting it to
--       DATE would truncate the time and lose every change made
--       during the same day.
-- NOTE: the header carries no order-line metrics (quantity,
--       unit price, discount live in sales_order_detail, which
--       is the fact grain we will build in Gold).
-- NOTE: sales_order_id is NOT NULL because it is the PRIMARY
--       KEY: a PK cannot live on a nullable column (a NULL is
--       "unknown", and "unknown" is not an identity).

IF OBJECT_ID('silver.erp_sales_order_header', 'U') IS NOT NULL
    DROP TABLE silver.erp_sales_order_header;

CREATE TABLE silver.erp_sales_order_header (
    sales_order_id         INT            NOT NULL,   -- business key
    order_date             DATETIME       NULL,
    due_date               DATETIME       NULL,
    ship_date              DATETIME       NULL,
    status                 TINYINT        NULL,
    online_order_flag      BIT            NULL,
    sales_order_number     NVARCHAR(25)   NULL,
    purchase_order_number  NVARCHAR(25)   NULL,
    account_number         NVARCHAR(15)   NULL,
    customer_id            INT            NULL,
    sales_person_id        INT            NULL,
    territory_id           INT            NULL,
    currency_rate_id       INT            NULL,
    sub_total              MONEY          NULL,
    tax_amount             MONEY          NULL,
    freight                MONEY          NULL,
    total_due              MONEY          NULL,
    modified_date          DATETIME       NULL,   -- watermark: never DATE
    customer_sk            INT            NULL,
    person_sk              INT            NULL,
    territory_sk           INT            NULL,

    -- technical columns
    row_hash               VARBINARY(32)  NULL,
    dwh_create_date        DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_order_header_create DEFAULT SYSDATETIME(),
    dwh_load_date          DATETIME2(3)   NOT NULL
        CONSTRAINT DF_silver_erp_sales_order_header_load   DEFAULT SYSDATETIME(),
    dwh_batch_id           BIGINT         NULL
);

ALTER TABLE silver.erp_sales_order_header
    ADD CONSTRAINT PK_silver_erp_sales_order_header PRIMARY KEY (sales_order_id);

CREATE INDEX IX_silver_erp_sales_order_header_customer_sk
    ON silver.erp_sales_order_header (customer_sk);

CREATE INDEX IX_silver_erp_sales_order_header_modified_date
    ON silver.erp_sales_order_header (modified_date);
GO

/*
=========================================================
Sequences (created only if missing)
=========================================================
Surrogate keys come from sequence objects. NEXT VALUE FOR
with OVER (ORDER BY ...) is supported from SQL Server 2012.
CREATE SEQUENCE is wrapped in EXEC(): like CREATE SCHEMA, it
must not be parsed inside a conditional block.
*/
IF SCHEMA_ID('dwh') IS NULL
    EXEC('CREATE SCHEMA dwh;');
GO

IF OBJECT_ID('dwh.seq_silver_category', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_category START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_subcategory', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_subcategory START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_product', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_product START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_customer', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_customer START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_store', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_store START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_sales_territory', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_sales_territory START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_person', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_person START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_country_region', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_country_region START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_sales_person', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_sales_person START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_employee', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_employee START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.seq_silver_person_address', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_person_address START WITH 1 INCREMENT BY 1;');
IF OBJECT_ID('dwh.erp_state_province', 'SO') IS NULL
    EXEC('CREATE SEQUENCE dwh.seq_silver_state_province START WITH 1 INCREMENT BY 1;');
GO

/*
=========================================================
Unknown member seed  (SK = -1)
=========================================================
The load procedures send a row here when its parent is
missing. Without this row, the NOT NULL DEFAULT -1 on
category_sk would point to a member that does not exist.
Run once, after the tables above exist. Safe to re-run.
*/
IF NOT EXISTS (SELECT 1 FROM silver.erp_category WHERE category_sk = -1)
    INSERT INTO silver.erp_category (category_sk, category_id, category_name)
    VALUES (-1, -1, 'Unknown');

IF NOT EXISTS (SELECT 1 FROM silver.erp_subcategory WHERE subcategory_sk = -1)
    INSERT INTO silver.erp_subcategory (subcategory_sk, subcategory_id, subcategory_name, category_sk)
    VALUES (-1, -1, 'Unknown', -1);

IF NOT EXISTS (SELECT 1 FROM silver.erp_product WHERE product_sk = -1)
    INSERT INTO silver.erp_product (product_sk, product_id, product_name, is_active, product_subcategory_sk)
    VALUES (-1, -1, 'Unknown', 1, -1);

IF NOT EXISTS (SELECT 1 FROM silver.erp_customer WHERE customer_sk = -1)
    INSERT INTO silver.erp_customer (customer_sk, customer_id, customer_type, is_active)
    VALUES (-1, -1, 'Unknown', 1);

IF NOT EXISTS (SELECT 1 FROM silver.erp_person WHERE person_sk = -1)
    INSERT INTO silver.erp_person (person_sk, person_id, person_name)
    VALUES (-1, -1, 'Unknown');

IF NOT EXISTS (SELECT 1 FROM silver.erp_store WHERE store_sk = -1)
    INSERT INTO silver.erp_store (store_sk, store_id, store_name)
    VALUES (-1, -1, 'Unknown');

IF NOT EXISTS (SELECT 1 FROM silver.erp_sales_territory WHERE territory_sk = -1)
    INSERT INTO silver.erp_sales_territory (territory_sk, territory_id, territory_name)
    VALUES (-1, -1, 'Unknown');

IF NOT EXISTS (SELECT 1 FROM silver.erp_country_region WHERE country_sk = -1)
    INSERT INTO silver.erp_country_region (country_sk, country_code, country_name)
    VALUES (-1, '-1', 'Unknown');

IF NOT EXISTS (SELECT 1 FROM silver.erp_sales_person WHERE sales_person_sk = -1)
    INSERT INTO silver.erp_sales_person (sales_person_sk, person_id, is_active)
    VALUES (-1, -1, 1);

IF NOT EXISTS (SELECT 1 FROM silver.erp_employee WHERE employee_sk = -1)
    INSERT INTO silver.erp_employee (employee_sk, job_title, person_sk)
    VALUES (-1,'Unknown',-1);

IF NOT EXISTS (SELECT 1 FROM silver.person_address WHERE address_sk = -1)
    INSERT INTO silver.person_address ( address_sk,address_id,address_line1,address_line2,city,state_province_id,postal_code,latitude,longitude )
    VALUES (-1, -1, 'Unknown', NULL, 'Unknown', -1, 'Unknown', NULL, NULL );
GO

/*
=========================================================
Tables still to define in this script
=========================================================
erp_sales_order_detail           (fact grain in Gold)
erp_employee
erp_department
erp_employee_department_history
They are referenced by etl.config but have no Silver DDL yet.
*/


