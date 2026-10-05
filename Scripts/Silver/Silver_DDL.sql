/*
=========================================================
Silver Layer Tables – Category, Subcategory, Product
=========================================================
Purpose:
    These tables represent the refined and standardized
    entities of the Silver layer in the Medallion Architecture.
    Data loaded here has passed Bronze-level quality checks
    and is ready for business rule transformations.

Notes:
    - All column names follow snake_case naming convention.
    - Surrogate keys (SK) are managed by SSIS during the load.
    - dwh_create_date / dwh_start_date / dwh_end_date track
      lineage and SCD Type 2 history.
*/

---------------------------------------------------------
-- Silver Category
---------------------------------------------------------

IF OBJECT_ID('silver.erp_category', 'U') IS NOT NULL
    DROP TABLE silver.erp_category;

CREATE TABLE silver.erp_category (
    category_sk INT NOT NULL,                 -- surrogate key
    category_id INT NULL,                     -- business key from source
    category_name NVARCHAR(50) NULL,          -- category description
    dwh_create_date DATETIME DEFAULT GETDATE() -- ETL load timestamp
);

---------------------------------------------------------
-- Silver Subcategory
---------------------------------------------------------

IF OBJECT_ID('silver.erp_subcategory', 'U') IS NOT NULL
    DROP TABLE silver.erp_subcategory;

CREATE TABLE silver.erp_subcategory (
    subcategory_sk INT NOT NULL,              -- surrogate key
    subcategory_id INT NULL,                  -- business key from source
    subcategory_name NVARCHAR(50) NULL,       -- subcategory description
    category_sk INT NULL,                     -- FK to silver.erp_category
    dwh_create_date DATETIME DEFAULT GETDATE() -- ETL load timestamp
);

---------------------------------------------------------
-- Silver Product
---------------------------------------------------------

IF OBJECT_ID('silver.erp_product', 'U') IS NOT NULL
    DROP TABLE silver.erp_product;

CREATE TABLE silver.erp_product (
    product_sk INT NOT NULL,                  -- surrogate key
    product_id INT NULL,                      -- business key from source
    product_name NVARCHAR(50) NULL,           -- product description
    product_number NVARCHAR(25) NULL,         -- product code
    make_flag BIT NULL,                       -- manufactured internally
    finished_goods_flag BIT NULL,             -- ready for sale
    color NVARCHAR(15) NULL,                  -- product color
    safety_stock_level SMALLINT NULL,         -- minimum stock
    reorder_point SMALLINT NULL,              -- reorder threshold
    standard_cost DECIMAL(19,4) NULL,         -- production cost
    list_price DECIMAL(19,4) NULL,            -- sale price
    size NCHAR(3) NULL,                       -- size code
    size_unit_measure_code NCHAR(3) NULL,     -- unit of measure
    weight_unit_measure_code NCHAR(3) NULL,   -- unit of measure
    weight DECIMAL(10,2) NULL,                -- product weight
    days_to_manufacture INT NULL,             -- production time
    product_line NCHAR(3) NULL,               -- product line
    class NCHAR(3) NULL,                      -- product class
    style NCHAR(3) NULL,                      -- product style
    sell_start_date DATE NULL,                -- start of sale period
    sell_end_date DATE NULL,                  -- end of sale period
    row_hash VARBINARY(8000) NULL,            -- hash for SCD Type 2
    dwh_start_date DATETIME NOT NULL,         -- SCD2 validity start
    dwh_end_date DATETIME NOT NULL,           -- SCD2 validity end
    is_active BIT NOT NULL,                   -- SCD2 active flag
    product_subcategory_sk INT NULL           -- FK to silver.erp_subcategory
);
