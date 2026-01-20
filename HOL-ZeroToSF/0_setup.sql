/***************************************************************************************************       
Asset:        Zero to Snowflake - Setup
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************/

/*----------------------------------------------------------------------------------
 U S E R   S U F F I X   V A R I A B L E S
 
 All objects in this lab are suffixed with the current user's name to allow
 multiple users to run the lab concurrently without naming conflicts.
----------------------------------------------------------------------------------*/

-- Set the user suffix (will be appended to all object names)
SET USER_SUFFIX = CURRENT_USER();
SELECT $USER_SUFFIX AS YOUR_USER_SUFFIX;

-- Database
SET DB_NAME = 'TB_101_' || $USER_SUFFIX;

-- Roles
SET ROLE_ADMIN = 'TB_ADMIN_' || $USER_SUFFIX;
SET ROLE_ENGINEER = 'TB_DATA_ENGINEER_' || $USER_SUFFIX;
SET ROLE_DEV = 'TB_DEV_' || $USER_SUFFIX;
SET ROLE_ANALYST = 'TB_ANALYST_' || $USER_SUFFIX;

-- Warehouses
SET WH_DE = 'TB_DE_WH_' || $USER_SUFFIX;
SET WH_DEV = 'TB_DEV_WH_' || $USER_SUFFIX;
SET WH_ANALYST = 'TB_ANALYST_WH_' || $USER_SUFFIX;
SET WH_CORTEX = 'TB_CORTEX_WH_' || $USER_SUFFIX;

-- Schemas (fully qualified)
SET SCH_PUBLIC = $DB_NAME || '.PUBLIC';
SET SCH_RAW_POS = $DB_NAME || '.RAW_POS';
SET SCH_RAW_CUSTOMER = $DB_NAME || '.RAW_CUSTOMER';
SET SCH_HARMONIZED = $DB_NAME || '.HARMONIZED';
SET SCH_ANALYTICS = $DB_NAME || '.ANALYTICS';
SET SCH_GOVERNANCE = $DB_NAME || '.GOVERNANCE';
SET SCH_RAW_SUPPORT = $DB_NAME || '.RAW_SUPPORT';
SET SCH_SEMANTIC_LAYER = $DB_NAME || '.SEMANTIC_LAYER';

-- Tables (fully qualified)
SET TBL_COUNTRY = $SCH_RAW_POS || '.COUNTRY';
SET TBL_FRANCHISE = $SCH_RAW_POS || '.FRANCHISE';
SET TBL_LOCATION = $SCH_RAW_POS || '.LOCATION';
SET TBL_MENU = $SCH_RAW_POS || '.MENU';
SET TBL_TRUCK = $SCH_RAW_POS || '.TRUCK';
SET TBL_ORDER_HEADER = $SCH_RAW_POS || '.ORDER_HEADER';
SET TBL_ORDER_DETAIL = $SCH_RAW_POS || '.ORDER_DETAIL';
SET TBL_TRUCK_DETAILS = $SCH_RAW_POS || '.TRUCK_DETAILS';
SET TBL_CUSTOMER_LOYALTY = $SCH_RAW_CUSTOMER || '.CUSTOMER_LOYALTY';
SET TBL_TRUCK_REVIEWS = $SCH_RAW_SUPPORT || '.TRUCK_REVIEWS';

-- Views (fully qualified)
SET VIEW_ORDERS_HARMONIZED = $SCH_HARMONIZED || '.ORDERS_V';
SET VIEW_LOYALTY_METRICS_HARMONIZED = $SCH_HARMONIZED || '.CUSTOMER_LOYALTY_METRICS_V';
SET VIEW_TRUCK_REVIEWS_HARMONIZED = $SCH_HARMONIZED || '.TRUCK_REVIEWS_V';
SET VIEW_ORDERS_ANALYTICS = $SCH_ANALYTICS || '.ORDERS_V';
SET VIEW_LOYALTY_METRICS_ANALYTICS = $SCH_ANALYTICS || '.CUSTOMER_LOYALTY_METRICS_V';
SET VIEW_TRUCK_REVIEWS_ANALYTICS = $SCH_ANALYTICS || '.TRUCK_REVIEWS_V';
SET VIEW_JAPAN_SALES = $SCH_ANALYTICS || '.JAPAN_MENU_ITEM_SALES_FEB_2022';
SET VIEW_ORDERS_SEMANTIC = $SCH_SEMANTIC_LAYER || '.ORDERS_V';
SET VIEW_LOYALTY_SEMANTIC = $SCH_SEMANTIC_LAYER || '.CUSTOMER_LOYALTY_METRICS_V';

-- Stages (fully qualified)
SET STAGE_S3LOAD = $SCH_PUBLIC || '.S3LOAD';
SET STAGE_TRUCK_REVIEWS = $SCH_PUBLIC || '.TRUCK_REVIEWS_S3LOAD';
SET STAGE_SEMANTIC_MODEL = $SCH_SEMANTIC_LAYER || '.SEMANTIC_MODEL_STAGE';

-- File Format
SET FF_CSV = $SCH_PUBLIC || '.CSV_FF';

-- Cortex Search Service
SET CORTEX_SEARCH = $SCH_HARMONIZED || '.TASTY_BYTES_REVIEW_SEARCH';

-- Display all variables for verification
SELECT 
    $USER_SUFFIX AS USER_SUFFIX,
    $DB_NAME AS DATABASE_NAME,
    $ROLE_ADMIN AS ROLE_ADMIN,
    $ROLE_ENGINEER AS ROLE_ENGINEER,
    $ROLE_DEV AS ROLE_DEV,
    $ROLE_ANALYST AS ROLE_ANALYST,
    $WH_DE AS WAREHOUSE_DE,
    $WH_DEV AS WAREHOUSE_DEV;


/*----------------------------------------------------------------------------------
 C R E A T E   R O L E S
----------------------------------------------------------------------------------*/
USE ROLE sysadmin;

-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"sql", "vignette": "setup"}}';

/*--
 • database, schema and warehouse creation
--*/

-- create database
CREATE OR REPLACE DATABASE identifier($DB_NAME);

-- create raw_pos schema
CREATE OR REPLACE SCHEMA identifier($SCH_RAW_POS);

-- create raw_customer schema
CREATE OR REPLACE SCHEMA identifier($SCH_RAW_CUSTOMER);

-- create harmonized schema
CREATE OR REPLACE SCHEMA identifier($SCH_HARMONIZED);

-- create analytics schema
CREATE OR REPLACE SCHEMA identifier($SCH_ANALYTICS);

-- create governance schema
CREATE OR REPLACE SCHEMA identifier($SCH_GOVERNANCE);

-- create raw_support
CREATE OR REPLACE SCHEMA identifier($SCH_RAW_SUPPORT);

-- Create schema for the Semantic Layer
CREATE OR REPLACE SCHEMA identifier($SCH_SEMANTIC_LAYER)
COMMENT = 'Schema for the business-friendly semantic layer, optimized for analytical consumption.';

-- create warehouses
CREATE OR REPLACE WAREHOUSE identifier($WH_DE)
    WAREHOUSE_SIZE = 'medium' -- Medium for initial data load - scaled down to XSmall at end of this script
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data engineering warehouse for tasty bytes';

CREATE OR REPLACE WAREHOUSE identifier($WH_DEV)
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'developer warehouse for tasty bytes';

-- create analyst warehouse
CREATE OR REPLACE WAREHOUSE identifier($WH_ANALYST)
    COMMENT = 'TastyBytes Analyst Warehouse'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'medium'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = true,
    AUTO_RESUME = true;

-- Create a dedicated medium warehouse for analytical workloads
CREATE OR REPLACE WAREHOUSE identifier($WH_CORTEX)
    WAREHOUSE_SIZE = 'medium'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Dedicated medium warehouse for Cortex Analyst and other analytical tools.';

-- create roles
USE ROLE securityadmin;

-- functional roles
CREATE ROLE IF NOT EXISTS identifier($ROLE_ADMIN)
    COMMENT = 'admin for tasty bytes';
    
CREATE ROLE IF NOT EXISTS identifier($ROLE_ENGINEER)
    COMMENT = 'data engineer for tasty bytes';
    
CREATE ROLE IF NOT EXISTS identifier($ROLE_DEV)
    COMMENT = 'developer for tasty bytes';
    
CREATE ROLE IF NOT EXISTS identifier($ROLE_ANALYST)
    COMMENT = 'analyst for tasty bytes';
    
-- role hierarchy
GRANT ROLE identifier($ROLE_ADMIN) TO ROLE sysadmin;
GRANT ROLE identifier($ROLE_ENGINEER) TO ROLE identifier($ROLE_ADMIN);
GRANT ROLE identifier($ROLE_DEV) TO ROLE identifier($ROLE_ENGINEER);
GRANT ROLE identifier($ROLE_ANALYST) TO ROLE identifier($ROLE_ENGINEER);

-- Grant roles to current user
GRANT ROLE identifier($ROLE_ADMIN) TO USER identifier($USER_SUFFIX);
GRANT ROLE identifier($ROLE_ENGINEER) TO USER identifier($USER_SUFFIX);
GRANT ROLE identifier($ROLE_DEV) TO USER identifier($USER_SUFFIX);
GRANT ROLE identifier($ROLE_ANALYST) TO USER identifier($USER_SUFFIX);

-- privilege grants
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE identifier($ROLE_ENGINEER);

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE identifier($ROLE_ADMIN);

USE ROLE securityadmin;

GRANT USAGE ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_ADMIN);
GRANT USAGE ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_ENGINEER);
GRANT USAGE ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_DEV);

GRANT USAGE ON ALL SCHEMAS IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_ADMIN);
GRANT USAGE ON ALL SCHEMAS IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_ENGINEER);
GRANT USAGE ON ALL SCHEMAS IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON SCHEMA identifier($SCH_RAW_SUPPORT) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON SCHEMA identifier($SCH_RAW_SUPPORT) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON SCHEMA identifier($SCH_RAW_SUPPORT) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON SCHEMA identifier($SCH_RAW_POS) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON SCHEMA identifier($SCH_RAW_POS) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON SCHEMA identifier($SCH_RAW_POS) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON SCHEMA identifier($SCH_ANALYTICS) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON SCHEMA identifier($SCH_ANALYTICS) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON SCHEMA identifier($SCH_ANALYTICS) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON SCHEMA identifier($SCH_GOVERNANCE) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON SCHEMA identifier($SCH_GOVERNANCE) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON SCHEMA identifier($SCH_GOVERNANCE) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON SCHEMA identifier($SCH_SEMANTIC_LAYER) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON SCHEMA identifier($SCH_SEMANTIC_LAYER) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON SCHEMA identifier($SCH_SEMANTIC_LAYER) TO ROLE identifier($ROLE_DEV);

-- warehouse grants
GRANT OWNERSHIP ON WAREHOUSE identifier($WH_DE) TO ROLE identifier($ROLE_ADMIN) COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE identifier($WH_DE) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON WAREHOUSE identifier($WH_DE) TO ROLE identifier($ROLE_ENGINEER);

GRANT ALL ON WAREHOUSE identifier($WH_DEV) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON WAREHOUSE identifier($WH_DEV) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON WAREHOUSE identifier($WH_DEV) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON WAREHOUSE identifier($WH_ANALYST) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON WAREHOUSE identifier($WH_ANALYST) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON WAREHOUSE identifier($WH_ANALYST) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON WAREHOUSE identifier($WH_CORTEX) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON WAREHOUSE identifier($WH_CORTEX) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON WAREHOUSE identifier($WH_CORTEX) TO ROLE identifier($ROLE_DEV);

-- future grants
GRANT ALL ON FUTURE TABLES IN SCHEMA identifier($SCH_RAW_POS) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON FUTURE TABLES IN SCHEMA identifier($SCH_RAW_POS) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON FUTURE TABLES IN SCHEMA identifier($SCH_RAW_POS) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON FUTURE TABLES IN SCHEMA identifier($SCH_RAW_CUSTOMER) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON FUTURE TABLES IN SCHEMA identifier($SCH_RAW_CUSTOMER) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON FUTURE TABLES IN SCHEMA identifier($SCH_RAW_CUSTOMER) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_ANALYTICS) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_ANALYTICS) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_ANALYTICS) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_GOVERNANCE) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_GOVERNANCE) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_GOVERNANCE) TO ROLE identifier($ROLE_DEV);

GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_SEMANTIC_LAYER) TO ROLE identifier($ROLE_ADMIN);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_SEMANTIC_LAYER) TO ROLE identifier($ROLE_ENGINEER);
GRANT ALL ON FUTURE VIEWS IN SCHEMA identifier($SCH_SEMANTIC_LAYER) TO ROLE identifier($ROLE_DEV);

-- Apply Masking Policy Grants
USE ROLE accountadmin;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE identifier($ROLE_ADMIN);
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE identifier($ROLE_ENGINEER);
  
-- Grants for tb_admin
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE identifier($ROLE_ADMIN);

-- Grants for tb_analyst
GRANT ALL ON SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_ANALYST);
GRANT ALL ON SCHEMA identifier($SCH_ANALYTICS) TO ROLE identifier($ROLE_ANALYST);
GRANT OPERATE, USAGE ON WAREHOUSE identifier($WH_ANALYST) TO ROLE identifier($ROLE_ANALYST);

-- Grants for cortex search service
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE identifier($ROLE_DEV);
GRANT USAGE ON SCHEMA identifier($SCH_HARMONIZED) TO ROLE identifier($ROLE_DEV);
GRANT USAGE ON WAREHOUSE identifier($WH_DE) TO ROLE identifier($ROLE_DEV);


-- raw_pos table build
USE ROLE sysadmin;
USE WAREHOUSE identifier($WH_DE);

/*--
 • file format and stage creation
--*/

CREATE OR REPLACE FILE FORMAT identifier($FF_CSV) 
type = 'csv';

CREATE OR REPLACE STAGE identifier($STAGE_S3LOAD)
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = (FORMAT_NAME = $FF_CSV);

CREATE OR REPLACE STAGE identifier($STAGE_TRUCK_REVIEWS)
COMMENT = 'Truck Reviews Stage'
url = 's3://sfquickstarts/tastybytes-voc/'
file_format = (FORMAT_NAME = $FF_CSV);

-- This stage will be used to upload your YAML files.
CREATE OR REPLACE STAGE identifier($STAGE_SEMANTIC_MODEL)
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Internal stage for uploading Cortex Analyst semantic model YAML files.';

/*--
 raw zone table build 
--*/

-- country table build
CREATE OR REPLACE TABLE identifier($TBL_COUNTRY)
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

-- franchise table build
CREATE OR REPLACE TABLE identifier($TBL_FRANCHISE) 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
);

-- location table build
CREATE OR REPLACE TABLE identifier($TBL_LOCATION)
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

-- menu table build
CREATE OR REPLACE TABLE identifier($TBL_MENU)
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- truck table build 
CREATE OR REPLACE TABLE identifier($TBL_TRUCK)
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- order_header table build
CREATE OR REPLACE TABLE identifier($TBL_ORDER_HEADER)
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);

-- order_detail table build
CREATE OR REPLACE TABLE identifier($TBL_ORDER_DETAIL) 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
);

-- customer loyalty table build
CREATE OR REPLACE TABLE identifier($TBL_CUSTOMER_LOYALTY)
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

/*--
 raw_suport zone table build 
--*/
CREATE OR REPLACE TABLE identifier($TBL_TRUCK_REVIEWS)
(
    order_id NUMBER(38,0),
    language VARCHAR(16777216),
    source VARCHAR(16777216),
    review VARCHAR(16777216),
    review_id NUMBER(38,0)  
);

/*--
 • harmonized view creation
--*/

-- orders_v view
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_ORDERS_HARMONIZED || ' AS
SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM ' || $TBL_ORDER_DETAIL || ' od
JOIN ' || $TBL_ORDER_HEADER || ' oh
    ON od.order_id = oh.order_id
JOIN ' || $TBL_TRUCK || ' t
    ON oh.truck_id = t.truck_id
JOIN ' || $TBL_MENU || ' m
    ON od.menu_item_id = m.menu_item_id
JOIN ' || $TBL_FRANCHISE || ' f
    ON t.franchise_id = f.franchise_id
JOIN ' || $TBL_LOCATION || ' l
    ON oh.location_id = l.location_id
LEFT JOIN ' || $TBL_CUSTOMER_LOYALTY || ' cl
    ON oh.customer_id = cl.customer_id';
EXECUTE IMMEDIATE $VIEW_SQL;

-- loyalty_metrics_v view
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_LOYALTY_METRICS_HARMONIZED || ' AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM ' || $TBL_CUSTOMER_LOYALTY || ' cl
JOIN ' || $TBL_ORDER_HEADER || ' oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail';
EXECUTE IMMEDIATE $VIEW_SQL;

-- truck_reviews_v view
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_TRUCK_REVIEWS_HARMONIZED || ' AS
SELECT DISTINCT
    r.review_id,
    r.order_id,
    oh.truck_id,
    r.language,
    source,
    r.review,
    t.primary_city,
    oh.customer_id,
    TO_DATE(oh.order_ts) AS date,
    m.truck_brand_name
FROM ' || $TBL_TRUCK_REVIEWS || ' r
JOIN ' || $TBL_ORDER_HEADER || ' oh
    ON oh.order_id = r.order_id
JOIN ' || $TBL_TRUCK || ' t
    ON t.truck_id = oh.truck_id
JOIN ' || $TBL_MENU || ' m
    ON m.menu_type_id = t.menu_type_id';
EXECUTE IMMEDIATE $VIEW_SQL;

/*--
 • analytics view creation
--*/

-- orders_v view
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_ORDERS_ANALYTICS || ' 
COMMENT = ''Tasty Bytes Order Detail View''
AS
SELECT DATE(o.order_ts) AS date, * FROM ' || $VIEW_ORDERS_HARMONIZED || ' o';
EXECUTE IMMEDIATE $VIEW_SQL;

-- customer_loyalty_metrics_v view
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_LOYALTY_METRICS_ANALYTICS || '
COMMENT = ''Tasty Bytes Customer Loyalty Member Metrics View''
AS
SELECT * FROM ' || $VIEW_LOYALTY_METRICS_HARMONIZED;
EXECUTE IMMEDIATE $VIEW_SQL;

-- truck_reviews_v view
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_TRUCK_REVIEWS_ANALYTICS || ' AS
SELECT * FROM ' || $VIEW_TRUCK_REVIEWS_HARMONIZED;
EXECUTE IMMEDIATE $VIEW_SQL;

GRANT USAGE ON SCHEMA identifier($SCH_RAW_SUPPORT) to ROLE identifier($ROLE_ADMIN);
GRANT SELECT ON TABLE identifier($TBL_TRUCK_REVIEWS) TO ROLE identifier($ROLE_ADMIN);

-- view for streamlit app
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_JAPAN_SALES || ' AS
SELECT
    DISTINCT menu_item_name,
    date,
    order_total
FROM ' || $VIEW_ORDERS_ANALYTICS || '
WHERE country = ''Japan''
    AND YEAR(date) = ''2022''
    AND MONTH(date) = ''2''
GROUP BY ALL
ORDER BY date';
EXECUTE IMMEDIATE $VIEW_SQL;

-- Orders view for the Semantic Layer
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_ORDERS_SEMANTIC || '
COMMENT = ''Provides a clean, business-friendly view of order data, filtered to include only orders from known customers and locations.''
AS
SELECT
    order_id::VARCHAR AS order_id,
    truck_id::VARCHAR AS truck_id,
    order_detail_id::VARCHAR AS order_detail_id,
    truck_brand_name,
    menu_type,
    primary_city,
    region,
    country,
    franchise_flag,
    franchise_id::VARCHAR AS franchise_id,
    location_id::VARCHAR AS location_id,
    customer_id::VARCHAR AS customer_id,
    gender,
    marital_status,
    menu_item_id::VARCHAR AS menu_item_id,
    menu_item_name,
    quantity,
    order_total,
    DATE(order_ts) AS order_date
FROM ' || $VIEW_ORDERS_HARMONIZED || '
WHERE
    customer_id IS NOT NULL 
    AND primary_city IS NOT NULL';
EXECUTE IMMEDIATE $VIEW_SQL;

-- Customer Loyalty Metrics view for the Semantic Layer
SET VIEW_SQL = 'CREATE OR REPLACE VIEW ' || $VIEW_LOYALTY_SEMANTIC || ' AS
SELECT
    cl.customer_id::VARCHAR AS customer_id,
    cl.city,
    cl.country,
    SUM(o.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT o.location_id::VARCHAR) WITHIN GROUP (ORDER BY o.location_id::VARCHAR) AS visited_location_ids_array
FROM ' || $VIEW_LOYALTY_METRICS_HARMONIZED || ' AS cl
JOIN ' || $VIEW_ORDERS_HARMONIZED || ' AS o
    ON cl.customer_id = o.customer_id
GROUP BY
    cl.customer_id,
    cl.city,
    cl.country';
EXECUTE IMMEDIATE $VIEW_SQL;

/*--
 raw zone table load 
--*/

-- truck_reviews table load
SET COPY_SQL = 'COPY INTO ' || $TBL_TRUCK_REVIEWS || ' FROM @' || $STAGE_TRUCK_REVIEWS || '/raw_support/truck_reviews/';
EXECUTE IMMEDIATE $COPY_SQL;

-- country table load
SET COPY_SQL = 'COPY INTO ' || $TBL_COUNTRY || ' FROM @' || $STAGE_S3LOAD || '/raw_pos/country/';
EXECUTE IMMEDIATE $COPY_SQL;

-- franchise table load
SET COPY_SQL = 'COPY INTO ' || $TBL_FRANCHISE || ' FROM @' || $STAGE_S3LOAD || '/raw_pos/franchise/';
EXECUTE IMMEDIATE $COPY_SQL;

-- location table load
SET COPY_SQL = 'COPY INTO ' || $TBL_LOCATION || ' FROM @' || $STAGE_S3LOAD || '/raw_pos/location/';
EXECUTE IMMEDIATE $COPY_SQL;

-- menu table load
SET COPY_SQL = 'COPY INTO ' || $TBL_MENU || ' FROM @' || $STAGE_S3LOAD || '/raw_pos/menu/';
EXECUTE IMMEDIATE $COPY_SQL;

-- truck table load
SET COPY_SQL = 'COPY INTO ' || $TBL_TRUCK || ' FROM @' || $STAGE_S3LOAD || '/raw_pos/truck/';
EXECUTE IMMEDIATE $COPY_SQL;

-- customer_loyalty table load
SET COPY_SQL = 'COPY INTO ' || $TBL_CUSTOMER_LOYALTY || ' FROM @' || $STAGE_S3LOAD || '/raw_customer/customer_loyalty/';
EXECUTE IMMEDIATE $COPY_SQL;

-- order_header table load
SET COPY_SQL = 'COPY INTO ' || $TBL_ORDER_HEADER || ' FROM @' || $STAGE_S3LOAD || '/raw_pos/order_header/';
EXECUTE IMMEDIATE $COPY_SQL;

-- Setup truck details
USE WAREHOUSE identifier($WH_DE);

-- order_detail table load
SET COPY_SQL = 'COPY INTO ' || $TBL_ORDER_DETAIL || ' FROM @' || $STAGE_S3LOAD || '/raw_pos/order_detail/';
EXECUTE IMMEDIATE $COPY_SQL;

-- add truck_build column
ALTER TABLE identifier($TBL_TRUCK)
ADD COLUMN truck_build OBJECT;

-- construct an object from year, make, model and store on truck_build column
SET UPDATE_SQL = 'UPDATE ' || $TBL_TRUCK || ' SET truck_build = OBJECT_CONSTRUCT(''year'', year, ''make'', make, ''model'', model)';
EXECUTE IMMEDIATE $UPDATE_SQL;

-- Messing up make data in truck_build object
SET UPDATE_SQL = 'UPDATE ' || $TBL_TRUCK || ' SET truck_build = OBJECT_INSERT(truck_build, ''make'', ''Ford'', TRUE) WHERE truck_build:make::STRING = ''Ford_'' AND truck_id % 2 = 0';
EXECUTE IMMEDIATE $UPDATE_SQL;

-- truck_details table build 
SET CREATE_SQL = 'CREATE OR REPLACE TABLE ' || $TBL_TRUCK_DETAILS || ' AS SELECT * EXCLUDE (year, make, model) FROM ' || $TBL_TRUCK;
EXECUTE IMMEDIATE $CREATE_SQL;

-- Create or replace the Cortex Search Service named 'tasty_bytes_review_search'. --
SET CORTEX_SQL = 'CREATE OR REPLACE CORTEX SEARCH SERVICE ' || $CORTEX_SEARCH || '
ON REVIEW 
ATTRIBUTES LANGUAGE, ORDER_ID, REVIEW_ID, TRUCK_BRAND_NAME, PRIMARY_CITY, DATE, SOURCE 
WAREHOUSE = ' || $WH_DE || '
TARGET_LAG = ''1 hour'' 
AS (
    SELECT
        REVIEW,             
        LANGUAGE,           
        ORDER_ID,           
        REVIEW_ID,          
        TRUCK_BRAND_NAME,  
        PRIMARY_CITY,       
        DATE,               
        SOURCE             
    FROM ' || $VIEW_TRUCK_REVIEWS_HARMONIZED || '
    WHERE REVIEW IS NOT NULL 
)';
EXECUTE IMMEDIATE $CORTEX_SQL;

USE ROLE securityadmin;
-- Additional Grants on semantic layer
GRANT SELECT ON VIEW identifier($VIEW_ORDERS_SEMANTIC) TO ROLE PUBLIC;
GRANT SELECT ON VIEW identifier($VIEW_LOYALTY_SEMANTIC) TO ROLE PUBLIC;
GRANT READ ON STAGE identifier($STAGE_SEMANTIC_MODEL) TO ROLE identifier($ROLE_ADMIN);
GRANT WRITE ON STAGE identifier($STAGE_SEMANTIC_MODEL) TO ROLE identifier($ROLE_ADMIN);

-- Configure Attendee Account Part 3 --
USE ROLE ACCOUNTADMIN;

-- ability to run across cloud if claude is not in your region:
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
 
 -- Create a database (shared, not user-specific)
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;

-- create a schema to store the agents
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE identifier($ROLE_DEV);
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE identifier($ROLE_DEV);

-- Grant the CREATE AGENT privilege on the agents schema
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE identifier($ROLE_DEV);

/*----------------------------------------------------------------------------------
 V E R I F Y   S E T U P
----------------------------------------------------------------------------------*/
SELECT 'Setup complete for user: ' || $USER_SUFFIX AS STATUS;
SELECT COUNT(*) AS CUSTOMER_COUNT FROM identifier($TBL_CUSTOMER_LOYALTY);
SELECT COUNT(*) AS ORDER_COUNT FROM identifier($TBL_ORDER_HEADER);
