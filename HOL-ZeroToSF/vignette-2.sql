/***************************************************************************************************       
Asset:        Zero to Snowflake - Simple Data Pipeline
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Simple Data Pipeline
1. Ingestion from External stage
2. Semi-Structured Data and the VARIANT data type
3. Dynamic Tables
4. Simple Pipeline with Dynamic Tables
5. Pipeline Visualization with the Directed Acyclic Graph (DAG)

****************************************************************************************************/

/*----------------------------------------------------------------------------------
 U S E R   S U F F I X   V A R I A B L E S
 
 All objects in this lab are suffixed with the current user's name to allow
 multiple users to run the lab concurrently without naming conflicts.
----------------------------------------------------------------------------------*/

-- Set the user suffix (will be appended to all object names)
SET USER_SUFFIX = CURRENT_USER();

-- Database
SET DB_NAME = 'TB_101_' || $USER_SUFFIX;

-- Roles
SET ROLE_ENGINEER = 'TB_DATA_ENGINEER_' || $USER_SUFFIX;

-- Warehouses
SET WH_DE = 'TB_DE_WH_' || $USER_SUFFIX;

-- Schemas (fully qualified)
SET SCH_PUBLIC = $DB_NAME || '.PUBLIC';
SET SCH_RAW_POS = $DB_NAME || '.RAW_POS';
SET SCH_HARMONIZED = $DB_NAME || '.HARMONIZED';

-- File Format
SET FF_CSV = $SCH_PUBLIC || '.CSV_FF';

-- Tables (fully qualified)
SET TBL_ORDER_HEADER = $SCH_RAW_POS || '.ORDER_HEADER';
SET TBL_ORDER_DETAIL = $SCH_RAW_POS || '.ORDER_DETAIL';
SET TBL_LOCATION = $SCH_RAW_POS || '.LOCATION';

-- New objects created in this vignette
SET STAGE_MENU = $SCH_RAW_POS || '.MENU_STAGE';
SET TBL_MENU_STAGING = $SCH_RAW_POS || '.MENU_STAGING';
SET DT_INGREDIENT = $SCH_HARMONIZED || '.INGREDIENT';
SET DT_INGREDIENT_MENU_LOOKUP = $SCH_HARMONIZED || '.INGREDIENT_TO_MENU_LOOKUP';
SET DT_INGREDIENT_USAGE = $SCH_HARMONIZED || '.INGREDIENT_USAGE_BY_TRUCK';

-- Display variables for verification
SELECT 
    $USER_SUFFIX AS USER_SUFFIX,
    $DB_NAME AS DATABASE_NAME,
    $ROLE_ENGINEER AS ROLE_ENGINEER,
    $WH_DE AS WAREHOUSE_DE;


ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "data_pipeline"}}';

/*
    We will assume the role of a TastyBytes data engineer with the intention of creating a data pipeline with raw menu data,
    so let's set our context appropriately.
*/
USE DATABASE identifier($DB_NAME);
USE ROLE identifier($ROLE_ENGINEER);
USE WAREHOUSE identifier($WH_DE);

/*  1. Ingestion from External stage
    ***************************************************************
    SQL Reference:
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
    ***************************************************************

    Right now our data currently sits in an Amazon S3 bucket in CSV format. We need to load this raw CSV data 
    into a stage so that we can COPY it INTO a staging table for us to work with.
    
    In Snowflake, a stage is a named database object that specifies a location where data files are stored, allowing 
    you to load or unload data into and out of tables. 

    When we create a stage we specify:
                                - The S3 bucket to pull the data from
                                - The file format to parse the data with, CSV in this case
*/

-- Create the menu stage
SET CREATE_SQL = 'CREATE OR REPLACE STAGE ' || $STAGE_MENU || '
COMMENT = ''Stage for menu data''
URL = ''s3://sfquickstarts/frostbyte_tastybytes/raw_pos/menu/''
FILE_FORMAT = (FORMAT_NAME = ''' || $FF_CSV || ''')';
EXECUTE IMMEDIATE $CREATE_SQL;

CREATE OR REPLACE TABLE identifier($TBL_MENU_STAGING)
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

-- With the stage and table in place, let's now load the data from the stage into the new menu_staging table.
SET COPY_SQL = 'COPY INTO ' || $TBL_MENU_STAGING || ' FROM @' || $STAGE_MENU;
EXECUTE IMMEDIATE $COPY_SQL;

-- Optional: Verify successful load
SELECT * FROM identifier($TBL_MENU_STAGING);

/*  2. Semi-Structured data in Snowflake
    *********************************************************************
    User-Guide:
    https://docs.snowflake.com/en/sql-reference/data-types-semistructured
    *********************************************************************
    
    Snowflake excels at handling semi-structured data like JSON using its VARIANT data type. It automatically parses, optimizes, 
    and indexes this data, enabling users to query it with standard SQL and specialized functions for easy extraction and analysis.
    Snowflake supports semi-structured data types such as JSON, Avro, ORC, Parquet or XML.
    
    The VARIANT object in the menu_item_health_metrics_obj column contains two main key-value pairs:
        - menu_item_id: A number representing the item's unique identifier.
        - menu_item_health_metrics: An array that holds objects detailing health information.
        
    Each object within the menu_item_health_metrics array has:
        - An ingredients array of strings.
        - Several dietary flags with string values of 'Y' and 'N'.
*/
SELECT menu_item_health_metrics_obj FROM identifier($TBL_MENU_STAGING);

/*
    This query uses special syntax to navigate the data's internal, JSON-like structure. 
    The colon operator (:) accesses data by its key name and square brackets ([]) select an element from an array by its numerical position. 
    We can then chain these operators together to extract the ingredients list from the nested object.
    
    Elements retrieved from VARIANT objects remain VARIANT type. 
    Casting these elements to their known data types improves query performance and enhances data quality.
    There are two different ways to achieve casting:
        - the CAST function
        - using the shorthand syntax: <source_expr> :: <target_data_type>

    Below is a query that combines all of these topics to get the menu item name, the menu item ID,
    and the list of ingredients needed. 
*/
SET SELECT_SQL = 'SELECT
    menu_item_name,
    CAST(menu_item_health_metrics_obj:menu_item_id AS INTEGER) AS menu_item_id,
    menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY AS ingredients
FROM ' || $TBL_MENU_STAGING;
EXECUTE IMMEDIATE $SELECT_SQL;

/*
    Another powerful function we can leverage when working with semi-structured data is FLATTEN.
    FLATTEN allows us to unwrap semi-structured data like JSON and Arrays and produce
    a row for every element within the specified object.

    We can use it to get a list of all ingredients from all of the menus used by our trucks.
*/
SET SELECT_SQL = 'SELECT
    i.value::STRING AS ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM ' || $TBL_MENU_STAGING || ' m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i';
EXECUTE IMMEDIATE $SELECT_SQL;

/*  3. Dynamic Tables
    **************************************************************
    User-Guide:
    https://docs.snowflake.com/en/user-guide/dynamic-tables-about
    **************************************************************
    
    It would be nice to have all of the ingredients stored in a structured format to easily query, filter and
    analyze individually. However, our food truck franchises are constantly adding new and exciting menu items
    to their menu, many of which use unique ingredients not yet in our database. 
    
    For this, we can use Dynamic Tables, a powerful tool designed to simplify data transformation pipelines.
    Dynamic Tables are a perfect fit for our use case for several reasons:
        - They are created using a declarative syntax, where their data is defined by a specified query.
        - Automatic data refresh means data remains fresh without requiring manual updates or custom scheduling. 
        - Data freshness managed by Snowflake Dynamic Tables extends not only to the dynamic table 
          itself but also to any downstream data objects that depend on it.

    To see these functionalities in action, we'll create a simple Dynamic Table pipeline and then add a new 
    menu item to the staging table to demonstrate automatic refreshes.

    We will start by creating the Dynamic Table for Ingredients.
*/
SET CREATE_SQL = 'CREATE OR REPLACE DYNAMIC TABLE ' || $DT_INGREDIENT || '
    LAG = ''1 minute''
    WAREHOUSE = ''' || $WH_DE || '''
AS
    SELECT
    ingredient_name,
    menu_ids
FROM (
    SELECT DISTINCT
        i.value::STRING AS ingredient_name,
        ARRAY_AGG(m.menu_item_id) AS menu_ids
    FROM ' || $TBL_MENU_STAGING || ' m,
        LATERAL FLATTEN(INPUT => menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i
    GROUP BY i.value::STRING
)';
EXECUTE IMMEDIATE $CREATE_SQL;

-- Let's verify that the ingredients Dynamic Table was successfully created
SELECT * FROM identifier($DT_INGREDIENT);

/*
    One of our sandwich trucks Better Off Bread has introduced a new menu item, a Banh Mi sandwich.
    This menu item introduces a few ingredients: French Baguette, Mayonnaise, and Pickled Daikon. 
    
    Dynamic table's automatic refresh means that updating our menu_staging table with this new menu 
    item will automatically reflect in the ingredient table. 
*/
SET INSERT_SQL = 'INSERT INTO ' || $TBL_MENU_STAGING || ' 
SELECT 
    10101,
    15,
    ''Sandwiches'',
    ''Better Off Bread'',
    157,
    ''Banh Mi'',
    ''Main'',
    ''Cold Option'',
    9.0,
    12.0,
    PARSE_JSON(''{
      "menu_item_health_metrics": [
        {
          "ingredients": [
            "French Baguette",
            "Mayonnaise",
            "Pickled Daikon",
            "Cucumber",
            "Pork Belly"
          ],
          "is_dairy_free_flag": "N",
          "is_gluten_free_flag": "N",
          "is_healthy_flag": "Y",
          "is_nut_free_flag": "Y"
        }
      ],
      "menu_item_id": 157
    }'')';
EXECUTE IMMEDIATE $INSERT_SQL;

/*
    Verify French Baguette, Pickled Daikon are showing in the ingredients table.
    You may see 'Query produced no results". This means the dynamic table hasn't refreshed yet.
    Allow at most 1 minute for the Dynamic Table lag setting to catch up
*/

SET SELECT_SQL = 'SELECT * FROM ' || $DT_INGREDIENT || ' WHERE ingredient_name IN (''French Baguette'', ''Pickled Daikon'')';
EXECUTE IMMEDIATE $SELECT_SQL;

/* 4. Simple Pipeline with Dynamic Tables

    Now let's create an ingredient to menu lookup dynamic table. This will let us see which menu items 
    use specific ingredients. Then we can determine which trucks need which ingredients and how many.
    Since this table is also a dynamic table, it will automatically refresh should any new ingredients be used 
    in any menu item that is added to the menu staging table.
*/
SET CREATE_SQL = 'CREATE OR REPLACE DYNAMIC TABLE ' || $DT_INGREDIENT_MENU_LOOKUP || '
    LAG = ''1 minute''
    WAREHOUSE = ''' || $WH_DE || '''
AS
SELECT
    i.ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM ' || $TBL_MENU_STAGING || ' m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients) f
JOIN ' || $DT_INGREDIENT || ' i ON f.value::STRING = i.ingredient_name';
EXECUTE IMMEDIATE $CREATE_SQL;

-- Verify ingredient to menu lookup created successfully
SET SELECT_SQL = 'SELECT * FROM ' || $DT_INGREDIENT_MENU_LOOKUP || ' ORDER BY menu_item_id';
EXECUTE IMMEDIATE $SELECT_SQL;

/*
    Run the next two insert queries to simulate an order of 2 Banh Mi sandwiches at truck #15 on
    January 27th 2022. After that we'll create another downstream dynamic table that shows us 
    ingredient usage by truck.
*/
SET INSERT_SQL = 'INSERT INTO ' || $TBL_ORDER_HEADER || '
SELECT 
    459520441,
    15,
    1030,
    101565,
    null,
    200322900,
    TO_TIMESTAMP_NTZ(''08:00:00'', ''hh:mi:ss''),
    TO_TIMESTAMP_NTZ(''14:00:00'', ''hh:mi:ss''),
    null,
    TO_TIMESTAMP_NTZ(''2022-01-27 08:21:08.000''),
    null,
    ''USD'',
    14.00,
    null,
    null,
    14.00';
EXECUTE IMMEDIATE $INSERT_SQL;
    
SET INSERT_SQL = 'INSERT INTO ' || $TBL_ORDER_DETAIL || '
SELECT
    904745311,
    459520441,
    157,
    null,
    0,
    2,
    14.00,
    28.00,
    null';
EXECUTE IMMEDIATE $INSERT_SQL;

/*
    Next, we'll create another dynamic table that summarizes the monthly usage of each ingredient by individual food trucks in the United States. 
    This allows our business to track ingredient consumption, which is crucial for optimizing inventory, controlling 
    costs, and making informed decisions about menu planning and supplier relationships.
    
    Note the two different methods used to extract parts of the date from our order timestamp:
      -> EXTRACT(<date part> FROM <datetime>) will isolate the specified date part from the given timestamp. There are several 
      date and time parts that can be used with EXTRACT function with the most common being YEAR, MONTH, DAY, HOUR, MINUTE, SECOND.
      -> MONTH(<datetime>) returns the month's index from 1-12. YEAR(<datetime>) and DAY(<datetime>) will do the same but for the year
      and day respectively.
*/

-- Next create the table
SET CREATE_SQL = 'CREATE OR REPLACE DYNAMIC TABLE ' || $DT_INGREDIENT_USAGE || '
    LAG = ''2 minute''
    WAREHOUSE = ''' || $WH_DE || '''
    AS 
    SELECT
        oh.truck_id,
        EXTRACT(YEAR FROM oh.order_ts) AS order_year,
        MONTH(oh.order_ts) AS order_month,
        i.ingredient_name,
        SUM(od.quantity) AS total_ingredients_used
    FROM ' || $TBL_ORDER_DETAIL || ' od
        JOIN ' || $TBL_ORDER_HEADER || ' oh ON od.order_id = oh.order_id
        JOIN ' || $DT_INGREDIENT_MENU_LOOKUP || ' iml ON od.menu_item_id = iml.menu_item_id
        JOIN ' || $DT_INGREDIENT || ' i ON iml.ingredient_name = i.ingredient_name
        JOIN ' || $TBL_LOCATION || ' l ON l.location_id = oh.location_id
    WHERE l.country = ''United States''
    GROUP BY
        oh.truck_id,
        order_year,
        order_month,
        i.ingredient_name
    ORDER BY
        oh.truck_id,
        total_ingredients_used DESC';
EXECUTE IMMEDIATE $CREATE_SQL;

/*
    Now, let's view the ingredient usage for truck #15 in January 2022 using our newly created
    ingredient_usage_by_truck view. 
*/
SET SELECT_SQL = 'SELECT
    truck_id,
    ingredient_name,
    SUM(total_ingredients_used) AS total_ingredients_used
FROM ' || $DT_INGREDIENT_USAGE || '
WHERE
    order_month = 1
    AND truck_id = 15
GROUP BY truck_id, ingredient_name
ORDER BY total_ingredients_used DESC';
EXECUTE IMMEDIATE $SELECT_SQL;

/*  5. Pipeline Visualization with the Directed Acyclic Graph (DAG)

    Finally, let's understand our pipeline's Directed Acyclic Graph, or DAG. 
    The DAG serves as a visualization of our data pipeline. You can use it to visually orchestrate complex data workflows, ensuring 
    tasks run in the correct order. You can use it to view lag metrics and configuration for each dynamic table in the pipeline and 
    also manually refresh tables if needed.

    To access the DAG:
    - Click the 'Data' button in the Navigation Menu to open the database screen
    - Click the arrow '>' next to your user-specific TB_101 database to expand it 
    - Expand 'HARMONIZED' then expand 'Dynamic Tables'
    - Click the 'INGREDIENT' table
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
USE ROLE accountadmin;

--Drop Dynamic Tables
DROP TABLE IF EXISTS identifier($TBL_MENU_STAGING);
DROP DYNAMIC TABLE IF EXISTS identifier($DT_INGREDIENT);
DROP DYNAMIC TABLE IF EXISTS identifier($DT_INGREDIENT_MENU_LOOKUP);
DROP DYNAMIC TABLE IF EXISTS identifier($DT_INGREDIENT_USAGE);

--Delete inserts
SET DELETE_SQL = 'DELETE FROM ' || $TBL_ORDER_DETAIL || ' WHERE order_detail_id = 904745311';
EXECUTE IMMEDIATE $DELETE_SQL;
SET DELETE_SQL = 'DELETE FROM ' || $TBL_ORDER_HEADER || ' WHERE order_id = 459520441';
EXECUTE IMMEDIATE $DELETE_SQL;

-- Unset Query Tag
ALTER SESSION UNSET query_tag;
-- Suspend warehouse
ALTER WAREHOUSE identifier($WH_DE) SUSPEND;
