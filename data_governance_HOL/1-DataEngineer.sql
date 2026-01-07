/***************************************************************************************************
| H | O | R | I | Z | O | N |   | L | A | B | S | 

Demo:         Horizon Lab (Data Engineer Persona)
Version:      HLab v1
Create Date:  Apr 17, 2024
Author:       Ravi Kumar
Reviewers:    Ben Weiss, Susan Devitt
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************

****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
Apr 17, 2024        Ravi Kumar           Initial Lab
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Snowflake’s approach to access control combines aspects from both of the following models:
  • Discretionary Access Control (DAC): Each object has an owner, who can in turn grant access to that object.
  • Role-based Access Control (RBAC): Access privileges are assigned to roles, which are in turn assigned to users.

The key concepts to understanding access control in Snowflake are:
  • Securable object: An entity to which access can be granted. Unless allowed by a grant, access is denied.
  • Role: An entity to which privileges can be granted. Roles are in turn assigned to users. Note that roles can also be assigned to other roles, creating a role hierarchy.
  • Privilege: A defined level of access to an object. Multiple distinct privileges may be used to control the granularity of access granted.
  • User: A user identity recognized by Snowflake, whether associated with a person or program.

  
In Summary:
  • In Snowflake, a Role is a container for Privileges to a Securable Object.
  • Privileges can be granted Roles
  • Roles can be granted to Users
  • Roles can be granted to other Roles (which inherit that Roles Privileges)
  • When Users choose a Role, they inherit all the Privileges of the Roles in the 
    hierarchy.
----------------------------------------------------------------------------------*/




/*************************************************/
/*************************************************/
/*           R B A C    &   D A C                */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step - System Defined Roles and Privileges

Let's first take a look at the Snowflake System Defined Roles and their privileges.
----------------------------------------------------------------------------------*/

USE ROLE HRZN_DATA_ENGINEER;
USE WAREHOUSE HRZN_WH;
USE DATABASE HRZN_DB;
USE SCHEMA HRZN_SCH;


--Let's take a look at the Roles currently in our account
SHOW ROLES;


-- this next query, will turn the output of our last SHOW command and allow us to filter on the Snowflake System Roles that
-- are provided as default in all Snowflake Accounts
--> Note: Depending on your permissions you may not see a result for every Role in the Where clause below.
SELECT
    "name",
    "comment"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" IN ('ORGADMIN','ACCOUNTADMIN','SYSADMIN','USERADMIN','SECURITYADMIN','PUBLIC');

/**
  Snowflake System Defined Role Definitions:
   1 - ORGADMIN: Role that manages operations at the organization level.
   2 - ACCOUNTADMIN: Role that encapsulates the SYSADMIN and SECURITYADMIN system-defined roles.
        It is the top-level role in the system and should be granted only to a limited/controlled number of users
        in your account.
   3 - SECURITYADMIN: Role that can manage any object grant globally, as well as create, monitor,
      and manage users and roles.
   4 - USERADMIN: Role that is dedicated to user and role management only.
   5 - SYSADMIN: Role that has privileges to create warehouses and databases in an account.
      If, as recommended, you create a role hierarchy that ultimately assigns all custom roles to the SYSADMIN role, this role also has
      the ability to grant privileges on warehouses, databases, and other objects to other roles.
   6 - PUBLIC: Pseudo-role that is automatically granted to every user and every role in your account. The PUBLIC role can own securable
      objects, just like any other role; however, the objects owned by the role are available to every other
      user and role in your account.

                                +---------------+
                                | ACCOUNTADMIN  |
                                +---------------+
                                  ^    ^     ^
                                  |    |     |
                    +-------------+-+  |    ++-------------+
                    | SECURITYADMIN |  |    |   SYSADMIN   |<------------+
                    +---------------+  |    +--------------+             |
                            ^          |     ^        ^                  |
                            |          |     |        |                  |
                    +-------+-------+  |     |  +-----+-------+  +-------+-----+
                    |   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
                    +---------------+  |     |  +-------------+  +-------------+
                            ^          |     |      ^              ^      ^
                            |          |     |      |              |      |
                            |          |     |      |              |    +-+-----------+
                            |          |     |      |              |    | CUSTOM ROLE |
                            |          |     |      |              |    +-------------+
                            |          |     |      |              |           ^
                            |          |     |      |              |           |
                            +----------+-----+---+--+--------------+-----------+
                                                 |
                                            +----+-----+
                                            |  PUBLIC  |
                                            +----------+
**/

/*----------------------------------------------------------------------------------
Step - Role Creation, GRANTS and SQL Variables

 Now that we understand System Defined Roles, let's begin leveraging them to create
 a Test Role to provide access to the customer table.
----------------------------------------------------------------------------------*/

-- let's use the Useradmin Role to create a Data Analyst Role
USE ROLE USERADMIN;

CREATE OR REPLACE ROLE HRZN_DATA_ANALYST
    COMMENT = 'Analyst Role';


-- now we will switch to Securityadmin to handle our privilege GRANTS
USE ROLE SECURITYADMIN;

-- first we will grant ALL privileges on the Development Warehouse to our Data Analyst Role
GRANT ALL ON WAREHOUSE HRZN_WH TO ROLE HRZN_DATA_ANALYST;

-- next we will grant only OPERATE and USAGE privileges to our Test Role
GRANT OPERATE, USAGE ON WAREHOUSE HRZN_WH TO ROLE HRZN_DATA_ANALYST;

-- before we proceed, let's SET a SQL Variable to equal our CURRENT_USER()
SET MY_USER_ID  = CURRENT_USER();

-- now we can GRANT our Role to the User we are currently logged in as
GRANT ROLE HRZN_DATA_ANALYST TO USER identifier($MY_USER_ID);

--Lets try and access the CUSTOMER TABLE.
SELECT * FROM HRZN_DB.HRZN_SCH.CUSTOMER;

--The previous query fails as the role hasn't been provided access to query the database, schema or the table CUSTOMER.

-- now we will grant USAGE on our Database and all Schemas within it
GRANT USAGE ON DATABASE HRZN_DB TO ROLE HRZN_DATA_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE HRZN_DB TO ROLE HRZN_DATA_ANALYST;

/**
 Snowflake Database and Schema Grants
  1 - MODIFY: Enables altering any settings of a database.
  2 - MONITOR: Enables performing the DESCRIBE command on the database.
  3 - USAGE: Enables using a database, including returning the database details in the
       SHOW DATABASES command output. Additional privileges are required to view or take
       actions on objects in a database.
  4 - ALL: Grants all privileges, except OWNERSHIP, on a database.
**/

-- we are going to test Data Governance features as our Test Role, so let's ensure it can run SELECT statements against our Data Model
GRANT SELECT ON ALL TABLES IN SCHEMA HRZN_DB.HRZN_SCH TO ROLE HRZN_DATA_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA HRZN_DB.HRZN_SCH TO ROLE HRZN_DATA_ANALYST;

    /**
     Snowflake View and Table Privilege Grants
      1 - SELECT: Enables executing a SELECT statement on a table/view.
      2 - INSERT: Enables executing an INSERT command on a table. 
      3 - UPDATE: Enables executing an UPDATE command on a table.
      4 - TRUNCATE: Enables executing a TRUNCATE TABLE command on a table.
      5 - DELETE: Enables executing a DELETE command on a table.
    **/


USE ROLE HRZN_DATA_ANALYST;

--Lets query the table again
SELECT * FROM HRZN_DB.HRZN_SCH.CUSTOMER;












/*************************************************/
/*************************************************/
/* D A T A      E N G I N E E R      R O L E */
/*************************************************/
/*************************************************/

USE ROLE HRZN_DATA_ENGINEER;


/*----------------------------------------------------------------------------------
Step  - Load the Data into the table

  The first Governance feature set we want to deploy and test will be Snowflake Tag
  Based Dynamic Data Masking. This will allow us to mask PII data in columns from
  our Test Role but not from more privileged Roles.
----------------------------------------------------------------------------------*/
--Load the file CustomerDataRaw.csv into the HRZN_DB.HRZN_SCH.CUSTOMER table via the snowsight UI
--Load the file CustomerOrders.csv into the HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS table via the snowsight UI
--Menu: Data -> Databases -> HRZN_DB -> HRZN_SCH -> Tables -> CUSTOMER



/*************************************************/
/*************************************************/
/* M E D A L L I O N   A R C H I T E C T U R E  */
/*************************************************/
/*************************************************/

/*----------------------------------------------------------------------------------
Step - Medallion Architecture (Bronze -> Silver -> Gold)

 This section implements a medallion architecture on top of the existing bronze
 tables (CUSTOMER and CUSTOMER_ORDERS). The architecture consists of:
 
 - Bronze Layer: Raw data tables (CUSTOMER, CUSTOMER_ORDERS) - already exist
 - Silver Layer: Cleansed/validated copies of bronze tables
 - Gold Layer: Business-level aggregations joining silver tables
 
 Each layer includes Data Metric Functions (DMFs) with expectations to monitor
 data quality across the pipeline.
----------------------------------------------------------------------------------*/

USE ROLE HRZN_DATA_ENGINEER;
USE DATABASE HRZN_DB;
USE SCHEMA HRZN_SCH;
USE WAREHOUSE HRZN_WH;


/*----------------------------------------------------------------------------------
 S I L V E R   L A Y E R
 
 Silver tables are cleansed copies of the bronze layer. In this implementation,
 they are direct copies that can be enhanced with additional data quality checks
 and transformations as needed.
----------------------------------------------------------------------------------*/

-- Create Silver Customer table as a copy of Bronze
CREATE OR REPLACE TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER AS
SELECT * FROM HRZN_DB.HRZN_SCH.CUSTOMER;

-- Create Silver Customer Orders table as a copy of Bronze
CREATE OR REPLACE TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS AS
SELECT * FROM HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS;


/*----------------------------------------------------------------------------------
 G O L D   L A Y E R
 
 Gold tables contain business-level aggregations and are optimized for analytics.
 This table joins customer information with aggregated order metrics.
----------------------------------------------------------------------------------*/

-- Create Gold Customer Order Summary joining Silver tables
CREATE OR REPLACE TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY AS
SELECT 
    c.ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.STATE,
    c.CITY,
    c.COMPANY,
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
    SUM(o.ORDER_AMOUNT) AS TOTAL_AMOUNT,
    SUM(o.ORDER_TAX) AS TOTAL_TAX,
    SUM(o.ORDER_TOTAL) AS TOTAL_REVENUE
FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER c
LEFT JOIN HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS o
    ON c.ID = o.CUSTOMER_ID
GROUP BY c.ID, c.FIRST_NAME, c.LAST_NAME, c.STATE, c.CITY, c.COMPANY;


/*----------------------------------------------------------------------------------
 C U S T O M   D M F s   F O R   M E D A L L I O N
 
 Custom Data Metric Functions to validate data quality across medallion layers:
 - referential_check: Validates foreign key relationships between tables
 - volume_check: Ensures row counts match between source and target tables
----------------------------------------------------------------------------------*/

-- Referential Check DMF: Returns count of orphaned foreign keys
CREATE OR REPLACE DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.REFERENTIAL_CHECK(
    arg_t1 TABLE (arg_c1 VARCHAR), 
    arg_t2 TABLE (arg_c2 FLOAT)
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM arg_t1
 WHERE arg_c1 NOT IN (SELECT arg_c2 FROM arg_t2)';

-- Volume Check DMF: Returns difference in row counts between two tables
CREATE OR REPLACE DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.VOLUME_CHECK(
    arg_t1 TABLE (arg_c1 VARCHAR), 
    arg_t2 TABLE (arg_c2 VARCHAR)
)
RETURNS NUMBER AS
'SELECT ABS(
    (SELECT COUNT(*) FROM arg_t1) - (SELECT COUNT(*) FROM arg_t2)
)';

-- Grant DMFs to relevant roles
GRANT ALL ON FUNCTION HRZN_DB.HRZN_SCH.REFERENTIAL_CHECK(TABLE(VARCHAR), TABLE(FLOAT)) TO ROLE PUBLIC;
GRANT ALL ON FUNCTION HRZN_DB.HRZN_SCH.VOLUME_CHECK(TABLE(VARCHAR), TABLE(VARCHAR)) TO ROLE PUBLIC;


/*----------------------------------------------------------------------------------
 V A L I D A T E   D M F s   -   M A N U A L   C H E C K S
 
 Run these queries to manually verify the DMF results before scheduling.
 These help validate that the data quality checks are working as expected.
----------------------------------------------------------------------------------*/

-- ==========================================
-- Check System DMFs on SILVER_CUSTOMER
-- ==========================================

-- Check row count (Volume)
SELECT SNOWFLAKE.CORE.ROW_COUNT(
    SELECT * FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER
) AS SILVER_CUSTOMER_ROW_COUNT;

-- Check null count on ID column
SELECT SNOWFLAKE.CORE.NULL_COUNT(
    SELECT ID FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER
) AS SILVER_CUSTOMER_ID_NULL_COUNT;

-- Check duplicate emails
SELECT SNOWFLAKE.CORE.DUPLICATE_COUNT(
    SELECT EMAIL FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER
) AS SILVER_CUSTOMER_EMAIL_DUPLICATES;


-- ==========================================
-- Check System DMFs on SILVER_CUSTOMER_ORDERS
-- ==========================================

-- Check row count (Volume)
SELECT SNOWFLAKE.CORE.ROW_COUNT(
    SELECT * FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS
) AS SILVER_CUSTOMER_ORDERS_ROW_COUNT;

-- Check null count on ORDER_ID column
SELECT SNOWFLAKE.CORE.NULL_COUNT(
    SELECT ORDER_ID FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS
) AS SILVER_CUSTOMER_ORDERS_ORDERID_NULL_COUNT;


-- ==========================================
-- Check Custom DMFs
-- ==========================================

-- Check referential integrity: CUSTOMER_IDs in SILVER_CUSTOMER_ORDERS should exist in SILVER_CUSTOMER
SELECT HRZN_DB.HRZN_SCH.REFERENTIAL_CHECK(
    SELECT CUSTOMER_ID FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS,
    SELECT ID FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER
) AS ORPHANED_CUSTOMER_IDS;

-- Check volume consistency: Silver orders should match Bronze orders
SELECT HRZN_DB.HRZN_SCH.VOLUME_CHECK(
    SELECT CUSTOMER_ID FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS,
    SELECT CUSTOMER_ID FROM HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS
) AS SILVER_VS_BRONZE_ORDERS_DIFF;

-- Check volume consistency: Silver customer should match Bronze customer
SELECT HRZN_DB.HRZN_SCH.VOLUME_CHECK(
    SELECT EMAIL FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER,
    SELECT EMAIL FROM HRZN_DB.HRZN_SCH.CUSTOMER
) AS SILVER_VS_BRONZE_CUSTOMER_DIFF;


-- ==========================================
-- Check System DMFs on GOLD_CUSTOMER_ORDER_SUMMARY
-- ==========================================

-- Check row count (Volume)
SELECT SNOWFLAKE.CORE.ROW_COUNT(
    SELECT * FROM HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY
) AS GOLD_SUMMARY_ROW_COUNT;

-- Check null count on ID column
SELECT SNOWFLAKE.CORE.NULL_COUNT(
    SELECT ID FROM HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY
) AS GOLD_SUMMARY_ID_NULL_COUNT;

-- Check freshness (seconds since last update)
SELECT SNOWFLAKE.CORE.FRESHNESS(
    SELECT * FROM HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY
) AS GOLD_SUMMARY_FRESHNESS_SECONDS;


/*----------------------------------------------------------------------------------
 A P P L Y   D M F s   W I T H   E X P E C T A T I O N S
 
 Apply Data Metric Functions with expectations to monitor data quality
 across all medallion layers. Expectations define thresholds that trigger
 alerts when violated.
----------------------------------------------------------------------------------*/

-- ==========================================
-- SILVER_CUSTOMER DMFs
-- ==========================================

-- Set the schedule for SILVER_CUSTOMER
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER SET DATA_METRIC_SCHEDULE = '5 minute';

-- Volume check: Ensure table has data
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON () 
    Expectation Volume_Check (value > 0);

-- Accuracy check: ID should never be null
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (ID) 
    Expectation ID_Not_Null (value = 0);

-- Uniqueness check: Email should be unique
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (EMAIL);


-- ==========================================
-- SILVER_CUSTOMER_ORDERS DMFs
-- ==========================================

-- Set the schedule for SILVER_CUSTOMER_ORDERS
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS SET DATA_METRIC_SCHEDULE = '5 minute';

-- Volume check: Ensure table has data
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON () 
    Expectation Volume_Check (value > 0);

-- Accuracy check: ORDER_ID should never be null
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (ORDER_ID) 
    Expectation OrderID_Not_Null (value = 0);

-- Referential integrity check: All CUSTOMER_IDs should exist in SILVER_CUSTOMER
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.REFERENTIAL_CHECK 
    ON (CUSTOMER_ID, TABLE (HRZN_DB.HRZN_SCH.SILVER_CUSTOMER(ID))) 
    Expectation FK_Check (value = 0);

-- Volume check: Silver should match Bronze row count
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.VOLUME_CHECK 
    ON (CUSTOMER_ID, TABLE (HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS(CUSTOMER_ID))) 
    Expectation NoDiff (value = 0);


-- ==========================================
-- GOLD_CUSTOMER_ORDER_SUMMARY DMFs
-- ==========================================

-- Set the schedule for GOLD_CUSTOMER_ORDER_SUMMARY
ALTER TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY SET DATA_METRIC_SCHEDULE = '5 minute';

-- Volume check: Ensure table has data
ALTER TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON () 
    Expectation Volume_Check (value > 0);

-- Accuracy check: ID should never be null
ALTER TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (ID) 
    Expectation ID_Not_Null (value = 0);

-- Freshness check: Data should be recent (within 30 minutes)
ALTER TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON () 
    Expectation Freshness_Check (value < 1800);


-- Review DMF schedules for all medallion tables
SELECT metric_name, ref_entity_name, schedule, schedule_status 
FROM TABLE(information_schema.data_metric_function_references(
    ref_entity_name => 'HRZN_DB.HRZN_SCH.SILVER_CUSTOMER', 
    ref_entity_domain => 'TABLE'));

SELECT metric_name, ref_entity_name, schedule, schedule_status 
FROM TABLE(information_schema.data_metric_function_references(
    ref_entity_name => 'HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS', 
    ref_entity_domain => 'TABLE'));

SELECT metric_name, ref_entity_name, schedule, schedule_status 
FROM TABLE(information_schema.data_metric_function_references(
    ref_entity_name => 'HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY', 
    ref_entity_domain => 'TABLE'));


/*----------------------------------------------------------------------------------
 G R A N T   P E R M I S S I O N S
 
 Grant SELECT permissions on new medallion tables to downstream roles
 to ensure compatibility with existing governance workflows.
----------------------------------------------------------------------------------*/

-- Grant permissions on SILVER_CUSTOMER
GRANT ALL ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER TO ROLE HRZN_DATA_GOVERNOR;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER TO ROLE HRZN_DATA_USER;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER TO ROLE HRZN_IT_ADMIN;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER TO ROLE HRZN_DATA_ANALYST;

-- Grant permissions on SILVER_CUSTOMER_ORDERS
GRANT ALL ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS TO ROLE HRZN_DATA_GOVERNOR;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS TO ROLE HRZN_DATA_USER;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS TO ROLE HRZN_IT_ADMIN;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS TO ROLE HRZN_DATA_ANALYST;

-- Grant permissions on GOLD_CUSTOMER_ORDER_SUMMARY
GRANT ALL ON TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY TO ROLE HRZN_DATA_GOVERNOR;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY TO ROLE HRZN_DATA_USER;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY TO ROLE HRZN_IT_ADMIN;
GRANT SELECT ON TABLE HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY TO ROLE HRZN_DATA_ANALYST;


-- Query to verify medallion tables were created successfully
SELECT 'SILVER_CUSTOMER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER
UNION ALL
SELECT 'SILVER_CUSTOMER_ORDERS', COUNT(*) FROM HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS
UNION ALL
SELECT 'GOLD_CUSTOMER_ORDER_SUMMARY', COUNT(*) FROM HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY;

-- Sample from Gold table to verify join worked correctly
SELECT * FROM HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY
ORDER BY TOTAL_REVENUE DESC NULLS LAST
LIMIT 10;


-- View DMF results from the monitoring view
SELECT 
    change_commit_time,
    measurement_time,
    table_schema,
    table_name,
    metric_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'HRZN_DB'
    AND table_name IN ('SILVER_CUSTOMER', 'SILVER_CUSTOMER_ORDERS', 'GOLD_CUSTOMER_ORDER_SUMMARY')
ORDER BY change_commit_time DESC;


/**
 The medallion architecture is now complete with:
 
 - Bronze Layer: CUSTOMER, CUSTOMER_ORDERS (original tables)
 - Silver Layer: SILVER_CUSTOMER, SILVER_CUSTOMER_ORDERS (validated copies)
 - Gold Layer: GOLD_CUSTOMER_ORDER_SUMMARY (aggregated customer order metrics)
 
 Each layer has DMFs with expectations to monitor:
 - Volume (row counts)
 - Accuracy (null checks on key columns)
 - Referential integrity (foreign key validation)
 - Freshness (data recency)
 
 The downstream files (2-DataGovernor_DataUser.sql and 3-Data-governor-Admin.sql)
 continue to use the bronze tables and remain fully compatible.
**/
