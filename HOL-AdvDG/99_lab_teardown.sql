
/***************************************************************************************************
| H | O | R | I | Z | O | N |   | L | A | B | S | 

Demo:         Horizon Lab
Version:      HLab v1
Create Date:  Apr 17, 2024
Author:       Ravi Kumar
Co-Authors:    Ben Weiss, Susan Devitt
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************/
/****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
Apr 17, 2024        Ravi Kumar           Initial Lab
***************************************************************************************************/

/*----------------------------------------------------------------------------------
 V A R I A B L E S
 
 This teardown script will clean up all objects for the current user.
----------------------------------------------------------------------------------*/


-- Define role names with user suffix

-- Define warehouse name

-- Define database and schema names

-- Define fully qualified schema paths

-- Define table names (fully qualified)


/********************/
-- T E A R   D O W N
/********************/

-- Display what will be dropped
SELECT 'Tearing down lab' AS STATUS;


-- Drop medallion tables first (owned by ENGINEER)
USE ROLE HRZN_DATA_ENGINEER;
USE WAREHOUSE HRZN_WH;

DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.SILVER_CUSTOMER;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.GOLD_CUSTOMER_ORDER_SUMMARY;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER_NY;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER_DC;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER_AR;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER_ORDER_SUMMARY_NY;
DROP VIEW IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER_ORDER_SUMMARY;
DROP STAGE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMERNYSTAGE;

-- Drop DMFs
DROP FUNCTION IF EXISTS HRZN_DB.HRZN_SCH.REFERENTIAL_CHECK(TABLE(VARCHAR), TABLE(FLOAT));
DROP FUNCTION IF EXISTS HRZN_DB.HRZN_SCH.VOLUME_CHECK(TABLE(VARCHAR), TABLE(VARCHAR));

-- Drop base tables
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER;
DROP TABLE IF EXISTS HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS;
DROP SCHEMA IF EXISTS HRZN_DB.HRZN_SCH;


-- Drop governor-owned objects
USE ROLE HRZN_DATA_GOVERNOR;

-- Drop classifiers schema and objects
DROP SCHEMA IF EXISTS HRZN_DB.CLASSIFIERS;

-- Drop policies, tags, and mapping table
DROP TABLE IF EXISTS HRZN_DB.TAG_SCHEMA.ROW_POLICY_MAP;
DROP SCHEMA IF EXISTS HRZN_DB.TAG_SCHEMA;
DROP SCHEMA IF EXISTS HRZN_DB.SEC_POLICIES_SCHEMA;


-- Drop database
USE ROLE HRZN_DATA_ENGINEER;
DROP DATABASE IF EXISTS HRZN_DB;


-- Drop roles (requires SECURITYADMIN)
USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS HRZN_DATA_ANALYST;
DROP ROLE IF EXISTS HRZN_DATA_GOVERNOR;
DROP ROLE IF EXISTS HRZN_DATA_USER;
DROP ROLE IF EXISTS HRZN_IT_ADMIN;
DROP ROLE IF EXISTS HRZN_DATA_ENGINEER;


-- Drop warehouse (requires SYSADMIN)
USE ROLE SYSADMIN;
DROP WAREHOUSE IF EXISTS HRZN_WH;


-- Confirm teardown complete
SELECT 'Lab teardown complete' AS STATUS;
