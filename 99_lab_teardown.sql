
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
 U S E R   S U F F I X   V A R I A B L E S
 
 All objects in this lab are suffixed with the current user's name.
 This teardown script will clean up all objects for the current user.
----------------------------------------------------------------------------------*/

-- Set the user suffix (must match 0_setup.sql)
SET USER_SUFFIX = CURRENT_USER();

-- Define role names with user suffix
SET ROLE_ENGINEER = 'HRZN_DATA_ENGINEER_' || $USER_SUFFIX;
SET ROLE_GOVERNOR = 'HRZN_DATA_GOVERNOR_' || $USER_SUFFIX;
SET ROLE_USER = 'HRZN_DATA_USER_' || $USER_SUFFIX;
SET ROLE_IT_ADMIN = 'HRZN_IT_ADMIN_' || $USER_SUFFIX;
SET ROLE_ANALYST = 'HRZN_DATA_ANALYST_' || $USER_SUFFIX;

-- Define warehouse name with user suffix
SET WH_NAME = 'HRZN_WH_' || $USER_SUFFIX;

-- Define database and schema names with user suffix
SET DB_NAME = 'HRZN_DB_' || $USER_SUFFIX;
SET SCH_NAME = 'HRZN_SCH';
SET SCH_CLASSIFIERS = 'CLASSIFIERS';
SET SCH_TAG = 'TAG_SCHEMA';
SET SCH_POLICIES = 'SEC_POLICIES_SCHEMA';

-- Define fully qualified schema paths
SET FQ_SCH = $DB_NAME || '.' || $SCH_NAME;
SET FQ_CLASSIFIERS = $DB_NAME || '.' || $SCH_CLASSIFIERS;
SET FQ_TAG = $DB_NAME || '.' || $SCH_TAG;
SET FQ_POLICIES = $DB_NAME || '.' || $SCH_POLICIES;

-- Define table names (fully qualified)
SET TBL_CUSTOMER = $FQ_SCH || '.CUSTOMER';
SET TBL_CUSTOMER_ORDERS = $FQ_SCH || '.CUSTOMER_ORDERS';
SET TBL_ROW_POLICY_MAP = $FQ_TAG || '.ROW_POLICY_MAP';


/********************/
-- T E A R   D O W N
/********************/

-- Display what will be dropped
SELECT 'Tearing down lab for user: ' || $USER_SUFFIX AS STATUS;


-- Drop medallion tables first (owned by ENGINEER)
USE ROLE identifier($ROLE_ENGINEER);
USE WAREHOUSE identifier($WH_NAME);

DROP TABLE IF EXISTS identifier($FQ_SCH || '.SILVER_CUSTOMER');
DROP TABLE IF EXISTS identifier($FQ_SCH || '.SILVER_CUSTOMER_ORDERS');
DROP TABLE IF EXISTS identifier($FQ_SCH || '.GOLD_CUSTOMER_ORDER_SUMMARY');
DROP TABLE IF EXISTS identifier($FQ_SCH || '.CUSTOMER_NY');
DROP TABLE IF EXISTS identifier($FQ_SCH || '.CUSTOMER_DC');
DROP TABLE IF EXISTS identifier($FQ_SCH || '.CUSTOMER_AR');
DROP TABLE IF EXISTS identifier($FQ_SCH || '.CUSTOMER_ORDER_SUMMARY_NY');
DROP VIEW IF EXISTS identifier($FQ_SCH || '.CUSTOMER_ORDER_SUMMARY');
DROP STAGE IF EXISTS identifier($FQ_SCH || '.CUSTOMERNYSTAGE');

-- Drop DMFs
DROP FUNCTION IF EXISTS identifier($FQ_SCH || '.REFERENTIAL_CHECK')(TABLE(VARCHAR), TABLE(FLOAT));
DROP FUNCTION IF EXISTS identifier($FQ_SCH || '.VOLUME_CHECK')(TABLE(VARCHAR), TABLE(VARCHAR));

-- Drop base tables
DROP TABLE IF EXISTS identifier($TBL_CUSTOMER);
DROP TABLE IF EXISTS identifier($TBL_CUSTOMER_ORDERS);
DROP SCHEMA IF EXISTS identifier($FQ_SCH);


-- Drop governor-owned objects
USE ROLE identifier($ROLE_GOVERNOR);

-- Drop classifiers schema and objects
DROP SCHEMA IF EXISTS identifier($FQ_CLASSIFIERS);

-- Drop policies, tags, and mapping table
DROP TABLE IF EXISTS identifier($TBL_ROW_POLICY_MAP);
DROP SCHEMA IF EXISTS identifier($FQ_TAG);
DROP SCHEMA IF EXISTS identifier($FQ_POLICIES);


-- Drop database
USE ROLE identifier($ROLE_ENGINEER);
DROP DATABASE IF EXISTS identifier($DB_NAME);


-- Drop roles (requires SECURITYADMIN)
USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS identifier($ROLE_ANALYST);
DROP ROLE IF EXISTS identifier($ROLE_GOVERNOR);
DROP ROLE IF EXISTS identifier($ROLE_USER);
DROP ROLE IF EXISTS identifier($ROLE_IT_ADMIN);
DROP ROLE IF EXISTS identifier($ROLE_ENGINEER);


-- Drop warehouse (requires SYSADMIN)
USE ROLE SYSADMIN;
DROP WAREHOUSE IF EXISTS identifier($WH_NAME);


-- Confirm teardown complete
SELECT 'Lab teardown complete for user: ' || $USER_SUFFIX AS STATUS;
