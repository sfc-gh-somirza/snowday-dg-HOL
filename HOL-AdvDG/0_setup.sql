
/***************************************************************************************************
| H | O | R | I | Z | O | N |   | L | A | B | S | 

Demo:         Horizon Lab
Version:      HLab v1
Create Date:  Apr 17, 2024
Author:       Ravi Kumar
Reviewers:    Ben Weiss, Susan Devitt
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
----------------------------------------------------------------------------------*/


-- Define all role names
SET ROLE_ENGINEER = 'HRZN_DATA_ENGINEER';
SET ROLE_GOVERNOR = 'HRZN_DATA_GOVERNOR';
SET ROLE_USER = 'HRZN_DATA_USER';
SET ROLE_IT_ADMIN = 'HRZN_IT_ADMIN';
SET ROLE_ANALYST = 'HRZN_DATA_ANALYST';

-- Define warehouse name
SET WH_NAME = 'HRZN_WH';

-- Define database and schema names
SET DB_NAME = 'HRZN_DB';
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
SET TBL_CUSTOMER_NY = $FQ_SCH || '.CUSTOMER_NY';
SET TBL_CUSTOMER_DC = $FQ_SCH || '.CUSTOMER_DC';
SET TBL_CUSTOMER_AR = $FQ_SCH || '.CUSTOMER_AR';
SET TBL_CUSTOMER_ORDER_SUMMARY_NY = $FQ_SCH || '.CUSTOMER_ORDER_SUMMARY_NY';
SET VIEW_CUSTOMER_ORDER_SUMMARY = $FQ_SCH || '.CUSTOMER_ORDER_SUMMARY';
SET STAGE_CUSTOMER_NY = $FQ_SCH || '.CUSTOMERNYSTAGE';

-- Display all variables for verification
SELECT 
    
    $ROLE_ENGINEER AS ROLE_ENGINEER,
    $ROLE_GOVERNOR AS ROLE_GOVERNOR,
    $ROLE_USER AS ROLE_USER,
    $ROLE_IT_ADMIN AS ROLE_IT_ADMIN,
    $WH_NAME AS WAREHOUSE,
    $DB_NAME AS DATABASE_NAME,
    $FQ_SCH AS MAIN_SCHEMA;


/*----------------------------------------------------------------------------------
 C R E A T E   R O L E S
----------------------------------------------------------------------------------*/
USE ROLE SECURITYADMIN;

CREATE OR REPLACE ROLE identifier($ROLE_ENGINEER);
CREATE OR REPLACE ROLE identifier($ROLE_GOVERNOR);
CREATE OR REPLACE ROLE identifier($ROLE_USER);
CREATE OR REPLACE ROLE identifier($ROLE_IT_ADMIN);

-- Grant roles to SYSADMIN for administration
GRANT ROLE identifier($ROLE_ENGINEER) TO ROLE SYSADMIN;
GRANT ROLE identifier($ROLE_GOVERNOR) TO ROLE SYSADMIN;
GRANT ROLE identifier($ROLE_USER) TO ROLE SYSADMIN;
GRANT ROLE identifier($ROLE_IT_ADMIN) TO ROLE SYSADMIN;

-- Grant roles to current user
GRANT ROLE identifier($ROLE_ENGINEER) TO USER CURRENT_USER();
GRANT ROLE identifier($ROLE_GOVERNOR) TO USER CURRENT_USER();
GRANT ROLE identifier($ROLE_USER) TO USER CURRENT_USER();
GRANT ROLE identifier($ROLE_IT_ADMIN) TO USER CURRENT_USER();


/*----------------------------------------------------------------------------------
 C R E A T E   W A R E H O U S E
----------------------------------------------------------------------------------*/
USE ROLE SYSADMIN;

CREATE OR REPLACE WAREHOUSE identifier($WH_NAME) WITH WAREHOUSE_SIZE='X-SMALL';

GRANT USAGE ON WAREHOUSE identifier($WH_NAME) TO ROLE identifier($ROLE_ENGINEER);
GRANT USAGE ON WAREHOUSE identifier($WH_NAME) TO ROLE identifier($ROLE_GOVERNOR);
GRANT USAGE ON WAREHOUSE identifier($WH_NAME) TO ROLE identifier($ROLE_USER);
GRANT USAGE ON WAREHOUSE identifier($WH_NAME) TO ROLE identifier($ROLE_IT_ADMIN);


/*----------------------------------------------------------------------------------
 C R E A T E   D A T A B A S E   &   S C H E M A S
----------------------------------------------------------------------------------*/
GRANT CREATE DATABASE ON ACCOUNT TO ROLE identifier($ROLE_ENGINEER);

USE ROLE identifier($ROLE_ENGINEER);

CREATE OR REPLACE DATABASE identifier($DB_NAME);
CREATE OR REPLACE SCHEMA identifier($FQ_SCH);

-- Grant permissions to GOVERNOR role
GRANT USAGE ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_GOVERNOR);
GRANT USAGE ON SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_GOVERNOR);
GRANT CREATE SCHEMA ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_GOVERNOR);
GRANT USAGE ON ALL SCHEMAS IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_GOVERNOR);
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_GOVERNOR);
GRANT SELECT ON ALL TABLES IN SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_GOVERNOR);
GRANT SELECT ON ALL VIEWS IN SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_GOVERNOR);

-- Grant permissions to IT_ADMIN role
GRANT USAGE ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_IT_ADMIN);
GRANT USAGE ON SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_IT_ADMIN);
GRANT CREATE SCHEMA ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_IT_ADMIN);

-- Grant permissions to DATA_USER role
GRANT USAGE ON DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_USER);
GRANT USAGE ON SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_USER);
GRANT USAGE ON ALL SCHEMAS IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_USER);
GRANT SELECT ON ALL TABLES IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_USER);


/*----------------------------------------------------------------------------------
 C R E A T E   A D D I T I O N A L   S C H E M A S
----------------------------------------------------------------------------------*/
USE ROLE identifier($ROLE_GOVERNOR);

-- Create Schema for classifiers
CREATE OR REPLACE SCHEMA identifier($FQ_CLASSIFIERS)
COMMENT = 'Schema containing Classifiers';

-- Create Schema for Tags
CREATE OR REPLACE SCHEMA identifier($FQ_TAG)
COMMENT = 'Schema containing Tags';

-- Create ROW_POLICY_MAP table
CREATE OR REPLACE TABLE identifier($TBL_ROW_POLICY_MAP)
    (role STRING, state_visibility STRING);

-- Insert role mapping
INSERT INTO identifier($TBL_ROW_POLICY_MAP)
    VALUES ('HRZN_DATA_USER', 'MA'); 

-- Create Schema for Security Policies
CREATE OR REPLACE SCHEMA identifier($FQ_POLICIES)
COMMENT = 'Schema containing Security Policies';


/*----------------------------------------------------------------------------------
 G R A N T   F U T U R E   P E R M I S S I O N S
----------------------------------------------------------------------------------*/
USE ROLE SECURITYADMIN;

GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_GOVERNOR);
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_GOVERNOR);

GRANT SELECT ON FUTURE TABLES IN SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_USER);
GRANT SELECT ON FUTURE TABLES IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_USER);

GRANT SELECT ON FUTURE TABLES IN SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_IT_ADMIN);
GRANT SELECT ON FUTURE TABLES IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_IT_ADMIN);

GRANT USAGE ON ALL SCHEMAS IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_IT_ADMIN);
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_IT_ADMIN);
GRANT SELECT ON ALL VIEWS IN DATABASE identifier($DB_NAME) TO ROLE identifier($ROLE_IT_ADMIN);
GRANT SELECT ON ALL TABLES IN SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_IT_ADMIN);
GRANT SELECT ON ALL VIEWS IN SCHEMA identifier($FQ_SCH) TO ROLE identifier($ROLE_IT_ADMIN);


/*----------------------------------------------------------------------------------
 G R A N T   D A T A   M E T R I C   &   G O V E R N A N C E   P E R M I S S I O N S
----------------------------------------------------------------------------------*/
USE ROLE ACCOUNTADMIN;

-- Grants for DATA_ENGINEER role
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE identifier($ROLE_ENGINEER);
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE identifier($ROLE_ENGINEER);

-- Grants for DATA_GOVERNOR role
GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE identifier($ROLE_GOVERNOR);
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE identifier($ROLE_GOVERNOR);
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE identifier($ROLE_GOVERNOR);
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE identifier($ROLE_GOVERNOR);
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE identifier($ROLE_GOVERNOR);

-- Grants for IT_ADMIN role
GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE identifier($ROLE_IT_ADMIN);
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE identifier($ROLE_IT_ADMIN);
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE identifier($ROLE_IT_ADMIN);
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE identifier($ROLE_IT_ADMIN);
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE identifier($ROLE_IT_ADMIN);


/*----------------------------------------------------------------------------------
 C R E A T E   T A B L E S   &   L O A D   D A T A
----------------------------------------------------------------------------------*/
USE ROLE identifier($ROLE_ENGINEER);
USE WAREHOUSE identifier($WH_NAME);

CREATE OR REPLACE TABLE identifier($TBL_CUSTOMER) (
    ID FLOAT,
    FIRST_NAME VARCHAR,
    LAST_NAME VARCHAR,
    STREET_ADDRESS VARCHAR,
    STATE VARCHAR,
    CITY VARCHAR,
    ZIP VARCHAR,
    PHONE_NUMBER VARCHAR,
    EMAIL VARCHAR,
    SSN VARCHAR,
    BIRTHDATE VARCHAR,
    JOB VARCHAR,
    CREDITCARD VARCHAR,
    COMPANY VARCHAR,
    OPTIN VARCHAR
);

CREATE OR REPLACE TABLE identifier($TBL_CUSTOMER_ORDERS) (
    CUSTOMER_ID VARCHAR,    
    ORDER_ID VARCHAR,    
    ORDER_TS DATE,    
    ORDER_CURRENCY VARCHAR,    
    ORDER_AMOUNT FLOAT,    
    ORDER_TAX FLOAT,    
    ORDER_TOTAL FLOAT
);

-- Load data from S3
COPY INTO identifier($TBL_CUSTOMER)
FROM s3://sfquickstarts/summit_2024_horizon_hol/CustomerDataRaw.csv
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);

COPY INTO identifier($TBL_CUSTOMER_ORDERS)
FROM s3://sfquickstarts/summit_2024_horizon_hol/CustomerOrders.csv
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);

-- Grant permissions on tables
GRANT ALL ON TABLE identifier($TBL_CUSTOMER) TO ROLE identifier($ROLE_GOVERNOR);
GRANT SELECT ON TABLE identifier($TBL_CUSTOMER) TO ROLE identifier($ROLE_USER);
GRANT SELECT ON TABLE identifier($TBL_CUSTOMER) TO ROLE identifier($ROLE_IT_ADMIN);

GRANT ALL ON TABLE identifier($TBL_CUSTOMER_ORDERS) TO ROLE identifier($ROLE_GOVERNOR);
GRANT SELECT ON TABLE identifier($TBL_CUSTOMER_ORDERS) TO ROLE identifier($ROLE_USER);
GRANT SELECT ON TABLE identifier($TBL_CUSTOMER_ORDERS) TO ROLE identifier($ROLE_IT_ADMIN);


/*----------------------------------------------------------------------------------
 G R A N T   P O L I C Y   P E R M I S S I O N S
----------------------------------------------------------------------------------*/
USE ROLE ACCOUNTADMIN;

GRANT APPLY TAG ON ACCOUNT TO ROLE identifier($ROLE_GOVERNOR);
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE identifier($ROLE_GOVERNOR);
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE identifier($ROLE_GOVERNOR);
GRANT DATABASE ROLE SNOWFLAKE.CLASSIFICATION_ADMIN TO ROLE identifier($ROLE_GOVERNOR);


/*----------------------------------------------------------------------------------
 C R E A T E   L I N E A G E   T A B L E S   &   V I E W S
----------------------------------------------------------------------------------*/
USE ROLE identifier($ROLE_ENGINEER);
USE DATABASE identifier($DB_NAME);
USE SCHEMA identifier($FQ_SCH);
USE WAREHOUSE identifier($WH_NAME);

-- Create state-specific customer tables
CREATE OR REPLACE TABLE identifier($TBL_CUSTOMER_NY) AS
SELECT * EXCLUDE ZIP FROM identifier($TBL_CUSTOMER) WHERE state='NY';

CREATE OR REPLACE TABLE identifier($TBL_CUSTOMER_DC) AS
SELECT * EXCLUDE ZIP FROM identifier($TBL_CUSTOMER) WHERE state='DC';

CREATE OR REPLACE TABLE identifier($TBL_CUSTOMER_AR) AS
SELECT * EXCLUDE ZIP FROM identifier($TBL_CUSTOMER) WHERE state='AR';

-- Create customer order summary view
CREATE OR REPLACE VIEW identifier($VIEW_CUSTOMER_ORDER_SUMMARY) AS
SELECT C.ID, C.FIRST_NAME, C.LAST_NAME, COUNT(CO.ORDER_ID) ORDERS_COUNT, SUM(CO.ORDER_TOTAL) ORDER_TOTAL
FROM identifier($TBL_CUSTOMER) C, identifier($TBL_CUSTOMER_ORDERS) CO
WHERE C.ID = CO.CUSTOMER_ID
GROUP BY 1,2,3;

-- Create NY customer order summary table
CREATE OR REPLACE TABLE identifier($TBL_CUSTOMER_ORDER_SUMMARY_NY) AS
SELECT CS.*
FROM identifier($TBL_CUSTOMER_NY) C, identifier($VIEW_CUSTOMER_ORDER_SUMMARY) CS
WHERE C.ID = CS.ID;

-- Create stage and copy data
CREATE OR REPLACE STAGE identifier($STAGE_CUSTOMER_NY);
COPY INTO @identifier($STAGE_CUSTOMER_NY) FROM identifier($TBL_CUSTOMER_ORDER_SUMMARY_NY);


/*----------------------------------------------------------------------------------
 V E R I F Y   S E T U P
----------------------------------------------------------------------------------*/
SELECT 'Setup complete' AS STATUS;
SELECT COUNT(*) AS CUSTOMER_COUNT FROM identifier($TBL_CUSTOMER);
SELECT COUNT(*) AS ORDER_COUNT FROM identifier($TBL_CUSTOMER_ORDERS);
