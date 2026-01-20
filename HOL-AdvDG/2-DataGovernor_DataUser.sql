
/***************************************************************************************************
| H | O | R | I | Z | O | N |   | L | A | B | S | 

Demo:         Horizon Lab (Data Governor / Steward Persona)
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
 V A R I A B L E S
 
 All objects in this lab are suffixed with the current user's name to allow
 multiple users to run the lab concurrently without naming conflicts.
----------------------------------------------------------------------------------*/











/*************************************************/
/*************************************************/
/* D A T A      U S E R      R O L E */
/*************************************************/
/*************************************************/
USE ROLE HRZN_DATA_USER;
USE WAREHOUSE HRZN_WH;
USE DATABASE HRZN_DB;
USE SCHEMA HRZN_DB.HRZN_SCH;




/*----------------------------------------------------------------------------------
Step - Discovery with Snowflake Horizon - Universal Search

 Having explored a wide variety of Governance functionality available in Snowflake,
 it is time to put it all together with Universal Search.

 Universal Search enables to easily find Account objects,
 Snowflake Marketplace listings, relevant Snowflake Documentation and Snowflake
 Community Knowledge Base articles.

 Universal Search understands your query and information about your database
 objects and can find objects with names that differ from your search terms.

 Even if you misspell or type only part of your search term, you can still see
 useful results.
----------------------------------------------------------------------------------*/

/**
 To leverage Universal Search in Snowsight:
  -> Use the Left Navigation Menu
  -> Select "Search" (Magnifying Glass)
  -> Enter Search criteria such as:
    - Tasty Bytes
    - Snowflake Best Practices
    - How to use Snowflake Column Masking
**/



-- Now, Let's look at the customer details
SELECT FIRST_NAME, LAST_NAME, STREET_ADDRESS, STATE, CITY, ZIP, PHONE_NUMBER, EMAIL, SSN, BIRTHDATE, CREDITCARD
FROM HRZN_DB.HRZN_SCH.CUSTOMER
SAMPLE (100 ROWS);

-- there is a lot of PII and sensitive data that needs to be protected
-- Further, there is no understanding of what fields contain the sensitive data.
-- To set this straight, we need to ensure that the right fields are classified and tagged properly.
-- Further, we need to mask PII and other senstive data.





/*************************************************/
/*************************************************/
/* D A T A      G O V E R N O R      R O L E */
/*************************************************/
/*************************************************/
USE ROLE HRZN_DATA_GOVERNOR;

/*----------------------------------------------------------------------------------
Step - Sensitive Data Classification

 In some cases, you may not know if there is sensitive data in a table.
 Snowflake Horizon provides the capability to attempt to automatically detect
 sensitive information and apply relevant Snowflake system defined privacy tags. 

 Classification is a multi-step process that associates Snowflake-defined system
 tags to columns by analyzing the fields and metadata for personal data. Data 
 Classification can be done via SQL or the Snowsight interface.

 Within this step we will be using SQL to classify a single table as well as all
 tables within a schema.

 To learn how to complete Data Classification within the Snowsight interface,
 please see the following documentation: 

 Using Snowsight to classify tables in a schema 
  * https://docs.snowflake.com/en/user-guide/governance-classify-using#using-sf-web-interface-to-classify-tables-in-a-schema
----------------------------------------------------------------------------------*/

/****************************************************/
-- A U T O.  C L A S S I F I C A T I O N --
/****************************************************/
--OPTIONAL: You can perform classification through the UI as well.
--Databases -> Your DB -> HRZN_SCH --> Click "..." -> Classify and Tag Sensitive Data

-- let's use SYSTEM$CLASSIFY to classify the data in the CUSTOMER table within the HRZN_SCH schema
CALL SYSTEM$CLASSIFY('HRZN_DB.HRZN_SCH.CUSTOMER', {'auto_tag': true});


-- now let's view the new Tags that Snowflake applied automatically to the CUSTOMER table via Data Classification
SELECT TAG_DATABASE, TAG_SCHEMA, OBJECT_NAME, COLUMN_NAME, TAG_NAME, TAG_VALUE
FROM TABLE(
  identifier('HRZN_DB' || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS')(
    'HRZN_DB.HRZN_SCH.CUSTOMER',
    'table'
));


--OPTIONAL You can perform classification through the UI as well.
--Databases -> Your DB -> HRZN_SCH --> Click "..." -> Classify and Tag Sensitive Data

-- let's use SYSTEM$CLASSIFY to classify the data in the tables within the HRZN_SCH schema
CALL SYSTEM$CLASSIFY_SCHEMA('HRZN_DB.HRZN_SCH', {'auto_tag': true});


-- once again, let's view the Tags applied within the Schema
SELECT * FROM TABLE(identifier('HRZN_DB' || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS')('HRZN_DB.HRZN_SCH.CUSTOMER','table'));



/*----------------------------------------------------------------------------------
Step - Sensitive Custom Classification

 Snowflake provides the CUSTOM_CLASSIFIER class in the SNOWFLAKE.DATA_PRIVACY schema
 to enable Data Engineers / Governors to extend their Data Classification capabilities based
 on their own knowledge of their data.
---------------------------------------------------------------------------------*/

/****************************************************/
-- C U S T O M   C L A S S I F I C A T I O N --
/****************************************************/
--As a best practice, Lets use a separate schema to store all the customer classifiers
USE SCHEMA HRZN_DB.CLASSIFIERS;

--Create a classifier for the credit card data
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER HRZN_DB.CLASSIFIERS.CREDITCARD();

SHOW SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER;

--Add the regex for each credit card type that we want to be classified into
-- Note: Custom classifier method calls require literal names
CALL CREDITCARD!ADD_REGEX('MC_PAYMENT_CARD','IDENTIFIER','^(?:5[1-5][0-9]{2}|222[1-9]|22[3-9][0-9]|2[3-6][0-9]{2}|27[01][0-9]|2720)[0-9]{12}$');
CALL CREDITCARD!ADD_REGEX('AMX_PAYMENT_CARD','IDENTIFIER','^3[4-7][0-9]{13}$');

SELECT CREDITCARD!LIST();

--OPTIONAL: Lets check if table has the data that matches the credit card number pattern
SELECT CREDITCARD FROM HRZN_DB.HRZN_SCH.CUSTOMER WHERE CREDITCARD REGEXP '^3[4-7][0-9]{13}$';

--Now, classify the data with custom classifier
CALL SYSTEM$CLASSIFY('HRZN_DB.HRZN_SCH.CUSTOMER', {'auto_tag': true, 'custom_classifiers': [HRZN_DB.CLASSIFIERS.CREDITCARD]});

--This statement shows if a column is classified as a particular tag
SELECT SYSTEM$GET_TAG('snowflake.core.semantic_category', 'HRZN_DB.HRZN_SCH.CUSTOMER' || '.CREDITCARD', 'column');



/**
 Moving forward as Schemas or Tables are created and updated we can use this exact
 process of Automatic and Custom Classification to maintain a strong governance posture 
 and build rich semantic-layer metadata.
**/





/********************/
-- T A G G I N G --
/********************/


/**
 A tag-based masking policy combines the object tagging and masking policy features
 to allow a masking policy to be set on a tag using an ALTER TAG command. When the data type in
 the masking policy signature and the data type of the column match, the tagged column is
 automatically protected by the conditions in the masking policy.
**/

USE SCHEMA HRZN_DB.TAG_SCHEMA;

--Step : Create TAGS
--Create cost_center tag and add comment
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.COST_CENTER ALLOWED_VALUES 'Sales','Marketing','Support';
ALTER TAG HRZN_DB.TAG_SCHEMA.COST_CENTER SET COMMENT = 'Respective Cost center for chargeback';

--Create tag for sensitive datasets and add comments
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.CONFIDENTIAL ALLOWED_VALUES 'Sensitive','Restricted','Highly Confidential';
ALTER TAG HRZN_DB.TAG_SCHEMA.CONFIDENTIAL SET COMMENT = 'Confidential information';
                                      
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.PII_TYPE ALLOWED_VALUES 'Email','Phone Number','Last Name';
ALTER TAG HRZN_DB.TAG_SCHEMA.PII_TYPE SET COMMENT = 'PII Columns';




--Step : Apply tags
--Apply tag on warehouse
ALTER WAREHOUSE HRZN_WH SET TAG HRZN_DB.TAG_SCHEMA.COST_CENTER = 'Sales';

--Apply tags at the table and column level
--Table Level
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER SET TAG HRZN_DB.TAG_SCHEMA.CONFIDENTIAL = 'Sensitive';  
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER SET TAG HRZN_DB.TAG_SCHEMA.COST_CENTER = 'Sales';  

--Column Level
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY EMAIL SET TAG HRZN_DB.TAG_SCHEMA.PII_TYPE = 'Email';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY PHONE_NUMBER SET TAG HRZN_DB.TAG_SCHEMA.PII_TYPE = 'Phone Number';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY LAST_NAME SET TAG HRZN_DB.TAG_SCHEMA.PII_TYPE = 'Last Name';


-- Query account usage view to check tags and reference
--Has a latency 
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE TAG_NAME = 'CONFIDENTIAL';
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE TAG_NAME = 'PII_TYPE';
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE TAG_NAME = 'COST_CENTER';



-- now we can use the TAG_REFERENCES_ALL_COLUMNS function to return the Tags associated with our Customer table

SELECT
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('HRZN_DB.HRZN_SCH.CUSTOMER','table'));






/****************************************************/
--  D Y N A M I C  D A T A  M A S K I N G  = 
-- Column-Level Security  to mask dynamically  --
/****************************************************/



/****************************************************/
-- C O N D I T I O N A L  P O L I C Y = 
-- Column-Level Security + Dynamic Data Masking  --
/****************************************************/
--Create masking policy for PII (using suffixed role name in condition)
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_PII AS
  (VAL CHAR) RETURNS CHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'HRZN_DATA_GOVERNOR') THEN VAL
      ELSE '***PII MASKED***'
    END;

--Create masking policy for Sensitive fields
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_SENSITIVE AS
  (VAL CHAR) RETURNS CHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'HRZN_DATA_GOVERNOR') THEN VAL
      ELSE '***SENSITIVE***'
    END;

--Apply policies to specific columns
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN SSN SET MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_PII;
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN CREDITCARD SET MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_SENSITIVE;

--Query the table
SELECT SSN, CREDITCARD FROM HRZN_DB.HRZN_SCH.CUSTOMER;




/*************************************************/
/*************************************************/
/* D A T A      U S E R      R O L E */
/*************************************************/
/*************************************************/
USE ROLE HRZN_DATA_USER;
USE WAREHOUSE HRZN_WH;

SELECT SSN, CREDITCARD FROM HRZN_DB.HRZN_SCH.CUSTOMER;




/*************************************************/
/*************************************************/
/* D A T A      G O V E R N O R      R O L E */
/*************************************************/
/*************************************************/
USE ROLE HRZN_DATA_GOVERNOR;
USE SCHEMA HRZN_DB.TAG_SCHEMA;
USE WAREHOUSE HRZN_WH;


--Opt In masking based on condition
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.CONDITIONALPOLICYDEMO 
   AS (phone_nbr STRING, optin STRING) RETURNS STRING ->
   CASE
      WHEN optin = 'Y' THEN phone_nbr
      ELSE '***OPT OUT***'
   END;

ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN PHONE_NUMBER SET
   MASKING POLICY HRZN_DB.TAG_SCHEMA.CONDITIONALPOLICYDEMO USING (PHONE_NUMBER, OPTIN);

SELECT PHONE_NUMBER, OPTIN FROM HRZN_DB.HRZN_SCH.CUSTOMER;









/****************************************************/
-- T A G   B A S E D   P O L I C I E S  --
-- Streamline process by grouping all these sensitive 
-- or PII columns under a common tag and apply masking for that tag.
/****************************************************/

--Create a Tag
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.PII_COL ALLOWED_VALUES 'PII-DATA','NON-PII';

--Apply to the table
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN LAST_NAME SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN BIRTHDATE SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN STREET_ADDRESS SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN CITY SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN STATE SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN ZIP SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';


--Create Masking Policy (using suffixed role name in condition)
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.PII_DATA_MASK AS (VAL STRING) RETURNS STRING ->
CASE
WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('HRZN_DB.TAG_SCHEMA.PII_COL') = 'PII-DATA' 
    AND CURRENT_ROLE() NOT IN ('HRZN_DATA_GOVERNOR', 'ACCOUNTADMIN') 
    THEN '**PII TAG MASKED**'
ELSE VAL
END;


--Apply Masking policy to the tag
ALTER TAG HRZN_DB.TAG_SCHEMA.PII_COL SET MASKING POLICY HRZN_DB.TAG_SCHEMA.PII_DATA_MASK;





/*************************************************/
/*************************************************/
/* D A T A      U S E R      R O L E */
/*************************************************/
/*************************************************/
--Lets switch roles to the user and Check if phone is visible
USE ROLE HRZN_DATA_USER;
SELECT FIRST_NAME, LAST_NAME, STREET_ADDRESS, CITY, STATE, ZIP 
FROM HRZN_DB.HRZN_SCH.CUSTOMER;





/*************************************************/
/*************************************************/
/* D A T A      G O V E R N O R      R O L E */
/*************************************************/
/*************************************************/
--Switch roles back to Governor
USE ROLE HRZN_DATA_GOVERNOR;
SELECT FIRST_NAME, LAST_NAME, STREET_ADDRESS, CITY, STATE, ZIP 
FROM HRZN_DB.HRZN_SCH.CUSTOMER;




/****************************************************/
-- R O W   A C C E S S     P O L I C I E S  --
/****************************************************/

/*----------------------------------------------------------------------------------
Step- Row-Access Policies

A row access policy is a schema-level object that determines whether a given row in a table or view can be viewed from the following types of statements: SELECT statements. Rows selected by UPDATE, DELETE, and MERGE statements.

Within our Customer table, the users with DATA_USER role should only see Customers who are
based in Massachusetts (MA)
----------------------------------------------------------------------------------*/
--We need to unset any existing masking policies on the column
USE ROLE HRZN_DATA_GOVERNOR;
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN STATE UNSET TAG HRZN_DB.TAG_SCHEMA.PII_COL;





/*************************************************/
/*************************************************/
/* D A T A      U S E R      R O L E */
/*************************************************/
/*************************************************/
USE ROLE HRZN_DATA_USER;
SELECT FIRST_NAME, STREET_ADDRESS, STATE, OPTIN, PHONE_NUMBER, EMAIL, JOB, COMPANY FROM HRZN_DB.HRZN_SCH.CUSTOMER;





/*************************************************/
/*************************************************/
/* D A T A      G O V E R N O R      R O L E */
/*************************************************/
/*************************************************/
USE ROLE HRZN_DATA_GOVERNOR;

--Let's use a mapping table ROW_POLICY_MAP to store the mapping 
--between the users and the data that they have access to
--For our lab, the mapping for the user is in the table ROW_POLICY_MAP
SELECT * FROM HRZN_DB.TAG_SCHEMA.ROW_POLICY_MAP; 


/**
 Snowflake supports row-level security through the use of Row Access Policies to
 determine which rows to return in the query result. The row access policy can be relatively
 simple to allow one particular role to view rows, or be more complex to include a mapping
 table in the policy definition to determine access to rows in the query result.
**/

-- Create Row Access Policy (using suffixed role names in condition)
CREATE OR REPLACE ROW ACCESS POLICY HRZN_DB.TAG_SCHEMA.CUSTOMER_STATE_RESTRICTIONS
    AS (STATE STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN ('ACCOUNTADMIN', 'HRZN_DATA_ENGINEER', 'HRZN_DATA_GOVERNOR') -- list of roles that will not be subject to the policy
        OR EXISTS -- this clause references our mapping table from above to handle the row level filtering
            (
            SELECT rp.ROLE
                FROM HRZN_DB.TAG_SCHEMA.ROW_POLICY_MAP rp
            WHERE 1=1
                AND rp.ROLE = CURRENT_ROLE()
                AND rp.STATE_VISIBILITY = STATE
            )
COMMENT = 'Policy to limit rows returned based on mapping table of ROLE and STATE';



 -- let's now apply the Row Access Policy to our State column in the Customer table
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER
    ADD ROW ACCESS POLICY HRZN_DB.TAG_SCHEMA.CUSTOMER_STATE_RESTRICTIONS ON (STATE);




/*************************************************/
/*************************************************/
/* D A T A      U S E R      R O L E */
/*************************************************/
/*************************************************/
-- with the policy successfully applied, let's test it using the Test Role
USE ROLE HRZN_DATA_USER;
SELECT FIRST_NAME, STREET_ADDRESS, STATE, OPTIN, PHONE_NUMBER, EMAIL, JOB, COMPANY FROM HRZN_DB.HRZN_SCH.CUSTOMER;
