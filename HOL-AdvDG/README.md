## **Overview**

Horizon is a suite of native Snowflake features that allow people easily find, understand, and trust data. In this lab you'll learn how Horizon ensures people have reliable and trustworthy data to make confident, data-driven decisions while ensuring observability and security of data assets.  
In this expert-led, hands-on lab, you will follow a step-by-step guide utilizing a provided sample database of synthetic customer orders. Using this example data, you will learn how Horizon can monitor and provide visibility into your data within Snowflake. We will examine Horizon features from three different personas within Snowflake

* a Data Engineer monitoring pipelines  
* a Data Governor monitoring and masking PII  
* a Data Governor Admin auditing access and lineage

### **Introduction to Horizon**

Before we dive into the lab, lets take a look at a typical governance workflow and learn a bit more about the personas we will be exploring today.

#### **Typical Governance Workflow**

**![img][image1]**

#### [**Data Engineer Persona Video**](https://youtu.be/MdZ1PaJWH2w?si=o8k8HDrzQjZ5Jhst)

#### [**Data Governor/Steward Persona Video**](https://youtu.be/bF6FAMeGEZc?si=mKxGlzJL6843B-FK)

#### [**Data Governor Admin Persona Video**](https://youtu.be/doView4YqUI?si=tQd_KP7YzIIvogla)

Now that you have the introduction to Horizon and our personas, lets get started.

### **\- What You’ll Learn**

* How to protect sensitive data using Snowflake's role-based masking policies  
* How to visualize column-level lineage for impact analysis  
* How to create a Horizon dashboard in Snowsight to monitor your data and policies  
* Analyzing profiling statistics for Snowflake assets.  
* Set up data quality monitoring rules using Data Metric Functions (DMF) and Expectations.  
* Create and apply custom DMFs.
* Monitor and investigate data quality issues using the Monitoring dashboard and the Data Lineage graph.  
* Best practices for monitoring data quality in Snowflake.

## **Setup**

All the scripts for this lab are available in the HOL-AdvDG folder.  
Let's get started\! First we will run the script **0\_setup.sql**  
1\. Download 0\_setup and upload it to Snowflake.  
2\. Run the whole worksheet.


## **Horizon as Data Engineer \- Data Quality Monitoring**

Trusted data is essential for confident decision-making, analytics, and AI. Snowflake’s Data Quality Framework provides a native, end-to-end approach for ensuring that data is accurate, complete, and reliable throughout its lifecycle.  
This framework combines proactive monitoring with interactive UI for root cause analysis and impact assessment into a single experience. Built directly on Snowflake, it enables scalable, automated data quality without requiring external tools or data movement. This quickstars will introduce a concepts such as:

* Data Profiling  
* System and Custom Data Metric Functions  
* Expectations  
* Data Quality Monitoring in Snowsight  
* Data Metric Scan function  
* Data Lineage

#### 

#### **Create sample dataset with quality issues**

Consider a data pipeline following a modern data architecture pattern with three distinct layers following the medallion architecture.  
![Data architecture][image2]

**Bronze Layer**: Raw data from multiple sources (databases, JSON, XML files) **Silver Layer**: Cleaned and enriched data with AI-powered insights **Gold Layer**: Analytics-ready data stored in managed Iceberg tables and regular Snowflake tables.

#### **Bronze Layer (ingestion)**

```sql
-- Bronze tables (CUSTOMER, CUSTOMER_ORDERS) are loaded from S3 during setup
-- See 0_setup.sql for the COPY INTO statements that load the raw data

INSERT INTO HRZN_DB.HRZN_SCH.CUSTOMER VALUES
    (1, 'John', 'Doe', '123 Elm St', 'CA', 'San Francisco', '94105', '123-456-7890', 'john.doe@email.com', '111-22-3333', '1985-04-12', 'Engineer', '4111111111111111', 'Acme Inc.', 'Y'),
    (2, 'Jane', 'Smith', NULL, 'CA', 'Los Angeles', '90001', NULL, 'jane.smith@email.com', NULL, '1990/07/25', 'Manager', '5500000000000004', 'Globex', 'N'),
    (3, 'Mike', 'Brown', '456 Oak St', 'NV', 'Las Vegas', '89101', '9999999999', 'mike.brown.com', '123-45-6789', 'bad-date', 'Analyst', '4000000000000002', NULL, 'Y'),
    (4, 'Anna', 'Lee', '789 Pine St', 'WA', 'Seattle', '98101', '206-555-1234', 'anna.lee@email.com', '222-33-4444', '1988-11-05', 'Designer', '340000000000009', 'Innotech', 'Y');

INSERT INTO HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS VALUES
    ('1', 'O1', 1, 9, 2025, 100, 8.25, 108.25, 'CREDIT_CARD'),
    ('2', 'O2', 2, 9, 2025, 200, NULL, 200, 'PAYPAL'),
    ('2', 'O3', 3, 9, 2025, 300, 60, 360, 'BANK_TRANSFER'),
    ('3', 'O4', NULL, 9, 2025, -50, 5, -45, 'CREDIT_CARD'),
    ('4', 'O5', 4, 9, 2025, 150, 12, 162, 'PAYPAL'),
    ('1', 'O6', 5, 9, 2025, 250, 20, 270, 'CREDIT_CARD');
    ('5', NULL, 5, 9, 2025, 250, 20, 270, 'VENMO');
```

#### **Silver Layer (clean orders only)**

```sql
-- Define Silver layer table variables
SET TBL_SILVER_CUSTOMER = 'HRZN_DB.HRZN_SCH' || '.SILVER_CUSTOMER';
SET TBL_SILVER_CUSTOMER_ORDERS = 'HRZN_DB.HRZN_SCH' || '.SILVER_CUSTOMER_ORDERS';

-- Create Silver Customer Orders table
CREATE OR REPLACE TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS AS
SELECT *, 
    DATE_TRUNC('month', order_ts) AS order_month
FROM HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS;

-- Create Silver Customer table
CREATE OR REPLACE TABLE identifier($TBL_SILVER_CUSTOMER) AS
SELECT * EXCLUDE birthdate,
    TO_DATE(birthdate, 'MM/DD/YY') AS birthdate,
    DATEDIFF('year', TO_DATE(birthdate, 'MM/DD/YY'), CURRENT_DATE()) AS age
FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

#### **Gold Layer (Aggregated by Customer State and Payment Type)**

```sql
-- Define Gold layer table variable
SET TBL_GOLD_CUSTOMER_ORDER_SUMMARY = 'HRZN_DB.HRZN_SCH' || '.GOLD_CUSTOMER_ORDER_SUMMARY';

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
FROM identifier($TBL_SILVER_CUSTOMER) c
LEFT JOIN HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS o
    ON c.ID = o.CUSTOMER_ID
GROUP BY c.ID, c.FIRST_NAME, c.LAST_NAME, c.STATE, c.CITY, c.COMPANY;
```

Let’s take a look at the Lineage tab to review our setup.  
![sales summary][image3]

In the next section, we will learn how to use data profiling statistics to analyze a dataset.

## **Analyze**

### **Analyze dataset with profiling statistics**

The first step in the data quality lifecycle is data profiling. Data profiling is the process of analyzing a dataset to understand its structure, content, and quality. It typically includes gathering statistics such as data types, value distributions, null counts, and uniqueness to identify patterns and potential data quality issues. Using Snowsight, you can easily profile a dataset by accessing the Data Quality Tab. This is an important role in helping you get started with continuous data quality monitoring by laying the groundwork for identifying data quality rules.  
![customer\_orders][image4]

## **Best Practices**

### **Best practices for monitoring data quality with DMFs and Expectations**

This section presents best practices and considerations for data quality strategy.  
At each layer of our pipeline we are interested in monitoring different aspects.  

Bronze: test source data 

Silver: validate transformations 

Gold: test business logic  

Organizations typically take different approaches to monitoring and enforcing data quality. Some focus on the Bronze layer to catch issues as early as possible, ensuring that all ingested data meets baseline expectations. Yet, bad data might emerge later in the pipeline. Others monitor at the Gold layer, prioritizing end-user experience by ensuring that curated datasets remain accurate and reliable. However, this makes root-cause analysis more complex. A third approach targets the Silver layer, validating transformations and business logic. But this requires ongoing maintenance as transformation evolves.  
While each strategy has merit, the most effective approach is hybrid monitoring across all three layers. This ensures comprehensive coverage, early detection of raw data issues, validation of transformations, and protection of business-critical outputs.

#### **Snowflake Data Metric Functions**

You can measure the quality of your data in Snowflake by using DMFs. Snowflake provides built-in [system DMFs](https://docs.snowflake.com/en/user-guide/data-quality-system-dmfs#system-dmfs) to measure common metrics without having to define them. You can also define your own custom DMFs to meet business specific requirements.  
All DMFs that are set on the table follow a [schedule](https://docs.snowflake.com/en/user-guide/data-quality-working#schedule-the-dmf-to-run) to automate the data quality measurement, and you can define a schedule to trigger DMFs based on time or based on DML events. After you schedule the DMFs to run, Snowflake records the results of the DMF in a [dedicated event table](https://docs.snowflake.com/en/user-guide/data-quality-results) for data metric functions.

#### **DMF Privilege Setup**

```sql
-- DMF privileges are granted during setup in 0_setup.sql
-- The following grants are applied to DATA_ENGINEER and DATA_GOVERNOR roles:
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE HRZN_DATA_ENGINEER;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE HRZN_DATA_ENGINEER;

GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE HRZN_DATA_GOVERNOR;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE HRZN_DATA_GOVERNOR;
```

#### **Monitoring Bronze Layer**

The bronze layer is the raw, unprocessed storage zone where ingested data is captured in its original format before any cleaning, transformation, or enrichment. It is important to monitor this layer for data quality to ensure the integrity of source data before downstream transformation and analytics.  
The following example associates volume, freshness, and null checks using DMFs and Expectations.

```sql
-- Set the schedule for Bronze CUSTOMER_ORDERS
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS SET DATA_METRIC_SCHEDULE = '5 minute';

-- Volume check: Ensure table has data
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON () 
    Expectation Volume_Check (value > 1);

-- Freshness check: Data should be recent
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON () 
    Expectation Freshness_Check (value < 1800);

-- Null count check: ORDER_ID should never be null
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (ORDER_ID) 
    Expectation Null_Check (value = 0);
```

#### **Monitoring Silver Layer**

The Silver layer is the transformation and enrichment zone where raw data from the Bronze layer is cleaned, joined, and reshaped to create intermediate datasets. Monitoring this layer is important to validate that transformations are applied correctly, business rules are enforced, and data anomalies are caught before reaching business-facing datasets. Ensuring quality at this stage helps prevent errors from propagating downstream and supports reliable analytics and reporting.  
Create a custom DMF for referential integrity check

```sql
-- Define DMF names (fully qualified)
SET DMF_REFERENTIAL_CHECK = 'HRZN_DB.HRZN_SCH' || '.REFERENTIAL_CHECK';
SET DMF_VOLUME_CHECK = 'HRZN_DB.HRZN_SCH' || '.VOLUME_CHECK';

-- Referential Check DMF: Returns count of orphaned foreign keys
CREATE OR REPLACE DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.REFERENTIAL_CHECK (
    arg_t1 TABLE (arg_c1 VARCHAR), 
    arg_t2 TABLE (arg_c2 FLOAT)
)
RETURNS NUMBER AS
 'SELECT COUNT(*) FROM arg_t1
  WHERE arg_c1 NOT IN (SELECT arg_c2 FROM arg_t2)';
```

Create a custom DMF that compares row counts between two tables

```sql
-- Volume Check DMF: Returns difference in row counts between two tables
CREATE OR REPLACE DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.VOLUME_CHECK (
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
```

Associate checks

```sql
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

-- Uniqueness check: ORDER_ID should be unique
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (ORDER_ID) 
    Expectation OrderID_dupes (value = 0);

-- Referential integrity check: All CUSTOMER_IDs should exist in SILVER_CUSTOMER
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.REFERENTIAL_CHECK 
    ON (CUSTOMER_ID, TABLE (identifier($TBL_SILVER_CUSTOMER)(ID))) 
    Expectation FK_Check (value = 0);

-- Volume check: Silver should match Bronze row count
ALTER TABLE HRZN_DB.HRZN_SCH.SILVER_CUSTOMER_ORDERS 
    ADD DATA METRIC FUNCTION HRZN_DB.HRZN_SCH.VOLUME_CHECK 
    ON (CUSTOMER_ID, TABLE (HRZN_DB.HRZN_SCH.CUSTOMER_ORDERS(CUSTOMER_ID))) 
    Expectation NoDiff (value = 0);
```

#### **Monitoring Gold Layer**

The Gold layer is the curated, consumption-ready zone where trusted datasets are delivered to dashboards, reports, and machine learning models. Monitoring this layer is critical to protect the end-user experience and ensure that business decisions are made using accurate and complete data. Quality checks at this stage catch any remaining issues before they impact stakeholders, providing confidence in the datasets that drive critical business outcomes.  

A common type of check for this layer is Accepted Values. This type of check ensures data consistency, enforces business rules, and prevents invalid values from propagating downstream into analytics, reporting, or machine learning models. It validates that a column contains only a predefined set of allowed values and compares each value in the column against a reference list of valid values. This is particularly useful for categorical columns, such as product items, payment types, order statuses, or region codes.  

For example, in a payment\_type column, the accepted values might be ('CREDIT\_CARD', 'PAYPAL', 'WIRE\_TRANSFER'). The check will flag any rows containing values outside this set, such as CHECK or BITCOIN.

```sql
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

-- Review DMF results from the monitoring view
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
```

Now that we’ve set up DMFs and Expectations to monitor our pipeline, let’s explore the Data Quality Tab in Snowsight to review the Monitoring Dashboard.

## **Review**

### **Review Data Quality Monitoring Dashboard in Snowsight**

Once the DMF setup is completed, you can review the DMF results by accessing the Quality Tab on Snowsight. Note that the first DMF run has to be completed before results start showing up in the dashboard.  
The interactive interface displays DQ metric results, trends, and incidents. You can drill down into failed quality checks, view impacted assets, and investigate specific records that violate a data quality check.

#### **Investigate failed quality checks**

Using the Monitoring Dashboard, you can drill down to DMF results and identify which records violated the quality check.

* Select a particular DMF to open the sidebar.  
* With the side panel open, select View Failed Records.  
* Execute the prepopulated query to see the records that failed the quality check. This query calls the SYSTEM$DATA\_METRIC\_SCAN function.

For information about using the SYSTEM$DATA\_METRIC\_SCAN function to remediate the data quality issues, see Using [SYSTEM$DATA\_METRIC\_SCAN to fix data](https://docs.snowflake.com/en/user-guide/data-quality-fixing.html#label-data-quality-remediate).  
Note: This function applies to [system DMFs](https://docs.snowflake.com/en/user-guide/data-quality-fixing.html#label-data-quality-remediation-supported-dmfs) only.

#### **Identify Impacted Assets**

The DMF side panel automatically displays the objects that are [downstream](https://docs.snowflake.com/en/user-guide/ui-snowsight-lineage.html#label-lineage-upstream-downstream) in the lineage of the object with which the DMF is associated. If there is a data quality issue, you can determine what other objects are possibly affected.  
![shipments][image5]

## **Horizon as Data Governor \- Know & protect your data**

**Overview**  
In today's world of data management, it is common to have policies and procedures that range from data quality and retention to personal data protection. A Data Governor within an organization defines and applies data policies. Here we will explore Horizon features such as **universal search** that makes it easier to find Account objects,Snowflake Marketplace listings, relevant Snowflake Documentation and Snowflake Community Knowledge Base articles.  
Note: Universal Search understands your query and information about your database objects and can find objects with names that differ from your search terms. Even if you misspell or type only part of your search term, you can still see useful results.  
To leverage Universal Search in Snowsight:

* Use the Left Navigation Menu  
* Select "Search" (Magnifying Glass)  
* Enter Search criteria such as:  
  * Snowflake Best Practices  
  * How to use Snowflake Column Masking

### **Create a new worksheet**

In snowsight upload worksheet 2\_Data\_Governor. Below is an explanation of the code.

Let's start by assuming the Data User role and using our Horizon Warehouse (synonymous with compute). This lets us see what access our Data Users have to our customer data.

```sql
USE ROLE HRZN_DATA_USER;
USE WAREHOUSE HRZN_WH;
USE DATABASE HRZN_DB;
USE SCHEMA HRZN_DB.HRZN_SCH;
```

Now, Let's look at the customer details

```sql
SELECT FIRST_NAME, LAST_NAME, STREET_ADDRESS, STATE, CITY, ZIP, PHONE_NUMBER, EMAIL, SSN, BIRTHDATE, CREDITCARD
FROM HRZN_DB.HRZN_SCH.CUSTOMER
SAMPLE (100 ROWS);
```

### **Protecting Sensitive Information**

Looking at this table we can see there is a lot of PII and sensitive data that needs to be protected. However, as a Data user, we may not understand what fields contain the sensitive data.  
To set this straight, we need to ensure that the right fields are classified and tagged properly. Further, we need to mask PII and other senstive data. Lets switch to the Data governor role and we can explore the Horizon features for classification, tagging and masking.

```sql
USE ROLE HRZN_DATA_GOVERNOR;
USE WAREHOUSE HRZN_WH;
USE DATABASE HRZN_DB;
USE SCHEMA HRZN_DB.HRZN_SCH;
```

#### **Sensitive Data Classification**

In some cases, you may not know if there is sensitive data in a table. Snowflake Horizon provides the capability to automatically detect sensitive information and apply relevant Snowflake system defined privacy tags.  
Classification is a multi-step process that associates Snowflake-defined system tags to columns by analyzing the fields and metadata for personal data. Data Classification can be done via SQL or the Snowsight interface.  
Within this step we will be using SQL to classify a single table as well as all tables within a schema.  
To learn how to complete Data Classification within the Snowsight interface, please see the following documentation:  
\[Using Snowsight to classify tables in a schema\] ([https://docs.snowflake.com/en/user-guide/governance-classify-using\#using-sf-web-interface-to-classify-tables-in-a-schema](https://docs.snowflake.com/en/user-guide/governance-classify-using#using-sf-web-interface-to-classify-tables-in-a-schema))

#### **Autoclassification for Sensitive information**

OPTIONAL: You can perform classification through the UI as well. \--Databases \-\> HRZN\_DB \-\> HRZN\_SCH \--\> Click "..." \-\> Classify and Tag Sensitive Data  
As our Raw Customer Schema only includes one table, let's use SYSTEM$CLASSIFY against it

```sql
CALL SYSTEM$CLASSIFY('HRZN_DB.HRZN_SCH.CUSTOMER', {'auto_tag': true});
```

Now let's view the new Tags Snowflake applied automatically via Data Classification

```sql
SELECT TAG_DATABASE, TAG_SCHEMA, OBJECT_NAME, COLUMN_NAME, TAG_NAME, TAG_VALUE
FROM TABLE(
  identifier('HRZN_DB' || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS')(
    'HRZN_DB.HRZN_SCH.CUSTOMER',
    'table'
));
```

OPTIONAL You can perform classification through the UI as well. \--Databases \-\> HRZN\_DB \-\> HRZN\_SCH \--\> Click "..." \-\> Classify and Tag Sensitive Data  
As our Raw Point-of-Sale Schema includes numerous tables, let's use SYSTEM$CLASSIFY\_SCHEMA against it

```sql
CALL SYSTEM$CLASSIFY_SCHEMA('HRZN_DB.HRZN_SCH', {'auto_tag': true});
```

Once again, let's view the Tags applied using the Customer table within the Schema

```sql
SELECT * FROM TABLE(identifier('HRZN_DB' || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS')('HRZN_DB.HRZN_SCH.CUSTOMER','table'));
```

#### **Custom Classification**

Snowflake provides the CUSTOM\_CLASSIFIER class in the SNOWFLAKE.DATA\_PRIVACY schema to enable Data Engineers / Governors to extend their Data Classification capabilities based on their own knowledge of their data.

```sql
-- Define classifier name variable
SET CLASSIFIER_CREDITCARD = $FQ_CLASSIFIERS || '.CREDITCARD';

-- Use classifiers schema
USE SCHEMA HRZN_DB.CLASSIFIERS;

-- Create a classifier for the credit card data
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER HRZN_DB.CLASSIFIERS.CREDITCARD();

SHOW SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER;

-- Add the regex for each credit card type that we want to be classified into
-- Note: Custom classifier method calls require literal names
CALL CREDITCARD!ADD_REGEX('MC_PAYMENT_CARD','IDENTIFIER','^(?:5[1-5][0-9]{2}|222[1-9]|22[3-9][0-9]|2[3-6][0-9]{2}|27[01][0-9]|2720)[0-9]{12}$');
CALL CREDITCARD!ADD_REGEX('AMX_PAYMENT_CARD','IDENTIFIER','^3[4-7][0-9]{13}$');

SELECT CREDITCARD!LIST();

-- OPTIONAL: Check if table has the data that matches the credit card number pattern
SELECT CREDITCARD FROM HRZN_DB.HRZN_SCH.CUSTOMER WHERE CREDITCARD REGEXP '^3[4-7][0-9]{13}$';

-- Classify the data with custom classifier
CALL SYSTEM$CLASSIFY('HRZN_DB.HRZN_SCH.CUSTOMER', {'auto_tag': true, 'custom_classifiers': [HRZN_DB.CLASSIFIERS.CREDITCARD]});
```

Note: This statement shows if a column is classified as a particular tag

```sql
SELECT SYSTEM$GET_TAG('snowflake.core.semantic_category', 'HRZN_DB.HRZN_SCH.CUSTOMER' || '.CREDITCARD', 'column');
```

Moving forward as Schemas or Tables are created and updated we can use this exact process of Automatic and Custom Classification to maintain a strong governance posture and build rich semantic-layer metadata.

#### **Tagging**

A tag-based masking policy combines the object tagging and masking policy features to allow a masking policy to be set on a tag using an ALTER TAG command. When the data type in the masking policy signature and the data type of the column match, the tagged column is automatically protected by the conditions in the masking policy.

```sql
-- Define tag names (fully qualified)
SET TAG_COST_CENTER = $FQ_TAG || '.COST_CENTER';
SET TAG_CONFIDENTIAL = $FQ_TAG || '.CONFIDENTIAL';
SET TAG_PII_TYPE = $FQ_TAG || '.PII_TYPE';

USE SCHEMA HRZN_DB.TAG_SCHEMA;
```

Create cost\_center tag and add comment

```sql
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.COST_CENTER ALLOWED_VALUES 'Sales','Marketing','Support';
ALTER TAG HRZN_DB.TAG_SCHEMA.COST_CENTER SET COMMENT = 'Respective Cost center for chargeback';
```

Create on sensitive datasets and add comments

```sql
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.CONFIDENTIAL ALLOWED_VALUES 'Sensitive','Restricted','Highly Confidential';
ALTER TAG HRZN_DB.TAG_SCHEMA.CONFIDENTIAL SET COMMENT = 'Confidential information';
                                      
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.PII_TYPE ALLOWED_VALUES 'Email','Phone Number','Last Name';
ALTER TAG HRZN_DB.TAG_SCHEMA.PII_TYPE SET COMMENT = 'PII Columns';
```

Apply tag on warehouse

```sql
ALTER WAREHOUSE HRZN_WH SET TAG HRZN_DB.TAG_SCHEMA.COST_CENTER = 'Sales';
```

Apply tags at the table and column level

```sql
--Table Level
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER SET TAG HRZN_DB.TAG_SCHEMA.CONFIDENTIAL = 'Sensitive';  
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER SET TAG HRZN_DB.TAG_SCHEMA.COST_CENTER = 'Sales';  
--Column Level
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY EMAIL SET TAG HRZN_DB.TAG_SCHEMA.PII_TYPE = 'Email';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY PHONE_NUMBER SET TAG HRZN_DB.TAG_SCHEMA.PII_TYPE = 'Phone Number';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY LAST_NAME SET TAG HRZN_DB.TAG_SCHEMA.PII_TYPE = 'Last Name';
```

Query account usage view to check tags and reference  
Note: The following VIEWs have a latency of about 20 min after creating TAG objects before they will be able to display data.

```sql
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE TAG_NAME = 'CONFIDENTIAL';
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE TAG_NAME = 'PII_TYPE';
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES WHERE TAG_NAME = 'COST_CENTER';
```

Now we can use the TAG\_REFERENCE\_ALL\_COLUMNS function to return the Tags associated with our customer order table.

```sql
SELECT
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('HRZN_DB.HRZN_SCH.CUSTOMER','table'));
```

#### **Dynamic Data Masking**

In Snowflake it is possible to use Column-Level Security to mask dynamically and create a conditional policy. Lets see how we can combine these to create a conditional masking policy.

```sql
-- Define policy names (fully qualified)
SET POLICY_MASK_PII = $FQ_TAG || '.MASK_PII';
SET POLICY_MASK_SENSITIVE = $FQ_TAG || '.MASK_SENSITIVE';

-- Create masking policy for PII (using suffixed role name in condition)
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_PII AS
  (VAL CHAR) RETURNS CHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', $ROLE_GOVERNOR) THEN VAL
      ELSE '***PII MASKED***'
    END;

-- Create masking policy for Sensitive fields
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_SENSITIVE AS
  (VAL CHAR) RETURNS CHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', $ROLE_GOVERNOR) THEN VAL
      ELSE '***SENSITIVE***'
    END;

-- Apply policies to specific columns
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN SSN SET MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_PII;
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN CREDITCARD SET MASKING POLICY HRZN_DB.TAG_SCHEMA.MASK_SENSITIVE;

-- Query the table
SELECT SSN, CREDITCARD FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

Now we can switch back to our Data User role from the beginning of the script. LEts see if the Data User still has access to sensitive data.

```sql
USE ROLE HRZN_DATA_USER;
USE WAREHOUSE HRZN_WH;
SELECT SSN, CREDITCARD FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

The data is masked for the Data User.

```sql
USE ROLE HRZN_DATA_GOVERNOR;
USE SCHEMA HRZN_DB.TAG_SCHEMA;
USE WAREHOUSE HRZN_WH;
```

The Data Governor can create opt-in masking based on condition

```sql
-- Define conditional policy variable
SET POLICY_CONDITIONAL = $FQ_TAG || '.CONDITIONALPOLICYDEMO';

-- Opt In masking based on condition
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.CONDITIONALPOLICYDEMO 
   AS (phone_nbr STRING, optin STRING) RETURNS STRING ->
   CASE
      WHEN optin = 'Y' THEN phone_nbr
      ELSE '***OPT OUT***'
   END;

ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN PHONE_NUMBER SET
   MASKING POLICY HRZN_DB.TAG_SCHEMA.CONDITIONALPOLICYDEMO USING (PHONE_NUMBER, OPTIN);

SELECT PHONE_NUMBER, OPTIN FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

Snowflake makes it possible to streamline the masking process by grouping all these sensitive or PII columns under a common tag and apply masking for that tag.

```sql
-- Define PII column tag and masking policy variables
SET TAG_PII_COL = $FQ_TAG || '.PII_COL';
SET POLICY_PII_DATA_MASK = $FQ_TAG || '.PII_DATA_MASK';

-- Create a Tag
CREATE OR REPLACE TAG HRZN_DB.TAG_SCHEMA.PII_COL ALLOWED_VALUES 'PII-DATA','NON-PII';

-- Apply to the table
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN LAST_NAME SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN BIRTHDATE SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN STREET_ADDRESS SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN CITY SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN STATE SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN ZIP SET TAG HRZN_DB.TAG_SCHEMA.PII_COL = 'PII-DATA';

-- Create Masking Policy (using suffixed role name in condition)
CREATE OR REPLACE MASKING POLICY HRZN_DB.TAG_SCHEMA.PII_DATA_MASK AS (VAL STRING) RETURNS STRING ->
CASE
WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN($TAG_PII_COL) = 'PII-DATA' 
    AND CURRENT_ROLE() NOT IN ($ROLE_GOVERNOR, 'ACCOUNTADMIN') 
    THEN '**PII TAG MASKED**'
ELSE VAL
END;

-- Apply Masking policy to the tag
ALTER TAG HRZN_DB.TAG_SCHEMA.PII_COL SET MASKING POLICY HRZN_DB.TAG_SCHEMA.PII_DATA_MASK;
```

Lets switch back to the Data User role and Check if the sensitive data is visible or masked

```sql
USE ROLE HRZN_DATA_USER;
SELECT FIRST_NAME, LAST_NAME, STREET_ADDRESS, CITY, STATE, ZIP 
FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

When we switch back to the Data Governor role we can see that the data is still present, just masked when required.

```sql
USE ROLE HRZN_DATA_GOVERNOR;
SELECT FIRST_NAME, LAST_NAME, STREET_ADDRESS, CITY, STATE, ZIP 
FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

#### **Row-Access Policies**

Now that our Data Governor is happy with our Tag Based Dynamic Masking controlling masking at the column level, we will now look to restrict access at the row level for our Data Analyst role.  
Within our Customer table, our role should only see Customers who are based in Massachussets(MA).  
First, We need to unset any exising masking policies on the column

```sql
-- We need to unset any existing masking policies on the column
USE ROLE HRZN_DATA_GOVERNOR;
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER MODIFY COLUMN STATE UNSET TAG HRZN_DB.TAG_SCHEMA.PII_COL;
```

Lets see what the data user can see.

```sql
USE ROLE HRZN_DATA_USER;
SELECT FIRST_NAME, STREET_ADDRESS, STATE, OPTIN, PHONE_NUMBER, EMAIL, JOB, COMPANY FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

We will need to use row level security to show only the Data for Massachusetts.

```sql
USE ROLE HRZN_DATA_GOVERNOR;
-- Let's use a mapping table ROW_POLICY_MAP to store the mapping 
-- between the users and the data that they have access to
-- For our lab, the mapping for the user is in the table ROW_POLICY_MAP
SELECT * FROM HRZN_DB.TAG_SCHEMA.ROW_POLICY_MAP; 
```

Note: Snowflake supports row-level security through the use of Row Access Policies to determine which rows to return in the query result. The row access policy can be relatively simple to allow one particular role to view rows, or be more complex to include a mapping table in the policy definition to determine access to rows in the query result.

```sql
-- Define row access policy variable
SET POLICY_CUSTOMER_STATE = $FQ_TAG || '.CUSTOMER_STATE_RESTRICTIONS';

-- Create Row Access Policy (using suffixed role names in condition)
CREATE OR REPLACE ROW ACCESS POLICY HRZN_DB.TAG_SCHEMA.CUSTOMER_STATE_RESTRICTIONS
    AS (STATE STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN ('ACCOUNTADMIN', $ROLE_ENGINEER, $ROLE_GOVERNOR) -- list of roles that will not be subject to the policy
        OR EXISTS -- this clause references our mapping table from above to handle the row level filtering
            (
            SELECT rp.ROLE
                FROM HRZN_DB.TAG_SCHEMA.ROW_POLICY_MAP rp
            WHERE 1=1
                AND rp.ROLE = CURRENT_ROLE()
                AND rp.STATE_VISIBILITY = STATE
            )
COMMENT = 'Policy to limit rows returned based on mapping table of ROLE and STATE';

-- Let's now apply the Row Access Policy to our State column in the Customer table
ALTER TABLE HRZN_DB.HRZN_SCH.CUSTOMER
    ADD ROW ACCESS POLICY HRZN_DB.TAG_SCHEMA.CUSTOMER_STATE_RESTRICTIONS ON (STATE);
```

With the policy successfully applied, let's test it using the Data User Role

```sql
USE ROLE HRZN_DATA_USER;
SELECT FIRST_NAME, STREET_ADDRESS, STATE, OPTIN, PHONE_NUMBER, EMAIL, JOB, COMPANY FROM HRZN_DB.HRZN_SCH.CUSTOMER;
```

Now that we've protected our data and our users can access it appropriately to address their questions, let's move on to explore the Governor Admin role.

## **Governor Admin \- Access & Audit**

**Overview**  
Access History provides insights into user queries encompassing what data was read and when, as well as what statements have performed a write operations. Access History is particularly important for Compliance, Auditing, and Governance.  
Within this step, we will walk through leveraging Access History to find when the last time our Raw data was read from and written to. In Snowsight create a new worksheet and rename it 3\_Governor\_Admin. Copy and paste each code block below and execute.  
Note: Access History latency is up to 3 hours. So, some of the queries below may not have results right away.

```sql
USE ROLE HRZN_IT_ADMIN;
USE DATABASE HRZN_DB;
USE SCHEMA HRZN_DB.HRZN_SCH;
USE WAREHOUSE HRZN_WH;
```

Let's check out how our data is being accessed

* How many queries have accessed each of our Raw layer tables directly?

```sql
SELECT 
    value:"objectName"::STRING AS object_name,
    COUNT(DISTINCT query_id) AS number_of_queries
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => direct_objects_accessed)
WHERE object_name ILIKE 'HRZN_DB' || '%'
GROUP BY object_name
ORDER BY number_of_queries DESC;
```

* What is the breakdown between Read and Write queries and when did they last occur?

```sql
SELECT 
    value:"objectName"::STRING AS object_name,
    CASE 
        WHEN object_modified_by_ddl IS NOT NULL THEN 'write'
        ELSE 'read'
    END AS query_type,
    COUNT(DISTINCT query_id) AS number_of_queries,
    MAX(query_start_time) AS last_query_start_time
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => direct_objects_accessed)
WHERE object_name ILIKE 'HRZN_DB' || '%'
GROUP BY object_name, query_type
ORDER BY object_name, number_of_queries DESC;

-- last few "read" queries
SELECT
    qh.user_name,    
    qh.query_text,
    value:objectName::string as "TABLE"
FROM snowflake.account_usage.query_history AS qh
JOIN snowflake.account_usage.access_history AS ah
ON qh.query_id = ah.query_id,
    LATERAL FLATTEN(input => ah.base_objects_accessed)
WHERE query_type = 'SELECT' AND
    value:objectName = 'HRZN_DB.HRZN_SCH.CUSTOMER' AND
    start_time > dateadd(day, -90, current_date());

-- last few "write" queries
SELECT
    qh.user_name,    
    qh.query_text,
    value:objectName::string as "TABLE"
FROM snowflake.account_usage.query_history AS qh
JOIN snowflake.account_usage.access_history AS ah
ON qh.query_id = ah.query_id,
    LATERAL FLATTEN(input => ah.base_objects_accessed)
WHERE query_type != 'SELECT' AND
    value:objectName = 'HRZN_DB.HRZN_SCH.CUSTOMER' AND
    start_time > dateadd(day, -90, current_date());
```

* Find longest running queries

```sql
SELECT
query_text,
user_name,
role_name,
database_name,
warehouse_name,
warehouse_size,
execution_status,
round(total_elapsed_time/1000,3) elapsed_sec
FROM snowflake.account_usage.query_history
ORDER BY total_elapsed_time desc
LIMIT 10;
```

* Find queries that have been executed against sensitive tables

```sql
SELECT
  q.USER_NAME,
  q.QUERY_TEXT,
  q.START_TIME,
  q.END_TIME
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q 
WHERE
  q.QUERY_TEXT ILIKE '%' || 'HRZN_DB.HRZN_SCH.CUSTOMER' || '%'
ORDER BY
  q.START_TIME DESC;
```

* Show the flow of sensitive data

```sql
SELECT
    *
FROM
(
    select
      directSources.value: "objectId"::varchar as source_object_id,
      directSources.value: "objectName"::varchar as source_object_name,
      directSources.value: "columnName"::varchar as source_column_name,
      'DIRECT' as source_column_type,
      om.value: "objectName"::varchar as target_object_name,
      columns_modified.value: "columnName"::varchar as target_column_name
    from
      (
        select
          *
        from
          snowflake.account_usage.access_history
      ) t,
      lateral flatten(input => t.OBJECTS_MODIFIED) om,
      lateral flatten(input => om.value: "columns", outer => true) columns_modified,
      lateral flatten(
        input => columns_modified.value: "directSources",
        outer => true
      ) directSources
    union
-- union part 2
    select
      baseSources.value: "objectId" as source_object_id,
      baseSources.value: "objectName"::varchar as source_object_name,
      baseSources.value: "columnName"::varchar as source_column_name,
      'BASE' as source_column_type,
      om.value: "objectName"::varchar as target_object_name,
      columns_modified.value: "columnName"::varchar as target_column_name
    from
      (
        select
          *
        from
          snowflake.account_usage.access_history
      ) t,
      lateral flatten(input => t.OBJECTS_MODIFIED) om,
      lateral flatten(input => om.value: "columns", outer => true) columns_modified,
      lateral flatten(
        input => columns_modified.value: "baseSources",
        outer => true
      ) baseSources
) col_lin
   WHERE
       (SOURCE_OBJECT_NAME = 'HRZN_DB.HRZN_SCH.CUSTOMER' OR TARGET_OBJECT_NAME = 'HRZN_DB.HRZN_SCH.CUSTOMER')
    AND
        (SOURCE_COLUMN_NAME IN (
                SELECT
                    COLUMN_NAME
                FROM
                (
                    SELECT
                        *
                    FROM TABLE(
                      identifier('HRZN_DB' || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS')(
                        'HRZN_DB.HRZN_SCH.CUSTOMER',
                        'table'
                      )
                    )
                )
                WHERE TAG_NAME IN ('CONFIDENTIAL','PII_COL','PII_TYPE') 
            )
            OR
            TARGET_COLUMN_NAME IN (
                SELECT
                    COLUMN_NAME
                FROM
                (
                    SELECT
                        *
                    FROM TABLE(
                      identifier('HRZN_DB' || '.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS')(
                        'HRZN_DB.HRZN_SCH.CUSTOMER',
                        'table'
                      )
                    )
                )
                WHERE TAG_NAME IN ('CONFIDENTIAL','PII_COL','PII_TYPE') -- Enter the relevant tag(s) to check against.
            )
            );
```

* How many queries have accessed each of our tables indirectly?

```sql
SELECT 
    base.value:"objectName"::STRING AS object_name,
    COUNT(DISTINCT query_id) AS number_of_queries
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN (input => base_objects_accessed) base,
LATERAL FLATTEN (input => direct_objects_accessed) direct
WHERE 1=1
    AND object_name ILIKE 'HRZN_DB' || '%'
    AND object_name <> direct.value:"objectName"::STRING -- base object is not direct object
GROUP BY object_name
ORDER BY number_of_queries DESC;
```

Direct Objects Accessed: Data objects directly named in the query explicitly. Base Objects Accessed: Base data objects required to execute a query.  
Clean up (Optional). Create a new worksheet named 99\_lab\_teardown. Copy and paste the entire Teardown Script from **99\_lab\_teardown.sql**

## **Conclusion And Resources**

You did it\! In this comprehensive lab, you have seen how Horizon:

* Secures data with role-based access control, governance policies, and more  
* Monitors data quality with both out-of-the-box and custom metrics  
* Audits data usage through Access History and Schema Change Tracking  
* Understands the flow of data through object dependencies and lineage

### **What You Learned**

* How to create stages, databases, tables, views, and virtual warehouses.  
* As a Data Engineer, how to implement Data Quality Monitoring and data metric functions  
* As a Data Governor, how to apply column-level and row-level security and how to use projection and aggregation constraints  
* As a Governor Admin, how to use data lineage and dependencies to audit access and understand the flow of data