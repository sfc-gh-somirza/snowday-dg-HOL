
USE ROLE ACCOUNTADMIN;
CREATE ROLE IF NOT EXISTS dq_tutorial_role;

CREATE DATABASE IF NOT EXISTS dq_tutorial_db;
CREATE SCHEMA IF NOT EXISTS sch;

grant usage on database dq_tutorial_db to role dq_tutorial_role;
grant usage on schema dq_tutorial_db.sch to role dq_tutorial_role;
use dq_tutorial_db.sch;

CREATE OR REPLACE TABLE CUSTOMER (
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


-- Customer Orders in Bronze with day/month/year columns + PAYMENT_TYPE
CREATE OR REPLACE TABLE CUSTOMER_ORDERS (
    CUSTOMER_ID VARCHAR,            
    ORDER_ID VARCHAR,               
    ORDER_DAY INT,                 
    ORDER_MONTH INT,               
    ORDER_YEAR INT,                
    ORDER_AMOUNT FLOAT,             
    ORDER_TAX FLOAT,                
    ORDER_TOTAL FLOAT,              
    PAYMENT_TYPE VARCHAR             -- e.g., 'CREDIT_CARD', 'PAYPAL', 'BANK_TRANSFER'
);



INSERT INTO CUSTOMER VALUES
    (1, 'John', 'Doe', '123 Elm St', 'CA', 'San Francisco', '94105', '123-456-7890', 'john.doe@email.com', '111-22-3333', '1985-04-12', 'Engineer', '4111111111111111', 'Acme Inc.', 'Y'),
    (2, 'Jane', 'Smith', NULL, 'CA', 'Los Angeles', '90001', NULL, 'jane.smith@email.com', NULL, '1990/07/25', 'Manager', '5500000000000004', 'Globex', 'N'),
    (3, 'Mike', 'Brown', '456 Oak St', 'NV', 'Las Vegas', '89101', '9999999999', 'mike.brown.com', '123-45-6789', 'bad-date', 'Analyst', '4000000000000002', NULL, 'Y'),
    (4, 'Anna', 'Lee', '789 Pine St', 'WA', 'Seattle', '98101', '206-555-1234', 'anna.lee@email.com', '222-33-4444', '1988-11-05', 'Designer', '340000000000009', 'Innotech', 'Y');

INSERT INTO CUSTOMER_ORDERS VALUES
    ('1', 'O1', 1, 9, 2025, 100, 8.25, 108.25, 'CREDIT_CARD'),
    ('2', 'O2', 2, 9, 2025, 200, NULL, 200, 'PAYPAL'),
    ('2', 'O3', 3, 9, 2025, 300, 60, 360, 'BANK_TRANSFER'),
    ('3', 'O4', NULL, 9, 2025, -50, 5, -45, 'CREDIT_CARD'),
    ('4', 'O5', 4, 9, 2025, 150, 12, 162, 'PAYPAL'),
    ('1', 'O6', 5, 9, 2025, 250, 20, 270, 'CREDIT_CARD');
    ('5', NULL, 5, 9, 2025, 250, 20, 270, 'VENMO');


CREATE OR REPLACE TABLE SILVER_CUSTOMER_ORDERS AS
SELECT
    CUSTOMER_ID,
    ORDER_ID,
    TRY_TO_DATE(ORDER_YEAR || '-' || ORDER_MONTH || '-' || ORDER_DAY, 'YYYY-MM-DD') AS ORDER_TS,
    ORDER_AMOUNT,
    ORDER_TAX,  
    ORDER_TOTAL,  
    PAYMENT_TYPE
FROM CUSTOMER_ORDERS;


CREATE OR REPLACE TABLE GOLD_SALES_SUMMARY AS
SELECT
    c.STATE,
    o.PAYMENT_TYPE,
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
    SUM(o.ORDER_AMOUNT) AS TOTAL_AMOUNT,
    SUM(o.ORDER_TAX) AS TOTAL_TAX,
    SUM(o.ORDER_TOTAL) AS TOTAL_REVENUE
FROM SILVER_CUSTOMER_ORDERS o
JOIN CUSTOMER c
    ON c.ID = TO_NUMBER(o.CUSTOMER_ID)
WHERE o.ORDER_AMOUNT IS NOT NULL
GROUP BY c.STATE, o.PAYMENT_TYPE;


grant execute data metric function on account to role dq_tutorial_role;
grant database role snowflake.data_metric_user to role dq_tutorial_role;
grant application role snowflake.data_quality_monitoring_viewer to role dq_tutorial_role;



ALTER TABLE CUSTOMER_ORDERS SET DATA_METRIC_SCHEDULE = '5 minutes';

ALTER TABLE CUSTOMER_ORDERS ADD DATA METRIC FUNCTION 
SNOWFLAKE.CORE.ROW_COUNT ON () Expectation Volume_Check (value > 1),   -- Row count (Volume)
SNOWFLAKE.CORE.FRESHNESS ON () Expectation Freshness_Check (value < 1800),  -- Freshness
SNOWFLAKE.CORE.NULL_COUNT ON (ORDER_ID) Expectation Null_Check (value = 0);   -- Null count

CREATE OR REPLACE DATA METRIC FUNCTION referential_check(
  arg_t1 TABLE (arg_c1 VARCHAR), arg_t2 TABLE (arg_c2 VARCHAR))
RETURNS NUMBER AS
 'SELECT COUNT(*) FROM arg_t1
  WHERE arg_c1 NOT IN (SELECT arg_c2 FROM arg_t2)';

CREATE OR REPLACE DATA METRIC FUNCTION volume_check(
  arg_t1 TABLE (arg_c1 VARCHAR), arg_t2 TABLE (arg_c2 VARCHAR))
RETURNS NUMBER AS
  'SELECT ABS(
      (SELECT COUNT(*) FROM arg_t1) - (SELECT COUNT(*) FROM arg_t2)
    )';

ALTER TABLE SILVER_CUSTOMER_ORDERS SET DATA_METRIC_SCHEDULE = '5 minutes';

ALTER TABLE SILVER_CUSTOMER_ORDERS ADD DATA METRIC FUNCTION referential_check ON (CUSTOMER_ID, TABLE (dq_tutorial_db.sch.CUSTOMER(ID))) Expectation FK_Check (value=0);

ALTER TABLE SILVER_CUSTOMER_ORDERS ADD DATA METRIC FUNCTION volume_check
    ON (CUSTOMER_ID, TABLE (dq_tutorial_db.sch.CUSTOMER_ORDERS(CUSTOMER_ID))) Expectation NoDiff (value=0);


ALTER TABLE GOLD_SALES_SUMMARY SET DATA_METRIC_SCHEDULE = '5 minutes';

ALTER TABLE GOLD_SALES_SUMMARY
  ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ACCEPTED_VALUES
    ON (
      payment_type,
      payment_type -> payment_type IN ('CREDIT_CARD', 'PAYPAL', 'BANK_TRANSFER')
    ) Expectation Payment_Type_Check (value=0);
