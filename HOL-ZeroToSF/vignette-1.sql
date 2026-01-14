/***************************************************************************************************       
Asset:        Zero to Snowflake - Getting Started with Snowflake
Version:      v2     
Copyright(c): 2025 Snowflake Inc. All rights reserved.
****************************************************************************************************

Getting Started with Snowflake
1. Virtual Warehouses & Settings
2. Using Persisted Query Results
3. Basic Data Transformation Techniques
4. Data Recovery with UNDROP
5. Resource Monitors
6. Budgets
7. Universal Search

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

-- Schemas (fully qualified)
SET SCH_RAW_POS = $DB_NAME || '.RAW_POS';
SET SCH_ANALYTICS = $DB_NAME || '.ANALYTICS';

-- Tables (fully qualified)
SET TBL_TRUCK_DETAILS = $SCH_RAW_POS || '.TRUCK_DETAILS';
SET TBL_TRUCK_DEV = $SCH_RAW_POS || '.TRUCK_DEV';
SET TBL_TRUCK = $SCH_RAW_POS || '.TRUCK';

-- Views (fully qualified)
SET VIEW_ORDERS_ANALYTICS = $SCH_ANALYTICS || '.ORDERS_V';

-- User-specific objects created in this vignette
SET MY_WH = 'MY_WH_' || $USER_SUFFIX;
SET MY_RESOURCE_MONITOR = 'MY_RESOURCE_MONITOR_' || $USER_SUFFIX;
SET MY_BUDGET = 'MY_BUDGET_' || $USER_SUFFIX;

-- Display variables for verification
SELECT 
    $USER_SUFFIX AS USER_SUFFIX,
    $DB_NAME AS DATABASE_NAME,
    $MY_WH AS MY_WAREHOUSE,
    $MY_RESOURCE_MONITOR AS MY_RESOURCE_MONITOR,
    $MY_BUDGET AS MY_BUDGET;


-- Before we start, run this query to set the session query tag.
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"tb_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"tastybytes", "vignette": "getting_started_with_snowflake"}}';

-- We'll begin by setting our Worksheet context. We will set our database, schema and role.

USE DATABASE identifier($DB_NAME);
USE ROLE accountadmin;

/*   1. Virtual Warehouses & Settings 
    **************************************************************
     User-Guide:
     https://docs.snowflake.com/en/user-guide/warehouses-overview
    **************************************************************
    
    Virtual Warehouses are the dynamic, scalable, and cost-effective computing power that lets you 
    perform analysis on your Snowflake data. Their purpose is to handle all your data processing needs 
    without you having to worry about the underlying technical details.

    Warehouse parameters:
      > WAREHOUSE_SIZE: 
            Size specifies the amount of compute resources available per cluster 
            in a warehouse. The available sizes range from X-Small to 6X-Large.
            Default: 'XSmall'
      > WAREHOUSE_TYPE:
            Defines the type of virtual warehouse, which dictates its architecture and behavior
            Types:
                'STANDARD' for general purpose workloads
                'SNOWPARK_OPTIMIZED' for memory-intensive workloads
            Default: 'STANDARD'
      > AUTO_SUSPEND:
            Specifies the period of inactivity after which the warehouse will automatically suspend itself.
            Default: 600s
      > INITIALLY_SUSPENDED:
            Determines whether the warehouse starts in a suspended state immediately after it is created.
            Default: TRUE
      > AUTO_RESUME:
            Determines whether the warehouse automatically resumes from a suspended state when a query is directed to it.
            Default: TRUE

        With that, let's create our first warehouse!
*/

-- Let's first look at the warehouses that already exist on our account that you have access privileges for
SHOW WAREHOUSES;

/*
    This returns the list of warehouses and their attributes: name, state (running or suspended), type, and size 
    among many others. 
    
    We can also view and manage all warehouses in Snowsight. To access the warehouse page, click the Admin button
    on the Navigation Menu, then click the 'Warehouses' link in the now expanded Admin category.
    
    Back on the warehouse page, we see a list of the warehouses on this account and their attributes.
*/

-- You can easily create a warehouse with a simple SQL command
CREATE OR REPLACE WAREHOUSE identifier($MY_WH)
    COMMENT = 'My TastyBytes warehouse'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    INITIALLY_SUSPENDED = true
    AUTO_RESUME = false;

/*
    Now that we have a warehouse, we should specify that this Worksheet uses this warehouse. We can
    do this either with a SQL command, or in the UI.
*/

-- Use the warehouse
USE WAREHOUSE identifier($MY_WH);

/*
    We can try running a simple query, however, you will see an error message in the results pane, informing
    us that our warehouse is suspended. Try it now.
*/
SELECT * FROM identifier($TBL_TRUCK_DETAILS);

/*    
    An active warehouse is required for running queries, as well as all DML operations, so we'll 
    need to resume our warehouse if we want to get insights from our data.
    
    The error message also came with a suggestion to run the SQL command:
    'ALTER warehouse resume'. Let's do just that!
*/
ALTER WAREHOUSE identifier($MY_WH) RESUME;

/* 
    We'll also set AUTO_RESUME to TRUE so we can avoid having to manually 
    resume the warehouse should it suspend again.
 */
ALTER WAREHOUSE identifier($MY_WH) SET AUTO_RESUME = TRUE;

--The warehouse is now running, so lets try to run the query from before 
SELECT * FROM identifier($TBL_TRUCK_DETAILS);

-- Now we are able to start running queries on our data

/* 
    Next, let's take a look at the power of warehouse scalability in Snowflake.
    
    Warehouses in Snowflake are designed for scalability and elasticity, giving you the power
    to adjust compute resources up or down based on workload needs.
    
    We can easily scale up our warehouses on the fly with a simple ALTER WAREHOUSE statement.
*/
ALTER WAREHOUSE identifier($MY_WH) SET warehouse_size = 'XLarge';

--Let's now take a look at the sales per truck.
SET SELECT_SQL = 'SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM ' || $VIEW_ORDERS_ANALYTICS || ' o
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC';
EXECUTE IMMEDIATE $SELECT_SQL;

/*
    With the Results panel open, take a quick look at the toolbar in the top right. Here we see options to search, 
    select columns, view query details and duration stats, view column stats, and download results. 
    
    Search - Use search terms to filter the results
    Column selection - Enable/disable columns to display in the results
      Query details - Contains information related to the query like the SQL text, rows returned, query ID, the 
      role and warehouse it was executed with.
    Query duration - Breaks down how long it took for the query to run by compilation, provisioning and execution times.
    Column stats - Displays data relating to the distributions of the columns on the results panel.
    Download results - Export and download the results as csv.
*/

/*  2. Using Persisted Query Results
    *******************************************************************
    User-Guide:
    https://docs.snowflake.com/en/user-guide/querying-persisted-results
    *******************************************************************
    
    Before we proceed, this is a great place to demonstrate another powerful feature in Snowflake: 
    the Query Result Cache.
    
    When we first ran the above query it took several seconds to complete, even with an XL warehouse.

    Run the same 'sales per truck' query above and note the total run time in the Query Duration pane.
    You'll notice that it took several seconds the first time you ran it to only a few hundred milliseconds 
    the next time. This is the query result cache in action.

    Open the Query History panel and compare the run times between the first time the query was run for the second time.
    
    Query Result Cache overview:
    - Results are retained for any query for 24 hours, however the timer is reset any time the query is executed.
    - Hitting the result cache requires almost no compute resources, ideal for frequently run reports or dashboards
      and managing credit consumption.
    - The cache resides in the Cloud Services Layer, meaning it is logically separated from individual warehouses. 
      This makes it globally accessible to all virtual warehouses & users within the same account.
*/

-- We'll now start working with a smaller dataset, so we can scale the warehouse back down
ALTER WAREHOUSE identifier($MY_WH) SET warehouse_size = 'XSmall';

/*  3. Basic Transformation Techniques

    Now that our warehouse is configured and running, the plan is to get an understanding of the distribution 
    of our trucks' manufacturers, however, this information is embedded in another column 'truck_build' that stores
    information about the year, make and model in a VARIANT data type. 

    VARIANT data types are examples of semi-structured data. They can store any type of data including OBJECT, 
    ARRAY and other VARIANT values. In our case, the truck_build stores a single OBJECT which contains three distinct 
    VARCHAR values for year, make and model.
    
    We'll now isolate all three properties into their own respective columns to allow for simpler and easier analytics. 
*/
SELECT truck_build FROM identifier($TBL_TRUCK_DETAILS);

/*  Zero Copy Cloning

    The truck_build column data consistently follows the same format. We'll need a separate column for 'make'
    to more easily perform quality analysis on it. The plan is to create a development copy of the truck table, add new columns 
    for year, make, and model, then extract and store each property from the truck build VARIANT object into these new columns.
 
    Snowflake's powerful Zero Copy Cloning lets us create identical, fully functional and separate copies of database 
    objects instantly, without using any additional storage space. 

    Zero Copy Cloning leverages Snowflake's unique micropartition architecture to share data between the cloned object and original copy.
    Any changes to either table will result in new micropartitions created for only the modified data. These new micro-partitions are
    now owned exclusively by the owner, whether its the clone or original cloned object. Basically, any changes made to one table,
    will not be to either the original or cloned copy.
*/

-- Create the truck_dev table as a Zero Copy clone of the truck table
SET CLONE_SQL = 'CREATE OR REPLACE TABLE ' || $TBL_TRUCK_DEV || ' CLONE ' || $TBL_TRUCK_DETAILS;
EXECUTE IMMEDIATE $CLONE_SQL;

-- Verify successful truck table clone into truck_dev 
SET SELECT_SQL = 'SELECT TOP 15 * FROM ' || $TBL_TRUCK_DEV || ' ORDER BY truck_id';
EXECUTE IMMEDIATE $SELECT_SQL;

/*
    Now that we have a development copy of the truck table, we can start by adding the new columns.
    Note: To run all three statements at once, select them and click the blue 'Run' button at the top right of the screen, or use your keyboard.
    
        Mac: command + return
        Windows: Ctrl + Enter
*/

ALTER TABLE identifier($TBL_TRUCK_DEV) ADD COLUMN IF NOT EXISTS year NUMBER;
ALTER TABLE identifier($TBL_TRUCK_DEV) ADD COLUMN IF NOT EXISTS make VARCHAR(255);
ALTER TABLE identifier($TBL_TRUCK_DEV) ADD COLUMN IF NOT EXISTS model VARCHAR(255);

/*
    Now let's update the new columns with the data extracted from the truck_build column.
    We will use the colon (:) operator to access the value of each key in the truck_build 
    column, then set that value to its respective column.
*/
SET UPDATE_SQL = 'UPDATE ' || $TBL_TRUCK_DEV || ' SET 
    year = truck_build:year::NUMBER,
    make = truck_build:make::VARCHAR,
    model = truck_build:model::VARCHAR';
EXECUTE IMMEDIATE $UPDATE_SQL;

-- Verify the 3 columns were successfully added to the table and populated with the extracted data from truck_build
SET SELECT_SQL = 'SELECT year, make, model FROM ' || $TBL_TRUCK_DEV;
EXECUTE IMMEDIATE $SELECT_SQL;

-- Now we can count the different makes and get a sense of the distribution in our TastyBytes food truck fleet.
SET SELECT_SQL = 'SELECT 
    make,
    COUNT(*) AS count
FROM ' || $TBL_TRUCK_DEV || '
GROUP BY make
ORDER BY make ASC';
EXECUTE IMMEDIATE $SELECT_SQL;

/*
    After running the query above, we notice a problem in our dataset. Some trucks' makes are 'Ford' and some are 'Ford_',
    giving us two different counts for the same truck manufacturer.
*/

-- First we'll use UPDATE to change any occurrence of 'Ford_' to 'Ford'
SET UPDATE_SQL = 'UPDATE ' || $TBL_TRUCK_DEV || ' SET make = ''Ford'' WHERE make = ''Ford_''';
EXECUTE IMMEDIATE $UPDATE_SQL;

-- Verify the make column has been successfully updated 
SET SELECT_SQL = 'SELECT truck_id, make FROM ' || $TBL_TRUCK_DEV || ' ORDER BY truck_id';
EXECUTE IMMEDIATE $SELECT_SQL;

/*
    The make column looks good now so let's SWAP the truck table with the truck_dev table
    This command atomically swaps the metadata and data between two tables, instantly promoting the truck_dev table 
    to become the new production truck table.
*/
SET SWAP_SQL = 'ALTER TABLE ' || $TBL_TRUCK_DETAILS || ' SWAP WITH ' || $TBL_TRUCK_DEV;
EXECUTE IMMEDIATE $SWAP_SQL;

-- Run the query from before to get an accurate make count
SET SELECT_SQL = 'SELECT 
    make,
    COUNT(*) AS count
FROM ' || $TBL_TRUCK_DETAILS || '
GROUP BY make
ORDER BY count DESC';
EXECUTE IMMEDIATE $SELECT_SQL;

/*
    The changes look great. We'll perform some cleanup on our dataset by first dropping the truck_build 
    column from the production database now that we have split that data into three separate columns.
    Then we can drop the truck_dev table since we no longer need it.
*/

-- We can drop the old truck build column with a simple ALTER TABLE ... DROP COLUMN command
ALTER TABLE identifier($TBL_TRUCK_DETAILS) DROP COLUMN truck_build;

-- Now we can drop the truck_dev table
DROP TABLE identifier($TBL_TRUCK_DETAILS);

/*  4. Data Recovery with UNDROP
	
    Oh no! We accidentally dropped the production truck table. 

    Luckily we can use the UNDROP command to restore the table back to its state before being dropped. 
    UNDROP is part of Snowflake's powerful Time Travel feature and allows for the restoration of dropped
    database objects within a configured data retention period (default 24 hours).

    Let's restore the production 'truck' table ASAP using UNDROP!
*/

-- Optional: run this query to verify the 'truck' table no longer exists
    -- Note: The error 'Table TRUCK does not exist or not authorized.' means the table was dropped.
DESCRIBE TABLE identifier($TBL_TRUCK_DETAILS);

--Run UNDROP on the production 'truck' table to restore it to the exact state it was in before being dropped
UNDROP TABLE identifier($TBL_TRUCK_DETAILS);

--Verify the table was successfully restored
SELECT * from identifier($TBL_TRUCK_DETAILS);

-- Now drop the real truck_dev table
DROP TABLE identifier($TBL_TRUCK_DEV);

/*  5. Resource Monitors
    ***********************************************************
    User-Guide:                                   
    https://docs.snowflake.com/en/user-guide/resource-monitors
    ***********************************************************

    Monitoring compute usage and spend is critical to any cloud-based workflow. Snowflake provides a simple 
    and straightforward way to track warehouse credit usage with Resource Monitors.

    With Resource Monitors you define credit quotas and then trigger certain actions on 
    associated warehouses upon reaching defined usage thresholds.

    Actions the resource monitor can take:
    -NOTIFY: Sends an email notification to specified users or roles.
    -SUSPEND: Suspends the associated warehouses when a threshold is reached.
              NOTE: Running queries are allowed to complete. 
    -SUSPEND_IMMEDIATE: Suspends the associated warehouses when a threshold is reached and
                        cancels all running queries.

    Now, we'll create a Resource Monitor for our warehouse

    Let's quickly set our account level role in Snowsight to accountadmin;
    To do so:
    - Click the User Icon in the bottom left of the screen
    - Hover over 'Switch Role'
    - Select 'ACCOUNTADMIN' in the role list panel

   Next we will use the accountadmin role in our Worksheet
*/
USE ROLE accountadmin;

-- Run the query below to create the resource monitor via SQL
CREATE OR REPLACE RESOURCE MONITOR identifier($MY_RESOURCE_MONITOR)
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY -- Can also be DAILY, WEEKLY, YEARLY, or NEVER (for a one-time quota)
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- With the Resource Monitor created, apply it to our warehouse
SET ALTER_SQL = 'ALTER WAREHOUSE ' || $MY_WH || ' SET RESOURCE_MONITOR = ' || $MY_RESOURCE_MONITOR;
EXECUTE IMMEDIATE $ALTER_SQL;

/*  6. Budgets
    ****************************************************
      User-Guide:                                   
      https://docs.snowflake.com/en/user-guide/budgets 
    ****************************************************
      
    In the previous step we configured a Resource Monitor that allows for monitoring
    credit usage for Warehouses. In this step we will create a Budget for a more holistic 
    and flexible approach to managing costs in Snowflake. 
    
    While Resource Monitors are tied specifically to warehouse and compute usage, Budgets can be used 
    to track costs and impose spending limits on any Snowflake object or service and notify users 
    when the dollar amount reaches a specified threshold.
*/

-- Let's first create our budget
SET BUDGET_SQL = 'CREATE OR REPLACE SNOWFLAKE.CORE.BUDGET ' || $MY_BUDGET || '() COMMENT = ''My Tasty Bytes Budget''';
EXECUTE IMMEDIATE $BUDGET_SQL;

/*
    Before we can configure our Budget we need to verify an email address on the account.

    To verify your email address:
    - Click the User Icon in the bottom left of the screen
    - Click Settings 
    - Enter your email address in the email field
    - Click 'Save'
    - Check your email and follow instructions to verify email
        NOTE: if you don't receive an email after a few minutes, click 'Resend Verification'
     
    With our new budget now in place, our email verified and our account-level role set to accountadmin, 
    lets head over to the Budgets page in Snowsight to add some resources to our Budget.

    To get to the Budgets page in Snowsight:
    - Click the Admin button on the Navigation Menu
    - Click the first item 'Cost Management'
    - Click the 'Budgets' tab
    
    If prompted to select a warehouse, select your user-specific warehouse, otherwise, ensure your warehouse is set 
    from the warehouse panel at the top right of the screen.
    
    On the budgets page we see metrics about our spend for the current period.
    In the middle of the screen shows a graph of the current spend with forecasted spend.
    At the bottom of the screen we see our budget we created earlier. Click
    that to view the Budget page
    
    Clicking the '<- Budget Details' at the top right of the screen reveals the
    Budget Details panel. Here we can view information about our budget and all 
    of the resources attached to it. We see there are no resources monitored so let's add some now.
    Click the 'Edit' button to open the Edit Budget panel;
    
    - Keep budget name the same
    - Set the spending limit to 100
    - Enter the email you verified earlier
    - Click the '+ Tags & Resources' button to add a couple of resources
    - Expand Databases, then your TB_101_<username> database, then check the box next to the ANALYTICS schema
    - Scroll down to and expand 'Warehouses'
    - Check the box for your user-specific warehouse
    - Click 'Done'
    - Back in the Edit Budget menu, click 'Save Changes'
*/

/*  7. Universal Search
    **************************************************************************
      User-Guide                                                             
      https://docs.snowflake.com/en/user-guide/ui-snowsight-universal-search  
    **************************************************************************

    Universal Search allows you to easily find any object in your account, plus explore data products in the Marketplace, 
    relevant Snowflake Documentation, and Community Knowledge Base articles.

    Let's try it now.
    - To use Universal Search, begin by clicking 'Search' in the Navigation Menu
    - Here we see the Universal Search UI. Let's put in our first search term.
    - Enter 'truck' into the search bar and observe the results. The top sections are categories 
      of relevant objects on your account, like databases, tables, views, stages, etc. Below your
      database objects you can see sections for relevant marketplace listings and documentation.

    - You may also provide search terms in natural language to describe what you're looking for. If we wanted to
    know where to start looking to answer which truck franchise has the most return customers, we could search
    something like 'Which truck franchise has the most loyal customer base?' Clicking 'View all >' button next to 
    the 'Tables & Views' section will allow us to view all of the relevant tables and views relevant to our query.

    Universal Search returns several tables and views from different schemas. Note also how the relevant columns
    are listed for each object. These are all excellent starting points for data-driven answers about return customers.
*/

-------------------------------------------------------------------------
--RESET--
-------------------------------------------------------------------------
-- Drop created objects
DROP RESOURCE MONITOR IF EXISTS identifier($MY_RESOURCE_MONITOR);
DROP TABLE IF EXISTS identifier($TBL_TRUCK_DEV);

-- Reset truck details
SET RESET_SQL = 'CREATE OR REPLACE TABLE ' || $TBL_TRUCK_DETAILS || ' AS SELECT * EXCLUDE (year, make, model) FROM ' || $TBL_TRUCK;
EXECUTE IMMEDIATE $RESET_SQL;

DROP WAREHOUSE IF EXISTS identifier($MY_WH);

-- Drop budget
SET DROP_SQL = 'DROP SNOWFLAKE.CORE.BUDGET IF EXISTS ' || $MY_BUDGET;
EXECUTE IMMEDIATE $DROP_SQL;

-- Unset Query Tag
ALTER SESSION UNSET query_tag;
