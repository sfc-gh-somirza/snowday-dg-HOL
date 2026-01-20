### **Overview**

Welcome to the Zero to Snowflake Quickstart\! This guide is a consolidated journey through key areas of the Snowflake AI Data Cloud. You will start with the fundamentals of warehousing and data transformation, build an automated data pipeline, then see how you can experiment with LLMs using the Cortex Playground to compare different models for summarizing text, use AISQL Functions to instantly analyze customer review sentiment with a simple SQL command, and harness Cortex Search for intelligent text discovery, and utilize Cortex Analyst for conversational business intelligence. Finally, you will learn to secure your data with powerful governance controls and enrich your analysis through seamless data collaboration.  
We'll apply these concepts using a sample dataset from our fictitious food truck, Tasty Bytes, to improve and streamline their data operations. We'll explore this dataset through several workload-specific scenarios, demonstrating the benefits Snowflake provides to businesses.

### **Who is Tasty Bytes?**

**![./assets/whoistb.png][image1]**

Our mission is to provide unique, high-quality food options in a convenient and cost-effective manner, emphasizing the use of fresh ingredients from local vendors. Their vision is to become the largest food truck network in the world with a zero carbon footprint.

### **What You Will Learn**

* **Vignette 1: Getting Started with Snowflake:** The fundamentals of Snowflake warehouses, caching, cloning, and Time Travel.  
* **Vignette 2: Simple Data Pipelines:** How to ingest and transform semi-structured data using Dynamic Tables.  
* **Vignette 3: Snowflake Cortex AI:** How to leverage Snowflake's comprehensive AI capabilities for experimentation, scalable analysis, AI-assisted development, and conversational business intelligence.

### **What You Will Build**

* A comprehensive understanding of the core Snowflake platform.  
* Configured Virtual Warehouses.  
* An automated ELT pipeline with Dynamic Tables.  
* A complete intelligence customer analytics platform leveraging Snowflake AI.  
* A robust data governance framework with roles and policies.  
* Enriched analytical views combining first- and third-party data.

## **Setup**

### **Overview**

In this guide, we will use [Snowflake Workspaces](https://app.snowflake.com/_deeplink/#/workspaces?utm_source=snowflake-devrel&utm_medium=developer-guides&utm_content=zero-to-snowflake&utm_cta=developer-guides-deeplink) to organize, edit, and run all the SQL scripts required for this course. We will create a dedicated SQL file for the setup and each vignette. This will keep our code organized and easy to manage.  
Let's walk through how to create your first SQL file, add the necessary setup code, and run it.

### **Important: User-Specific Object Naming**

All objects created in this lab are suffixed with your username (via `CURRENT_USER()`) to allow multiple users to run the lab concurrently without naming conflicts. 

For example, if your username is `JSMITH`, your database will be named `TB_101_JSMITH` and your warehouse will be `TB_DE_WH_JSMITH`.

Each SQL file begins with a section that sets up these user-specific variables:

```sql
-- Set the user suffix (will be appended to all object names)
SET USER_SUFFIX = CURRENT_USER();

-- Database
SET DB_NAME = 'TB_101_' || $USER_SUFFIX;

-- Warehouses
SET WH_DE = 'TB_DE_WH_' || $USER_SUFFIX;

-- And so on for all objects...
```

**Important:** Always run this variable setup section first when starting a new vignette. The rest of the SQL code uses these variables with the `identifier($VARIABLE)` syntax to reference your user-specific objects.

### **Step 1 \- Create Your Setup SQL File**

First, we need a place to put our setup script.

1. **Navigate to** [Workspaces](https://app.snowflake.com/_deeplink/#/workspaces?utm_source=snowflake-devrel&utm_medium=developer-guides&utm_content=zero-to-snowflake&utm_cta=developer-guides-deeplink)**:** In the left-hand navigation menu of the Snowflake UI, click on **Projects** » [Workspaces](https://app.snowflake.com/_deeplink/#/workspaces?utm_source=snowflake-devrel&utm_medium=developer-guides&utm_content=zero-to-snowflake-deeplink). This is the central hub for all your SQL files.  
2. **Create a New SQL File:** Find and click the **\+ Add New** button in the top-left corner of the [Workspaces](https://app.snowflake.com/_deeplink/#/workspaces?utm_source=snowflake-devrel&utm_medium=developer-guides&utm_content=zero-to-snowflake&utm_cta=developer-guides-deeplink) area, then select **SQL File**. This will generate a new, blank SQL file.  
3. **Rename the SQL File:** Your new SQL file will have a name based on the timestamp it was created. Give it a descriptive name like **Zero To Snowflake \- Setup**.

### **Step 2 \- Add and Run the Setup Script**

Now that you have your SQL file, it's time to add the setup SQL and execute it.

1. **Copy the SQL Code:** Click the link for the [setup file](https://github.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/blob/main/scripts/setup.sql) and copy it to your clipboard.  
2. **Paste into your SQL File:** Return to your Zero To Snowflake Setup SQL file in Snowflake and paste the entire script into the editor.  
3. **Run the Script:** To execute all the commands in the SQL file sequentially, click the **"Run All"** button located at the top-left of the editor. This will perform all the necessary setup actions, such as creating roles, schemas, and warehouses that you will need for the upcoming vignettes.

![./assets/create\_a\_worksheet.gif][image2]

### **Looking Ahead**

The process you just completed for creating a new SQL file is the exact same workflow you will use for every subsequent vignette in this course.  
For each new vignette, you will:

1. Create a **new** SQL file.  
2. Give it a descriptive name (e.g., Vignette 1 \- Getting Started with Snowflake).  
3. Copy and paste the SQL script for that specific vignette.  
4. Each SQL file has all of the necessary instructions and commands to follow along.

## **Get Started with Snowflake**

**![./assets/getting\_started\_header.png][image3]**

### **Overview**

Within this Vignette, we will learn about core Snowflake concepts by exploring Virtual Warehouses, using the query results cache, performing basic data transformations, leveraging data recovery with Time Travel, and using Universal Search to find objects.

### **What You Will Learn**

* How to create, configure, and scale a Virtual Warehouse.  
* How to leverage the Query Result Cache.  
* How to use Zero-Copy Cloning for development.  
* How to transform and clean data.  
* How to instantly recover a dropped table using UNDROP.  
* How to use Universal Search to find objects and information.

### **What You Will Build**

* A Snowflake Virtual Warehouse  
* A development copy of a table using Zero-Copy Clone

### **Get the SQL code and paste it into your SQL File.**

**Copy and paste the SQL code from this** [file](https://github.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/blob/main/scripts/vignette-1.sql) **in a new SQL File to follow along in Snowflake. Note that once you've reached the end of the SQL File you can skip to Step 10 \- Simple Data Pipeline**

### **Virtual Warehouses and Settings**

#### **Overview**

Virtual Warehouses are the dynamic, scalable, and cost-effective computing power that lets you perform analysis on your Snowflake data. Their purpose is to handle all your data processing needs without you having to worry about the underlying technical details.

#### **Step 1 \- Setting Context**

First, lets set our session context. To run the queries, highlight the three queries at the top of your SQL file and click the "► Run" button.

```sql
-- First, run the user suffix variable setup at the top of your SQL file
-- Then run these context commands:
USE DATABASE identifier($DB_NAME);
USE ROLE accountadmin;
```

#### **Step 2 \- Creating a Warehouse**

Let's create our first warehouse\! This command creates a new X-Small warehouse that will initially be suspended.

```sql
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
```

**Virtual Warehouses**: A virtual warehouse, often referred to simply as a “warehouse”, is a cluster of compute resources in Snowflake. Warehouses are required for queries, DML operations, and data loading. For more information, see the [Warehouse Overview](https://docs.snowflake.com/en/user-guide/warehouses-overview).

#### **Step 3 \- Using and Resuming a Warehouse**

Now that we have a warehouse, we must set it as the active warehouse for our session. Execute the next statement.

```sql
USE WAREHOUSE identifier($MY_WH);
```

If you try to run the query below, it will fail, because the warehouse is suspended and does not have AUTO\_RESUME enabled.

```sql
SELECT * FROM identifier($TBL_TRUCK_DETAILS);
```

Let's resume it and set it to auto-resume in the future.

```sql
ALTER WAREHOUSE identifier($MY_WH) RESUME;
ALTER WAREHOUSE identifier($MY_WH) SET AUTO_RESUME = TRUE;
```

Now, try the query again. It should execute successfully.

```sql
SELECT * FROM identifier($TBL_TRUCK_DETAILS);
```

#### **Step 4 \- Scaling a Warehouse**

Warehouses in Snowflake are designed for elasticity. We can scale our warehouse up on the fly to handle a more intensive workload. Let's scale our warehouse to a Medium.

```sql
ALTER WAREHOUSE identifier($MY_WH) SET warehouse_size = 'medium';
```

With our medium warehouse, let's run a query to calculate total sales per truck brand. Note: This query uses dynamic SQL because it references a view.

```sql
SET SELECT_SQL = 'SELECT
    o.truck_brand_name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.price) AS total_sales
FROM ' || $VIEW_ORDERS_ANALYTICS || ' o
GROUP BY o.truck_brand_name
ORDER BY total_sales DESC';
EXECUTE IMMEDIATE $SELECT_SQL;
```

### **Query Result Cache**

#### **Overview**

This is a great place to demonstrate another powerful feature in Snowflake: the Query Result Cache. When you first ran the 'sales per truck' query, it likely took several seconds. If you run the exact same query again, the result will be nearly instantaneous. This is because the query results were cached in Snowflake's Query Result Cache.

#### **Step 1 \- Re-running a Query**

Run the same 'sales per truck' query from the previous step. Note the execution time in the query details pane. It should be much faster.

```sql
-- Run the same query again
EXECUTE IMMEDIATE $SELECT_SQL;
```

![assets/vignette-1/query\_result\_cache.png][image4]

**Query Result Cache**: Results are retained for any query for 24 hours. Hitting the result cache requires almost no compute resources, making it ideal for frequently run reports or dashboards. The cache resides in the Cloud Services Layer, making it globally accessible to all users and warehouses in the account. For more information, please visit the [documentation on using persisted query results](https://docs.snowflake.com/en/user-guide/querying-persisted-results).

#### **Step 2 \- Scaling Down**

We will now be working with smaller datasets, so we can scale our warehouse back down to an X-Small to conserve credits.

```sql
ALTER WAREHOUSE identifier($MY_WH) SET warehouse_size = 'XSmall';
```

### **Basic Transformation Techniques**

#### **Overview**

In this section, we will see some basic transformation techniques to clean our data and use Zero-Copy Cloning to create development environments. Our goal is to analyze the manufacturers of our food trucks, but this data is currently nested inside a VARIANT column.

#### **Step 1 \- Creating a Development Table with Zero-Copy Clone**

First, let's take a look at the truck\_build column.

```sql
SELECT truck_build FROM identifier($TBL_TRUCK_DETAILS);
```

This table contains data about the make, model and year of each truck, but it is nested, or embedded in a special data type called a VARIANT. We can perform operations on this column to extract these values, but first we'll create a development copy of the table.  
Let's create a development copy of our truck\_details table. Snowflake's Zero-Copy Cloning lets us create an identical, fully independent copy of the table instantly, without using additional storage.

```sql
SET CLONE_SQL = 'CREATE OR REPLACE TABLE ' || $TBL_TRUCK_DEV || ' CLONE ' || $TBL_TRUCK_DETAILS;
EXECUTE IMMEDIATE $CLONE_SQL;
```

[Zero-Copy Cloning](https://docs.snowflake.com/en/user-guide/object-clone): Cloning creates a copy of a database object without duplicating the storage. Changes made to either the original or the clone are stored as new micro-partitions, leaving the other object untouched.

#### **Step 2 \- Adding New Columns and Transforming Data**

Now that we have a safe development table, let's add columns for year, make, and model. Then, we will extract the data from the truck\_build VARIANT column and populate our new columns.

```sql
-- Add new columns
ALTER TABLE identifier($TBL_TRUCK_DEV) ADD COLUMN IF NOT EXISTS year NUMBER;
ALTER TABLE identifier($TBL_TRUCK_DEV) ADD COLUMN IF NOT EXISTS make VARCHAR(255);
ALTER TABLE identifier($TBL_TRUCK_DEV) ADD COLUMN IF NOT EXISTS model VARCHAR(255);

-- Extract and update data
SET UPDATE_SQL = 'UPDATE ' || $TBL_TRUCK_DEV || ' SET 
    year = truck_build:year::NUMBER,
    make = truck_build:make::VARCHAR,
    model = truck_build:model::VARCHAR';
EXECUTE IMMEDIATE $UPDATE_SQL;
```

#### **Step 3 \- Cleaning the Data**

Let's run a query to see the distribution of truck makes.

```sql
SET SELECT_SQL = 'SELECT 
    make,
    COUNT(*) AS count
FROM ' || $TBL_TRUCK_DEV || '
GROUP BY make
ORDER BY make ASC';
EXECUTE IMMEDIATE $SELECT_SQL;
```

Did you notice anything odd about the results from the last query? We can see a data quality issue: 'Ford' and 'Ford\_' are being treated as separate manufacturers. Let's easily fix this with a simple UPDATE statement.

```sql
SET UPDATE_SQL = 'UPDATE ' || $TBL_TRUCK_DEV || ' SET make = ''Ford'' WHERE make = ''Ford_''';
EXECUTE IMMEDIATE $UPDATE_SQL;
```

Here we're saying we want to set the row's make value to Ford wherever it is Ford\_. This will ensure none of the Ford makes have the underscore, giving us a unified make count.

#### **Step 4 \- Promoting to Production with SWAP**

Our development table is now cleaned and correctly formatted. We can instantly promote it to be the new production table using the SWAP WITH command. This atomically swaps the two tables.

```sql
SET SWAP_SQL = 'ALTER TABLE ' || $TBL_TRUCK_DETAILS || ' SWAP WITH ' || $TBL_TRUCK_DEV;
EXECUTE IMMEDIATE $SWAP_SQL;
```

#### **Step 5 \- Cleanup**

Now that the swap is complete, we can drop the unnecessary truck\_build column from our new production table. We also need to drop the old production table, which is now named truck\_dev. But for the sake of the next lesson, we will "accidentally" drop the main table.

```sql
ALTER TABLE identifier($TBL_TRUCK_DETAILS) DROP COLUMN truck_build;

-- Accidentally drop the production table!
DROP TABLE identifier($TBL_TRUCK_DETAILS);
```

#### **Step 6 \- Data Recovery with UNDROP**

Oh no\! We accidentally dropped the production truck\_details table. Luckily, Snowflake's Time Travel feature allows us to recover it instantly. The UNDROP command restores dropped objects.

#### **Step 7 \- Verify the Drop**

If you run a DESCRIBE command on the table, you will get an error stating it does not exist.

```sql
DESCRIBE TABLE identifier($TBL_TRUCK_DETAILS);
```

#### **Step 8 \- Restore the Table with UNDROP**

Let's restore the truck\_details table to the exact state it was in before being dropped.

```sql
UNDROP TABLE identifier($TBL_TRUCK_DETAILS);
```

[Time Travel & UNDROP](https://docs.snowflake.com/en/user-guide/data-time-travel): Snowflake Time Travel enables accessing historical data at any point within a defined period. This allows for restoring data that has been modified or deleted. UNDROP is a feature of Time Travel that makes recovery from accidental drops trivial.

#### **Step 9 \- Verify Restoration and Clean Up**

Verify the table was successfully restored by selecting from it. Then, we can safely drop the actual development table, truck\_dev.

```sql
-- Verify the table was restored
SELECT * from identifier($TBL_TRUCK_DETAILS);

-- Now drop the real truck_dev table
DROP TABLE identifier($TBL_TRUCK_DEV);
```

### **Universal Search**

#### **Overview**

Universal Search allows you to easily find any object in your account, plus explore data products in the Marketplace, relevant Snowflake Documentation, and Community Knowledge Base articles.

#### **Step 1 \- Searching for an Object**

Let's try it now.

1. Click **Search** in the Navigation Menu on the left.  
2. Enter truck into the search bar.  
3. Observe the results. You will see categories of objects on your account, such as tables and views, as well as relevant documentation.

![assets/vignette-1/universal\_search\_truck.png][image5]

#### **Step 2 \- Using Natural Language Search**

You can also use natural language. For example, search for: Which truck franchise has the most loyal customer base? Universal search will return relevant tables and views, even highlighting columns that might help answer your question, providing an excellent starting point for analysis.  
![assets/vignette-1/universal\_search\_natural\_language\_query.png][image6]

## **Simple Data Pipeline**

**![./assets/data\_pipeline\_header.png][image7]**

### **Overview**

Within this vignette, we will learn how to build a simple, automated data pipeline in Snowflake. We will start by ingesting raw, semi-structured data from an external stage, and then use the power of Snowflake's Dynamic Tables to transform and enrich that data, creating a pipeline that automatically stays up-to-date as new data arrives.

### **What You Will Learn**

* How to ingest data from an external S3 stage.  
* How to query and transform semi-structured VARIANT data.  
* How to use the FLATTEN function to parse arrays.  
* How to create and chain Dynamic Tables.  
* How an ELT pipeline automatically processes new data.  
* How to visualize a pipeline using the Directed Acyclic Graph (DAG).

### **What You Will Build**

* An external Stage for data ingestion.  
* A staging table for raw data.  
* A multi-step data pipeline using three chained Dynamic Tables.

### **Get the SQL and paste it into your SQL File.**

**Copy and paste the SQL from this** [file](https://github.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/blob/main/scripts/vignette-2.sql) **in a new SQL File to follow along in Snowflake. Note that once you've reached the end of the SQL File you can skip to Step 16 \- Snowflake Cortex AI.**

### **External Stage Ingestion**

#### **Overview**

Our raw menu data currently sits in an Amazon S3 bucket as CSV files. To begin our pipeline, we first need to ingest this data into Snowflake. We will do this by creating a Stage to point to the S3 bucket and then using the COPY command to load the data into a staging table.

#### **Step 1 \- Set Context**

First, let's set our session context to use the correct database, role, and warehouse. Execute the first few queries in your SQL file.

```sql
-- First, run the user suffix variable setup at the top of your SQL file
-- Then run these context commands:
USE DATABASE identifier($DB_NAME);
USE ROLE identifier($ROLE_ENGINEER);
USE WAREHOUSE identifier($WH_DE);
```

#### **Step 2 \- Create Stage and Staging Table**

A Stage is a Snowflake object that specifies an external location where data files are stored. We'll create a stage that points to our public S3 bucket. Then, we'll create the table that will hold this raw data.

```sql
-- Create the menu stage (uses dynamic SQL)
SET CREATE_SQL = 'CREATE OR REPLACE STAGE ' || $STAGE_MENU || '
COMMENT = ''Stage for menu data''
URL = ''s3://sfquickstarts/frostbyte_tastybytes/raw_pos/menu/''
FILE_FORMAT = (FORMAT_NAME = ''' || $FF_CSV || ''')';
EXECUTE IMMEDIATE $CREATE_SQL;

-- Create the staging table
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
```

#### **Step 3 \- Copy Data into Staging Table**

With the stage and table in place, let's load the data from the stage into our menu\_staging table using the COPY INTO command.

```sql
SET COPY_SQL = 'COPY INTO ' || $TBL_MENU_STAGING || ' FROM @' || $STAGE_MENU;
EXECUTE IMMEDIATE $COPY_SQL;
```

[COPY INTO TABLE](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table): This powerful command loads data from a staged file into a Snowflake table. It is the primary method for bulk data ingestion.

### **Semi-Structured Data**

#### **Overview**

Snowflake excels at handling semi-structured data like JSON using its native VARIANT data type. One of the columns we ingested, menu\_item\_health\_metrics\_obj, contains JSON. Let's explore how to query it.

#### **Step 1 \- Querying VARIANT Data**

Let's look at the raw JSON. Notice it contains nested objects and arrays.

```sql
SELECT menu_item_health_metrics_obj FROM identifier($TBL_MENU_STAGING);
```

We can use special syntax to navigate the JSON structure. The colon (:) accesses keys by name, and square brackets (\[\]) access array elements by index. We can also cast results to explicit data types using the CAST function or the double-colon shorthand (::).

```sql
SET SELECT_SQL = 'SELECT
    menu_item_name,
    CAST(menu_item_health_metrics_obj:menu_item_id AS INTEGER) AS menu_item_id,
    menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY AS ingredients
FROM ' || $TBL_MENU_STAGING;
EXECUTE IMMEDIATE $SELECT_SQL;
```

#### **Step 2 \- Parsing Arrays with FLATTEN**

The FLATTEN function is a powerful tool for un-nesting arrays. It produces a new row for each element in an array. Let's use it to create a list of every ingredient for every menu item.

```sql
SET SELECT_SQL = 'SELECT
    i.value::STRING AS ingredient_name,
    m.menu_item_health_metrics_obj:menu_item_id::INTEGER AS menu_item_id
FROM ' || $TBL_MENU_STAGING || ' m,
    LATERAL FLATTEN(INPUT => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i';
EXECUTE IMMEDIATE $SELECT_SQL;
```

[Semi-Structured Data Types](https://docs.snowflake.com/en/sql-reference/data-types-semistructured): Snowflake's VARIANT, OBJECT, and ARRAY types allow you to store and query semi-structured data directly, without needing to define a rigid schema upfront.

### **Dynamic Tables**

#### **Overview**

Our franchises are constantly adding new menu items. We need a way to process this new data automatically. For this, we can use Dynamic Tables, a powerful tool designed to simplify data transformation pipelines by declaratively defining the result of a query and letting Snowflake handle the refreshes.

#### **Step 1 \- Creating the First Dynamic Table**

We'll start by creating a dynamic table that extracts all unique ingredients from our staging table. We set a LAG of '1 minute', which tells Snowflake the maximum amount of time this table's data can be behind the source data.

```sql
SET CREATE_SQL = 'CREATE OR REPLACE DYNAMIC TABLE ' || $DT_INGREDIENT || '
    LAG = ''1 minute''
    WAREHOUSE = ''' || $WH_DE || '''
AS
    SELECT ingredient_name, menu_ids
FROM (
    SELECT DISTINCT
        i.value::STRING AS ingredient_name,
        ARRAY_AGG(m.menu_item_id) AS menu_ids
    FROM ' || $TBL_MENU_STAGING || ' m,
        LATERAL FLATTEN(INPUT => menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients::ARRAY) i
    GROUP BY i.value::STRING
)';
EXECUTE IMMEDIATE $CREATE_SQL;
```

[Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about): Dynamic Tables automatically refresh as their underlying source data changes, simplifying ELT pipelines and ensuring data freshness without manual intervention or complex scheduling.

#### **Step 2 \- Testing the Automatic Refresh**

Let's see the automation in action. One of our trucks has added a Banh Mi sandwich, which contains new ingredients for French Baguette and Pickled Daikon. Let's insert this new menu item into our staging table.

```sql
SET INSERT_SQL = 'INSERT INTO ' || $TBL_MENU_STAGING || ' 
SELECT 
    10101, 15, ''Sandwiches'', ''Better Off Bread'', 157, ''Banh Mi'', ''Main'', ''Cold Option'', 9.0, 12.0,
    PARSE_JSON(''{...}'')';  -- JSON structure shown in SQL file
EXECUTE IMMEDIATE $INSERT_SQL;
```

Now, query the harmonized.ingredient table. Within a minute, you should see the new ingredients appear automatically.

```sql
-- You may need to wait up to 1 minute and re-run this query
SET SELECT_SQL = 'SELECT * FROM ' || $DT_INGREDIENT || ' WHERE ingredient_name IN (''French Baguette'', ''Pickled Daikon'')';
EXECUTE IMMEDIATE $SELECT_SQL;
```

### **Build Out the Pipeline**

#### **Overview**

Now we can build a multi-step pipeline by creating more dynamic tables that read from other dynamic tables. This creates a chain, or a Directed Acyclic Graph (DAG), where updates automatically flow from the source to the final output.

#### **Step 1 \- Creating a Lookup Table**

Let's create a lookup table that maps ingredients to the menu items they are used in. This dynamic table reads from our harmonized.ingredient dynamic table.

```sql
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
```

#### **Step 2 \- Adding Transactional Data**

Let's simulate an order of two Banh Mi sandwiches by inserting records into our order tables.

```sql
-- Insert into order_header (dynamic SQL)
SET INSERT_SQL = 'INSERT INTO ' || $TBL_ORDER_HEADER || '
SELECT 459520441, 15, 1030, 101565, null, 200322900,
    TO_TIMESTAMP_NTZ(''08:00:00'', ''hh:mi:ss''),
    TO_TIMESTAMP_NTZ(''14:00:00'', ''hh:mi:ss''),
    null, TO_TIMESTAMP_NTZ(''2022-01-27 08:21:08.000''),
    null, ''USD'', 14.00, null, null, 14.00';
EXECUTE IMMEDIATE $INSERT_SQL;
    
-- Insert into order_detail
SET INSERT_SQL = 'INSERT INTO ' || $TBL_ORDER_DETAIL || '
SELECT 904745311, 459520441, 157, null, 0, 2, 14.00, 28.00, null';
EXECUTE IMMEDIATE $INSERT_SQL;
```

#### **Step 3 \- Creating the Final Pipeline Table**

Finally, let's create our final dynamic table. This one joins our order data with our ingredient lookup tables to create a summary of monthly ingredient usage per truck. This table depends on the other dynamic tables, completing our pipeline.

```sql
-- This dynamic table joins order data with ingredient lookups (see full SQL in file)
SET CREATE_SQL = 'CREATE OR REPLACE DYNAMIC TABLE ' || $DT_INGREDIENT_USAGE || '
    LAG = ''2 minute''
    WAREHOUSE = ''' || $WH_DE || '''
    AS 
    SELECT oh.truck_id, EXTRACT(YEAR FROM oh.order_ts) AS order_year,
           MONTH(oh.order_ts) AS order_month, i.ingredient_name,
           SUM(od.quantity) AS total_ingredients_used
    FROM ' || $TBL_ORDER_DETAIL || ' od
        JOIN ' || $TBL_ORDER_HEADER || ' oh ON od.order_id = oh.order_id
        JOIN ' || $DT_INGREDIENT_MENU_LOOKUP || ' iml ON od.menu_item_id = iml.menu_item_id
        JOIN ' || $DT_INGREDIENT || ' i ON iml.ingredient_name = i.ingredient_name
        JOIN ' || $TBL_LOCATION || ' l ON l.location_id = oh.location_id
    WHERE l.country = ''United States''
    GROUP BY oh.truck_id, order_year, order_month, i.ingredient_name
    ORDER BY oh.truck_id, total_ingredients_used DESC';
EXECUTE IMMEDIATE $CREATE_SQL;
```

#### **Step 4 \- Querying the Final Output**

Now, let's query the final table in our pipeline. After a few minutes for the refreshes to complete, you will see the ingredient usage for two Banh Mis from the order we inserted in a previous step. The entire pipeline updated automatically.

```sql
-- You may need to wait up to 2 minutes and re-run this query
SET SELECT_SQL = 'SELECT truck_id, ingredient_name, SUM(total_ingredients_used) AS total_ingredients_used
FROM ' || $DT_INGREDIENT_USAGE || '
WHERE order_month = 1 AND truck_id = 15
GROUP BY truck_id, ingredient_name
ORDER BY total_ingredients_used DESC';
EXECUTE IMMEDIATE $SELECT_SQL;
```

### **Visualize the Pipeline**

#### **Overview**

Finally, let's visualize our pipeline's Directed Acyclic Graph, or DAG. The DAG shows how our data flows through the tables, and it can be used to monitor the health and lag of our pipeline.

#### **Step 1 \- Accessing the Graph View**

To access the DAG in Snowsight:

1. Navigate to **Data** » **Database**.  
2. In the database object explorer, expand your database **TB\_101\_\<your\_username\>** (e.g., TB\_101\_JSMITH) and the schema **HARMONIZED**.  
3. Click on **Dynamic Tables**.  
4. Select any of the dynamic tables you created (e.g., INGREDIENT\_USAGE\_BY\_TRUCK).  
5. Click on the **Graph** tab in the main window.

You will now see a visualization of your pipeline, showing how the base tables flow into your dynamic tables.  
![assets/vignette-2/dag.png][image8]

## **Snowflake Cortex AI**

### **Overview**

Welcome to the Zero to Snowflake guide focused on Snowflake Cortex AI\!  
Within this guide, we will explore Snowflake's complete AI platform through a progressive journey from experimentation into unified business intelligence. We'll learn AI capabilities by building a comprehensive customer intelligence system using Cortex Playground for AI experimentation, Cortex AI Functions for production-scale analysis, Cortex Search for semantic text searching, and Cortex Analyst for natural language analytics.

* For more detail on Snowflake Cortex AI, please visit the [Snowflake AI and ML Overview documentation](https://docs.snowflake.com/en/guides-overview-ai-features).

### **What You Will Learn**

* How to Experiment with AI Using AI Cortex Playground for model testing and prompt optimization.  
* How to Scale AI Analysis with Cortex AI Functions for production-scale customer review processing.  
* How to enable semantic discovery with Cortex Search for intelligent text and review finding.  
* How to create conversational analytics with Cortex Analyst for natural language business intelligence.

### **What You Will Build**

Through this journey, you’ll construct a complete intelligence customer analytics platform:  
**Phase 1: AI Foundation**

* Production-scale Review Analysis pipeline using Cortex AI Functions for systematic customer feedback processing.

**Phase 2: Intelligent Development & Discovery**

* Semantic Search Engine using Cortex Search for instant customer feedback discovery and operational intelligence.

**Phase 3: Conversational Intelligence**

* Natural Language Business Analytics Interface using Cortex Analyst for conversational data exploration.  
* Unified AI Business Intelligence Platform using Snowflake Intelligence that connects customer voice with business performance

### **AI Functions**

**![./assets/ai\_functions\_header.png][image9]**

#### **Overview**

You've successfully experimented with AI models in Cortex Playground to analyze individual customer reviews. Now, it's time to scale\! This guide shows you how to use [AI Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql) to process thousands of reviews, turning experimental insights into production-ready intelligence. You'll learn to:

1. **USE SENTIMENT()** to score and label truck customer reviews.  
2. **Use AI\_CLASSIFY()** to categorize reviews by themes.  
3. **Use EXTRACT\_ANSWER()** to pull specific complaints or praise.  
4. **Use AI\_SUMMARIZE\_AGG()** to generate quick summaries per truck brand.

### **Get the SQL code and paste it into your SQL File.**

Copy and paste the SQL from this [file](https://github.com/Snowflake-Labs/sfguide-getting-started-from-zero-to-snowflake/blob/main/scripts/vignette-3-aisql.sql) in a new SQL File to follow along in Snowflake.

### **Step 1 \- Setting Context**

First, let's set our session context. We will assume the role of a TastyBytes data analyst with the intention of leveraging AISQL functions to gain insights from customer reviews.

```sql
-- First, run the user suffix variable setup at the top of your SQL file
-- Then run these context commands:
USE ROLE identifier($ROLE_ANALYST);
USE DATABASE identifier($DB_NAME);
USE WAREHOUSE identifier($WH_ANALYST);
```

#### **Step 2 \- Sentiment Analysis at Scale**

Analyze customer sentiment across all food truck brands to identify which trucks are performing best and create fleet-wide customer satisfaction metrics. In Cortex Playground, we analyzed individual reviews manually. Now we’ll use the SENTIMENT() function to automatically score customer reviews from \-1 (negative) to \+1 (positive), following Snowflake's official sentiment ranges.  
**Business Question:** “How do customers feel about each of our truck brands overall?”  
Please execute this query to analyze customer sentiment across our food truck network and categorize feedback.

```sql
SET SQL_QUERY = 'SELECT
    truck_brand_name,
    COUNT(*) AS total_reviews,
    AVG(CASE WHEN sentiment >= 0.5 THEN sentiment END) AS avg_positive_score,
    AVG(CASE WHEN sentiment BETWEEN -0.5 AND 0.5 THEN sentiment END) AS avg_neutral_score,
    AVG(CASE WHEN sentiment <= -0.5 THEN sentiment END) AS avg_negative_score
FROM (
    SELECT truck_brand_name, SNOWFLAKE.CORTEX.SENTIMENT(review) AS sentiment
    FROM ' || $VIEW_TRUCK_REVIEWS || '
    WHERE language ILIKE ''%en%'' AND review IS NOT NULL
    LIMIT 10000
)
GROUP BY truck_brand_name
ORDER BY total_reviews DESC';
EXECUTE IMMEDIATE $SQL_QUERY;
```

![assets/vignette-3/sentiment.png][image10]

**Key Insight**: Notice how we transitioned from analyzing reviews one at a time in Cortex Playground to systematically processing thousands. The SENTIMENT() function automatically scored every review and categorized them into Positive, Negative, and Neutral \- giving us instant fleet-wide customer satisfaction metrics.  
**Sentiment Score Ranges**:

* Positive: 0.5 to 1  
* Neutral: \-0.5 to 0.5  
* Negative: \-0.5 to \-1

#### **Step 3 \- Categorize Customer Feedback**

Now, let's categorize all reviews to understand what aspects of our service customers are talking about most. We'll use the AI\_CLASSIFY() function, which automatically categorizes reviews into user-defined categories based on AI understanding, rather than simple keyword matching. In this step, we will categorize customer feedback into business-relevant operational areas and analyze their distribution patterns.  
**Business Question:** “What are customers primarily commenting on \- food quality, service, or delivery experience?"  
Execute the Classification Query:

```sql
SET SQL_QUERY = 'WITH classified_reviews AS (
  SELECT truck_brand_name,
    AI_CLASSIFY(review, [''Food Quality'', ''Pricing'', ''Service Experience'', ''Staff Behavior'']):labels[0] AS feedback_category
  FROM ' || $VIEW_TRUCK_REVIEWS || '
  WHERE language ILIKE ''%en%'' AND review IS NOT NULL AND LENGTH(review) > 30
  LIMIT 10000
)
SELECT truck_brand_name, feedback_category, COUNT(*) AS number_of_reviews
FROM classified_reviews
GROUP BY truck_brand_name, feedback_category
ORDER BY truck_brand_name, number_of_reviews DESC';
EXECUTE IMMEDIATE $SQL_QUERY;
```

![assets/vignette-3/classify.png][image11]

**Key Insight**: Observe how AI\_CLASSIFY() automatically categorized thousands of reviews into business-relevant themes such as Food Quality, Service Experience, and more. We can instantly see that Food Quality is the most discussed topic across our truck brands, providing the operations team with clear, actionable insight into customer priorities.

#### **Step 4 \- Extract Specific Insights**

Next, to gain precise answers from unstructured text, we'll utilize the EXTRACT\_ANSWER() function. This powerful function enables us to ask specific business questions about customer feedback and receive direct answers. In this step, our goal is to identify precise operational issues mentioned in customer reviews, highlighting specific problems that require immediate attention.  
**Business question:** “What specific improvement or complaint is mentioned in this review?"  
Let's execute the next query:

```sql
SET SQL_QUERY = 'SELECT
    truck_brand_name, primary_city,
    LEFT(review, 100) || ''...'' AS review_preview,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(review,
        ''What specific improvement or complaint is mentioned in this review?''
    ) AS specific_feedback
FROM ' || $VIEW_TRUCK_REVIEWS || '
WHERE language = ''en'' AND review IS NOT NULL AND LENGTH(review) > 50
ORDER BY truck_brand_name, primary_city ASC
LIMIT 10000';
EXECUTE IMMEDIATE $SQL_QUERY;
```

![assets/vignette-3/extract.png][image12]

**Key Insight**: Notice how EXTRACT\_ANSWER() distills specific, actionable insights from long customer reviews. Rather than manual review, this function automatically identifies concrete feedback like "friendly staff was saving grace" and "hot dogs are cooked to perfection." The result is a transformation of dense text into specific, quotable feedback that the operations team can leverage instantly.

#### **Step 5 \- Generate Executive Summaries**

Finally, to create concise summaries of customer feedback, we'll use the AI\_SUMMARIZE\_AGG() function. This powerful function generates short, coherent summaries from lengthy unstructured text. In this step, our goal is to distill the essence of customer reviews for each truck brand into digestible summaries, providing quick overviews of overall sentiment and key points.  
**Business Question:** “What are the key themes and overall sentiment for each truck brand?”  
Execute the Summarization Query:

```sql
SET SQL_QUERY = 'SELECT truck_brand_name, AI_SUMMARIZE_AGG(review) AS review_summary
FROM (
    SELECT truck_brand_name, review
    FROM ' || $VIEW_TRUCK_REVIEWS || '
    LIMIT 100
)
GROUP BY truck_brand_name';
EXECUTE IMMEDIATE $SQL_QUERY;
```

![assets/vignette-3/summarize.png][image13]

**Key Insight**: The AI\_SUMMARIZE\_AGG() function condenses lengthy reviews into clear, brand-level summaries. These summaries highlight recurring themes and sentiment trends, providing decision-makers with quick overviews of each food truck's performance and enabling faster understanding of customer perception without reading individual reviews.

#### **Conclusion**

We've successfully demonstrated the transformative power of AI Functions, shifting customer feedback analysis from individual review processing to systemic, production-scale intelligence. Our journey through these four core functions clearly illustrates how each serves a distinct analytical purpose, transforming raw customer voices into comprehensive business intelligence—systematic, scalable, and immediately actionable. What once required individual review analysis now processes thousands of reviews in seconds, providing both the emotional context and specific details crucial for data-driven operational improvements.

### **Cortex Search**

**![./assets/cortex\_search\_header.png][image14]**

#### **Overview**

While AI-powered tools excel at generating complex analytical queries, a common daily challenge for customer service teams is quickly finding specific customer reviews for complaints or compliments. Traditional keyword search often falls short, missing the nuances of natural language.  
[Snowflake Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview) solves this by providing low-latency, high-quality "fuzzy" search over your Snowflake text data. It quickly sets up hybrid (vector and keyword) search engines, handling embeddings, infrastructure, and tuning for you. Under the hood, Cortex Search combines semantic (meaning-based) and lexical (keyword-based) retrieval with intelligent re-ranking to deliver the most relevant results. In this lab, you will configure a search service, connect it to customer review data, and run semantic queries to proactively identify key customer feedback.

#### **Step 1 \- Access Cortex Search**

1. Open Snowsight and navigate to the AI & ML Studio, then select **Cortex Search**.  
2. Click **Create** to begin setup.

This opens the search service configuration interface, where you’ll define how Snowflake indexes and interprets your text data.  
![assets/vignette-3/cortex-search-access.png][image15]

#### **Step 2 \- Configure the Search Service**

In the **New service** configuration screen:

1. Select **Database** and **Schema**:  
   * Choose your user-specific database **TB\_101\_\<your\_username\>** from the Databases dropdown  
   * Choose **HARMONIZED** from the Schemas dropdown  
2. Enter the **Service name**: customer\_feedback\_intelligence  
3. Click the **Next** button at the bottom right to proceed.

![assets/vignette-3/cortex-search-new-service.png][image16]

#### **Step 3 \- Connect to Review Data**

The wizard will now guide you through several configuration screens. Follow these steps:

1. **Select data screen:**  
   * From the Views dropdown, select TRUCK\_REVIEWS\_V  
   * Click **Next**  
2. **Select search column screen:**  
   * Choose REVIEW (this is the text column that will be semantically searched)  
   * Click **Next**  
3. **Select attributes screen:**  
   * Select columns for filtering results: TRUCK\_BRAND\_NAME, PRIMARY\_CITY, REVIEW\_ID  
   * Click **Next**  
4. **Select columns screen:**  
   * Choose other columns to include in search results such as DATE, LANGUAGE, etc.  
   * Click **Next**  
5. **Configure indexing screen:**  
   * **Warehouse**: Select COMPUTE\_WH from the dropdown  
   * Accept the other default settings  
   * Click **Create** to build the search service

assets/vignette-3/cortex-search-walkthrough.gif

**Note**: Creating the search service includes building the index, so the initial setup may take a little longer. If the creation process is taking an extended period, you can seamlessly continue the lab by using a pre-configured search service:

1. From the left-hand menu in Snowsight, navigate to **AI & ML**, then click on **Cortex Search**.  
2. In the Cortex Search view, locate the dropdown filter (as highlighted in the image below). Select or ensure this filter is set to your user-specific database **TB\_101\_\<your\_username\>** and **HARMONIZED** schema.  
3. In the list of "Search services" that appears, click on the pre-built service named **TASTY\_BYTES\_REVIEW\_SEARCH**.  
4. Once inside the service's details page, click on **Playground** in the top right corner to begin using the search service for the lab.  
* **Once any search service is active (either your new one or the pre-configured one), queries will run with low latency and scale seamlessly.**

**![assets/vignette-3/cortex-search-existing-service.png][image17]**

Behind this simple UI, Cortex Search is performing a complex task. It analyzes the text in your "REVIEW" column, using an AI model to generate semantic embeddings, which are numerical representations of the text's meaning. These embeddings are then indexed, allowing for high-speed conceptual searches later on. In just a few clicks, you have taught Snowflake to understand the intent behind your reviews.

#### **Step 4 \- Run Semantic Query**

When the service shows as "Active", click on **Playground** and enter the natural language prompt in the search bar:  
**Prompt \- 1:** Customers getting sick  
![assets/vignette-3/cortex-search-prompt1.png][image18]

**Key Insight**: Notice Cortex Search isn’t just finding customers \- it’s finding CONDITIONS that could MAKE customers sick. That is the difference between reactive keyword search and proactive semantic understanding.  
Now try another query:  
**Prompt \- 2:** Angry customers  
![assets/vignette-3/cortex-search-prompt2.png][image19]

**Key Insight**: These customers are about to churn, but they never said “I’m angry.” They expressed frustration in their own words. Cortex Search understands the emotion behind the language, helping you identify and save at-risk customers before they leave.

#### **Conclusion**

Ultimately, Cortex Search transforms how Tasty Bytes analyzes customer feedback. It empowers the customer service manager to move beyond simply sifting through reviews, to truly understand and proactively act upon the voice of the customer at scale, driving better operational decisions and enhancing customer loyalty.  
In the next module \- Cortex Analyst \- you'll use natural language to query structured data.

### **Cortex Analyst**

**![./assets/cortex\_analyst\_header.png][image20]**

#### **Overview**

A business analyst at Tasty Bytes needs to enable self-service analytics, allowing the business team to ask complex questions in natural language and get instant insights without relying on data analysts to write SQL. While previous AI tools helped with finding reviews and complex query generation, the demand now is for **conversational analytics** that directly transforms structured business data into immediate insights.  
[Snowflake Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst) empowers business users to ask sophisticated questions directly, seamlessly extracting value from their analytics data through natural language interaction. This lab will guide you through designing a semantic model, connecting it to your business data, configuring relationships and synonyms, and then executing advanced business intelligence queries using natural language.

#### **Step 1 \- Design Semantic Model**

Let's begin by navigating to Cortex Analyst in Snowsight and configuring our semantic model foundations.

1. Navigate to **Cortex Analyst** under **AI & ML Studio** in Snowsight.

![assets/vignette-3/cortex-analyst-nav.png][image21]

2. **Set Role and Warehouse:**  
   * Change role to TB\_DEV.  
   * Set Warehouse to TB\_CORTEX\_WH.  
   * Click **Create new model**.

![assets/vignette-3/cortex-analyst-setup.png][image22]

3. On the **Getting Started** page, configure the following:  
   * **DATABASE**: Your user-specific database TB\_101\_\<your\_username\>  
   * **SCHEMA**: SEMANTIC\_LAYER  
   * **Name**: tasty\_bytes\_business\_analytics  
   * **Description**: Semantic model for Tasty Bytes executive analytics, covering customer loyalty and order performance data for natural language querying  
   * Click **Next**.

![assets/vignette-3/cortex-analyst-getting-started.png][image23]

#### **Step 2 \- Select & Configure Tables and Columns**

In the **Select tables** step, let's choose our analytics views.

1. Select the core business tables:  
   * **DATABASE**: Your user-specific database TB\_101\_\<your\_username\>  
   * **SCHEMA**: SEMANTIC\_LAYER  
   * **VIEWS**: Select Customer\_Loyalty\_Metrics\_v and Orders\_v.  
   * Click **Next**.  
2. On the **Select columns** page, ensure both selected tables are active, then click **Create and Save**.

#### **Step 3 \- Edit Logical Table & Add Synonyms**

Now, let's add table synonyms and a primary key for better natural language understanding.

1. In the customer\_loyalty\_metrics\_v table, copy and paste the following synonyms into the Synonyms box:

```
Customers, customer_data, loyalty, customer_metrics, customer_info
```

2.   
   Set the **Primary Key** to customer\_id from the dropdown.  
3. For the orders\_v table, copy and paste the following synonyms:

```
Orders, transactions, sales, purchases, order_data
```

4.   
   After making these changes, click **Save** in the top right corner.

#### **Step 4 \- Configure Table Relationships**

After creating the semantic model, let's establish the relationship between our logical tables.

1. Click **Relationships** in the left-hand navigation.  
2. Click **Add relationship**.  
3. Configure the relationship as follows:  
   * **Relationship name**: orders\_to\_customer\_loyalty\_metrics  
   * **Left table**: ORDERS\_V  
   * **Right table**: CUSTOMER\_LOYALTY\_METRICS\_V  
   * **Join columns**: Set CUSTOMER\_ID \= CUSTOMER\_ID.  
4. Click **Add relationship**

**![assets/vignette-3/cortex-analyst-table-relationship.png][image24]**

**Upon completion**, simply use the **Save** option at the top of the UI. This will finalize your semantic view, making your semantic model ready for sophisticated natural language queries.  
To access the **Cortex Analyst chat interface** in fullscreen mode, you would:

1. Click the **three-dot menu (ellipsis)** next to the "Share" button at the top right.  
2. From the dropdown menu, select **"Enter fullscreen mode."**

**![assets/vignette-3/cortex-analyst-interface.png][image25]**

#### **Step 5 \- Execute Customer Segmentation Intelligence**

With our semantic model and relationships active, let's demonstrate sophisticated natural language analysis by running our first complex business query.

1. Navigate to the Cortex Analyst query interface.  
2. Enter the following prompt:

```
Show customer groups by marital status and gender, with their total spending per customer and average order value. Break this down by city and region, and also include the year of the orders so I can see when the spending occurred. In addition to the yearly breakdown, calculate each group’s total lifetime spending and their average order value across all years. Rank the groups to highlight which demographics spend the most per year and which spend the most overall.
```

![assets/vignette-3/cortex-analyst-prompt1.png][image26]

**Key Insight**: Instantly delivers comprehensive intelligence by combining multi-table joins, demographic segmentation, geographic insights, and lifetime value analysis \- insights that would require 40+ lines of SQL and hours of analyst effort.

#### **Step 6 \- Generate Advanced Business Intelligence**

Having seen basic segmentation, let's now demonstrate enterprise-grade SQL that showcases the full power of conversational business intelligence.

1. Clear the context by clicking the refresh icon.  
2. Enter the following prompt:

```
I want to understand our customer base better. Can you group customers by their total spending (high, medium, low spenders), then show me their ordering patterns differ? Also compare how our franchise locations perform versus company-owned stores for each spending group.
```

![assets/vignette-3/cortex-analyst-prompt2.png][image27]

**Key Insight**: Notice how Cortex Analyst seamlessly bridges the gap between a business user's simple, natural language question and the sophisticated, multi-faceted SQL query required to answer it. It automatically constructs the complex logic, including CTEs, window functions, and detailed aggregations, that would typically demand a skilled data analyst.

#### **Conclusion**

Through these rigorous steps, we've forged a robust Cortex Analyst semantic model. This isn't just an improvement; it's a transformative tool designed to liberate users across various industries from the constraints of SQL, enabling them to surface profound business intelligence through intuitive natural language queries. Our multi-layered analyses, while showcased through the Tasty Bytes use case, powerfully illustrate how this model drastically cuts down on the time and effort traditionally needed for deep insights, thereby democratizing access to data and fueling a culture of informed, agile decision-making on a broad scale.