Project Name:
    Customer Performance Metrics

Author:
    Claude Mack

Project Goal:
    Build an end-to-end ELT pipeline from a data source to data warehouse
    using Python, SQL and DBT and data models to answer the following questions:
        * Top 10 stores per transacted amount
        * Top 10 products sold
        * Average transacted amount per store typology and country
        * Percentage of transactions per device type
        * Average time for a store to perform its 5 first transactions

Project Files:
    * CSV files:
      - raw source data files
    * SQL files:
      - sql scripts required for the ELT
    * Pipeline files:
      - customer_performance_prototype.sql -> a prototype ELT pipeline in a
        single SQL script, which is used for testing the basic solution
      - customer_performance_mara-etl.py -> the ELT pipeline written with the
        mara-etl tool, which is a very lightweight tool inspired by airflow,
        and this is the tool that I have the most experience with
      - customer_performance_airflow-etl.py -> the ELT pipeline written with
        airflow, which I added to show how it's possible to adapt my experience
        with mara-etl to airflow

Introduction:
    First, I like to outline a potential solution. Here are some notes I made on
    my phone the same day that I received the project details.
 
    Data sources:
        devices.csv, stores.csv, transactions.csv

    Possible keys for joining the data sources:
        devices.csv - device_id, device_type_id, store_id
        stores.csv - store_id, customer_id
        transactions.csv - transaction_id, device_id, product_sku

    Joins to combine the data sources:
        Main source table - transactions
        Left join devices using device_id
        Left join stores using store_id

    Pseudocode to answer each of the above questions:
        Top 10 stores per transacted amount:
            sum(transacted amount) group by store_id order by sum desc limit 10
        Top 10 products sold:
            count(*) group by product_sku order by count desc limit 10
        Avg amount per store_type and country:
            avg(transacted amount) group by store_type, country
            order by store_type, avg desc, country
        Percentage of transactions by device type:
            count(*)/count(*) over () as percentage group by device_type
            order by percentage desc
        Avg time to 5 transactions:
            CTE with first 5 transactions of each store.
            avg(5th date - 1st date) group by customer_id order by avg asc

First round of questions for the stakeholder(s): 
    After outlining a potential solution, I like to check-in with the stakeholder(s)
    to make sure I am on the right track and to ask any clarification questions that I might have):
    * Should the focus only be on 'accepted' transactions for now? Later, it will
      be easy to also analyze other types of transactions like 'rejected'.
    * For the transaction date, should created_at or happened_at be used?
    * Do they want the average of the total transacted amount grouped by store_type and country,
      or only the average single-purchase transaction amount?
    * Why are there two different columns called product_name? I would also explain that I can
      only use the product_sku to show the top 10 products sold, because the names are not
      unique and multiple names can refer to the same product

Implement a prototype for the solution:
    * In this case, for the prototype I simply created a single SQL script that loads and joins
      the data, and then executes queries that answers each of the questions above.
    * The purpose of this step is to test out the SQL and explore the data a little to make sure
      that my solution outline is feasible.
    * The SQL script is called customer_performance_prototype.sql (The code assumes that happened_at
      is the relevant timestamp for the transactions)

Second round of questions for the stakeholder(s):
    * How often does the data need to be updated? daily/hourly/continuous stream?
    * Are the query results from the prototype a sufficient basis for the data model, 
      or are there other data columns/fields/attributes that should be included?
    * Do the numbers from the aggregations seem reasonable given the stakeholder's domain knowledge?

Implement a first version of the data pipeline:
    * Extraction:
      - If the data sources are extracted from APIs then use the python requests package.
      - If the data sources are extracted from the production database, the use SQL that
        queries the database directly and outputs the results to csvs.
      - For the pipeline I implemented, I assumed that the data sources are extracted from
        the production database.
    * Load:
      - Load CSVs from the extraction stage directly into staging tables in the data warehouse.
      - Depending on how complex the use-case might become, it could make sense to transform the
        staging tables into facts and dimensions in the warehouse.
      - If quick deployment is better, then the data from the staging tables can be combined into
        a single flat table that can be directly used by the stakeholder(s)/data-visualization tool.
    * Transform:
      - Use relatively short but clear table/column names based on the source data fieldnames.
      - Perform the aggregations based on the stakeholder replies to the fisrt and second round of
        questions above.

Next Steps:
    * Use DBT to build the models and manage the model dependencies
      - this should make the code for the last nodes of the pipeline cleaner
        and easier to maintain
      - for example, the avg_time_to_5_transactions could be calculated with
        the following DBT code:
            SELECT
                customer_id,
                count(distinct store_id) as num_stores,
                extract(day from avg(days_to_5_transactions)) as avg_days_to_5_transactions
            FROM
                {{ref('transaction_rate')}}
            GROUP BY
                customer_id
    * Check with the stakeholders to see if they want to:
      - capture historical data like stores, products, customers that change
        their location/names
      - discuss other metrics like if any devices/stores/customers seem to have
        an abnormally high number of refused/cancelled transactions. perhaps there
        is a particular problem in that case that could be addressed 
    * Refactor code/schemas/models:
      - the SQL for the transaction rate in particular seems like it could
        be done in a cleaner and more elegant way
      - also, the upsert of the transactions and transaction_rate tables needs to
        be tested at large scales to make sure it still performs well
