""" An airflow ETL pipeline that loads data models for customer performance metrics. """

from datetime import timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG((
    "customer_performance",
    description = "Build data models for customer performance metrics",
    schedule_interval = timedelta(days = 1),
    start_date = days_ago(1)
)

load_schema = PostgresOperator(
    task_id = f"load_schema",
    postgres_conn_id = "target_db",
    sql = "sql/load_schema.sql",
    dag = dag 
)

sources = ["devices", "stores", "transactions"]
extract_csv = {
    source: PostgresOperator(
        task_id = f"extract_{source}",
        postgres_conn_id = "production_db",
        sql = "sql/extract_csv_airflow.sql",
        params = {"tablename": source},
        dag = dag 
    )
    for source in sources
}
load_staging = {
    source: PostgresOperator(
        task_id = f"load_{source}",
        postgres_conn_id = "target_db",
        sql = "COPY {{params.tablename}} FROM '{{params.csv_path}}' CSV HEADER",
        params = {"tablename": f"staging_{source}", "csv_path": f"csvs/{source}.csv"},
        dag = dag 
    )
    for source in sources
}

models = [
    "transactions_model",
    "top_10_stores",
    "top_10_products",
    "avg_transaction",
    "transactions_per_device",
    "transaction_rate",
    "avg_time_to_5_transactions"
]
load_model = {
    model: PostgresOperator(
        task_id = f"load_{model}",
        postgres_conn_id = "target_db",
        sql = f"sql/load_{model}.sql",
        dag = dag 
    )
    for model in models
}

# Set the task dependencies
for table in extract_csv.keys():
    load_schema >> extract_csv[table]
    extract_csv[table] >> load_staging[table]
    load_staging[table] >> load_model["transactions_model"]

for model in load_model.keys():
    if model != "transactions_model":
        load_model["transactions_model"] >> load_model[model]
    elif model == "avg_time_to_5_transactions":
        load_model["transaction_rate"] >> load_model[model]
