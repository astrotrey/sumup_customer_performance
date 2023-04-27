""" A mara ETL pipeline that loads data models for customer performance metrics. """

from data_integration.pipelines import Pipeline, Task
from data_integration.commands.sql import ExecuteSQL

pipeline = Pipeline(
    id = "customer_performance",
    description = "Build data models for customer performance metrics"
)

pipeline.add(
    Task(
        id = f"load_schema",
        description = f"Load schema for staging tables and data models",
        commands = [
            ExecuteSQL(
                db_alias = "target_db",
                sql_file_name = f"sql/load_schema.sql"
            )
        ] 
    )
)

sources = ["devices", "stores", "transactions"]
for source in sources:
    pipeline.add(
        Task(
            id = f"extract_{source}",
            description = f"Extract CSV for {source}",
            commands = [
                ExecuteSQL(
                    db_alias = "production_db",
                    sql_file_name = f"sql/extract_csv.sql",
                    replace = {"tablename":source}
                )
            ]
        )
    )
   
    pipeline.add(
        Task(
            id = f"load_{source}",
            description = f"Load staging table for {source}",
            commands = [
                ExecuteSQL(
                    db_alias = "target_db",
                    sql_file_name = f"sql/load_staging.sql",
                    replace = {"tablename":f"staging_{source}"}
                )
            ] 
        ),
        upstreams = [f"extract_{source}"]
    )

pipeline.add(
    Task(
        id = f"load_transactions_model",
        description = f"Load transactions model as source for all other models",
        commands = [
            ExecuteSQL(
                db_alias = "target_db",
                sql_file_name = f"sql/load_transactions_model.sql"
            )
        ] 
    ),
    upstreams = [f"load_{source}" for source in sources]
)

models = [
    "top_10_stores",
    "top_10_products",
    "avg_transaction_amount",
    "transactions_per_device",
    "transaction_rate",
    "avg_time_to_5_transactions"
]
for model in models:
    if model == "avg_time_to_5_transactions":
        upstreams = [f"transaction_rate"]
    else:
        upstreams = [f"load_transactions_model"]

    pipeline.add(
        Task(
            id = f"load_{model}",
            description = f"Load {model}",
            commands = [
                ExecuteSQL(
                    db_alias = "target_db",
                    sql_file_name = f"sql/load_{model}.sql"
                )
            ] 
        ),
        upstreams = upstreams
