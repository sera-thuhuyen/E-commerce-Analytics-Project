from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

TABLES = [
    "orders",
    "order_items",
    "order_item_refunds",
    "products",
    "website_sessions",
    "website_pageviews",
]

SPARK_CMD = (
    'docker exec '
    '-e SQLSERVER_PASSWORD="$SQLSERVER_PASSWORD" '
    'spark-master '
    '/opt/spark/bin/spark-submit '
    '--jars /opt/spark-project/jars/mssql-jdbc-13.4.0.jre11.jar '
    '/opt/spark-project/jobs/extract.py'
)

with DAG(
    dag_id="ecommerce_analytics_extraction_dag",
    default_args=default_args,
    description="Phase 1b: Extract 6 tables sequentially",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce-analytics", "extraction", "phase-1"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Xoá raw folder trước
    cleanup = BashOperator(
        task_id="cleanup_raw",
        bash_command=(
            "docker exec spark-master bash -c "
            "'rm -rf /opt/spark-project/raw/ && mkdir -p /opt/spark-project/raw/'"
        ),
    )

    # Tạo tasks tuần tự
    extraction_tasks = [
        BashOperator(
            task_id=f"extract_{table}",
            bash_command=SPARK_CMD,
            env={"SQLSERVER_PASSWORD": "{{ var.value.get('sqlserver_password', '') }}"},
        )
        for table in TABLES
    ]

    # Chain tuần tự: start >> cleanup >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> end
    start >> cleanup
    cleanup >> extraction_tasks[0]
    for i in range(len(extraction_tasks) - 1):
        extraction_tasks[i] >> extraction_tasks[i + 1]
    extraction_tasks[-1] >> end
