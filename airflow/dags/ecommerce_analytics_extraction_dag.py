"""
commerce_analytics_extraction_dag.py
=======================
Phase 1b — Airflow DAG: Extract data from SQL Server → Parquet (via PySpark)

Orchestration Strategy:
  - 6 extraction tasks chạy theo 2 wave:
      Wave 1 (nhỏ, parallel): orders, order_items, order_item_refunds, products
      Wave 2 (lớn, riêng):    website_sessions, website_pageviews
  - 1 validation task tổng hợp cuối cùng
  - Retry logic: 2 lần, delay 5 phút
  - Schedule: daily @ 01:00 UTC (08:00 ICT)
  - Backfill-safe: idempotent — mỗi run ghi vào partition run_date=YYYY-MM-DD

Author: <your-name>
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ─── Constants ────────────────────────────────────────────────────────────────

SPARK_MASTER_URL   = "spark://spark-master:7077"
JDBC_JAR_PATH      = "/opt/spark-project/jars/mssql-jdbc-13.4.0.jre11.jar"
OUTPUT_BASE_PATH   = "/opt/spark-project/raw"   # maps to ./spark/raw/ on host

SQL_SERVER_HOST    = "45.124.94.158"
SQL_SERVER_PORT    = "1433"
SQL_SERVER_DB      = "xomdata_dataset"
SQL_SERVER_SCHEMA  = "web_analytics"
SQL_SERVER_USER    = "thuhuyenftu2"
# Password loaded from Airflow Variable or env — never hardcode in production
SQL_SERVER_PWD_ENV = "SQLSERVER_PASSWORD"

# Table configs: name → expected_min_rows (for validation)
TABLES = {
    "orders":               30_000,
    "order_items":          38_000,
    "order_item_refunds":    1_500,
    "products":                  3,
    "website_sessions":    460_000,
    "website_pageviews":  1_100_000,
}

# ─── Default DAG Args ─────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,   # set True + configure SMTP in Airflow for alerts
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─── Core Extraction Function ─────────────────────────────────────────────────

def extract_table(table_name: str, run_date: str, **context) -> dict:
    """
    Extract một table từ SQL Server via JDBC, validate, lưu Parquet.

    Chạy trong Airflow worker (Python process) — submit Spark job thông qua
    SparkSession với remote master URL. Worker container cần pyspark installed.

    Returns dict với metadata để XCom push.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit, current_timestamp

    logger = logging.getLogger(__name__)
    logger.info(f"[{table_name}] Starting extraction for run_date={run_date}")

    # Lấy password từ environment (được set trong docker-compose hoặc Airflow Variable)
    password = os.environ.get(SQL_SERVER_PWD_ENV, "")
    if not password:
        # Fallback: thử Airflow Variable
        try:
            from airflow.models import Variable
            password = Variable.get("sqlserver_password", default_var="")
        except Exception:
            pass
    if not password:
        raise EnvironmentError(
            f"SQL Server password not found. Set env var '{SQL_SERVER_PWD_ENV}' "
            "or Airflow Variable 'sqlserver_password'."
        )

    jdbc_url = (
        f"jdbc:sqlserver://{SQL_SERVER_HOST}:{SQL_SERVER_PORT};"
        f"databaseName={SQL_SERVER_DB};"
        "TrustServerCertificate=true;"
        "loginTimeout=30;"
    )

    output_path = f"{OUTPUT_BASE_PATH}/{table_name}/run_date={run_date}"

    # ── Build SparkSession ──────────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName(f"commerce_analytics_extract_{table_name}_{run_date}")
        .master(SPARK_MASTER_URL)
        .config("spark.jars", JDBC_JAR_PATH)
        # Tối ưu cho extraction: không cần shuffle nhiều
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "1g")
        # Tránh OOM với bảng lớn: fetch theo batch
        .config("spark.executor.extraJavaOptions",
                "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info(f"[{table_name}] Reading via JDBC...")

        # ── Read from SQL Server ────────────────────────────────────────────
        read_options = {
            "url":          jdbc_url,
            "dbtable":      f"{SQL_SERVER_SCHEMA}.{table_name}",
            "user":         SQL_SERVER_USER,
            "password":     password,
            "driver":       "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "fetchsize":    "10000",   # rows per JDBC fetch
        }

        # Partitioned read cho bảng lớn (website_sessions, website_pageviews)
        # Spark sẽ đọc song song nhiều partitions thay vì 1 luồng tuần tự
        large_tables = {"website_sessions", "website_pageviews", "order_items"}
        if table_name in large_tables:
            # Dùng partitionColumn để Spark chia load thành numPartitions luồng
            read_options.update({
                "partitionColumn": _get_partition_column(table_name),
                "lowerBound":      "1",
                "upperBound":      "2000000",
                "numPartitions":   "4",
            })

        df = spark.read.format("jdbc").options(**read_options).load()

        # ── Validate row count ──────────────────────────────────────────────
        row_count = df.count()
        min_expected = TABLES[table_name]
        logger.info(f"[{table_name}] Row count: {row_count:,} (min expected: {min_expected:,})")

        if row_count < min_expected:
            raise ValueError(
                f"[{table_name}] Data quality FAIL: got {row_count:,} rows, "
                f"expected >= {min_expected:,}. Aborting write."
            )

        # ── Add metadata columns ────────────────────────────────────────────
        df = (
            df
            .withColumn("_extracted_at", current_timestamp())
            .withColumn("_run_date", lit(run_date))
            .withColumn("_source_table", lit(f"{SQL_SERVER_SCHEMA}.{table_name}"))
        )

        # ── Write Parquet ───────────────────────────────────────────────────
        logger.info(f"[{table_name}] Writing Parquet to: {output_path}")
        (
            df.coalesce(1)          # 1 file per table — phù hợp scale nhỏ
            .write
            .mode("overwrite")      # idempotent: chạy lại cùng ngày sẽ overwrite
            .parquet(output_path)
        )

        logger.info(f"[{table_name}] ✅ Extraction complete. {row_count:,} rows → {output_path}")

        result = {
            "table":       table_name,
            "run_date":    run_date,
            "row_count":   row_count,
            "output_path": output_path,
            "status":      "success",
        }
        return result

    except Exception as e:
        logger.error(f"[{table_name}] ❌ Extraction FAILED: {e}")
        raise
    finally:
        spark.stop()


def _get_partition_column(table_name: str) -> str:
    """Map table → column phù hợp để Spark parallel-read."""
    mapping = {
        "website_sessions":   "website_session_id",
        "website_pageviews":  "website_pageview_id",
        "order_items":        "order_item_id",
        "orders":             "order_id",
        "order_item_refunds": "order_item_refund_id",
        "products":           "product_id",
    }
    return mapping.get(table_name, "1")   # fallback (products không cần partition)


# ─── Validation Function ───────────────────────────────────────────────────────

def validate_extraction(**context) -> None:
    """
    Pull XCom results từ tất cả extraction tasks, tổng hợp report.
    Fail DAG nếu bất kỳ table nào không đạt.
    """
    logger = logging.getLogger(__name__)
    ti = context["ti"]
    run_date = context["ds"]   # YYYY-MM-DD string

    logger.info("=" * 60)
    logger.info("EXTRACTION VALIDATION REPORT")
    logger.info(f"Run date: {run_date}")
    logger.info("=" * 60)

    failed_tables = []
    total_rows = 0

    for table_name in TABLES:
        task_id = f"extract_{table_name}"
        result = ti.xcom_pull(task_ids=task_id)

        if result is None:
            logger.error(f"  ❌ {table_name:<30} No XCom data (task may have failed)")
            failed_tables.append(table_name)
            continue

        status    = result.get("status", "unknown")
        row_count = result.get("row_count", 0)
        total_rows += row_count

        icon = "✅" if status == "success" else "❌"
        logger.info(f"  {icon} {table_name:<30} {row_count:>10,} rows  [{status}]")

        if status != "success":
            failed_tables.append(table_name)

    logger.info("-" * 60)
    logger.info(f"  Total rows extracted: {total_rows:,}")
    logger.info("=" * 60)

    if failed_tables:
        raise ValueError(
            f"Extraction FAILED for tables: {failed_tables}. "
            "Check individual task logs for details."
        )

    logger.info("✅ All tables extracted successfully!")


# ─── DAG Definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="commerce_analytics_extraction_dag",
    default_args=default_args,
    description="Phase 1b: Extract 6 tables from SQL Server → Parquet via PySpark",
    schedule="0 1 * * *",  # Daily @ 01:00 UTC = 08:00 ICT
    start_date=datetime(2025, 1, 1),
    catchup=False,                   # Không chạy backfill lịch sử
    max_active_runs=1,               # Tránh concurrent runs
    tags=["commerce-analytics", "extraction", "phase-1", "pyspark"],
) as dag:

    # ── Boundary tasks ──────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Extraction tasks ────────────────────────────────────────────────────
    # Dùng dictionary comprehension để tạo task cho mỗi table
    extraction_tasks = {
        table_name: PythonOperator(
            task_id=f"extract_{table_name}",
            python_callable=extract_table,
            op_kwargs={
                "table_name": table_name,
                "run_date":   "{{ ds }}",   # Airflow template: YYYY-MM-DD
            },
            # Override retry cho bảng lớn — cần thêm thời gian
            execution_timeout=timedelta(hours=3) if table_name in {
                "website_sessions", "website_pageviews"
            } else timedelta(hours=1),
        )
        for table_name in TABLES
    }

    # ── Validation task ─────────────────────────────────────────────────────
    validate = PythonOperator(
        task_id="validate_extraction",
        python_callable=validate_extraction,
        trigger_rule=TriggerRule.ALL_DONE,   # chạy dù upstream fail để có đầy đủ report
    )

    # ── Task Dependencies ───────────────────────────────────────────────────
    #
    # Wave 1 (nhỏ, <1M rows) — chạy parallel ngay sau start:
    #   orders, order_items, order_item_refunds, products
    #
    # Wave 2 (lớn) — chạy sau Wave 1 hoàn tất (tránh OOM Spark):
    #   website_sessions, website_pageviews
    #
    # Sơ đồ:
    #   start ──► [orders, order_items, order_item_refunds, products]  ──► wave2_gate
    #                                                                         │
    #                                                             [website_sessions]
    #                                                             [website_pageviews]
    #                                                                         │
    #                                                               validate ──► end

    wave1_tables = ["orders", "order_items", "order_item_refunds", "products"]
    wave2_tables = ["website_sessions", "website_pageviews"]

    wave2_gate = EmptyOperator(task_id="wave2_gate")

    # start → Wave 1 tasks
    for t in wave1_tables:
        start >> extraction_tasks[t]

    # Wave 1 tasks → wave2_gate
    for t in wave1_tables:
        extraction_tasks[t] >> wave2_gate

    # wave2_gate → Wave 2 tasks
    for t in wave2_tables:
        wave2_gate >> extraction_tasks[t]

    # All extraction tasks → validate → end
    for t in TABLES:
        extraction_tasks[t] >> validate

    validate >> end