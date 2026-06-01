# ================================================================
# spark/jobs/extract.py
# ================================================================
# Nhiệm vụ: Đọc 6 bảng từ SQL Server bằng PySpark JDBC
#           Validate dữ liệu (row count, null check, schema)
#           Lưu ra file Parquet trong data/raw/
#
# Chạy lệnh:
#   docker exec spark-master /opt/spark/bin/spark-submit \
#     --jars /opt/spark-project/jars/mssql-jdbc-13.4.0.jre11.jar \
#     /opt/spark-project/jobs/extract.py
# ================================================================

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

# ── Cấu hình kết nối SQL Server ──────────────────────────────
MSSQL_HOST     = os.getenv("SQLSERVER_HOST", "45.124.94.158")
MSSQL_PORT     = os.getenv("SQLSERVER_PORT", "1433")
MSSQL_DB       = os.getenv("SQLSERVER_DB",   "xomdata_dataset")
MSSQL_USER     = os.getenv("SQLSERVER_USER", "thuhuyenftu2")
MSSQL_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "")
MSSQL_SCHEMA   = "web_analytics"

# ── Đường dẫn output (bên trong container) ───────────────────
OUTPUT_DIR = "/opt/spark-project/raw"

# ── 6 bảng cần extract ───────────────────────────────────────
TABLES = [
    "orders",
    "order_items",
    "order_item_refunds",
    "products",
    "website_sessions",
    "website_pageviews",
]

# ── JDBC URL ──────────────────────────────────────────────────
JDBC_URL = (
    f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};"
    f"databaseName={MSSQL_DB};"
    f"trustServerCertificate=true;"
    f"encrypt=true;"
)

JDBC_PROPERTIES = {
    "user":     MSSQL_USER,
    "password": MSSQL_PASSWORD,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "fetchSize": "10000",
}


def create_spark_session() -> SparkSession:
    """
    Tạo SparkSession.
    JAR được truyền qua --jars khi spark-submit nên không cần
    config thêm ở đây.
    """
    spark = (
        SparkSession.builder
        .appName("FuzzyFactory_Extraction")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        # Giảm log noise
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootLogger=WARN")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ SparkSession started | version: {spark.version}")
    return spark


def extract_table(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Đọc 1 bảng từ SQL Server qua JDBC.

    Tham số:
        spark      : SparkSession đang chạy
        table_name : tên bảng trong schema web_analytics

    Trả về:
        DataFrame chứa toàn bộ dữ liệu của bảng
    """
    full_table = f"{MSSQL_SCHEMA}.{table_name}"
    print(f"\n📥 Extracting: {full_table}")

    df = spark.read.jdbc(
        url=JDBC_URL,
        table=full_table,
        properties=JDBC_PROPERTIES,
    )
    return df


def validate(df: DataFrame, table_name: str) -> bool:
    """
    Kiểm tra cơ bản sau khi đọc dữ liệu:
    1. Row count > 0
    2. In ra schema để kiểm tra kiểu dữ liệu
    3. Kiểm tra null ở cột đầu tiên (thường là ID)

    Trả về True nếu pass, False nếu fail.
    """
    row_count = df.count()
    col_count = len(df.columns)

    print(f"   📊 Rows: {row_count:,}  |  Columns: {col_count}")
    print(f"   📋 Schema:")
    df.printSchema()

    # Kiểm tra row count
    if row_count == 0:
        print(f"   ❌ FAIL: {table_name} has 0 rows!")
        return False

    # Kiểm tra null ở cột ID (cột đầu tiên)
    id_col = df.columns[0]
    null_count = df.filter(df[id_col].isNull()).count()
    if null_count > 0:
        print(f"   ⚠️  WARNING: {null_count} null values in {id_col}")
    else:
        print(f"   ✅ No nulls in {id_col}")

    # Show 3 dòng mẫu
    print(f"   👀 Sample data:")
    df.show(3, truncate=True)

    return True


def save_parquet(df: DataFrame, table_name: str) -> str:
    """
    Lưu DataFrame thành file Parquet.

    Tại sao Parquet?
    - Lưu theo cột (columnar) → query nhanh hơn CSV nhiều lần
    - Tự động nén → file nhỏ hơn
    - Giữ nguyên kiểu dữ liệu (int, date, float...)
    - Đây là format chuẩn trong data engineering

    Trả về đường dẫn đã lưu.
    """
    # Thêm timestamp vào folder để track theo ngày chạy
    run_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{OUTPUT_DIR}/{table_name}/run_date={run_date}"

    # coalesce(1) → gộp thành 1 file duy nhất
    # Với dataset nhỏ (~500K rows) thì 1 file là ổn
    # Dataset lớn hơn thì bỏ coalesce để Spark tự chia
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    print(f"   💾 Saved to: {output_path}")
    return output_path


def run():
    """
    Hàm chính: chạy toàn bộ pipeline extraction.
    """
    print("=" * 60)
    print("  Fuzzy Factory — Phase 1: Data Extraction")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    spark = create_spark_session()

    results = []

    for table in TABLES:
        try:
            # 1. Extract
            df = extract_table(spark, table)

            # 2. Validate
            is_valid = validate(df, table)

            if not is_valid:
                results.append((table, "FAILED", "validation error"))
                continue

            # 3. Save
            path = save_parquet(df, table)
            results.append((table, "SUCCESS", path))

        except Exception as e:
            print(f"   ❌ ERROR extracting {table}: {str(e)}")
            results.append((table, "FAILED", str(e)))

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  EXTRACTION SUMMARY")
    print("=" * 60)
    for table, status, info in results:
        icon = "✅" if status == "SUCCESS" else "❌"
        print(f"  {icon}  {table:<25} {status}")

    failed = [r for r in results if r[1] == "FAILED"]
    if failed:
        print(f"\n  ⚠️  {len(failed)} table(s) failed.")
        spark.stop()
        sys.exit(1)
    else:
        print(f"\n  🎉 All {len(TABLES)} tables extracted successfully!")

    spark.stop()


if __name__ == "__main__":
    run()