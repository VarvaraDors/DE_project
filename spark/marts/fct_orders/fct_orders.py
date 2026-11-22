import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
FCT_ORDERS_PATH = f"s3a://{BUCKET}/marts/fct_orders"


spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # перезапись только затронутых партиций

    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, help="Дата запуска загрузки, ГГГГ-ММ-ДД")
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

df_orders_with_payments = (
    spark.read.parquet(
        f"s3a://{BUCKET}/intermediate/int_orders_with_payments"
    )
    .filter(
        (F.col("order_date") >= start_date)
        & (F.col("order_date") < load_date)
    )
)
df_fct_orders = (
    df_orders_with_payments.filter(~F.col("is_test_order"))
)
df_fct_orders = (
    df_fct_orders
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "ordered_at",
        "order_status",
        "order_amount",
        "paid_amount",
        "refunded_amount",
        "net_paid_amount",
        "successful_payments_count",
        "first_paid_at",
        "is_confirmed"
    )
)

(
    df_fct_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(FCT_ORDERS_PATH)
)

print(f"Сохранено: {FCT_ORDERS_PATH}")