import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
INT_ORDERS_WITH_PAYMENTS_PATH = f"s3a://{BUCKET}/intermediate/int_orders_with_payments"


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

df_orders = (
    spark.read.parquet(f"s3a://{BUCKET}/staging/stg_orders")
    .filter(
        (F.col("order_date") >= start_date) & (F.col("order_date") < load_date)
    )
)

df_payments = (
    spark.read.parquet(f"s3a://{BUCKET}/staging/stg_payments")
    .filter(
        ((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date))
        | F.col("paid_dt").isNull()
    )
)

df_order_payments = (
    df_payments
         .groupBy("order_id").agg(
            F.count(F.when((F.col("payment_status") == "success") & (~F.col("is_refund")), 1))
            .alias("successful_payments_count"),

        F.coalesce(
            F.sum(
                F.when((F.col("payment_status") == "success") & (~F.col("is_refund")), 
                    F.col("amount"))),
                    F.lit(0)
                ).alias("paid_amount"),

        F.coalesce(
            -F.sum(
                F.when(
                    F.col("is_refund"), F.col("amount"))),
                    F.lit(0)
                ).alias("refunded_amount"),

        F.min(
            F.when(
                (F.col("payment_status") == "success") & 
                (~F.col("is_refund")),
                F.col("paid_at"),
            )
        ).alias("first_paid_at"),

        F.count(
            F.when(F.col("payment_status") == "success", 1)
                ).alias("success_operations_count"),

        F.count(
            F.when(F.col("payment_status") == "failed", 1)
                ).alias("failed_operations_count"),

        F.count(
            F.when(F.col("payment_status") == "pending", 1)
                ).alias("pending_operations_count")
         )
)

df_join = (
    df_orders.join(
        df_order_payments,
        on="order_id",
        how="left"
    )
)

orders_with_payments = (
    df_join
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("ordered_at"),
        F.col("order_date"),
        F.col("order_status"),
        F.col("order_amount"),
        F.col("is_test_order"),
        F.coalesce(F.col("successful_payments_count"), F.lit(0)).alias("successful_payments_count"),
        F.coalesce(F.col("paid_amount"), F.lit(0)).alias("paid_amount"),
        F.coalesce(F.col("refunded_amount"), F.lit(0)).alias("refunded_amount"),

        (F.coalesce(F.col("paid_amount"), F.lit(0)) 
            - F.coalesce(F.col("refunded_amount"), F.lit(0))
        ).alias("net_paid_amount"),

        F.col("first_paid_at"),

        ((F.col("order_status") == "paid")
            & (F.coalesce(F.col("successful_payments_count"), 
                F.lit(0)) > 0)
        ).alias("is_confirmed"),

        F.coalesce(F.col("success_operations_count"), F.lit(0)).alias("success_operations_count"),
        F.coalesce(F.col("failed_operations_count"), F.lit(0)).alias("failed_operations_count"),
        F.coalesce(F.col("pending_operations_count"), F.lit(0)).alias("pending_operations_count")
        )
)

(
    orders_with_payments.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(INT_ORDERS_WITH_PAYMENTS_PATH)
)

print(f"Сохранено: {INT_ORDERS_WITH_PAYMENTS_PATH}")
