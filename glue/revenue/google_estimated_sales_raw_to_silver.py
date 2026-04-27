import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, substring, to_date, year, month

from utils import validate_required_columns, validate_not_empty, build_raw_path, build_silver_path

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bucket_name",
        "report_month"
    ]
)

bucket_name = args["bucket_name"]
report_month = args["report_month"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_path = build_raw_path(
    bucket_name=bucket_name,
    domain="revenue",
    source_system="google",
    report_type="estimated_sales",
    report_month=report_month
)

output_path = build_silver_path(
    bucket_name=bucket_name,
    domain="revenue",
    source_system="google",
    report_type="estimated_sales"
)

df = spark.read.option("header", True).csv(input_path)

validate_required_columns(
    df,
    [
        "Order Number",
        "Order Charged Date",
        "Package Name",
        "Product Title",
        "Product Type",
        "Country of Buyer",
        "Currency of Sale",
        "Amount (Buyer Currency)",
        "Amount (Merchant Currency)",
    ],
    "google_estimated_sales"
)
validate_not_empty(df, "google_estimated_sales")

final_df = (
    df
    .withColumn("source_system", lit("google"))
    .withColumn("report_type", lit("estimated_sales"))
    .withColumn("app_id", col("Package Name"))
    .withColumn("app_name", col("Package Name"))
    .withColumn("product_id", col("Product Title"))
    .withColumn("product_name", col("Product Title"))
    .withColumn("transaction_date", to_date(substring(col("Order Charged Date"), 1, 10), "yyyy-MM-dd"))
    .withColumn("country_code", col("Country of Buyer"))
    .withColumn("currency_code", col("Currency of Sale"))
    .withColumn("units", lit(1).cast("int"))
    .withColumn("gross_amount", col("Amount (Buyer Currency)").cast("double"))
    .withColumn("net_amount", col("Amount (Merchant Currency)").cast("double"))
    .withColumn("year", year(col("transaction_date")))
    .withColumn("month", month(col("transaction_date")))
    .select(
        "report_type",
        "app_id",
        "app_name",
        "product_id",
        "product_name",
        "transaction_date",
        "country_code",
        "currency_code",
        "units",
        "gross_amount",
        "net_amount",
        "source_system",
        "year",
        "month"
    )
)

(
    final_df
    .write
    .mode("overwrite")
    .partitionBy("source_system", "year", "month")
    .parquet(output_path)
)

job.commit()