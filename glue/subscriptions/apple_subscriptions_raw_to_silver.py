import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit

from utils import (
    validate_required_columns,
    validate_not_empty,
    build_raw_path,
    build_silver_path,
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket_name", "report_month"]
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
    domain="subscriptions",
    source_system="apple",
    report_type="subscriptions",
    report_month=report_month,
)

output_path = build_silver_path(
    bucket_name=bucket_name,
    domain="subscriptions",
    source_system="apple",
    report_type="subscriptions",
)

df = spark.read.option("header", True).csv(input_path)

validate_required_columns(
    df,
    [
        "Report Month",
        "App Name",
        "Subscription SKU",
        "Country Code",
        "Active Subscribers",
        "New Subscribers",
        "Developer Proceeds",
    ],
    "apple_subscriptions",
)
validate_not_empty(df, "apple_subscriptions")

final_df = (
    df
    .withColumn("source_system", lit("apple"))
    .withColumn("report_type", lit("subscriptions"))
    .withColumn("report_month", col("Report Month"))
    .withColumn("app_id", col("App Name"))
    .withColumn("app_name", col("App Name"))
    .withColumn("subscription_sku", col("Subscription SKU"))
    .withColumn("country_code", col("Country Code"))
    .withColumn("active_subscribers", col("Active Subscribers").cast("int"))
    .withColumn("new_subscribers", col("New Subscribers").cast("int"))
    .withColumn("developer_proceeds", col("Developer Proceeds").cast("double"))
    .select(
        "report_type",
        "report_month",
        "app_id",
        "app_name",
        "subscription_sku",
        "country_code",
        "active_subscribers",
        "new_subscribers",
        "developer_proceeds",
        "source_system",
    )
)

(
    final_df
    .write
    .mode("overwrite")
    .partitionBy("source_system", "report_month")
    .parquet(output_path)
)

job.commit()