import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, to_date, year, month

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
    domain="app_performance",
    source_system="apple",
    report_type="installs",
    report_month=report_month,
)

output_path = build_silver_path(
    bucket_name=bucket_name,
    domain="app_performance",
    source_system="apple",
    report_type="installs",
)

df = spark.read.option("header", True).csv(input_path)

validate_required_columns(
    df,
    ["Report Date", "App Name", "Country Code", "App Units"],
    "apple_installs",
)
validate_not_empty(df, "apple_installs")

final_df = (
    df
    .withColumn("source_system", lit("apple"))
    .withColumn("report_type", lit("installs"))
    .withColumn("app_id", col("App Name"))
    .withColumn("app_name", col("App Name"))
    .withColumn("transaction_date", to_date(col("Report Date"), "yyyy-MM-dd"))
    .withColumn("country_code", col("Country Code"))
    .withColumn("installs", col("App Units").cast("int"))
    .withColumn("year", year(col("transaction_date")))
    .withColumn("month", month(col("transaction_date")))
    .select(
        "report_type",
        "app_id",
        "app_name",
        "transaction_date",
        "country_code",
        "installs",
        "source_system",
        "year",
        "month",
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