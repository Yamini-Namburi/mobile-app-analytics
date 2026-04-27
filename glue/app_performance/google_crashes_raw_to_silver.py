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
    source_system="google",
    report_type="crashes",
    report_month=report_month,
)

output_path = build_silver_path(
    bucket_name=bucket_name,
    domain="app_performance",
    source_system="google",
    report_type="crashes",
)

df = spark.read.option("header", True).csv(input_path)

validate_required_columns(
    df,
    ["Report Date", "Package Name", "Country", "Crash Rate", "ANR Rate"],
    "google_crashes",
)
validate_not_empty(df, "google_crashes")

final_df = (
    df
    .withColumn("source_system", lit("google"))
    .withColumn("report_type", lit("crashes"))
    .withColumn("app_id", col("Package Name"))
    .withColumn("app_name", col("Package Name"))
    .withColumn("metric_date", to_date(col("Report Date"), "yyyy-MM-dd"))
    .withColumn("country_code", col("Country"))
    .withColumn("crash_rate", col("Crash Rate").cast("double"))
    .withColumn("anr_rate", col("ANR Rate").cast("double"))
    .withColumn("year", year(col("metric_date")))
    .withColumn("month", month(col("metric_date")))
    .select(
        "report_type",
        "app_id",
        "app_name",
        "metric_date",
        "country_code",
        "crash_rate",
        "anr_rate",
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