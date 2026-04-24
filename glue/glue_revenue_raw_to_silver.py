from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, substring, to_date, year, month

spark = SparkSession.builder.appName("glue_revenue_raw_to_silver").getOrCreate()


# CHANGE ONLY IF YOUR BUCKET NAME IS DIFFERENT

BUCKET = "mobile-app-analytics-dev-yourname"


# S3 RAW INPUT PATHS

GOOGLE_SALES_PATH = f"s3://{BUCKET}/raw/revenue/google/estimated_sales/report_month=202604/google_sales.csv"
GOOGLE_EARNINGS_PATH = f"s3://{BUCKET}/raw/revenue/google/earnings/report_month=202604/google_earnings.csv"
APPLE_SALES_PATH = f"s3://{BUCKET}/raw/revenue/apple/sales/report_date=2026-04-20/apple_sales.csv"
APPLE_FINANCE_PATH = f"s3://{BUCKET}/raw/revenue/apple/finance/report_month=2026-03/apple_finance.csv"


# S3 SILVER OUTPUT PATH

SILVER_OUTPUT_PATH = f"s3://{BUCKET}/silver/revenue/"


# READ RAW CSV FILES

google_sales_df = spark.read.option("header", True).csv(GOOGLE_SALES_PATH)
google_earnings_df = spark.read.option("header", True).csv(GOOGLE_EARNINGS_PATH)
apple_sales_df = spark.read.option("header", True).csv(APPLE_SALES_PATH)
apple_finance_df = spark.read.option("header", True).csv(APPLE_FINANCE_PATH)


# VALIDATION HELPERS

def validate_required_columns(df, required_cols, dataset_name):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise Exception(f"{dataset_name} missing columns: {missing}")

def validate_not_empty(df, dataset_name):
    if df.limit(1).count() == 0:
        raise Exception(f"{dataset_name} is empty")


# VALIDATE RAW SCHEMAS

validate_required_columns(
    google_sales_df,
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
    "google_sales_df"
)
validate_not_empty(google_sales_df, "google_sales_df")

validate_required_columns(
    google_earnings_df,
    [
        "Transaction Date",
        "Package Name",
        "Description",
        "Transaction Type",
        "Country of Sale",
        "Buyer Currency",
        "Buyer Amount",
        "Merchant Currency",
        "Merchant Amount",
    ],
    "google_earnings_df"
)
validate_not_empty(google_earnings_df, "google_earnings_df")

validate_required_columns(
    apple_sales_df,
    [
        "Provider",
        "Provider Country",
        "SKU",
        "Developer",
        "Title",
        "Version",
        "Product Type Identifier",
        "Units",
        "Developer Proceeds",
        "Begin Date",
        "End Date",
        "Country Code",
        "Currency of Proceeds",
    ],
    "apple_sales_df"
)
validate_not_empty(apple_sales_df, "apple_sales_df")

validate_required_columns(
    apple_finance_df,
    [
        "Start Date",
        "End Date",
        "SKU",
        "Country Code",
        "Currency",
        "Units",
        "Customer Price",
        "Developer Proceeds",
    ],
    "apple_finance_df"
)
validate_not_empty(apple_finance_df, "apple_finance_df")


# STANDARDIZE GOOGLE ESTIMATED SALES

google_sales_std = (
    google_sales_df
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
    .select(
        "source_system",
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
        "net_amount"
    )
)


# STANDARDIZE GOOGLE EARNINGS

google_earnings_std = (
    google_earnings_df
    .withColumn("source_system", lit("google"))
    .withColumn("report_type", lit("earnings"))
    .withColumn("app_id", col("Package Name"))
    .withColumn("app_name", col("Package Name"))
    .withColumn("product_id", col("Description"))
    .withColumn("product_name", col("Description"))
    .withColumn("transaction_date", to_date(col("Transaction Date"), "yyyy-MM-dd"))
    .withColumn("country_code", col("Country of Sale"))
    .withColumn("currency_code", col("Buyer Currency"))
    .withColumn("units", lit(1).cast("int"))
    .withColumn("gross_amount", col("Buyer Amount").cast("double"))
    .withColumn("net_amount", col("Merchant Amount").cast("double"))
    .select(
        "source_system",
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
        "net_amount"
    )
)


# STANDARDIZE APPLE SALES

apple_sales_std = (
    apple_sales_df
    .withColumn("source_system", lit("apple"))
    .withColumn("report_type", lit("sales"))
    .withColumn("app_id", col("Title"))
    .withColumn("app_name", col("Title"))
    .withColumn("product_id", col("SKU"))
    .withColumn("product_name", col("SKU"))
    .withColumn("transaction_date", to_date(col("Begin Date"), "yyyy-MM-dd"))
    .withColumn("country_code", col("Country Code"))
    .withColumn("currency_code", col("Currency of Proceeds"))
    .withColumn("units", col("Units").cast("int"))
    .withColumn("gross_amount", lit(None).cast("double"))
    .withColumn("net_amount", col("Developer Proceeds").cast("double"))
    .select(
        "source_system",
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
        "net_amount"
    )
)

# STANDARDIZE APPLE FINANCE

apple_finance_std = (
    apple_finance_df
    .withColumn("source_system", lit("apple"))
    .withColumn("report_type", lit("finance"))
    .withColumn("app_id", lit("example_app"))
    .withColumn("app_name", lit("Example App"))
    .withColumn("product_id", col("SKU"))
    .withColumn("product_name", col("SKU"))
    .withColumn("transaction_date", to_date(col("Start Date"), "yyyy-MM-dd"))
    .withColumn("country_code", col("Country Code"))
    .withColumn("currency_code", col("Currency"))
    .withColumn("units", col("Units").cast("int"))
    .withColumn("gross_amount", col("Customer Price").cast("double"))
    .withColumn("net_amount", col("Developer Proceeds").cast("double"))
    .select(
        "source_system",
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
        "net_amount"
    )
)


# UNION ALL DATASETS

final_df = (
    google_sales_std
    .unionByName(google_earnings_std)
    .unionByName(apple_sales_std)
    .unionByName(apple_finance_std)
    .withColumn("year", year(col("transaction_date")))
    .withColumn("month", month(col("transaction_date")))
)


# WRITE PARTITIONED PARQUET TO SILVER

(
    final_df
    .write
    .mode("overwrite")
    .partitionBy("source_system", "year", "month")
    .parquet(SILVER_OUTPUT_PATH)
)

print(f"Silver parquet written to {SILVER_OUTPUT_PATH}")

spark.stop()