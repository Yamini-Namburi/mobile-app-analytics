def validate_required_columns(df, required_cols, dataset_name):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise Exception(f"{dataset_name} missing columns: {missing}")


def validate_not_empty(df, dataset_name):
    if df.limit(1).count() == 0:
        raise Exception(f"{dataset_name} is empty")


def build_raw_path(bucket_name, domain, source_system, report_type, report_month):
    return f"s3://{bucket_name}/raw/{domain}/{source_system}/{report_type}/report_month={report_month}/"


def build_silver_path(bucket_name, domain, source_system, report_type):
    return f"s3://{bucket_name}/silver/{domain}/{source_system}/{report_type}/"