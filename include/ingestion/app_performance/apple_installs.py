from io import BytesIO

import boto3
from io import BytesIO

import boto3
import requests

MOCK_API_URL = "http://host.docker.internal:8001/apple/reports/installs"

S3_BUCKET = "mobile-app-analytics-dev-yourname"
S3_KEY = "raw/app_performance/apple/installs/report_month=202604/apple_installs_202604.csv"
AWS_REGION = "eu-north-1"


def main():
    response = requests.get(MOCK_API_URL, timeout=60)
    response.raise_for_status()

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_fileobj(BytesIO(response.content), S3_BUCKET, S3_KEY)

    print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()