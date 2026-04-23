from io import BytesIO
import requests
import boto3

MOCK_API_URL = "http://127.0.0.1:8001/google/reports/earnings"

S3_BUCKET = "mobile-app-analytics-dev-yourname"
S3_KEY = "raw/revenue/google/earnings/report_month=202604/google_earnings.csv"
AWS_REGION = "eu-north-1"

def main():
    response = requests.get(MOCK_API_URL, timeout=60)
    response.raise_for_status()

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_fileobj(BytesIO(response.content), S3_BUCKET, S3_KEY)

    print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")

if __name__ == "__main__":
    main()