from pathlib import Path
import requests

MOCK_API_URL = "http://127.0.0.1:8000/google/reports/estimated-sales"
OUTPUT_FILE = Path("data/raw/revenue/google/estimated_sales/report_month=202604/google_sales.csv")


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(MOCK_API_URL, timeout=60)
    response.raise_for_status()

    OUTPUT_FILE.write_bytes(response.content)

    print(f"Saved raw file to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()