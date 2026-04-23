from pathlib import Path
import requests

MOCK_API_URL = "http://127.0.0.1:8000/apple/reports/sales"
OUTPUT_FILE = Path("data/raw/revenue/apple/sales/report_date=2026-04-20/apple_sales.csv")


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(MOCK_API_URL, timeout=60)
    response.raise_for_status()

    OUTPUT_FILE.write_bytes(response.content)

    print(f"Saved raw file to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()