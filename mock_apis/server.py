from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

app = FastAPI(title="Mock Revenue APIs")

BASE_DIR = Path(__file__).resolve().parent.parent
SAMPLE_DIR = BASE_DIR / "sample_reports"


def file_or_404(path: Path) -> Path:
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    return path


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/google/reports/estimated-sales")
def google_estimated_sales():
    file_path = SAMPLE_DIR / "google" / "estimated_sales" / "google_sales_202604.csv"
    return FileResponse(file_or_404(file_path), filename="google_sales_202604.csv", media_type="text/csv")


@app.get("/google/reports/earnings")
def google_earnings():
    file_path = SAMPLE_DIR / "google" / "earnings" / "google_earnings_202604.csv"
    return FileResponse(file_or_404(file_path), filename="google_earnings_202604.csv", media_type="text/csv")


@app.get("/apple/reports/sales")
def apple_sales():
    file_path = SAMPLE_DIR / "apple" / "sales" / "apple_sales_2026-04-20.csv"
    return FileResponse(file_or_404(file_path), filename="apple_sales_2026-04-20.csv", media_type="text/csv")


@app.get("/apple/reports/finance")
def apple_finance():
    file_path = SAMPLE_DIR / "apple" / "finance" / "apple_finance_2026-03.csv"
    return FileResponse(file_or_404(file_path), filename="apple_finance_2026-03.csv", media_type="text/csv")