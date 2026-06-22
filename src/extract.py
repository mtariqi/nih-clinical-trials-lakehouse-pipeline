import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests

from config import (
    API_BASE_URL, API_PAGE_SIZE, API_FIELDS,
    DATA_RAW_DIR, WATERMARK_FILE,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _load_watermark() -> str:
    """Return last successful extract date or 30 days ago as default."""
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            return f.read().strip()
    return (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")


def _save_watermark(date_str: str) -> None:
    Path(WATERMARK_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        f.write(date_str)


def fetch_trials(min_date: str | None = None) -> list[dict]:
    """
    Incrementally fetch studies updated since min_date.
    Returns a flat list of study dicts.
    """
    min_date = min_date or _load_watermark()
    logger.info(f"Fetching trials updated since {min_date}")

    params = {
        "format": "json",
        "pageSize": API_PAGE_SIZE,
        "fields": ",".join(API_FIELDS),
        "filter.advanced": f"AREA[LastUpdatePostDate]RANGE[{min_date},MAX]",
        "sort": "LastUpdatePostDate:asc",
    }

    all_studies = []
    page_token = None

    while True:
        if page_token:
            params["pageToken"] = page_token

        response = requests.get(API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        studies = payload.get("studies", [])
        all_studies.extend(studies)
        logger.info(f"  Fetched {len(studies)} studies (total: {len(all_studies)})")

        page_token = payload.get("nextPageToken")
        if not page_token:
            break

    return all_studies


def save_raw(studies: list[dict], run_date: str | None = None) -> str:
    """
    Archive raw JSON to data/raw/YYYY/MM/DD/studies.json.
    Returns the output path.
    """
    run_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")
    year, month, day = run_date.split("-")
    out_dir = Path(DATA_RAW_DIR) / year / month / day
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "studies.json"
    with open(out_path, "w") as f:
        json.dump({"run_date": run_date, "count": len(studies), "studies": studies}, f)

    logger.info(f"Saved {len(studies)} raw studies to {out_path}")
    return str(out_path)


def extract(run_date: str | None = None) -> str:
    """Full extract step: fetch + save + update watermark."""
    run_date = run_date or datetime.utcnow().strftime("%Y-%m-%d")
    studies = fetch_trials()
    if not studies:
        logger.warning("No new studies found — skipping save.")
        return ""

    out_path = save_raw(studies, run_date)
    _save_watermark(run_date)
    return out_path


if __name__ == "__main__":
    path = extract()
    print(f"Extraction complete: {path}")
