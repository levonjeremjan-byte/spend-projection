"""
CZ Spend Projection — March 2026
─────────────────────────────────
To run: python3 run_cz_march_2026.py

Monthly workflow:
  1. Update AM files in CZ/AM Individual Campaign Files/ with new weeks
  2. Download fresh daily_campaign_spend CSV → CZ/daily_campaign_spend.csv
  3. Refresh GMV:  python3 fetch_gmv.py --country cz --start 2025-09-01 --output CZ/daily_gmv.csv
  4. Copy this file, update target_weeks, and run

To replicate for another country:
  1. Copy this file to e.g. run_ee_march_2026.py
  2. Change country_code, paths, exclude_ams, target_weeks
  3. Run
"""

from pathlib import Path
from spend_projection import run

ROOT = Path(__file__).parent

CONFIG = {
    # ── Country ──────────────────────────────────────────────
    "country_code": "CZ",
    "country_name": "Czechia",
    "projection_label": "March 2026",
    "year": 2026,

    # ── Weeks to project (ISO week numbers) ──────────────────
    # March 2026 = W10–W13
    "target_weeks": [10, 11, 12, 13],

    # ── Error margin (displayed on summary rows) ─────────────
    "error_margin": 0.25,

    # ── File paths ───────────────────────────────────────────
    "am_files_dir": str(ROOT / "AM Individual Campaign Files"),
    "historical_csv": str(ROOT / "daily_campaign_spend (4).csv"),
    "gmv_csv": str(ROOT / "cz_daily_gmv.csv"),
    "output_dir": str(ROOT / "output"),

    # ── Exclude from projection ──────────────────────────────
    # AMs whose campaigns are NOT part of AM budget
    "exclude_ams": [
        "Klára Bradová",   # BDM retail — not AM budget
    ],
    # Spend objectives to exclude (locations = 100% provider-funded)
    "exclude_spend_objectives": [
        "provider_campaign_locations",
    ],
}

if __name__ == "__main__":
    run(CONFIG)
