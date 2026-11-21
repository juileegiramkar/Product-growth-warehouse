"""CLI: write raw daily partitions to the data lake.

  python -m generator.generate --date 2026-09-01            # one day (Airflow)
  python -m generator.generate --start 2026-08-21 --end 2026-11-18
  python -m generator.generate --all                         # SIM_START + SIM_DAYS
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, timedelta
from pathlib import Path

from . import config as C
from .simulation import day_index
from .writer import write_day


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--date", type=date.fromisoformat)
    ap.add_argument("--start", type=date.fromisoformat)
    ap.add_argument("--end", type=date.fromisoformat)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--raw-dir", type=Path,
                    default=Path(os.getenv("PULSE_RAW_DIR", "data/lake/raw")))
    a = ap.parse_args()

    if a.date:
        days = [a.date]
    elif a.all or (a.start and a.end):
        start = a.start or C.SIM_START
        end = a.end or (C.SIM_START + timedelta(days=C.SIM_DAYS - 1))
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    else:
        ap.error("pass --date, --start/--end or --all")

    totals = {"events": 0, "users": 0}
    t0 = time.time()
    for d in days:
        idx = day_index(d)
        if idx < 0:
            print(f"skip {d}: before simulation start {C.SIM_START}")
            continue
        stats = write_day(idx, a.raw_dir)
        totals["events"] += stats["events"]
        totals["users"] += stats["users"]
        print(json.dumps(stats))
    print(json.dumps({"days": len(days), **totals, "seconds": round(time.time() - t0, 1)}))


if __name__ == "__main__":
    main()
