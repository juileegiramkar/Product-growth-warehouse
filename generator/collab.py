"""Second event set: "Canvas", a collaborative design tool (Figma-style).

Same lake layout and the same kinds of messiness as Pulse, so the identical
Spark -> dbt pattern applies:

    python -m generator.collab --all
    -> data/lake/raw/collab_events/date=YYYY-MM-DD/part.parquet

Event types: file_open, file_edit, comment, invite_sent, invite_accepted.
Planted pattern: teams where an invite is accepted in week 1 keep editing.
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import config as C
from .writer import write_parquet_atomic

N_TEAMS = 1_500
SCHEMA = pa.schema([("event_id", pa.string()), ("event_type", pa.string()), ("user_id", pa.int64()),
                    ("team_id", pa.int64()), ("file_id", pa.string()), ("target_user_id", pa.int64()),
                    ("ts", pa.string()), ("client", pa.string()), ("received_at", pa.string())])


def _teams() -> pd.DataFrame:
    r = np.random.default_rng([C.SEED, 9_000])
    n = N_TEAMS
    t = pd.DataFrame({
        "team_id": np.arange(1, n + 1),
        "created_day": r.integers(0, C.SIM_DAYS, n),
        "seats": 1 + r.poisson(3, n),
    })
    t["collaborative"] = r.random(n) < 0.45            # invite accepted in week 1
    t["lifetime_days"] = np.where(t.collaborative, r.exponential(120, n), r.exponential(12, n))
    t["first_user"] = 500_000 + np.cumsum(np.r_[0, t.seats.values[:-1] + 2])
    return t


def day_events(day_idx: int) -> pd.DataFrame:
    teams = _teams()
    r = np.random.default_rng([C.SEED, 9_001, day_idx])
    age = day_idx - teams.created_day.values
    alive = (age >= 0) & (age <= teams.lifetime_days.values)
    p_active = np.where(teams.collaborative.values, 0.75, 0.45)
    active = teams[alive & (r.random(len(teams)) < p_active)]
    d0 = (datetime.combine(C.SIM_START + timedelta(days=day_idx), datetime.min.time(),
                           tzinfo=timezone.utc).timestamp())
    rows = []
    for tm in active.itertuples():
        seats = tm.seats + (2 if tm.collaborative and day_idx - tm.created_day >= 3 else 0)
        n_users = max(1, r.binomial(seats, 0.6))
        users = tm.first_user + r.choice(seats, n_users, replace=False)
        for u in users:
            start = d0 + r.uniform(7, 20) * 3600
            n_ev = 1 + r.poisson(12 if tm.collaborative else 5)
            ts = start + np.cumsum(r.exponential(40, n_ev))
            types = r.choice(["file_open", "file_edit", "comment"], n_ev, p=[.12, .75, .13])
            types[0] = "file_open"
            fid = f"f{tm.team_id}-{r.integers(1, 6)}"
            for k in range(n_ev):
                rows.append((types[k], int(u), tm.team_id, fid, None, ts[k]))
        if day_idx - tm.created_day <= 7 and r.random() < 0.3:
            inviter = int(tm.first_user)
            invitee = int(tm.first_user + tm.seats + r.integers(0, 2))
            t_inv = d0 + r.uniform(9, 18) * 3600
            rows.append(("invite_sent", inviter, tm.team_id, None, invitee, t_inv))
            if tm.collaborative:
                rows.append(("invite_accepted", invitee, tm.team_id, None, inviter,
                             t_inv + r.uniform(60, 7200)))
    df = pd.DataFrame(rows, columns=["event_type", "user_id", "team_id", "file_id", "target_user_id", "ts"])
    if df.empty:
        return df
    n = len(df)
    df.insert(0, "event_id", [f"{a:016x}{b:016x}" for a, b in zip(r.integers(0, 2**63, n), r.integers(0, 2**63, n))])
    df["client"] = r.choice(["desktop", "web", None], n, p=[.55, .43, .02])
    dt = pd.to_datetime(df.ts, unit="s", utc=True)
    fmt = r.random(n)
    df["ts_out"] = np.where(fmt < .5, dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3] + "Z",
                            np.where(fmt < .9, (df.ts * 1000).round().astype("int64").astype(str),
                                     dt.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]))
    df["received_at"] = (dt + pd.to_timedelta(r.uniform(1, 30, n), unit="s")).dt.strftime(
        "%Y-%m-%dT%H:%M:%S.%f").str[:-3] + "Z"
    dups = df[r.random(n) < C.P_DUP_SAME_ID]                 # redeliveries
    df = pd.concat([df, dups], ignore_index=True)
    return pd.DataFrame({"event_id": df.event_id, "event_type": df.event_type, "user_id": df.user_id,
                         "team_id": df.team_id, "file_id": df.file_id,
                         "target_user_id": pd.array(df.target_user_id, dtype="Int64"),
                         "ts": df.ts_out, "client": df.client, "received_at": df.received_at})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=date.fromisoformat)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--raw-dir", type=Path, default=Path(os.getenv("PULSE_RAW_DIR", "data/lake/raw")))
    a = ap.parse_args()
    days = [a.date] if a.date else [C.SIM_START + timedelta(days=i) for i in range(C.SIM_DAYS)]
    total = 0
    for d in days:
        df = day_events((d - C.SIM_START).days)
        out = a.raw_dir / "collab_events" / f"date={d.isoformat()}"
        out.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False), out / "part.parquet")
        total += len(df)
    print({"days": len(days), "collab_events": total})


if __name__ == "__main__":
    main()
