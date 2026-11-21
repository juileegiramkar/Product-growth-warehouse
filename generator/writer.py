"""Turns clean simulated events into what the ingestion layer *receives*.

The raw partition for arrival day D contains:
  * events that happened on D and arrived on time
  * late events that happened on D-1 / D-2 and only arrived on D
  * exact redeliveries (same event_id), some of them a day later
  * client retries (new event_id, identical payload)
and every row carries a timestamp string in whatever format its client uses.
"""
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

from . import config as C
from .simulation import (DAY_S, core_events, day_start_ts, rng, users_for_day,
                         users_upto)

EVENT_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("user_id", pa.int64()),
    ("ts", pa.string()),                 # mixed formats on purpose
    ("device", pa.string()),
    ("app_version", pa.string()),
    ("client_session_id", pa.string()),
    ("step_name", pa.string()),
    ("post_id", pa.string()),
    ("content_type", pa.string()),
    ("target_user_id", pa.int64()),      # followee / author of liked post
    ("received_at", pa.string()),
])

USER_SCHEMA = pa.schema([
    ("user_id", pa.int64()),
    ("username", pa.string()),
    ("signup_ts", pa.string()),
    ("country", pa.string()),
    ("device", pa.string()),
    ("acquisition_channel", pa.string()),
    ("age_bucket", pa.string()),
    ("received_at", pa.string()),
])

MESSY_DEVICE = {"ios": "iOS", "android": "Android ", "web": "WEB"}
APP_VERSIONS = {"ios": ["5.11.2", "5.12.0", "5.12.1"],
                "android": ["5.10.4", "5.12.0", "5.12.3"],
                "web": ["2026.10", "2026.11", "2026.11"]}


def _iso_z(ts: pd.Series) -> pd.Series:
    dt = pd.to_datetime(ts, unit="s", utc=True)
    return dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3] + "Z"


def _format_ts(df: pd.DataFrame, country: pd.Series, device: pd.Series) -> pd.Series:
    ts = df["ts"].astype(float)
    out = pd.Series(index=df.index, dtype=object)
    dt = pd.to_datetime(ts, unit="s", utc=True)

    ios = device == "ios"
    if ios.any():  # local wall-clock time + explicit offset
        off = country[ios].map(C.COUNTRY_OFFSET_MIN).astype(int)
        local = dt[ios] + pd.to_timedelta(off, unit="m")
        sign = np.where(off >= 0, "+", "-")
        hh = (off.abs() // 60).astype(str).str.zfill(2)
        mm = (off.abs() % 60).astype(str).str.zfill(2)
        out[ios] = (local.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:-3]
                    + sign + hh + ":" + mm)
    andr = device == "android"
    epoch = andr & df["android_epoch"]
    out[epoch] = (ts[epoch] * 1000).round().astype("int64").astype(str)
    out[andr & ~epoch] = _iso_z(ts[andr & ~epoch])
    web = device == "web"
    out[web] = dt[web].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3]

    us = df["us_format"]
    out[us] = dt[us].dt.strftime("%m/%d/%Y %H:%M:%S")
    bad = df["bad_clock"]
    if bad.any():
        out[bad] = np.where(df.loc[bad, "event_id"].str[0] < "8",
                            "1970-01-01T00:00:" + (ts[bad] % 60).astype(int).astype(str).str.zfill(2) + ".000Z",
                            "2099-" + dt[bad].dt.strftime("%m-%dT%H:%M:%S.000Z"))
    return out


def raw_events_partition(day_idx: int) -> pd.DataFrame:
    frames = []
    for lag in (0, 1, 2):
        if day_idx - lag < 0:
            continue
        ev = core_events(day_idx - lag)
        on_time_or_late = ev[ev["delay_days"] == lag].copy()
        on_time_or_late["received_at_s"] = np.where(
            lag == 0, on_time_or_late["ts"] + on_time_or_late["receive_lag_s"],
            day_start_ts(day_idx) + on_time_or_late["late_receive_s"])
        frames.append(on_time_or_late)

    today = core_events(day_idx)
    same = today[today["dup_same_id"] & ~today["dup_next_day"] & (today["delay_days"] == 0)].copy()
    same["received_at_s"] = same["ts"] + same["receive_lag_s"] + 60
    retry = today[today["dup_retry"] & (today["delay_days"] == 0)].copy()
    retry["event_id"] = retry["retry_id"]
    retry["received_at_s"] = retry["ts"] + retry["receive_lag_s"] + 5
    frames += [same, retry]
    if day_idx >= 1:
        yday = core_events(day_idx - 1)
        nxt = yday[yday["dup_next_day"] & (yday["delay_days"] == 0)].copy()
        nxt["received_at_s"] = day_start_ts(day_idx) + nxt["late_receive_s"]
        frames.append(nxt)

    df = pd.concat(frames, ignore_index=True)
    users = users_upto(day_idx).set_index("user_id")
    device = df["user_id"].map(users["device"])
    country = df["user_id"].map(users["country"])
    df["ts_str"] = _format_ts(df, country, device)
    vidx = (df["user_id"] % 3).to_numpy()
    df["app_version"] = [APP_VERSIONS[d][i] for d, i in zip(device, vidx)]
    dev_out = device.astype(object).copy()
    dev_out[df["messy_device"]] = device[df["messy_device"]].map(MESSY_DEVICE)
    dev_out[df["null_device"]] = None
    df["device_out"] = dev_out
    df["received_at"] = _iso_z(df["received_at_s"])
    df = df.sort_values("received_at_s", kind="stable")

    out = pd.DataFrame({
        "event_id": df["event_id"],
        "event_type": df["event_type"],
        "user_id": df["user_id"].astype("int64"),
        "ts": df["ts_str"],
        "device": df["device_out"],
        "app_version": df["app_version"],
        "client_session_id": df["client_session_id"],
        "step_name": df["step_name"],
        "post_id": df["post_id"],
        "content_type": df["content_type"],
        "target_user_id": pd.array(df["target_user_id"], dtype="Int64"),
        "received_at": df["received_at"],
    })
    return out.reset_index(drop=True)


_FAKE = Faker()


def _usernames(user_ids) -> list[str]:
    """Deterministic per-user fake username (same user -> same name)."""
    names = []
    for uid in user_ids:
        _FAKE.seed_instance(C.SEED * 10_000_000 + int(uid))
        names.append(f"{_FAKE.user_name()}{int(uid) % 1000}")
    return names


def raw_users_partition(day_idx: int) -> pd.DataFrame:
    new = users_for_day(day_idx).copy()
    r = rng(day_idx, 77)
    new["username"] = _usernames(new["user_id"])
    new["received_at_s"] = new["signup_ts"] + r.uniform(1, 5, len(new))
    frames = [new]
    if day_idx >= 1:  # profile re-sent a day later with a changed device
        prev = users_for_day(day_idx - 1)
        pick = r.random(len(prev)) < C.P_USER_RESEND
        resend = prev[pick].copy()
        resend["username"] = _usernames(resend["user_id"])
        resend["device"] = np.where(resend["device"] == "web", "ios", "web")
        resend["received_at_s"] = day_start_ts(day_idx) + r.uniform(0, DAY_S, len(resend))
        frames.append(resend)
    df = pd.concat(frames, ignore_index=True)
    n = len(df)
    signup = pd.to_datetime(df["signup_ts"], unit="s", utc=True)
    alt_fmt = r.random(n) < 0.10
    signup_str = np.where(alt_fmt, signup.dt.strftime("%Y-%m-%d %H:%M:%S"),
                          signup.dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    device = df["device"].astype(object).copy()
    device[r.random(n) < C.P_NULL_DEVICE] = None
    messy = r.random(n) < C.P_MESSY_DEVICE_CASE
    device[messy & device.notna()] = device[messy & device.notna()].map(MESSY_DEVICE)
    return pd.DataFrame({
        "user_id": df["user_id"].astype("int64"),
        "username": df["username"],
        "signup_ts": signup_str,
        "country": df["country"],
        "device": device,
        "acquisition_channel": df["acquisition_channel"],
        "age_bucket": df["age_bucket"],
        "received_at": _iso_z(df["received_at_s"]),
    })


def write_parquet_atomic(table: pa.Table, path: Path, **kwargs) -> None:
    """Write to a hidden temp file, then rename over the target. A killed run
    never leaves a truncated part.parquet (readers skip dot-files)."""
    tmp = path.with_name(f".{path.name}.tmp")
    pq.write_table(table, tmp, **kwargs)
    os.replace(tmp, path)


def write_day(day_idx: int, raw_dir: Path) -> dict:
    d = C.SIM_START + timedelta(days=day_idx)
    stats = {"date": d.isoformat()}
    for name, frame, schema in (("events", raw_events_partition(day_idx), EVENT_SCHEMA),
                                ("users", raw_users_partition(day_idx), USER_SCHEMA)):
        out = raw_dir / name / f"date={d.isoformat()}"
        out.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)
        write_parquet_atomic(table, out / "part.parquet", compression="snappy")
        stats[name] = table.num_rows
    return stats
