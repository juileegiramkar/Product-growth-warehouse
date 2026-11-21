"""Vectorised, deterministic simulation of the Pulse social app.

Key idea: every random draw comes from a generator seeded by
(SEED, day_index, stream), so any single day can be regenerated on its own
(which is what the daily Airflow task does) and always yields the same data.

Layers:
  users_for_day(d)   -> users who signed up on day d, with latent traits,
                        onboarding outcomes and retention propensity
  core_events(d)     -> clean "truth" events that happened on day d
  raw partition(d)   -> what the ingestion layer actually receives on day d:
                        late arrivals, duplicates, mixed timestamp formats,
                        null/messy devices, broken clocks (see writer.py)
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import lru_cache

import numpy as np
import pandas as pd

from . import config as C

DAY_S = 86_400
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# RNG stream ids
S_SIGNUP_COUNT, S_USERS, S_ACTIVITY, S_SESSIONS, S_TARGETS, S_IDS, S_MESS = range(7)


def rng(day_idx: int, stream: int) -> np.random.Generator:
    return np.random.default_rng([C.SEED, day_idx + 10_000, stream])


def day_index(d: date) -> int:
    return (d - C.SIM_START).days


def day_start_ts(day_idx: int) -> float:
    d = C.SIM_START + timedelta(days=day_idx)
    return (datetime(d.year, d.month, d.day, tzinfo=timezone.utc) - EPOCH).total_seconds()


def is_weekend(day_idx: int) -> bool:
    return (C.SIM_START + timedelta(days=day_idx)).weekday() >= 5


def _sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def _hour_seconds(r: np.random.Generator, n: int) -> np.ndarray:
    """Seconds-into-day following the diurnal HOUR_WEIGHTS curve."""
    w = np.asarray(C.HOUR_WEIGHTS, dtype=float)
    hours = r.choice(24, size=n, p=w / w.sum())
    return hours * 3600 + r.uniform(0, 3600, size=n)


def group_cumsum(values: np.ndarray, sizes: np.ndarray) -> np.ndarray:
    """Cumulative sum restarting at each group (groups are contiguous)."""
    cs = np.cumsum(values)
    starts = np.repeat(np.cumsum(sizes) - sizes, sizes)
    return cs - np.concatenate([[0.0], cs])[starts]


# --------------------------------------------------------------------------
# users
# --------------------------------------------------------------------------
@lru_cache(maxsize=None)
def signup_count(day_idx: int) -> int:
    if day_idx < 0:
        return 0
    lam = C.BASE_SIGNUPS_PER_DAY * (1 + C.SIGNUP_GROWTH_PER_DAY * day_idx)
    if is_weekend(day_idx):
        lam *= C.WEEKEND_SIGNUP_LIFT
    return int(rng(day_idx, S_SIGNUP_COUNT).poisson(lam))


@lru_cache(maxsize=None)
def user_id_offset(day_idx: int) -> int:
    """Number of users who signed up before day_idx."""
    return sum(signup_count(i) for i in range(day_idx))


@lru_cache(maxsize=None)
def users_for_day(day_idx: int) -> pd.DataFrame:
    n = signup_count(day_idx)
    r = rng(day_idx, S_USERS)
    uid = C.FIRST_USER_ID + user_id_offset(day_idx) + np.arange(n)
    signup_ts = day_start_ts(day_idx) + _hour_seconds(r, n)
    df = pd.DataFrame({
        "user_id": uid.astype("int64"),
        "signup_day": day_idx,
        "signup_ts": signup_ts,
        "country": r.choice(C.COUNTRIES, size=n, p=C.COUNTRY_P),
        "device": r.choice(C.DEVICES, size=n, p=C.DEVICE_P),
        "acquisition_channel": r.choice(C.CHANNELS, size=n, p=C.CHANNEL_P),
        "age_bucket": r.choice(C.AGE_BUCKETS, size=n, p=C.AGE_P),
    })
    z = r.normal(size=n)  # latent engagement propensity (unobserved confounder)
    df["z"] = z

    # onboarding: each step independently, correlated through z
    q = np.full(n, C.BASE_D7_RETENTION)
    for k, step in enumerate(C.STEPS):
        p_ever = _sigmoid(C.STEP_INTERCEPT[k] + C.STEP_PROPENSITY_WEIGHT * z)
        ever = r.random(n) < p_ever
        within24 = ever & (r.random(n) < C.P_COMPLETE_WITHIN_24H)
        in_first_session = r.random(n) < 0.72
        # seconds after signup the step was completed
        quick = 40 + k * 150 + r.uniform(0, 120, n)                 # same session
        later_today = r.uniform(3_600, 23.5 * 3_600, n)              # hours later
        late = r.integers(1, 7, n) * DAY_S + r.uniform(0, DAY_S, n)  # day 1-6+
        delta = np.where(within24, np.where(in_first_session, quick, later_today), late)
        df[f"{step}_delta"] = np.where(ever, delta, np.nan)
        df[f"{step}_24h"] = within24
        q *= np.where(within24, C.STEP_RETENTION_MULT[step], 1.0)

    q *= df["acquisition_channel"].map(C.CHANNEL_RETENTION_MULT).to_numpy()
    q *= df["device"].map(C.DEVICE_RETENTION_MULT).to_numpy()
    q *= np.exp(C.PROPENSITY_RETENTION_WEIGHT * z)
    q = np.clip(q, 0.01, 0.92)
    df["q"] = q
    df["loyal"] = r.random(n) < np.minimum(0.95, C.LOYAL_SCALE * q)
    df["churn_day"] = r.exponential(C.LOYAL_LIFETIME_DAYS, n)  # loyal users stop after this
    return df


@lru_cache(maxsize=None)
def users_upto(day_idx: int) -> pd.DataFrame:
    """All users who signed up on or before day_idx."""
    frames = [users_for_day(i) for i in range(day_idx + 1)]
    frames = [f for f in frames if len(f)]
    return pd.concat(frames, ignore_index=True) if frames else users_for_day(0).iloc[:0]


# --------------------------------------------------------------------------
# events
# --------------------------------------------------------------------------
EVENT_COLS = ["event_type", "user_id", "ts", "step_name", "post_id",
              "content_type", "target_user_id", "client_session_id"]


def _frame(**cols) -> pd.DataFrame:
    n = len(next(iter(cols.values())))
    base = {c: [None] * n for c in EVENT_COLS}
    base.update(cols)
    return pd.DataFrame(base)[EVENT_COLS]


def _follow_targets(r, users_available: int, actors: np.ndarray) -> np.ndarray:
    """Popularity-skewed followees (early users are more popular), never self."""
    if users_available <= 1:
        return np.full(len(actors), C.FIRST_USER_ID, dtype="int64")
    t = C.FIRST_USER_ID + np.floor(users_available * r.random(len(actors)) ** 2.2).astype("int64")
    return np.where(t == actors, np.where(t > C.FIRST_USER_ID, t - 1, t + 1), t)


def _activity_prob(users: pd.DataFrame, day_idx: int) -> np.ndarray:
    t = day_idx - users["signup_day"].to_numpy()
    loyal = users["loyal"].to_numpy() & (t <= users["churn_day"].to_numpy())
    p_loyal = C.LOYAL_DAILY_P * (C.LOYAL_DECAY_FLOOR + (1 - C.LOYAL_DECAY_FLOOR)
                                 * np.exp(-t / C.LOYAL_DECAY_DAYS))
    p_casual = C.CASUAL_DAILY_P * np.exp(-t / C.CASUAL_DECAY_DAYS)
    p = np.where(loyal, p_loyal, p_casual)
    if is_weekend(day_idx):
        p = p * C.WEEKEND_ACTIVITY_LIFT
    p = np.minimum(p, 0.97)
    return np.where(t == 0, 1.0, p)


@lru_cache(maxsize=8)
def _core_without_targets(day_idx: int) -> pd.DataFrame:
    """Events that happened on day_idx (like/comment post targets not yet set)."""
    if day_idx < 0:
        return _frame(event_type=[])
    d0 = day_start_ts(day_idx)
    d1 = d0 + DAY_S
    parts: list[pd.DataFrame] = []
    r_t = rng(day_idx, S_TARGETS)

    # (a) signups --------------------------------------------------------
    new = users_for_day(day_idx)
    if len(new):
        uid = new["user_id"].to_numpy()
        ts = new["signup_ts"].to_numpy()
        parts.append(_frame(event_type=["app_open"] * len(new), user_id=uid, ts=ts - 2))
        parts.append(_frame(event_type=["signup"] * len(new), user_id=uid, ts=ts))

    # (b) onboarding steps for users who signed up in the last 8 days ----
    recent = pd.concat([users_for_day(i) for i in range(max(0, day_idx - 7), day_idx + 1)],
                       ignore_index=True)
    n_users_now = user_id_offset(day_idx) + signup_count(day_idx)
    for step in C.STEPS:
        ts_all = recent["signup_ts"].to_numpy() + recent[f"{step}_delta"].to_numpy()
        m = (ts_all >= d0) & (ts_all < d1)
        if not m.any():
            continue
        uid = recent["user_id"].to_numpy()[m]
        ts = ts_all[m]
        late = recent[f"{step}_delta"].to_numpy()[m] >= DAY_S
        if late.any():  # coming back later to finish onboarding opens the app
            parts.append(_frame(event_type=["app_open"] * int(late.sum()),
                                user_id=uid[late], ts=ts[late] - 30))
        if step == "follow_5_accounts":
            actors = np.repeat(uid, 5)
            fts = np.repeat(ts, 5) - np.tile(np.arange(5, 0, -1) * 6.0, len(uid))
            parts.append(_frame(event_type=["follow"] * len(actors), user_id=actors, ts=fts,
                                target_user_id=_follow_targets(r_t, n_users_now, actors)))
        if step == "first_post":
            parts.append(_frame(event_type=["post_created"] * len(uid), user_id=uid, ts=ts - 3,
                                content_type=r_t.choice(C.CONTENT_TYPES, len(uid), p=C.CONTENT_P)))
        parts.append(_frame(event_type=["onboarding_step"] * len(uid), user_id=uid, ts=ts,
                            step_name=[step] * len(uid)))

    # (c) regular sessions -------------------------------------------------
    users = users_upto(day_idx)
    r_a = rng(day_idx, S_ACTIVITY)
    active = r_a.random(len(users)) < _activity_prob(users, day_idx)
    au = users[active]
    if len(au):
        r_s = rng(day_idx, S_SESSIONS)
        loyal = (au["loyal"].to_numpy()
                 & (day_idx - au["signup_day"].to_numpy() <= au["churn_day"].to_numpy()))
        is_new = (au["signup_day"].to_numpy() == day_idx)
        wk = C.WEEKEND_ACTIVITY_LIFT if is_weekend(day_idx) else 1.0
        lam = np.where(loyal, C.EXTRA_SESSIONS_LOYAL, C.EXTRA_SESSIONS_CASUAL) * wk
        n_sess = np.where(is_new, r_s.poisson(0.7, len(au)), 1 + r_s.poisson(lam))
        s_uid = np.repeat(au["user_id"].to_numpy(), n_sess)
        s_loyal = np.repeat(loyal, n_sess)
        s_signup = np.repeat(np.where(is_new, au["signup_ts"].to_numpy(), -np.inf), n_sess)
        n_s = len(s_uid)

        # actions per session and their spacing
        mu = np.where(s_loyal, C.ACTIONS_PER_SESSION_LOYAL, C.ACTIONS_PER_SESSION_CASUAL)
        n_act = np.minimum(r_s.poisson(mu), 30)
        gaps = np.clip(r_s.exponential(30, n_act.sum()), 1, 120)
        offs = group_cumsum(gaps, n_act) + 2
        dur = np.zeros(n_s)
        last_idx = np.cumsum(n_act) - 1
        has = n_act > 0
        dur[has] = offs[last_idx[has]]

        # session starts: diurnal draws, sorted per user, then pushed apart so
        # consecutive sessions are separated by > 30 minutes of inactivity
        raw = np.sort((np.repeat(np.arange(len(au)), n_sess) * 1e6
                       + _hour_seconds(r_s, n_s)))  # sort within user
        start_in_day = raw - np.floor(raw / 1e6) * 1e6
        first = np.r_[True, s_uid[1:] != s_uid[:-1]]
        diffs = np.diff(start_in_day, prepend=0.0)
        prev_dur = np.r_[0.0, dur[:-1]]
        diffs = np.where(first, start_in_day, np.maximum(diffs, prev_dur + 35 * 60))
        sess_sizes = np.diff(np.r_[np.flatnonzero(first), n_s])
        starts = d0 + group_cumsum(diffs, sess_sizes)
        # on signup day, shift the user's sessions to start after the signup
        # session (shifting the whole group keeps the >30 min spacing)
        first_start = np.repeat(starts[first], sess_sizes)
        shift = np.where(np.isfinite(s_signup),
                         np.maximum(0.0, s_signup + 3_600 - first_start), 0.0)
        starts = starts + shift
        keep = starts + dur < d1
        sid = np.array([f"c{u}-{day_idx}-{i}" for u, i in
                        zip(s_uid, np.arange(n_s))], dtype=object)
        parts.append(_frame(event_type=["app_open"] * int(keep.sum()), user_id=s_uid[keep],
                            ts=starts[keep], client_session_id=sid[keep]))

        a_sess = np.repeat(np.arange(n_s), n_act)
        a_keep = keep[a_sess]
        a_uid = s_uid[a_sess][a_keep]
        a_ts = (starts[a_sess] + offs)[a_keep]
        a_type = r_s.choice(C.ACTION_TYPES, len(a_uid), p=C.ACTION_P)
        is_follow = a_type == "follow"
        targets = np.full(len(a_uid), None, dtype=object)
        targets[is_follow] = _follow_targets(r_t, n_users_now, a_uid[is_follow])
        content = np.full(len(a_uid), None, dtype=object)
        is_post = a_type == "post_created"
        content[is_post] = r_t.choice(C.CONTENT_TYPES, int(is_post.sum()), p=C.CONTENT_P)
        parts.append(_frame(event_type=a_type, user_id=a_uid, ts=a_ts,
                            target_user_id=targets, content_type=content))

    ev = pd.concat(parts, ignore_index=True).sort_values(["ts", "user_id"], kind="stable")
    ev = ev.reset_index(drop=True)
    is_post = ev["event_type"] == "post_created"
    ev.loc[is_post, "post_id"] = [f"p{day_idx:04d}{i:06d}" for i in range(int(is_post.sum()))]
    return ev


@lru_cache(maxsize=8)
def core_events(day_idx: int) -> pd.DataFrame:
    """Truth events for the day with post targets, ids and delivery metadata."""
    ev = _core_without_targets(day_idx).copy()
    r = rng(day_idx, S_TARGETS + 100)
    # likes/comments target posts created the previous day (author = target)
    pool = _core_without_targets(day_idx - 1)
    pool = pool[pool["event_type"] == "post_created"]
    eng = ev["event_type"].isin(["like", "comment"]).to_numpy()
    if len(pool) == 0:
        ev = ev[~eng].reset_index(drop=True)
    else:
        pick = np.floor(len(pool) * r.random(int(eng.sum())) ** 1.5).astype(int)
        ev.loc[eng, "post_id"] = pool["post_id"].to_numpy()[pick]
        ev.loc[eng, "target_user_id"] = pool["user_id"].to_numpy()[pick]
        # don't like your own post
        own = eng.copy()
        own[eng] = ev.loc[eng, "target_user_id"].to_numpy() == ev.loc[eng, "user_id"].to_numpy()
        ev = ev[~own].reset_index(drop=True)

    n = len(ev)
    r_id = rng(day_idx, S_IDS)
    hi, lo = r_id.integers(0, 2**63, n), r_id.integers(0, 2**63, n)
    ev.insert(0, "event_id", [f"{a:016x}{b:016x}" for a, b in zip(hi, lo)])

    r_m = rng(day_idx, S_MESS)
    late = r_m.random(n) < C.P_LATE
    ev["delay_days"] = np.where(late, np.where(r_m.random(n) < 0.7, 1, 2), 0)
    ev["dup_same_id"] = r_m.random(n) < C.P_DUP_SAME_ID
    ev["dup_next_day"] = ev["dup_same_id"] & (r_m.random(n) < 0.3)
    ev["dup_retry"] = r_m.random(n) < C.P_DUP_RETRY
    ev["bad_clock"] = r_m.random(n) < C.P_BAD_CLOCK
    ev["null_device"] = r_m.random(n) < C.P_NULL_DEVICE
    ev["messy_device"] = r_m.random(n) < C.P_MESSY_DEVICE_CASE
    ev["us_format"] = r_m.random(n) < C.P_US_TS_FORMAT
    ev["android_epoch"] = r_m.random(n) < 0.6
    ev["receive_lag_s"] = r_m.uniform(0.2, 30, n)
    ev["late_receive_s"] = r_m.uniform(0, DAY_S, n)
    ev["retry_id"] = [f"{a:016x}{b:016x}" for a, b in
                      zip(r_m.integers(0, 2**63, n), r_m.integers(0, 2**63, n))]
    ev["day_idx"] = day_idx
    return ev
