"""Simulation parameters for the Pulse synthetic social app.

Everything is deterministic given SEED: generating a given day twice produces
byte-identical events, so Airflow retries/backfills are idempotent.
"""
from __future__ import annotations

import os
from datetime import date

SEED = int(os.getenv("PULSE_SEED", "42"))
# First simulated day. Default = 90 days of history ending 2026-11-18.
SIM_START = date.fromisoformat(os.getenv("PULSE_SIM_START", "2026-08-21"))
SIM_DAYS = int(os.getenv("PULSE_SIM_DAYS", "90"))

FIRST_USER_ID = 100_000

# ---- signups -------------------------------------------------------------
BASE_SIGNUPS_PER_DAY = 560          # ~57k users over 90 days
SIGNUP_GROWTH_PER_DAY = 0.004       # gentle linear growth
WEEKEND_SIGNUP_LIFT = 1.15

COUNTRIES = ["US", "IN", "BR", "GB", "DE", "ID", "MX", "NG", "JP", "CA"]
COUNTRY_P = [0.30, 0.18, 0.10, 0.08, 0.07, 0.07, 0.06, 0.05, 0.05, 0.04]
# UTC offset in minutes (used by iOS clients that send local time + offset)
COUNTRY_OFFSET_MIN = {"US": -300, "IN": 330, "BR": -180, "GB": 0, "DE": 60,
                      "ID": 420, "MX": -360, "NG": 60, "JP": 540, "CA": -300}
DEVICES = ["ios", "android", "web"]
DEVICE_P = [0.42, 0.43, 0.15]
CHANNELS = ["organic", "ads", "referral"]
CHANNEL_P = [0.50, 0.32, 0.18]
AGE_BUCKETS = ["13-17", "18-24", "25-34", "35-44", "45+"]
AGE_P = [0.08, 0.34, 0.31, 0.16, 0.11]

# ---- onboarding ----------------------------------------------------------
STEPS = ["profile_photo", "follow_5_accounts", "first_post", "enable_notifications"]
# logit intercepts of eventually completing each step (~70% / 45% / 33% / 52%)
STEP_INTERCEPT = [1.05, -0.25, -0.85, 0.10]
STEP_PROPENSITY_WEIGHT = 0.9        # latent engagement z -> step completion
P_COMPLETE_WITHIN_24H = 0.80        # of those who ever complete a step

# ---- the planted pattern -------------------------------------------------
# Probability a user is active on day 7. follow_5_accounts in the first 24h
# is the real (causal, in the simulation) driver; other steps barely matter.
BASE_D7_RETENTION = 0.17
STEP_RETENTION_MULT = {
    "profile_photo": 1.10,
    "follow_5_accounts": 2.50,
    "first_post": 1.10,
    "enable_notifications": 1.15,
}
CHANNEL_RETENTION_MULT = {"organic": 1.0, "ads": 0.80, "referral": 1.25}
DEVICE_RETENTION_MULT = {"ios": 1.10, "android": 1.0, "web": 0.75}
PROPENSITY_RETENTION_WEIGHT = 0.25  # confounding: engaged users do more steps AND retain

# ---- activity decay ------------------------------------------------------
LOYAL_SCALE = 1.6                   # P(loyal) = min(0.95, LOYAL_SCALE * q)
LOYAL_DAILY_P = 0.52                # loyal core: high, slowly decaying activity
LOYAL_DECAY_FLOOR = 0.80
LOYAL_DECAY_DAYS = 15.0
LOYAL_LIFETIME_DAYS = 60.0          # even loyal users churn eventually (exponential lifetime)
CASUAL_DAILY_P = 0.55               # everyone else: fast exponential decay
CASUAL_DECAY_DAYS = 2.2
WEEKEND_ACTIVITY_LIFT = 1.25        # weekly seasonality

# ---- in-day behaviour ----------------------------------------------------
EXTRA_SESSIONS_LOYAL = 0.6          # sessions/day = 1 + Poisson(.)
EXTRA_SESSIONS_CASUAL = 0.3
ACTIONS_PER_SESSION_LOYAL = 2.6
ACTIONS_PER_SESSION_CASUAL = 1.6
ACTION_TYPES = ["like", "comment", "follow", "post_created"]
ACTION_P = [0.64, 0.13, 0.11, 0.12]
CONTENT_TYPES = ["text", "photo", "video"]
CONTENT_P = [0.50, 0.38, 0.12]
# relative weight of session starts by UTC hour (evening peak)
HOUR_WEIGHTS = [2, 1.5, 1, 1, 1, 1.2, 1.8, 2.5, 3, 3, 3, 3.2,
                3.5, 3.5, 3.4, 3.5, 3.8, 4.2, 4.8, 5.2, 5.4, 5, 4, 3]

# ---- messy data (on purpose) --------------------------------------------
P_LATE = 0.015            # arrives 1-2 days after it happened
P_DUP_SAME_ID = 0.008     # exact redelivery (same event_id)
P_DUP_RETRY = 0.003       # client retry: new event_id, same payload
P_NULL_DEVICE = 0.02
P_MESSY_DEVICE_CASE = 0.03   # "iOS", "Android ", ...
P_US_TS_FORMAT = 0.03        # "MM/dd/yyyy HH:mm:ss"
P_BAD_CLOCK = 0.0005         # device clock garbage (1970 / 2099)
P_USER_RESEND = 0.005        # user profile re-sent next day with changed device
