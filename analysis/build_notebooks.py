"""Builds (and with --execute, runs) the analysis notebooks from source cells.

    python analysis/build_notebooks.py --execute

Keeping notebooks generated from this file makes them diff-able in git.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import nbformat as nbf

HERE = Path(__file__).resolve().parent

SETUP = r'''
import json, warnings
from pathlib import Path
import duckdb, numpy as np, pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
warnings.filterwarnings("ignore")

ROOT = Path.cwd().parent if Path.cwd().name == "analysis" else Path.cwd()
EXTRACTS, IMAGES = ROOT / "data" / "extracts", ROOT / "docs" / "images"
IMAGES.mkdir(parents=True, exist_ok=True)

# Query the warehouse marts through their Parquet extracts (lock-free while dbt runs)
con = duckdb.connect()
for p in EXTRACTS.glob("*/data.parquet"):
    con.execute(f"create or replace view {p.parent.name} as select * from read_parquet('{p.as_posix()}')")
q = lambda sql: con.execute(sql).df()

BLUE, ORANGE, AQUA, GRAY = "#2a78d6", "#eb6834", "#1baf7a", "#a3a29c"
plt.rcParams.update({"figure.dpi": 110, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": .25, "axes.titleweight": "bold",
                     "axes.titlesize": 12, "font.size": 10})
'''

# --------------------------------------------------------------------------
GROWTH = [
    ("md", "# Pulse growth analysis\n\nDAU/MAU, 7-day retention cohorts, the onboarding funnel, and "
           "**which onboarding steps predict retention**. All numbers come from the dbt marts "
           "(`core.*`, `metrics.*`) built by the Airflow pipeline."),
    ("code", SETUP),
    ("md", "## 0. Data volume"),
    ("code", r'''
dq = json.loads((EXTRACTS / "dq_runs.json").read_text())
raw_rows = max(r["raw_rows"] for r in dq if r["run"] == "full") if any(r["run"] == "full" for r in dq) \
    else sum(r["raw_rows"] for r in dq)
vol = q("""
select (select count(*) from dim_users)          as users,
       (select count(*) from fct_user_activity)  as user_days,
       (select count(*) from fct_engagement)     as engagement_events,
       (select count(*) from dim_posts)          as posts,
       (select min(activity_date) from fct_user_activity) as first_day,
       (select max(activity_date) from fct_user_activity) as last_day
""")
vol.insert(0, "raw_events", raw_rows)
vol.T.rename(columns={0: "value"})
'''),
    ("md", "## 1. DAU, MAU and stickiness\n\nMAU = distinct users active in the trailing 30 days. "
           "The plan's correlated-subquery version is in `dbt/analyses/plan_queries.sql`; production "
           "uses the `mtr_daily_active_users` model:"),
    ("code", r'''
dau = q("""
select activity_date, dau, wau, mau, dau_mau_stickiness, is_full_mau_window, is_weekend
from mtr_daily_active_users order by activity_date
""")
fig, ax = plt.subplots(1, 2, figsize=(12, 3.8), gridspec_kw={"width_ratios": [3, 2]})
ax[0].plot(dau.activity_date, dau.mau, color=ORANGE, lw=2, label="MAU (30d)")
ax[0].plot(dau.activity_date, dau.wau, color=GRAY, lw=2, ls=":", label="WAU (7d)")
ax[0].plot(dau.activity_date, dau.dau, color=BLUE, lw=2, label="DAU")
ax[0].set_title("Active users"); ax[0].legend(frameon=False, loc="upper left")
full = dau[dau.is_full_mau_window]
ax[1].plot(full.activity_date, full.dau_mau_stickiness, color=BLUE, lw=2)
ax[1].set_title("Stickiness (DAU/MAU)"); ax[1].yaxis.set_major_formatter(plt.matplotlib.ticker.PercentFormatter(1))
for a in ax: a.tick_params(axis="x", rotation=30)
plt.tight_layout(); plt.savefig(IMAGES / "dau_mau.png", bbox_inches="tight"); plt.show()
wk = dau[dau.is_full_mau_window].groupby("is_weekend").dau_mau_stickiness.mean()
print(f"Latest: DAU {dau.dau.iloc[-1]:,}  MAU {dau.mau.iloc[-1]:,}  stickiness {dau.dau_mau_stickiness.iloc[-1]:.1%}")
print(f"Weekly seasonality: stickiness {wk[True]:.1%} on weekends vs {wk[False]:.1%} on weekdays")
'''),
    ("md", "## 2. Retention cohorts\n\nDay-7 retention = active on exactly `signup_date + 7` "
           "(only cohorts whose day 7 has happened)."),
    ("code", r'''
d7 = q("""
select u.signup_week as cohort_week, count(*) as users,
       avg(case when r.d7_retained then 1.0 else 0 end) as d7_retention
from dim_users u join fct_user_retention r using (user_id)
where r.d7_eligible group by 1 order by 1
""")
heat = q("select * from mtr_retention_weekly")
pv = heat.pivot(index="cohort_week", columns="weeks_since_signup", values="retention_rate").sort_index()
fig, ax = plt.subplots(1, 2, figsize=(13, 4.6), gridspec_kw={"width_ratios": [3, 2]})
im = ax[0].imshow(pv.values, cmap="Blues", vmin=0, vmax=.7, aspect="auto")
ax[0].set_xticks(range(pv.shape[1]), [f"W{c}" for c in pv.columns])
ax[0].set_yticks(range(pv.shape[0]), [pd.Timestamp(d).strftime("%b %d") for d in pv.index])
for i in range(pv.shape[0]):
    for j in range(pv.shape[1]):
        v = pv.values[i, j]
        if not np.isnan(v):
            ax[0].text(j, i, f"{v:.0%}", ha="center", va="center", fontsize=7,
                       color="white" if v > .45 else "#222")
ax[0].set_title("Weekly retention by signup cohort"); ax[0].grid(False)
ax[1].bar(range(len(d7)), d7.d7_retention, color=BLUE, width=.7)
ax[1].set_xticks(range(len(d7)), [pd.Timestamp(d).strftime("%b %d") for d in d7.cohort_week], rotation=45)
ax[1].yaxis.set_major_formatter(plt.matplotlib.ticker.PercentFormatter(1))
ax[1].set_title("Day-7 retention by weekly cohort")
plt.tight_layout(); plt.savefig(IMAGES / "retention.png", bbox_inches="tight"); plt.show()
d7.style.format({"d7_retention": "{:.1%}", "users": "{:,}"})
'''),
    ("md", "## 3. Onboarding funnel\n\nsignup → profile photo → follow 5 accounts → first post → "
           "notifications. *Reached* = completed this step **and all earlier ones**."),
    ("code", r'''
fun = q("select * from mtr_onboarding_funnel order by step_order")
fig, ax = plt.subplots(figsize=(8, 3.4))
ax.barh(fun.step_label, fun.pct_of_signups, color=BLUE, height=.6)
for i, (p, c) in enumerate(zip(fun.pct_of_signups, fun.step_conversion)):
    ax.text(p + .01, i, f"{p:.0%}" + ("" if pd.isna(c) else f"  ({c:.0%} step conv.)"), va="center", fontsize=9)
ax.invert_yaxis(); ax.set_xlim(0, 1.25); ax.xaxis.set_major_formatter(plt.matplotlib.ticker.PercentFormatter(1))
ax.set_title("Onboarding funnel"); ax.grid(axis="y", visible=False)
plt.tight_layout(); plt.savefig(IMAGES / "funnel.png", bbox_inches="tight"); plt.show()
fun[["step_label", "users_reached", "pct_of_signups", "step_conversion", "pct_completed_any_order", "pct_reached_24h"]]
'''),
    ("md", "## 4. Which onboarding steps predict retention?\n\nFor each step: day-7 retention of users "
           "who did vs didn't complete it in their **first 24 hours**, with a chi-square test and a "
           "95% CI on the risk ratio."),
    ("code", r'''
users = q("""
select u.*, r.d7_retained, r.d7_eligible
from dim_users u join fct_user_retention r using (user_id)
where r.d7_eligible
""")
steps = ["profile_photo", "follow_5_accounts", "first_post", "enable_notifications"]

def compare(df, flag):
    a = df[df[flag]]; b = df[~df[flag]]
    r1, n1, r0, n0 = a.d7_retained.sum(), len(a), b.d7_retained.sum(), len(b)
    chi2, p, *_ = stats.chi2_contingency([[r1, n1 - r1], [r0, n0 - r0]], correction=False)
    p1, p0 = r1 / n1, r0 / n0
    se = np.sqrt((1 - p1) / r1 + (1 - p0) / r0)
    rr = p1 / p0
    return dict(did_users=n1, did_d7=p1, didnt_users=n0, didnt_d7=p0, lift_pp=(p1 - p0) * 100,
                risk_ratio=rr, rr_ci_low=rr * np.exp(-1.96 * se), rr_ci_high=rr * np.exp(1.96 * se),
                chi2=chi2, p_value=p)

res = pd.DataFrame({s: compare(users, f"did_{s}_24h") for s in steps}).T
res.style.format({"did_users": "{:,.0f}", "didnt_users": "{:,.0f}", "did_d7": "{:.1%}", "didnt_d7": "{:.1%}",
                  "lift_pp": "{:+.1f}", "risk_ratio": "{:.2f}×", "rr_ci_low": "{:.2f}", "rr_ci_high": "{:.2f}",
                  "chi2": "{:,.0f}", "p_value": "{:.1e}"})
'''),
    ("code", r'''
fig, ax = plt.subplots(figsize=(8, 3.6))
x = np.arange(len(steps)); w = .38
ax.bar(x - w/2, res.did_d7, w, color=BLUE, label="completed in first 24h")
ax.bar(x + w/2, res.didnt_d7, w, color=GRAY, label="did not")
for i in range(len(steps)):
    ax.text(i - w/2, res.did_d7.iloc[i] + .008, f"{res.did_d7.iloc[i]:.0%}", ha="center", fontsize=9)
    ax.text(i + w/2, res.didnt_d7.iloc[i] + .008, f"{res.didnt_d7.iloc[i]:.0%}", ha="center", fontsize=9)
ax.set_xticks(x, ["Profile photo", "Follow 5 accounts", "First post", "Notifications"])
ax.yaxis.set_major_formatter(plt.matplotlib.ticker.PercentFormatter(1))
ax.set_title("Day-7 retention by first-24h onboarding step"); ax.legend(frameon=False)
plt.tight_layout(); plt.savefig(IMAGES / "onboarding_retention.png", bbox_inches="tight"); plt.show()
'''),
    ("md", "### 4b. Controlling for the other steps\n\nSteps are correlated (engaged users do more of "
           "everything), so a raw comparison overstates each one. A logistic regression with all four "
           "steps plus acquisition channel, device and country isolates each step's association."),
    ("code", r'''
import statsmodels.formula.api as smf
m = users.assign(**{f"{s}": users[f"did_{s}_24h"].astype(int) for s in steps},
                 d7=users.d7_retained.astype(int))
fit = smf.logit("d7 ~ profile_photo + follow_5_accounts + first_post + enable_notifications"
                " + C(acquisition_channel) + C(device) + C(country)", data=m).fit(disp=False)
odds = pd.DataFrame({"odds_ratio": np.exp(fit.params), "ci_low": np.exp(fit.conf_int()[0]),
                     "ci_high": np.exp(fit.conf_int()[1]), "p_value": fit.pvalues}).loc[steps]
print(f"n = {int(fit.nobs):,}, pseudo R² = {fit.prsquared:.3f}")
odds.style.format({"odds_ratio": "{:.2f}", "ci_low": "{:.2f}", "ci_high": "{:.2f}", "p_value": "{:.1e}"})
'''),
    ("md", "### 4c. Dose-response: the earlier the follows, the better retention"),
    ("code", r'''
onb = q("select user_id, hours_to_follow_5_accounts from fct_onboarding")
dd = users.merge(onb, on="user_id")
bins = [-1, 1, 6, 24, 72, 24 * 8, np.inf]
labels = ["<1h", "1-6h", "6-24h", "1-3d", "3-8d", "never"]
dd["follow_timing"] = pd.cut(dd.hours_to_follow_5_accounts.fillna(1e9), bins, labels=labels)
dose = dd.groupby("follow_timing", observed=True).agg(users=("user_id", "size"), d7_retention=("d7_retained", "mean"))
dose.style.format({"users": "{:,}", "d7_retention": "{:.1%}"})
'''),
    ("md", "## 5. Write-up"),
    ("code", r'''
f = res.loc["follow_5_accounts"]
others = res.drop("follow_5_accounts")
o = odds.loc["follow_5_accounts"]
text = f"""
**Insight.** Users who follow 5+ accounts in their first 24 hours retain at **{f.did_d7:.1%}** on day 7
vs **{f.didnt_d7:.1%}** for those who don't: **{f.risk_ratio:.1f}×** (95% CI {f.rr_ci_low:.2f}–{f.rr_ci_high:.2f},
χ² = {f.chi2:,.0f}, p < 0.001, n = {int(f.did_users + f.didnt_users):,}). It is the only step whose association
survives controlling for the other steps, channel, device and country (adjusted odds ratio {o.odds_ratio:.2f},
95% CI {o.ci_low:.2f}–{o.ci_high:.2f}); the other steps' raw lifts ({others.risk_ratio.min():.2f}–{others.risk_ratio.max():.2f}×)
shrink to adjusted odds ratios of {odds.drop('follow_5_accounts').odds_ratio.min():.2f}–{odds.drop('follow_5_accounts').odds_ratio.max():.2f}.
Retention also falls steadily the later the follows happen ({dose.d7_retention.iloc[0]:.0%} within the first hour vs
{dose.d7_retention.iloc[-1]:.0%} for users who never do it).

**Recommendation.** Onboarding should push follows earlier: put suggested accounts right after signup
(before the profile photo), pre-select popular and friends-of-friends accounts, and keep nudging users
who skip it in their first session.

**Limitations.** This is correlation, not causation. Users who are already more engaged both follow more
and retain more, and the regression only controls for what we observe. Day-7 activity is a single-day
metric, and the data is synthetic. **To confirm**, run an A/B test that prompts a random half of new users
to follow 5 accounts during onboarding and compare day-7 retention (sized in `ab_test_onboarding.ipynb`).
"""
(ROOT / "analysis" / "INSIGHT.md").write_text("# Onboarding → retention insight\n" + text, encoding="utf-8")
from IPython.display import Markdown; Markdown(text)
'''),
]

# --------------------------------------------------------------------------
AB = [
    ("md", "# A/B test: prompting new users to follow 5 accounts\n\nThe observational analysis says "
           "early follows predict retention. This notebook (1) sizes an experiment to test it causally, "
           "(2) simulates running it with the same user model that generated the warehouse, and "
           "(3) analyses it: SRM check, two-proportion z-test, confidence intervals, bootstrap and segments."),
    ("code", SETUP + r'''
import sys; sys.path.insert(0, str(ROOT))
from generator import config as C
from statsmodels.stats.power import NormalIndPower
from statsmodels.stats.proportion import proportion_effectsize, proportions_ztest, confint_proportions_2indep
rng = np.random.default_rng(2026)
'''),
    ("md", "## 1. Power analysis\n\nBaseline = current day-7 retention from the warehouse. "
           "Minimum detectable effect: +2 percentage points."),
    ("code", r'''
base = q("select avg(case when d7_retained then 1.0 else 0 end) as p from fct_user_retention where d7_eligible").p[0]
signups_per_day = q("select count(*) / count(distinct signup_date) as n from dim_users").n[0]
rows = []
for mde in [0.01, 0.015, 0.02, 0.03]:
    es = proportion_effectsize(base + mde, base)
    n = NormalIndPower().solve_power(effect_size=es, alpha=0.05, power=0.8, ratio=1)
    rows.append(dict(mde_pp=mde * 100, n_per_arm=int(np.ceil(n)), days_of_signups=np.ceil(2 * n / signups_per_day)))
power = pd.DataFrame(rows)
print(f"baseline d7 = {base:.1%}, ~{signups_per_day:,.0f} signups/day")
power
'''),
    ("md", "## 2. Simulate the experiment\n\nUsers are randomised 50/50. Treatment shows a "
           "\"follow 5 accounts\" prompt, raising first-24h completion from the observed rate to ~60%. "
           "Retention then follows the generator's causal model (follows ×2.5 on retention propensity, "
           "plus the latent engagement confounder). Some users skip the prompt, so we measure the "
           "**intent-to-treat** effect."),
    ("code", r'''
N = int(power.loc[power.mde_pp == 2, "n_per_arm"].iloc[0]) * 2
z = rng.normal(size=N)
arm = np.where(rng.random(N) < 0.5, "treatment", "control")
device = rng.choice(C.DEVICES, N, p=C.DEVICE_P)
channel = rng.choice(C.CHANNELS, N, p=C.CHANNEL_P)
sig = lambda x: 1 / (1 + np.exp(-x))
p_follow_ctrl = sig(C.STEP_INTERCEPT[1] + C.STEP_PROPENSITY_WEIGHT * z) * C.P_COMPLETE_WITHIN_24H
p_follow = np.where(arm == "treatment", np.maximum(p_follow_ctrl, 0.60), p_follow_ctrl)
followed = rng.random(N) < p_follow
qprop = C.BASE_D7_RETENTION * np.where(followed, C.STEP_RETENTION_MULT["follow_5_accounts"], 1)
qprop *= pd.Series(channel).map(C.CHANNEL_RETENTION_MULT).values * pd.Series(device).map(C.DEVICE_RETENTION_MULT).values
qprop *= np.exp(C.PROPENSITY_RETENTION_WEIGHT * z)
retained = rng.random(N) < np.clip(qprop, 0, .92)
exp = pd.DataFrame(dict(arm=arm, device=device, channel=channel, followed=followed, d7=retained))
exp.groupby("arm").agg(users=("d7", "size"), follow_24h=("followed", "mean"), d7_retention=("d7", "mean"))
'''),
    ("md", "## 3. Analyse\n\n**Sample-ratio mismatch** first: if assignment isn't ~50/50 the "
           "randomisation is broken and nothing else can be trusted."),
    ("code", r'''
counts = exp.arm.value_counts()
srm_p = stats.chisquare(counts.values).pvalue
print(f"SRM check: {dict(counts)}  p = {srm_p:.3f}  ->", "OK" if srm_p > 0.001 else "SAMPLE RATIO MISMATCH")

t, c = exp[exp.arm == "treatment"].d7, exp[exp.arm == "control"].d7
zstat, p = proportions_ztest([t.sum(), c.sum()], [len(t), len(c)])
lo, hi = confint_proportions_2indep(t.sum(), len(t), c.sum(), len(c), method="wald")
boot = [rng.choice(t.values, len(t)).mean() - rng.choice(c.values, len(c)).mean() for _ in range(2000)]
summary = pd.Series({
    "control d7": f"{c.mean():.2%}", "treatment d7": f"{t.mean():.2%}",
    "absolute lift": f"{(t.mean() - c.mean()) * 100:+.2f} pp",
    "relative lift": f"{t.mean() / c.mean() - 1:+.1%}",
    "95% CI (Wald)": f"[{lo * 100:+.2f}, {hi * 100:+.2f}] pp",
    "95% CI (bootstrap)": f"[{np.percentile(boot, 2.5) * 100:+.2f}, {np.percentile(boot, 97.5) * 100:+.2f}] pp",
    "z": f"{zstat:.2f}", "p-value": f"{p:.2e}",
})
summary.to_frame("result")
'''),
    ("code", r'''
seg = (exp.groupby(["device", "arm"]).d7.agg(["mean", "size"]).unstack("arm"))
seg["lift_pp"] = (seg[("mean", "treatment")] - seg[("mean", "control")]) * 100
fig, ax = plt.subplots(1, 2, figsize=(11, 3.4))
ax[0].hist(np.array(boot) * 100, bins=40, color=BLUE, alpha=.85)
ax[0].axvline(0, color="#555", lw=1); ax[0].set_title("Bootstrap distribution of lift (pp)")
ax[1].bar(seg.index, seg.lift_pp, color=BLUE, width=.55); ax[1].set_title("Lift by device (pp)")
plt.tight_layout(); plt.savefig(IMAGES / "ab_test.png", bbox_inches="tight"); plt.show()
seg.round(4)
'''),
    ("md", "## 4. Decision\n\nThe prompt raises 24h follow completion, and day-7 retention rises with it. "
           "The ITT lift is much smaller than the 2.1× observational gap because (a) only the users the prompt "
           "*moves* can benefit, and (b) part of the observational gap was confounding from engagement. "
           "**Ship if** the CI excludes zero and guardrails (day-1 sessions, unfollow rate, notification "
           "opt-outs) don't regress. Note that on real traffic the effect size is unknown before the test; "
           "this simulation shows the analysis, not the answer."),
]

# --------------------------------------------------------------------------
FORECAST = [
    ("md", "# Weekly active users forecast\n\nHolt-Winters exponential smoothing (additive trend, "
           "weekly seasonality) on DAU and WAU, back-tested on the last 14 days, then a 28-day forecast "
           "with simulated prediction intervals."),
    ("code", SETUP + r'''
from statsmodels.tsa.holtwinters import ExponentialSmoothing
'''),
    ("code", r'''
s = q("select activity_date, dau, wau from mtr_daily_active_users order by 1").set_index("activity_date")
s.index = pd.DatetimeIndex(s.index, freq="D")
H = 14
results = {}
for col in ["dau", "wau"]:
    y = s[col].astype(float).iloc[7:]          # skip the launch week (WAU window not full)
    train, test = y.iloc[:-H], y.iloc[-H:]
    m = ExponentialSmoothing(train, trend="add", seasonal="add", seasonal_periods=7).fit()
    pred = m.forecast(H)
    mape = (np.abs(pred - test) / test).mean()
    naive = (np.abs(train.iloc[-7:].values.repeat(2)[:H] - test.values) / test.values).mean()
    results[col] = dict(mape=mape, seasonal_naive_mape=naive)
pd.DataFrame(results).T.style.format("{:.2%}")
'''),
    ("code", r'''
fig, axes = plt.subplots(1, 2, figsize=(13, 3.8))
for ax, col, color in zip(axes, ["dau", "wau"], [BLUE, ORANGE]):
    y = s[col].astype(float).iloc[7:]
    m = ExponentialSmoothing(y, trend="add", seasonal="add", seasonal_periods=7).fit()
    fc = m.forecast(28)
    sims = m.simulate(28, repetitions=500, error="add", anchor="end")
    lo, hi = np.percentile(sims, 5, axis=1), np.percentile(sims, 95, axis=1)
    ax.plot(y.index, y, color=color, lw=2, label="actual")
    ax.plot(fc.index, fc, color=color, lw=2, ls="--", label="forecast")
    ax.fill_between(fc.index, lo, hi, color=color, alpha=.15, label="90% interval")
    ax.set_title(f"{col.upper()} – 28-day forecast"); ax.legend(frameon=False, loc="upper left")
    ax.tick_params(axis="x", rotation=30)
    print(f"{col.upper()} in 28 days: {fc.iloc[-1]:,.0f}  (90% PI {lo[-1]:,.0f}–{hi[-1]:,.0f})")
plt.tight_layout(); plt.savefig(IMAGES / "forecast.png", bbox_inches="tight"); plt.show()
'''),
    ("md", "**Caveats.** 90 days is short; trend extrapolation assumes acquisition keeps growing linearly. "
           "In production this would be re-fit daily by an Airflow task and compared with a "
           "cohort-based model (signup forecast × retention curve)."),
]


def build(cells, path: Path):
    nb = nbf.v4.new_notebook()
    nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
    nb.cells = [nbf.v4.new_markdown_cell(src.strip()) if kind == "md"
                else nbf.v4.new_code_cell(src.strip()) for kind, src in cells]
    nbf.write(nb, path)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    a = ap.parse_args()
    paths = [build(GROWTH, HERE / "growth_analysis.ipynb"),
             build(AB, HERE / "ab_test_onboarding.ipynb"),
             build(FORECAST, HERE / "wau_forecast.ipynb")]
    if a.execute:
        for p in paths:
            subprocess.run([sys.executable, "-m", "jupyter", "nbconvert", "--to", "notebook",
                            "--execute", "--inplace", "--ExecutePreprocessor.timeout=600", str(p)],
                           check=True, cwd=HERE)
            print("executed", p.name)


if __name__ == "__main__":
    main()
