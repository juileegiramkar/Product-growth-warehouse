"""Pulse growth dashboard (one page).

Reads the Parquet extracts written by the Airflow task refresh_dashboard_extract
(never the live DuckDB file, which dbt holds a write lock on).

    streamlit run dashboard/app.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from scipy import stats

EXTRACTS = Path(os.getenv("PULSE_EXTRACTS_DIR",
                          Path(__file__).resolve().parents[1] / "data" / "extracts"))

# reference palette (validated categorical order + sequential blue ramp)
BLUE, ORANGE, GRAY = "#2a78d6", "#eb6834", "#a3a29c"
SEQ_BLUE = ["#f4f8fd", "#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

st.set_page_config(page_title="Pulse growth dashboard", page_icon="📈", layout="wide")


@st.cache_data(ttl=300)
def load(name: str) -> pd.DataFrame:
    return pd.read_parquet(EXTRACTS / name / "data.parquet")


def base_layout(fig: go.Figure, height: int = 320, **kw) -> go.Figure:
    opts = dict(height=height, margin=dict(l=8, r=8, t=40, b=8),
                hovermode="x unified", legend=dict(orientation="h", y=-0.12, x=0, yanchor="top"),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    opts.update(kw)
    fig.update_layout(**opts)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.18)", zeroline=False)
    return fig


def two_prop_test(r_yes, n_yes, r_no, n_no):
    """Chi-square test + 95% CI for the risk ratio (log method)."""
    table = np.array([[r_yes, n_yes - r_yes], [r_no, n_no - r_no]])
    chi2, p, _, _ = stats.chi2_contingency(table, correction=False)
    p1, p0 = r_yes / n_yes, r_no / n_no
    rr = p1 / p0
    se = np.sqrt((1 - p1) / r_yes + (1 - p0) / r_no)
    lo, hi = np.exp(np.log(rr) - 1.96 * se), np.exp(np.log(rr) + 1.96 * se)
    return dict(p1=p1, p0=p0, rr=rr, lo=lo, hi=hi, chi2=chi2, p=p,
                diff_pp=(p1 - p0) * 100)


if not (EXTRACTS / "mtr_daily_active_users" / "data.parquet").exists():
    st.title("Pulse growth dashboard")
    st.info("No extracts yet. The Airflow DAG writes them after its first successful run "
            "(task `refresh_dashboard_extract`), or run `scripts/bootstrap.sh`.")
    st.stop()

dau = load("mtr_daily_active_users").sort_values("activity_date")
heat = load("mtr_retention_weekly")
cohorts = load("mtr_cohort_retention").sort_values("cohort_week")
funnel = load("mtr_onboarding_funnel").sort_values("step_order")
onb = load("mtr_onboarding_retention")
manifest = json.loads((EXTRACTS / "_manifest.json").read_text())

# ---------------------------------------------------------------- header
st.title("Pulse · product growth")
st.caption(f"Warehouse extract refreshed {manifest['refreshed_at']} · "
           f"{manifest['rows']['dim_users']:,} users · "
           f"{manifest['rows']['fct_user_activity']:,} user-days · "
           f"{manifest['rows']['fct_engagement']:,} engagement events")

last = dau.iloc[-1]
prev_week = dau.iloc[-8] if len(dau) >= 8 else dau.iloc[0]
mature = cohorts.dropna(subset=["d7_retention"])
d7_recent = (mature.tail(4)["d7_retention"] * mature.tail(4)["d7_eligible_users"]).sum() \
    / mature.tail(4)["d7_eligible_users"].sum()
k = st.columns(4)
k[0].metric("DAU", f"{int(last.dau):,}", f"{(last.dau / prev_week.dau - 1):+.1%} vs 7d ago")
k[1].metric("MAU (30-day)", f"{int(last.mau):,}", f"{(last.mau / prev_week.mau - 1):+.1%} vs 7d ago")
k[2].metric("DAU / MAU", f"{last.dau_mau_stickiness:.1%}",
            f"{(last.dau_mau_stickiness - prev_week.dau_mau_stickiness) * 100:+.1f} pp")
k[3].metric("Day-7 retention", f"{d7_recent:.1%}", "last 4 mature cohorts", delta_color="off")

# ---------------------------------------------------------------- insight card
f5 = onb[onb.step_name == "follow_5_accounts"].set_index("completed_in_24h")
t = two_prop_test(f5.loc[True, "d7_retained_users"], f5.loc[True, "users"],
                  f5.loc[False, "d7_retained_users"], f5.loc[False, "users"])
with st.container(border=True):
    st.markdown("#### 💡 Insight: following 5 accounts on day 1 is the strongest retention signal")
    st.markdown(
        f"Users who follow 5+ accounts in their first 24 hours retain at **{t['p1']:.1%}** on day 7 "
        f"vs **{t['p0']:.1%}** for those who don't: **{t['rr']:.1f}×** "
        f"(95% CI {t['lo']:.2f}–{t['hi']:.2f}, χ² = {t['chi2']:,.0f}, p < 0.001; "
        f"n = {int(f5['users'].sum()):,} users with a full 7 days). "
        "Other steps show much smaller lifts that mostly shrink once follows are controlled for "
        "(see `analysis/growth_analysis.ipynb`).\n\n"
        "**Recommendation:** move the *follow suggestions* step earlier in onboarding and make it "
        "harder to skip. **Caveat:** this is correlational. Engaged users both follow more and "
        "retain more. Confirm with an A/B test that prompts half of new users to follow accounts "
        "(`analysis/ab_test_onboarding.ipynb` sizes it).")

# ---------------------------------------------------------------- DAU / MAU
c1, c2 = st.columns([3, 2])
with c1:
    fig = go.Figure()
    fig.add_scatter(x=dau.activity_date, y=dau.mau, name="MAU (trailing 30d)",
                    line=dict(color=ORANGE, width=2))
    fig.add_scatter(x=dau.activity_date, y=dau.wau, name="WAU (trailing 7d)",
                    line=dict(color=GRAY, width=2, dash="dot"))
    fig.add_scatter(x=dau.activity_date, y=dau.dau, name="DAU",
                    line=dict(color=BLUE, width=2))
    base_layout(fig, title="Active users", yaxis_title="users")
    st.plotly_chart(fig, use_container_width=True)
with c2:
    full = dau[dau.is_full_mau_window]
    fig = go.Figure()
    fig.add_scatter(x=full.activity_date, y=full.dau_mau_stickiness, name="DAU/MAU",
                    line=dict(color=BLUE, width=2), hovertemplate="%{y:.1%}")
    base_layout(fig, title="Stickiness (DAU / MAU)", yaxis_tickformat=".0%", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Shown once a full 30-day MAU window exists. Spikes are weekends.")

# ---------------------------------------------------------------- retention heatmap
c1, c2 = st.columns([3, 2])
with c1:
    pv = heat.pivot(index="cohort_week", columns="weeks_since_signup", values="retention_rate")
    sizes = heat.groupby("cohort_week")["cohort_users"].first()
    pv = pv.sort_index()
    ylabels = [f"{pd.Timestamp(d):%b %d} (n={sizes[d]:,})" for d in pv.index]
    text = pv.map(lambda v: "" if pd.isna(v) else f"{v:.0%}")
    fig = go.Figure(go.Heatmap(
        z=pv.values, x=[f"W{c}" for c in pv.columns], y=ylabels, text=text.values,
        texttemplate="%{text}", colorscale=[[i / (len(SEQ_BLUE) - 1), c] for i, c in enumerate(SEQ_BLUE)],
        zmin=0, zmax=0.7, xgap=2, ygap=2, colorbar=dict(title="retained", tickformat=".0%"),
        hovertemplate="cohort %{y}<br>week %{x}: %{z:.1%}<extra></extra>"))
    base_layout(fig, height=420, title="Weekly retention by signup cohort",
                xaxis_title="weeks since signup", hovermode="closest")
    fig.update_yaxes(autorange="reversed", gridcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
with c2:
    fig = go.Figure()
    for col, name, color in (("d1_retention", "Day 1", GRAY), ("d7_retention", "Day 7", BLUE),
                             ("d30_retention", "Day 30", ORANGE)):
        fig.add_scatter(x=cohorts.cohort_week, y=cohorts[col], name=name, mode="lines+markers",
                        line=dict(color=color, width=2), marker=dict(size=8))
    base_layout(fig, height=420, title="Day-N retention by cohort", yaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------- onboarding
c1, c2 = st.columns(2)
with c1:
    fig = go.Figure(go.Bar(
        x=funnel.pct_of_signups, y=funnel.step_label, orientation="h", marker_color=BLUE,
        text=[f"{p:.0%}" + ("" if pd.isna(c) else f"  ({c:.0%} of prev)")
              for p, c in zip(funnel.pct_of_signups, funnel.step_conversion)],
        textposition="outside", customdata=funnel.users_reached,
        hovertemplate="%{y}: %{customdata:,} users (%{x:.1%})<extra></extra>"))
    base_layout(fig, height=340, title="Onboarding funnel (completed this and all earlier steps)",
                xaxis_tickformat=".0%", hovermode="closest", showlegend=False)
    fig.update_xaxes(range=[0, 1.25])
    fig.update_yaxes(autorange="reversed", gridcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
with c2:
    o = onb.sort_values("step_order")
    fig = go.Figure()
    for flag, name, color in ((True, "Completed in first 24h", BLUE), (False, "Did not", GRAY)):
        s = o[o.completed_in_24h == flag]
        fig.add_bar(x=s.step_label, y=s.d7_retention, name=name, marker_color=color,
                    text=[f"{v:.0%}" for v in s.d7_retention], textposition="outside",
                    customdata=s.users, hovertemplate="%{x}<br>" + name +
                    ": %{y:.1%} (n=%{customdata:,})<extra></extra>")
    base_layout(fig, height=340, title="Day-7 retention by onboarding step (first 24h)",
                yaxis_tickformat=".0%", barmode="group", hovermode="closest")
    fig.update_layout(bargap=0.3, bargroupgap=0.06)
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------- data quality
dq_path = EXTRACTS / "dq_runs.json"
if dq_path.exists():
    runs = json.loads(dq_path.read_text())
    if runs:
        with st.expander("Data quality: what the Spark job cleaned"):
            dq = pd.DataFrame(runs)
            cols = ["run", "raw_rows", "quarantined_bad_ts", "dup_event_id_removed",
                    "dup_natural_key_removed", "null_device_filled", "device_case_fixed",
                    "late_events_in_output", "rows_written"]
            st.dataframe(dq[[c for c in cols if c in dq]].sort_values("run", ascending=False),
                         hide_index=True, use_container_width=True)

with st.expander("Data tables"):
    st.dataframe(dau, hide_index=True, use_container_width=True)
    st.dataframe(cohorts, hide_index=True, use_container_width=True)
