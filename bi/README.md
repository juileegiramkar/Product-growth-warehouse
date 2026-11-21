# Tableau / Power BI version of the dashboard

Every pipeline run (`refresh_dashboard_extract`) writes BI-ready extracts:

| File | Grain | Use for |
|---|---|---|
| `data/extracts/csv/mtr_daily_active_users.csv` | day | DAU / WAU / MAU / stickiness lines |
| `data/extracts/csv/mtr_retention_weekly.csv` | cohort × week | retention heatmap |
| `data/extracts/csv/mtr_cohort_retention.csv` | cohort | D1/D7/D14/D30 lines |
| `data/extracts/csv/mtr_onboarding_funnel.csv` | step | funnel bars |
| `data/extracts/csv/mtr_onboarding_retention.csv` | step × did/didn't | insight chart |
| `data/extracts/csv/fct_user_activity.csv`, `dim_users.csv` | user-day / user | ad-hoc slicing (join on `user_id`) |

Parquet copies of every mart sit next to them (`data/extracts/<table>/data.parquet`).
Power BI reads those directly, and so does Tableau through its Parquet connector
(2024.1 and later).

## Tableau Public (about half a day)
1. **Connect → Text file** → `mtr_daily_active_users.csv`. Add the other CSVs as separate data sources.
2. **Active users**: Columns `activity_date` (exact date, continuous), Rows `Measure Values` filtered to DAU / WAU / MAU.
3. **Stickiness**: `dau_mau_stickiness` as a line, filter `is_full_mau_window = True`, format as %.
4. **Retention heatmap** (`mtr_retention_weekly`): Columns `weeks_since_signup` (discrete), Rows `cohort_week`,
   Marks = Square, Color = `AVG(retention_rate)` with a sequential blue palette, Label = the same value.
5. **Funnel** (`mtr_onboarding_funnel`): Rows `step_label` sorted by `step_order`, Columns `pct_of_signups`, horizontal bars.
6. **Insight** (`mtr_onboarding_retention`): Columns `step_label`, Rows `d7_retention`, Color `completed_in_24h`, side-by-side bars.
   Add a text box with the finding from `analysis/INSIGHT.md`.
7. Put the views on a single dashboard (1400 × 1000) and **Save to Tableau Public**. Link it in the README.

## Power BI Desktop
1. **Get data → Parquet** (or Text/CSV) for each mart above.
2. Line chart: axis `activity_date`, values `dau`, `wau`, `mau`. Second line chart for `dau_mau_stickiness`.
3. Matrix visual for the heatmap: rows `cohort_week`, columns `weeks_since_signup`, values `retention_rate`,
   with conditional formatting → background color scale.
4. Clustered bar for the funnel and clustered column for the insight chart.
5. Measure for the headline: `Follow5 Lift = DIVIDE(CALCULATE(AVERAGE(mtr_onboarding_retention[d7_retention]), mtr_onboarding_retention[step_name]="follow_5_accounts", mtr_onboarding_retention[completed_in_24h]=TRUE()), CALCULATE(AVERAGE(mtr_onboarding_retention[d7_retention]), mtr_onboarding_retention[step_name]="follow_5_accounts", mtr_onboarding_retention[completed_in_24h]=FALSE()))`
