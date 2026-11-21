"""Capture README screenshots (dashboard, dbt lineage, Airflow) with Playwright + local Edge/Chrome.

    pip install playwright && python scripts/screenshots.py dashboard dbt airflow
"""
import os
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

AIRFLOW = os.getenv("AIRFLOW_URL", "http://localhost:8080")
OUT = Path(__file__).resolve().parents[1] / "docs" / "images"


def dashboard(page):
    page.goto("http://localhost:8501", wait_until="networkidle")
    page.set_viewport_size({"width": 1400, "height": 2350})
    page.goto("http://localhost:8501", wait_until="networkidle")
    page.wait_for_selector("text=Insight", timeout=60_000)
    page.wait_for_timeout(5000)
    page.screenshot(path=OUT / "dashboard.png")
    page.set_viewport_size({"width": 1400, "height": 1000})


def dbt(page):
    page.goto("http://localhost:8081/#!/overview?g_v=1",
              wait_until="networkidle")
    page.wait_for_timeout(6000)
    page.screenshot(path=OUT / "dbt_lineage.png")


def airflow(page):
    page.goto(AIRFLOW + "/login/")
    page.fill("#username", "admin")
    page.fill("#password", "admin")
    page.click("input[type=submit]")
    page.set_viewport_size({"width": 2400, "height": 1000})  # room for 90+ run columns
    page.goto(AIRFLOW + "/dags/pulse_daily_pipeline/grid?num_runs=100", wait_until="networkidle")
    page.wait_for_timeout(5000)
    page.screenshot(path=OUT / "airflow_dag.png")


if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge")
        page = browser.new_page(viewport={"width": 1400, "height": 1000}, color_scheme="light")
        for name in sys.argv[1:] or ["dashboard"]:
            globals()[name](page)
            print("saved", name)
        browser.close()
